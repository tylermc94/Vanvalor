import discord
from discord.ext import commands
from discord import app_commands
import json
import os
import uuid
import asyncio
from datetime import datetime, timedelta
import dateparser
import pytz
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
import re

DATA_PATH = "data/polls.json"

NUMBER_EMOJIS = ["1⃣", "2⃣", "3⃣", "4⃣", "5⃣", "6⃣", "7⃣", "8⃣", "9⃣"]
# Regional-indicator letters extend voting past the 9 keycap-number emojis.
LETTER_EMOJIS = [chr(0x1F1E6 + i) for i in range(11)]  # 🇦-🇰
OPTION_EMOJIS = NUMBER_EMOJIS + LETTER_EMOJIS  # up to 20 options

# Maps day-of-week names to APScheduler cron values
DAY_MAP = {
    "monday": "mon", "tuesday": "tue", "wednesday": "wed",
    "thursday": "thu", "friday": "fri", "saturday": "sat", "sunday": "sun"
}

# dateparser doesn't recognize these nonstandard day abbreviations
DAY_ABBR_FIXES = {"thur": "thu", "thurs": "thu", "tues": "tue", "weds": "wed"}

TIEBREAKER_DURATION_MINUTES = 30


def normalize_shorthand_datetime(text):
    """Expand shorthand dateparser chokes on: bare am/pm ('7p' -> '7pm') and
    nonstandard day abbreviations ('Thur' -> 'Thu')."""
    result = re.sub(r"\b(\d{1,2})\s*([apAP])\b", r"\1\2m", text)
    result = re.sub(
        r"\b(thur|thurs|tues|weds)\b",
        lambda m: DAY_ABBR_FIXES[m.group(0).lower()],
        result,
        flags=re.IGNORECASE,
    )
    return result


def parse_timezone(text):
    """Extract timezone from text, defaulting to US/Eastern."""
    tz_aliases = {
        "est": "US/Eastern", "edt": "US/Eastern",
        "cst": "US/Central", "cdt": "US/Central",
        "mst": "US/Mountain", "mdt": "US/Mountain",
        "pst": "US/Pacific", "pdt": "US/Pacific",
        "utc": "UTC", "gmt": "UTC",
        "cet": "Europe/Stockholm", "cest": "Europe/Stockholm",
        "set": "Europe/Stockholm",
    }
    lower = text.lower()
    for abbr, tz in tz_aliases.items():
        if abbr in lower:
            return tz
    return "US/Eastern"


def parse_recurrence(text):
    """Parse a recurrence string like 'every Monday at 9am EST' into cron components.
    Returns dict with cron fields or None if not recurring."""
    lower = text.lower().strip()
    if lower == "none" or lower == "no":
        return None

    # Strip leading "every" if present
    cleaned = lower
    if cleaned.startswith("every "):
        cleaned = cleaned[6:]

    # Parse the time expression
    parsed = dateparser.parse(cleaned, settings={
        'PREFER_DATES_FROM': 'future',
        'RETURN_AS_TIMEZONE_AWARE': True,
    })
    if not parsed:
        return None

    # Find the day of week from the text
    day_of_week = None
    for day_name, cron_day in DAY_MAP.items():
        if day_name in lower:
            day_of_week = cron_day
            break

    tz = parse_timezone(text)

    return {
        "day_of_week": day_of_week,
        "hour": parsed.hour,
        "minute": parsed.minute,
        "timezone": tz,
    }


def to_discord_timestamp(dt, style="F"):
    """Convert a datetime to a Discord timestamp string that auto-converts to each user's timezone.
    Styles: F=full, f=short, t=time, T=long time, d=date, D=long date, R=relative"""
    unix = int(dt.timestamp())
    return f"<t:{unix}:{style}>"


class PollCoreModal(discord.ui.Modal):
    """Step 1: the 5 core poll fields. Discord modals cap out at 5 text inputs,
    so ping target / post channel / recurrence live on PollOptionsView after submit."""

    def __init__(self, cog, *, mode="create", modify_id=None, defaults=None):
        title = {"create": "Schedule a Poll", "modify": "Modify Poll", "clone": "Clone Poll"}[mode]
        super().__init__(title=title)
        self.cog = cog
        self.mode = mode
        self.modify_id = modify_id
        self.carry_over = defaults or {}

        self.question = discord.ui.TextInput(
            label="Poll question",
            default=self.carry_over.get("question", ""),
            max_length=200,
        )
        self.options = discord.ui.TextInput(
            label=f"Options, comma-separated (max {len(OPTION_EMOJIS)})",
            style=discord.TextStyle.paragraph,
            default=self.carry_over.get("options_raw", ""),
            placeholder="Mon 7pm, Tue 7pm, Sat 8am, Sun 7pm",
            max_length=1000,
        )
        self.send_time = discord.ui.TextInput(
            label="Send time",
            default=self.carry_over.get("send_time_raw", ""),
            placeholder='"Monday at 9am EST", "in 2 hours"',
        )
        self.duration = discord.ui.TextInput(
            label="Poll duration",
            default=self.carry_over.get("duration_raw", "24 hours"),
            placeholder='"24 hours", "2 days"',
        )
        self.vote_threshold = discord.ui.TextInput(
            label="Minimum votes to count (0 = no minimum)",
            default=str(self.carry_over.get("vote_threshold", 0)),
            max_length=3,
        )
        for item in (self.question, self.options, self.send_time, self.duration, self.vote_threshold):
            self.add_item(item)

    async def on_submit(self, interaction: discord.Interaction):
        errors = []

        options_list = [o.strip() for o in self.options.value.split(",") if o.strip()]
        if len(options_list) < 2:
            errors.append("Provide at least 2 options.")
        elif len(options_list) > len(OPTION_EMOJIS):
            errors.append(f"Maximum {len(OPTION_EMOJIS)} options allowed.")

        send_parsed = dateparser.parse(
            normalize_shorthand_datetime(self.send_time.value),
            settings={'PREFER_DATES_FROM': 'future', 'RETURN_AS_TIMEZONE_AWARE': True},
        )
        if not send_parsed:
            errors.append(f'Couldn\'t understand send time "{self.send_time.value}".')

        duration_hours = self.cog._parse_duration(self.duration.value)
        if duration_hours is None:
            errors.append(f'Couldn\'t understand duration "{self.duration.value}".')

        vote_threshold = None
        try:
            vote_threshold = int(self.vote_threshold.value)
            if vote_threshold < 0:
                raise ValueError
        except ValueError:
            errors.append("Vote threshold must be a non-negative number.")

        if errors:
            retry_defaults = dict(self.carry_over)
            retry_defaults.update({
                "question": self.question.value,
                "options_raw": self.options.value,
                "send_time_raw": self.send_time.value,
                "duration_raw": self.duration.value,
                "vote_threshold": self.vote_threshold.value,
            })
            await interaction.response.send_message(
                "⚠️ " + "\n⚠️ ".join(errors),
                view=RetryView(self.cog, mode=self.mode, modify_id=self.modify_id, defaults=retry_defaults),
                ephemeral=True,
            )
            return

        data = dict(self.carry_over)
        data.update({
            "question": self.question.value,
            "options_raw": self.options.value,
            "send_time_raw": self.send_time.value,
            "send_time_parsed": send_parsed.isoformat(),
            "timezone": parse_timezone(self.send_time.value),
            "duration_raw": self.duration.value,
            "duration_hours": duration_hours,
            "vote_threshold": vote_threshold,
        })
        data.setdefault("ping_target", "@everyone")
        data.setdefault("post_channel_id", interaction.channel_id)
        data.setdefault("repeat_raw", "none")
        data.setdefault("schedule_cron", None)

        view = PollOptionsView(self.cog, mode=self.mode, modify_id=self.modify_id,
                                data=data, creator_id=interaction.user.id)
        await interaction.response.send_message(embed=view.build_embed(), view=view)
        view.message = await interaction.original_response()


class RetryView(discord.ui.View):
    """Shown when PollCoreModal validation fails, so the user can fix just the bad field."""

    def __init__(self, cog, *, mode, modify_id, defaults):
        super().__init__(timeout=300)
        self.cog = cog
        self.mode = mode
        self.modify_id = modify_id
        self.defaults = defaults

    @discord.ui.button(label="Fix & Resubmit", style=discord.ButtonStyle.primary)
    async def retry(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(
            PollCoreModal(self.cog, mode=self.mode, modify_id=self.modify_id, defaults=self.defaults)
        )


class RecurrenceModal(discord.ui.Modal):
    def __init__(self, options_view):
        super().__init__(title="Set Recurrence")
        self.options_view = options_view
        current = options_view.data.get("repeat_raw", "none")
        self.recurrence = discord.ui.TextInput(
            label="Repeat schedule",
            placeholder='"every Monday at 9am EST", or leave blank for none',
            default=current if current.lower() not in ("none", "no") else "",
            required=False,
        )
        self.add_item(self.recurrence)

    async def on_submit(self, interaction: discord.Interaction):
        text = self.recurrence.value.strip() or "none"
        recurrence = parse_recurrence(text)
        if text.lower() not in ("none", "no") and recurrence is None:
            await interaction.response.send_message(
                f'Couldn\'t understand recurrence "{text}". Try something like "every Monday at 9am EST".',
                ephemeral=True,
            )
            return
        self.options_view.data["repeat_raw"] = text
        self.options_view.data["schedule_cron"] = recurrence
        await interaction.response.edit_message(embed=self.options_view.build_embed(), view=self.options_view)


class PollOptionsView(discord.ui.View):
    """Step 2: post channel, ping target, and recurrence — plus confirm/cancel."""

    def __init__(self, cog, *, mode, modify_id, data, creator_id):
        super().__init__(timeout=300)
        self.cog = cog
        self.mode = mode
        self.modify_id = modify_id
        self.data = data
        self.creator_id = creator_id
        self.message = None

        self.channel_select = discord.ui.ChannelSelect(
            placeholder="Post channel (defaults to this channel)",
            channel_types=[discord.ChannelType.text],
            min_values=0, max_values=1, row=0,
        )
        self.channel_select.callback = self.on_channel_select
        self.add_item(self.channel_select)

        self.role_select = discord.ui.RoleSelect(
            placeholder="Ping a role instead (optional)",
            min_values=0, max_values=1, row=1,
        )
        self.role_select.callback = self.on_role_select
        self.add_item(self.role_select)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.creator_id:
            await interaction.response.send_message(
                "Only the person creating this poll can use these controls.", ephemeral=True
            )
            return False
        return True

    async def on_timeout(self):
        if self.message:
            try:
                await self.message.edit(
                    content="Poll setup timed out (5 minute limit). Use `/schedule poll` to start again.",
                    embed=None, view=None,
                )
            except discord.HTTPException:
                pass

    def build_embed(self):
        options_list = [o.strip() for o in self.data.get("options_raw", "").split(",") if o.strip()]
        options_display = "\n".join(f"  {OPTION_EMOJIS[i]} {opt}" for i, opt in enumerate(options_list)) or "?"

        send_time_display = self.data.get("send_time_raw", "?")
        if self.data.get("send_time_parsed"):
            try:
                parsed_dt = datetime.fromisoformat(self.data["send_time_parsed"])
                send_time_display = to_discord_timestamp(parsed_dt, "F")
            except (ValueError, TypeError):
                pass

        repeat_text = self.data.get("repeat_raw", "none")
        if repeat_text.lower() in ("none", "no"):
            repeat_text = "No (one-time poll)"

        post_channel = f"<#{self.data['post_channel_id']}>" if self.data.get("post_channel_id") else "This channel"

        embed = discord.Embed(title="Poll Setup — Confirm to schedule", color=discord.Color.gold())
        embed.add_field(name="Question", value=self.data.get("question", "?"), inline=False)
        embed.add_field(name="Options", value=options_display, inline=False)
        embed.add_field(name="Ping", value=self.data.get("ping_target") or "No ping", inline=True)
        embed.add_field(name="Post In", value=post_channel, inline=True)
        embed.add_field(name="Send Time", value=send_time_display, inline=True)
        embed.add_field(name="Repeat", value=repeat_text, inline=True)
        embed.add_field(name="Duration", value=self.data.get("duration_raw", "?"), inline=True)
        embed.add_field(name="Vote Threshold", value=str(self.data.get("vote_threshold", 0)), inline=True)
        embed.set_footer(text="Use the menus/buttons below to adjust, then Confirm.")
        return embed

    async def refresh(self, interaction: discord.Interaction):
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    async def on_channel_select(self, interaction: discord.Interaction):
        self.data["post_channel_id"] = self.channel_select.values[0].id
        await self.refresh(interaction)

    async def on_role_select(self, interaction: discord.Interaction):
        self.data["ping_target"] = self.role_select.values[0].mention
        await self.refresh(interaction)

    @discord.ui.button(label="@everyone", style=discord.ButtonStyle.secondary, row=2)
    async def ping_everyone(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.data["ping_target"] = "@everyone"
        await self.refresh(interaction)

    @discord.ui.button(label="@here", style=discord.ButtonStyle.secondary, row=2)
    async def ping_here(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.data["ping_target"] = "@here"
        await self.refresh(interaction)

    @discord.ui.button(label="No ping", style=discord.ButtonStyle.secondary, row=2)
    async def ping_none(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.data["ping_target"] = ""
        await self.refresh(interaction)

    @discord.ui.button(label="One-time", style=discord.ButtonStyle.secondary, row=3)
    async def repeat_none(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.data["repeat_raw"] = "none"
        self.data["schedule_cron"] = None
        await self.refresh(interaction)

    @discord.ui.button(label="Set recurrence…", style=discord.ButtonStyle.secondary, row=3)
    async def repeat_set(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RecurrenceModal(self))

    @discord.ui.button(label="Confirm & Schedule", style=discord.ButtonStyle.success, row=4)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.stop()
        await self.cog._finalize_poll_from_data(interaction, self.mode, self.modify_id, self.data)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger, row=4)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.stop()
        await interaction.response.edit_message(content="Poll creation cancelled.", embed=None, view=None)


class Polls(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.polls = {}
        self.load_polls()

    def save_polls(self):
        with open(DATA_PATH, "w") as f:
            json.dump({"polls": self.polls}, f, indent=2, default=str)

    def load_polls(self):
        try:
            with open(DATA_PATH, "r") as f:
                content = f.read()
                if content:
                    data = json.loads(content)
                    self.polls = data.get("polls", {})
                    print(f"Loaded {len(self.polls)} polls.")
                else:
                    self.polls = {}
        except FileNotFoundError:
            self.polls = {}

    @commands.Cog.listener()
    async def on_ready(self):
        """Restore scheduler jobs for existing polls AFTER the scheduler has started.

        Previously this was in cog_load(), which runs before the scheduler starts.
        Jobs registered before scheduler.start() with near-future trigger times
        would misfire and be silently skipped by APScheduler's default 1-second
        misfire_grace_time.
        """
        registered = 0
        for poll_id, poll in self.polls.items():
            if poll["status"] == "scheduled":
                self._register_send_job(poll_id, poll)
                registered += 1
            elif poll["status"] == "active":
                self._register_resolve_job(poll_id, poll)
                registered += 1
        print(f"[Polls] Registered {registered} scheduler jobs for {len(self.polls)} polls.")

    def _register_send_job(self, poll_id, poll):
        """Register a scheduler job to post a poll."""
        scheduler = self.bot.scheduler
        short_id = poll_id[:8]

        # Check if next_send_time is in the future — if so, use a DateTrigger
        # for the initial send regardless of whether the poll is recurring.
        # After the poll fires, _handle_recurrence will re-register with a
        # CronTrigger for subsequent recurring sends.
        send_time = datetime.fromisoformat(poll["next_send_time"])
        tz = pytz.timezone(poll.get("schedule_timezone", "US/Eastern"))
        now = datetime.now(tz)
        if send_time.tzinfo is None:
            send_time = tz.localize(send_time)

        use_cron = (poll.get("recurring") and poll.get("schedule_cron")
                    and send_time <= now)

        if use_cron:
            # Recurring poll that has already had its initial send —
            # use CronTrigger for the next occurrence
            cron = poll["schedule_cron"]
            trigger = CronTrigger(
                day_of_week=cron.get("day_of_week"),
                hour=cron["hour"],
                minute=cron["minute"],
                timezone=cron.get("timezone", "US/Eastern"),
            )
            print(f"[Polls] Registering recurring send job for poll {short_id} "
                  f"(cron: day={cron.get('day_of_week')}, {cron['hour']}:{cron.get('minute', 0):02d} "
                  f"{cron.get('timezone', 'US/Eastern')})")
        else:
            # Initial send (one-shot or first occurrence of recurring poll)
            if send_time <= now:
                print(f"[Polls] Poll {short_id} send time is in the past ({send_time}), scheduling for 5s from now")
                send_time = now + timedelta(seconds=5)
            trigger = DateTrigger(run_date=send_time)
            recurring_note = " (first occurrence of recurring poll)" if poll.get("recurring") else ""
            print(f"[Polls] Registering one-shot send job for poll {short_id} at {send_time.isoformat()}{recurring_note}")

        scheduler.add_job(
            self.post_poll,
            trigger,
            args=[poll_id],
            id=f"poll_send_{poll_id}",
            replace_existing=True,
        )
        print(f"[Polls] Job poll_send_{short_id} added to scheduler (scheduler running: {scheduler.running})")

    def _register_resolve_job(self, poll_id, poll):
        """Register a scheduler job to resolve a poll."""
        scheduler = self.bot.scheduler
        short_id = poll_id[:8]
        send_time = datetime.fromisoformat(poll["next_send_time"])
        tz = pytz.timezone(poll.get("schedule_timezone", "US/Eastern"))
        if send_time.tzinfo is None:
            send_time = tz.localize(send_time)
        resolve_time = send_time + timedelta(hours=poll["poll_duration_hours"])

        now = datetime.now(tz)

        if resolve_time <= now:
            print(f"[Polls] Poll {short_id} resolve time is in the past ({resolve_time}), scheduling for 5s from now")
            resolve_time = now + timedelta(seconds=5)

        scheduler.add_job(
            self.resolve_poll,
            DateTrigger(run_date=resolve_time),
            args=[poll_id],
            id=f"poll_resolve_{poll_id}",
            replace_existing=True,
        )
        print(f"[Polls] Registered resolve job for poll {short_id} at {resolve_time.isoformat()}")

    async def post_poll(self, poll_id):
        """Post a poll message with reaction emojis."""
        short_id = poll_id[:8]
        print(f"[Polls] post_poll fired for poll {short_id}")
        poll = self.polls.get(poll_id)
        if not poll:
            print(f"[Polls] Poll {short_id} not found in self.polls, aborting")
            return

        # Use the target post channel, not the setup channel
        post_channel_id = poll.get("post_channel_id", poll["channel_id"])
        channel = self.bot.get_channel(post_channel_id)
        if not channel:
            print(f"[Polls] Could not find channel {post_channel_id} for poll {short_id}")
            return

        now = datetime.now(pytz.utc)
        end_time = now + timedelta(hours=poll["poll_duration_hours"])

        # Build the poll embed
        embed = discord.Embed(
            title=poll["question"],
            color=discord.Color.blue(),
            timestamp=now,
        )

        options_text = ""
        for i, option in enumerate(poll["options"]):
            options_text += f"{option['emoji']} {option['label']}\n"
        embed.add_field(name="Options", value=options_text, inline=False)

        # Use Discord timestamps so everyone sees their own timezone
        embed.add_field(
            name="Poll Ends",
            value=f"{to_discord_timestamp(end_time, 'F')} ({to_discord_timestamp(end_time, 'R')})",
            inline=False,
        )
        embed.set_footer(text=f"React to vote! Minimum {poll['vote_threshold']} votes needed per option.")

        # Send with ping
        ping = poll.get("ping_target", "")
        msg = await channel.send(content=ping, embed=embed)

        # Add reaction emojis
        for option in poll["options"]:
            await msg.add_reaction(option["emoji"])
            await asyncio.sleep(0.3)

        # Update poll state
        poll["active_message_id"] = msg.id
        poll["post_channel_id"] = post_channel_id
        poll["status"] = "active"
        poll["next_send_time"] = now.isoformat()
        self.save_polls()
        print(f"[Polls] Poll {short_id} state changed: scheduled -> active (message {msg.id})")

        # Schedule resolution
        self._register_resolve_job(poll_id, poll)

    async def resolve_poll(self, poll_id):
        """Resolve a poll: count votes, announce results, create event."""
        short_id = poll_id[:8]
        print(f"[Polls] resolve_poll fired for poll {short_id}")
        poll = self.polls.get(poll_id)
        if not poll:
            print(f"[Polls] Poll {short_id} not found in self.polls, aborting")
            return

        post_channel_id = poll.get("post_channel_id", poll["channel_id"])
        channel = self.bot.get_channel(post_channel_id)
        if not channel:
            return

        # Fetch the poll message to read reactions
        try:
            msg = await channel.fetch_message(poll["active_message_id"])
        except (discord.NotFound, discord.HTTPException):
            await channel.send(f"Could not find poll message for **{poll['question']}**. Poll resolution failed.")
            return

        # Count votes (subtract 1 for the bot's own reaction)
        results = []
        for i, option in enumerate(poll["options"]):
            emoji = option["emoji"]
            vote_count = 0
            for reaction in msg.reactions:
                if str(reaction.emoji) == emoji:
                    vote_count = reaction.count - 1
                    break
            results.append({
                "label": option["label"],
                "emoji": emoji,
                "votes": vote_count,
            })

        # Sort by votes descending
        results.sort(key=lambda x: x["votes"], reverse=True)

        # Filter by threshold
        threshold = poll.get("vote_threshold", 0)
        qualifying = [r for r in results if r["votes"] >= threshold]

        # Check for ties at the top
        if len(qualifying) >= 2 and qualifying[0]["votes"] == qualifying[1]["votes"]:
            top_votes = qualifying[0]["votes"]
            tied = [r for r in qualifying if r["votes"] == top_votes]

            # Check if this is already a tiebreaker poll
            if poll.get("is_tiebreaker"):
                # Tiebreaker also tied — announce all tied options, no event
                await self._announce_unresolved_tie(channel, poll, tied)
            else:
                # Run a tiebreaker poll
                await self._run_tiebreaker(channel, poll, poll_id, tied)
                return  # Don't do normal recurrence yet; tiebreaker handles it

        elif qualifying:
            # Clear winner
            await self._announce_results(channel, poll, results, qualifying, threshold)
            # Create event from winner
            await self._try_create_event(poll, qualifying[0])
        else:
            # No qualifying options
            embed = discord.Embed(
                title=f"Poll Results: {poll['question']}",
                color=discord.Color.red(),
                timestamp=datetime.now(pytz.utc),
            )
            embed.add_field(
                name="Results",
                value=f"No options met the minimum threshold of {threshold} vote(s).",
                inline=False,
            )
            ping = poll.get("ping_target", "")
            await channel.send(content=ping, embed=embed)

        # Handle recurrence
        self._handle_recurrence(poll_id, poll)

    async def _announce_results(self, channel, poll, results, qualifying, threshold):
        """Send the results embed for a poll with a clear winner."""
        embed = discord.Embed(
            title=f"Poll Results: {poll['question']}",
            color=discord.Color.green(),
            timestamp=datetime.now(pytz.utc),
        )

        results_text = ""
        for i, r in enumerate(qualifying):
            medal = ["\U0001f947", "\U0001f948", "\U0001f949"][i] if i < 3 else f"#{i+1}"
            results_text += f"{medal} {r['label']} — **{r['votes']}** vote(s)\n"
        embed.add_field(name="Results", value=results_text, inline=False)

        winner = qualifying[0]
        embed.add_field(
            name="Winner",
            value=f"**{winner['label']}** with {winner['votes']} vote(s)!",
            inline=False,
        )

        # Show options that didn't meet threshold
        disqualified = [r for r in results if r["votes"] < threshold and r["votes"] > 0]
        if disqualified:
            dq_text = ", ".join([f"{r['label']} ({r['votes']})" for r in disqualified])
            embed.add_field(
                name=f"Below threshold ({threshold} votes needed)",
                value=dq_text,
                inline=False,
            )

        ping = poll.get("ping_target", "")
        await channel.send(content=ping, embed=embed)

    async def _announce_unresolved_tie(self, channel, poll, tied):
        """Announce that even the tiebreaker resulted in a tie."""
        embed = discord.Embed(
            title=f"Tiebreaker Results: {poll['question']}",
            color=discord.Color.orange(),
            timestamp=datetime.now(pytz.utc),
        )
        tied_text = "\n".join([f"- **{r['label']}** ({r['votes']} votes)" for r in tied])
        embed.add_field(
            name="Still tied!",
            value=f"The tiebreaker poll also ended in a tie:\n{tied_text}\n\nYou'll need to decide among yourselves!",
            inline=False,
        )
        ping = poll.get("ping_target", "")
        await channel.send(content=ping, embed=embed)

    async def _run_tiebreaker(self, channel, parent_poll, parent_poll_id, tied_options):
        """Create and post a tiebreaker poll with only the tied options."""
        await channel.send(
            f"**Tie detected!** {len(tied_options)} options tied with {tied_options[0]['votes']} vote(s) each. "
            f"Running a {TIEBREAKER_DURATION_MINUTES}-minute tiebreaker poll..."
        )

        # Create a tiebreaker poll
        tiebreaker_id = str(uuid.uuid4())
        options = []
        for i, opt in enumerate(tied_options):
            options.append({"label": opt["label"], "emoji": OPTION_EMOJIS[i]})

        post_channel_id = parent_poll.get("post_channel_id", parent_poll["channel_id"])

        tiebreaker = {
            "id": tiebreaker_id,
            "guild_id": parent_poll["guild_id"],
            "channel_id": parent_poll["channel_id"],
            "post_channel_id": post_channel_id,
            "creator_id": parent_poll["creator_id"],
            "question": f"Tiebreaker: {parent_poll['question']}",
            "options": options,
            "ping_target": parent_poll.get("ping_target", ""),
            "vote_threshold": 0,
            "schedule_cron": None,
            "schedule_timezone": parent_poll.get("schedule_timezone", "US/Eastern"),
            "next_send_time": datetime.now(pytz.utc).isoformat(),
            "poll_duration_hours": TIEBREAKER_DURATION_MINUTES / 60,
            "status": "scheduled",
            "active_message_id": None,
            "recurring": False,
            "is_tiebreaker": True,
            "parent_poll_id": parent_poll_id,
            "created_at": datetime.now(pytz.utc).isoformat(),
        }

        self.polls[tiebreaker_id] = tiebreaker
        self.save_polls()

        # Post the tiebreaker immediately
        await self.post_poll(tiebreaker_id)

    def _handle_recurrence(self, poll_id, poll):
        """Handle recurring poll re-scheduling after resolution."""
        short_id = poll_id[:8]
        # If this is a tiebreaker, handle the parent poll's recurrence instead
        if poll.get("is_tiebreaker"):
            parent_id = poll.get("parent_poll_id")
            if parent_id and parent_id in self.polls:
                parent = self.polls[parent_id]
                if parent.get("recurring") and parent.get("schedule_cron"):
                    parent["status"] = "scheduled"
                    parent["active_message_id"] = None
                    self.save_polls()
                    print(f"[Polls] Tiebreaker {short_id} resolved, re-scheduling parent {parent_id[:8]}: active -> scheduled")
                    self._register_send_job(parent_id, parent)
                else:
                    parent["status"] = "completed"
                    self.save_polls()
                    print(f"[Polls] Tiebreaker {short_id} resolved, parent {parent_id[:8]}: active -> completed")
            # Mark tiebreaker as completed
            poll["status"] = "completed"
            self.save_polls()
            print(f"[Polls] Tiebreaker {short_id}: active -> completed")
            return

        if poll.get("recurring") and poll.get("schedule_cron"):
            poll["status"] = "scheduled"
            poll["active_message_id"] = None
            self.save_polls()
            print(f"[Polls] Recurring poll {short_id}: active -> scheduled (re-registering)")
            self._register_send_job(poll_id, poll)
        else:
            poll["status"] = "completed"
            self.save_polls()
            print(f"[Polls] Poll {short_id}: active -> completed")

    async def _try_create_event(self, poll, winner):
        """Attempt to create a Discord scheduled event from the winning poll option."""
        guild = self.bot.get_guild(poll["guild_id"])
        if not guild:
            return

        parsed = dateparser.parse(normalize_shorthand_datetime(winner["label"]), settings={
            'PREFER_DATES_FROM': 'future',
            'RETURN_AS_TIMEZONE_AWARE': True,
        })

        if not parsed:
            post_channel_id = poll.get("post_channel_id", poll["channel_id"])
            channel = self.bot.get_channel(post_channel_id)
            if channel:
                await channel.send(
                    f"Could not auto-create a server event for **{winner['label']}** "
                    f"(not parseable as a date/time). You can create it manually!"
                )
            return

        if parsed <= datetime.now(parsed.tzinfo):
            return

        try:
            await guild.create_scheduled_event(
                name=poll["question"],
                description=f"Scheduled via poll. Winning time: {winner['label']}",
                start_time=parsed,
                end_time=parsed + timedelta(hours=3),
                entity_type=discord.EntityType.external,
                privacy_level=discord.PrivacyLevel.guild_only,
                location="Discord",
            )
            post_channel_id = poll.get("post_channel_id", poll["channel_id"])
            channel = self.bot.get_channel(post_channel_id)
            if channel:
                await channel.send(f"A server event has been created for **{winner['label']}**!")
        except discord.HTTPException as e:
            print(f"Failed to create scheduled event: {e}")

    # ---- Slash Commands ----

    schedule_group = app_commands.Group(name="schedule", description="Schedule polls and events")

    @schedule_group.command(name="poll", description="Create a new scheduled poll")
    async def schedule_poll(self, interaction: discord.Interaction):
        """Open the poll creation form."""
        await interaction.response.send_modal(PollCoreModal(self, mode="create"))

    events_group = app_commands.Group(name="events", description="Manage scheduled polls")

    @events_group.command(name="list", description="List all active scheduled polls")
    async def events_list(self, interaction: discord.Interaction):
        guild_polls = {pid: p for pid, p in self.polls.items()
                       if p["guild_id"] == interaction.guild_id and not p.get("is_tiebreaker")}

        if not guild_polls:
            await interaction.response.send_message("No scheduled polls found.", ephemeral=True)
            return

        embed = discord.Embed(title="Scheduled Polls", color=discord.Color.blue())

        for pid, p in guild_polls.items():
            short_id = pid[:8]
            status_emoji = {"scheduled": "\U0001f550", "active": "\U0001f7e2", "completed": "✅"}.get(p["status"], "❓")

            info = f"Status: {status_emoji} {p['status']}"
            if p.get("recurring"):
                cron = p.get("schedule_cron", {})
                day = cron.get("day_of_week", "?")
                info += f"\nRepeats: Every {day} at {cron.get('hour', '?')}:{cron.get('minute', 0):02d}"
            if p.get("next_send_time") and p["status"] == "scheduled":
                try:
                    send_dt = datetime.fromisoformat(p["next_send_time"])
                    info += f"\nNext send: {to_discord_timestamp(send_dt, 'F')} ({to_discord_timestamp(send_dt, 'R')})"
                except (ValueError, TypeError):
                    info += f"\nNext send: {p['next_send_time']}"

            post_ch = p.get("post_channel_id", p["channel_id"])
            info += f"\nPosts to: <#{post_ch}>"
            info += f"\nThreshold: {p.get('vote_threshold', 0)} votes"
            info += f"\nOptions: {', '.join(o['label'] for o in p['options'])}"
            info += f"\nID: `{short_id}`"

            embed.add_field(name=p["question"], value=info, inline=False)

        embed.set_footer(text="Use /events delete, /events modify, or /events clone with the poll ID.")
        await interaction.response.send_message(embed=embed)

    @events_group.command(name="delete", description="Delete a scheduled poll")
    @app_commands.describe(poll_id="The poll ID (first 8 characters shown in /events list)")
    async def events_delete(self, interaction: discord.Interaction, poll_id: str):
        full_id = self._find_poll_id(poll_id, interaction.guild_id)
        if not full_id:
            await interaction.response.send_message(f"No poll found with ID `{poll_id}`.", ephemeral=True)
            return

        poll = self.polls[full_id]
        scheduler = self.bot.scheduler
        for job_prefix in ["poll_send_", "poll_resolve_"]:
            try:
                scheduler.remove_job(f"{job_prefix}{full_id}")
            except Exception:
                pass

        del self.polls[full_id]
        self.save_polls()
        await interaction.response.send_message(f"Deleted poll: **{poll['question']}**")

    @events_group.command(name="modify", description="Modify a scheduled poll")
    @app_commands.describe(poll_id="The poll ID (first 8 characters shown in /events list)")
    async def events_modify(self, interaction: discord.Interaction, poll_id: str):
        full_id = self._find_poll_id(poll_id, interaction.guild_id)
        if not full_id:
            await interaction.response.send_message(f"No poll found with ID `{poll_id}`.", ephemeral=True)
            return

        poll = self.polls[full_id]
        defaults = {
            "question": poll["question"],
            "options_raw": ", ".join(o["label"] for o in poll["options"]),
            "send_time_raw": self._format_send_time_default(poll),
            "duration_raw": str(poll.get("poll_duration_hours", 24)),
            "vote_threshold": poll.get("vote_threshold", 0),
            "ping_target": poll.get("ping_target", "@everyone"),
            "post_channel_id": poll.get("post_channel_id", poll["channel_id"]),
            "repeat_raw": "none",
            "schedule_cron": poll.get("schedule_cron"),
        }
        if poll.get("recurring") and poll.get("schedule_cron"):
            cron = poll["schedule_cron"]
            defaults["repeat_raw"] = (
                f"every {cron.get('day_of_week', '?')} at "
                f"{cron.get('hour', 0):02d}:{cron.get('minute', 0):02d} "
                f"{cron.get('timezone', 'US/Eastern')}"
            )
        await interaction.response.send_modal(
            PollCoreModal(self, mode="modify", modify_id=full_id, defaults=defaults)
        )

    @events_group.command(name="clone", description="Clone a scheduled poll")
    @app_commands.describe(poll_id="The poll ID (first 8 characters shown in /events list)")
    async def events_clone(self, interaction: discord.Interaction, poll_id: str):
        full_id = self._find_poll_id(poll_id, interaction.guild_id)
        if not full_id:
            await interaction.response.send_message(f"No poll found with ID `{poll_id}`.", ephemeral=True)
            return

        poll = self.polls[full_id]
        defaults = {
            "question": poll["question"],
            "options_raw": ", ".join(o["label"] for o in poll["options"]),
            "send_time_raw": "",
            "duration_raw": str(poll.get("poll_duration_hours", 24)),
            "vote_threshold": poll.get("vote_threshold", 0),
            "ping_target": poll.get("ping_target", "@everyone"),
            "post_channel_id": poll.get("post_channel_id", poll["channel_id"]),
            "repeat_raw": "none",
            "schedule_cron": None,
        }
        await interaction.response.send_modal(
            PollCoreModal(self, mode="clone", modify_id=None, defaults=defaults)
        )

    def _format_send_time_default(self, poll):
        """Render a poll's stored next_send_time as text dateparser can re-parse,
        for pre-filling the modal on /events modify."""
        try:
            dt = datetime.fromisoformat(poll["next_send_time"])
            tz = pytz.timezone(poll.get("schedule_timezone", "US/Eastern"))
            dt = tz.localize(dt) if dt.tzinfo is None else dt.astimezone(tz)
            return dt.strftime("%Y-%m-%d %I:%M%p %Z")
        except (KeyError, ValueError, TypeError):
            return ""

    def _find_poll_id(self, short_id, guild_id):
        """Find a full poll ID from a short prefix, scoped to a guild."""
        short_id = short_id.lower().strip()
        for pid, poll in self.polls.items():
            if pid.lower().startswith(short_id) and poll["guild_id"] == guild_id:
                return pid
        return None

    async def _finalize_poll_from_data(self, interaction, mode, modify_id, data):
        """Create/update the poll from the modal+view data and schedule it."""
        options_list = [o.strip() for o in data.get("options_raw", "").split(",") if o.strip()]
        options = [{"label": label, "emoji": OPTION_EMOJIS[i]} for i, label in enumerate(options_list)]

        recurrence = parse_recurrence(data.get("repeat_raw", "none"))
        recurring = recurrence is not None

        duration_hours = data.get("duration_hours") or self._parse_duration(data.get("duration_raw", "24 hours")) or 24
        tz = data.get("timezone", "US/Eastern")
        post_channel_id = data.get("post_channel_id", interaction.channel_id)

        if modify_id:
            poll_id = modify_id
            for prefix in ("poll_send_", "poll_resolve_"):
                try:
                    self.bot.scheduler.remove_job(f"{prefix}{poll_id}")
                except Exception:
                    pass
        else:
            poll_id = str(uuid.uuid4())

        poll = {
            "id": poll_id,
            "guild_id": interaction.guild_id,
            "channel_id": interaction.channel_id,
            "post_channel_id": post_channel_id,
            "creator_id": interaction.user.id,
            "question": data["question"],
            "options": options,
            "ping_target": data.get("ping_target", "@everyone"),
            "vote_threshold": data.get("vote_threshold", 0),
            "schedule_cron": recurrence,
            "schedule_timezone": tz,
            "next_send_time": data.get("send_time_parsed", datetime.now(pytz.utc).isoformat()),
            "poll_duration_hours": duration_hours,
            "status": "scheduled",
            "active_message_id": None,
            "recurring": recurring,
            "created_at": datetime.now(pytz.utc).isoformat(),
        }

        self.polls[poll_id] = poll
        self.save_polls()

        print(f"[Polls] Poll {poll_id[:8]} {'modified' if modify_id else 'created'}: "
              f"question='{data['question']}', send_time={data.get('send_time_parsed')}, "
              f"tz={tz}, recurring={recurring}")
        self._register_send_job(poll_id, poll)

        action = "modified" if modify_id else "created"
        await interaction.response.edit_message(
            content=(
                f"Poll {action} and scheduled! ID: `{poll_id[:8]}`\n"
                f"Poll will be posted in <#{post_channel_id}>.\n"
                f"Use `/events list` to see all scheduled polls."
            ),
            embed=None,
            view=None,
        )

    def _parse_duration(self, text):
        """Parse a duration string like '24 hours' or '2 days' into hours."""
        if not text:
            return None
        text = text.lower().strip()
        try:
            return float(text)
        except ValueError:
            pass

        match = re.match(r"(\d+(?:\.\d+)?)\s*(hours?|hrs?|days?|d|h|minutes?|mins?|m)", text)
        if match:
            value = float(match.group(1))
            unit = match.group(2)
            if unit.startswith("d"):
                return value * 24
            elif unit.startswith("h"):
                return value
            elif unit.startswith("m"):
                return value / 60
        return None


async def setup(bot):
    await bot.add_cog(Polls(bot))
