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

NUMBER_EMOJIS = ["1\u20e3", "2\u20e3", "3\u20e3", "4\u20e3", "5\u20e3", "6\u20e3", "7\u20e3", "8\u20e3", "9\u20e3"]

# Maps day-of-week names to APScheduler cron values
DAY_MAP = {
    "monday": "mon", "tuesday": "tue", "wednesday": "wed",
    "thursday": "thu", "friday": "fri", "saturday": "sat", "sunday": "sun"
}

TIEBREAKER_DURATION_MINUTES = 30


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


class Polls(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.polls = {}
        self.active_creations = {}  # (guild_id, user_id) -> creation state
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
            options.append({"label": opt["label"], "emoji": NUMBER_EMOJIS[i]})

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

        parsed = dateparser.parse(winner["label"], settings={
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
        """Start the multi-step poll creation dialog."""
        key = (interaction.guild_id, interaction.user.id)

        if key in self.active_creations:
            await interaction.response.send_message(
                "You already have a poll creation in progress! Finish or cancel it first.",
                ephemeral=True,
            )
            return

        self.active_creations[key] = {
            "step": 1,
            "channel_id": interaction.channel_id,
            "guild_id": interaction.guild_id,
            "creator_id": interaction.user.id,
            "last_interaction": datetime.now(pytz.utc),
            "data": {},
        }

        await interaction.response.send_message(
            "Let's create a scheduled poll! I'll ask you a series of questions.\n\n"
            "**Step 1/9:** What is the poll question?\n"
            "*(e.g., \"When can everyone play D&D this week?\")*",
            ephemeral=False,
        )

    @schedule_group.command(name="cancel", description="Cancel poll creation in progress")
    async def schedule_cancel(self, interaction: discord.Interaction):
        key = (interaction.guild_id, interaction.user.id)
        if key in self.active_creations:
            del self.active_creations[key]
            await interaction.response.send_message("Poll creation cancelled.", ephemeral=True)
        else:
            await interaction.response.send_message("No poll creation in progress.", ephemeral=True)

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
            status_emoji = {"scheduled": "\U0001f550", "active": "\U0001f7e2", "completed": "\u2705"}.get(p["status"], "\u2753")

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

        key = (interaction.guild_id, interaction.user.id)
        if key in self.active_creations:
            await interaction.response.send_message(
                "You already have a poll creation in progress! Finish or cancel it first.",
                ephemeral=True,
            )
            return

        poll = self.polls[full_id]
        post_ch = poll.get("post_channel_id", poll["channel_id"])
        self.active_creations[key] = {
            "step": 1,
            "channel_id": interaction.channel_id,
            "guild_id": interaction.guild_id,
            "creator_id": interaction.user.id,
            "last_interaction": datetime.now(pytz.utc),
            "mode": "modify",
            "modify_id": full_id,
            "data": {
                "question": poll["question"],
                "options_raw": ", ".join(o["label"] for o in poll["options"]),
                "ping_target": poll.get("ping_target", "@everyone"),
                "post_channel_id": post_ch,
                "send_time_raw": poll.get("next_send_time", ""),
                "repeat_raw": "none",
                "duration_raw": str(poll.get("poll_duration_hours", 24)),
                "vote_threshold": poll.get("vote_threshold", 0),
            },
        }

        current = self.active_creations[key]["data"]
        await interaction.response.send_message(
            f"Modifying poll: **{poll['question']}**\n"
            f"Type your new answer at each step, or type **keep** to keep the current value.\n\n"
            f"**Step 1/9:** What is the poll question?\n"
            f"*Current: {current['question']}*",
        )

    @events_group.command(name="clone", description="Clone a scheduled poll")
    @app_commands.describe(poll_id="The poll ID (first 8 characters shown in /events list)")
    async def events_clone(self, interaction: discord.Interaction, poll_id: str):
        full_id = self._find_poll_id(poll_id, interaction.guild_id)
        if not full_id:
            await interaction.response.send_message(f"No poll found with ID `{poll_id}`.", ephemeral=True)
            return

        key = (interaction.guild_id, interaction.user.id)
        if key in self.active_creations:
            await interaction.response.send_message(
                "You already have a poll creation in progress! Finish or cancel it first.",
                ephemeral=True,
            )
            return

        poll = self.polls[full_id]
        post_ch = poll.get("post_channel_id", poll["channel_id"])
        self.active_creations[key] = {
            "step": 1,
            "channel_id": interaction.channel_id,
            "guild_id": interaction.guild_id,
            "creator_id": interaction.user.id,
            "last_interaction": datetime.now(pytz.utc),
            "mode": "clone",
            "data": {
                "question": poll["question"],
                "options_raw": ", ".join(o["label"] for o in poll["options"]),
                "ping_target": poll.get("ping_target", "@everyone"),
                "post_channel_id": post_ch,
                "send_time_raw": "",
                "repeat_raw": "none",
                "duration_raw": str(poll.get("poll_duration_hours", 24)),
                "vote_threshold": poll.get("vote_threshold", 0),
            },
        }

        current = self.active_creations[key]["data"]
        await interaction.response.send_message(
            f"Cloning poll: **{poll['question']}**\n"
            f"Type your new answer at each step, or type **keep** to keep the cloned value.\n\n"
            f"**Step 1/9:** What is the poll question?\n"
            f"*Current: {current['question']}*",
        )

    def _find_poll_id(self, short_id, guild_id):
        """Find a full poll ID from a short prefix, scoped to a guild."""
        short_id = short_id.lower().strip()
        for pid, poll in self.polls.items():
            if pid.lower().startswith(short_id) and poll["guild_id"] == guild_id:
                return pid
        return None

    # ---- Multi-Step Dialog Listener ----

    @commands.Cog.listener()
    async def on_message(self, message):
        if message.author.bot:
            return

        key = (getattr(message.guild, "id", None), message.author.id)
        if key not in self.active_creations:
            return

        creation = self.active_creations[key]

        # Check if message is in the same channel
        if message.channel.id != creation["channel_id"]:
            return

        # Timeout check (5 minutes)
        elapsed = (datetime.now(pytz.utc) - creation["last_interaction"]).total_seconds()
        if elapsed > 300:
            del self.active_creations[key]
            await message.channel.send("Poll creation timed out (5 minute limit). Use `/schedule poll` to start again.")
            return

        creation["last_interaction"] = datetime.now(pytz.utc)
        content = message.content.strip()
        is_modify = creation.get("mode") in ("modify", "clone")
        data = creation["data"]
        step = creation["step"]

        try:
            await self._handle_step(message, key, creation, content, is_modify, data, step)
        except Exception as e:
            print(f"Error in poll creation step {step}: {e}")
            import traceback
            traceback.print_exc()
            await message.channel.send(f"Something went wrong: {e}\nPoll creation cancelled.")
            self.active_creations.pop(key, None)

    async def _handle_step(self, message, key, creation, content, is_modify, data, step):

        if step == 1:
            # Poll question
            if is_modify and content.lower() == "keep":
                pass
            else:
                data["question"] = content

            creation["step"] = 2
            hint = f"\n*Current: {data.get('options_raw', '')}*" if is_modify else ""
            await message.channel.send(
                f"**Step 2/9:** List the response options, separated by commas.\n"
                f"*(e.g., \"Friday Night, Saturday Morning, Saturday Night\")*{hint}"
            )

        elif step == 2:
            # Options
            if is_modify and content.lower() == "keep":
                pass
            else:
                options = [o.strip() for o in content.split(",") if o.strip()]
                if len(options) < 2:
                    await message.channel.send("Please provide at least 2 options, separated by commas.")
                    return
                if len(options) > 9:
                    await message.channel.send("Maximum 9 options allowed. Please try again.")
                    return
                data["options_raw"] = content

            creation["step"] = 3
            hint = f"\n*Current: {data.get('ping_target', '')}*" if is_modify else ""
            await message.channel.send(
                f"**Step 3/9:** Who should be pinged when the poll is posted?\n"
                f"*(e.g., @everyone, @here, or mention a role)*{hint}"
            )

        elif step == 3:
            # Ping target
            if is_modify and content.lower() == "keep":
                pass
            else:
                data["ping_target"] = content

            # Step 4: Channel selection
            creation["step"] = 4
            # List text channels in the guild
            guild = message.guild
            if guild:
                text_channels = [ch for ch in guild.text_channels if ch.permissions_for(guild.me).send_messages]
                channel_list = "\n".join([f"  - <#{ch.id}>" for ch in text_channels[:20]])
                current_hint = ""
                if is_modify and data.get("post_channel_id"):
                    current_hint = f"\n*Current: <#{data['post_channel_id']}>*"
                await message.channel.send(
                    f"**Step 4/9:** Which channel should the poll be posted in?\n"
                    f"*(Mention a channel like #general, or type \"here\" to post in this channel)*\n\n"
                    f"Available channels:\n{channel_list}{current_hint}"
                )
            else:
                creation["step"] = 5  # skip if no guild context
                await self._ask_send_time(message.channel, data, is_modify)

        elif step == 4:
            # Channel selection
            if is_modify and content.lower() == "keep":
                pass
            elif content.lower() == "here":
                data["post_channel_id"] = message.channel.id
            else:
                # Try to extract channel ID from mention like <#123456>
                match = re.match(r"<#(\d+)>", content)
                if match:
                    ch_id = int(match.group(1))
                    ch = message.guild.get_channel(ch_id) if message.guild else None
                    if ch:
                        data["post_channel_id"] = ch_id
                    else:
                        await message.channel.send("I couldn't find that channel. Please try again (mention it with #).")
                        return
                else:
                    # Try to find by name
                    if message.guild:
                        found = discord.utils.get(message.guild.text_channels, name=content.strip("#").lower())
                        if found:
                            data["post_channel_id"] = found.id
                        else:
                            await message.channel.send("I couldn't find that channel. Please mention it with # or type \"here\".")
                            return
                    else:
                        data["post_channel_id"] = message.channel.id

            creation["step"] = 5
            await self._ask_send_time(message.channel, data, is_modify)

        elif step == 5:
            # Send time
            if is_modify and content.lower() == "keep":
                if not data.get("send_time_raw"):
                    await message.channel.send("No existing send time to keep. Please provide one.")
                    return
            else:
                parsed = dateparser.parse(content, settings={
                    'PREFER_DATES_FROM': 'future',
                    'RETURN_AS_TIMEZONE_AWARE': True,
                })
                if not parsed:
                    await message.channel.send("I couldn't understand that time. Please try again. (e.g., \"Monday at 9am EST\")")
                    return
                data["send_time_raw"] = content
                data["send_time_parsed"] = parsed.isoformat()
                tz = parse_timezone(content)
                data["timezone"] = tz
                # Show the time using Discord's auto-converting timestamp
                discord_ts = to_discord_timestamp(parsed, "F")
                relative_ts = to_discord_timestamp(parsed, "R")
                await message.channel.send(f"Got it — I'll send the poll at: {discord_ts} ({relative_ts})")

            creation["step"] = 6
            hint = f"\n*Current: {data.get('repeat_raw', 'none')}*" if is_modify else ""
            await message.channel.send(
                f"**Step 6/9:** Should this poll repeat? If so, provide the schedule.\n"
                f"*(e.g., \"every Monday at 9am EST\" or type \"none\")*{hint}"
            )

        elif step == 6:
            # Repeat schedule
            if is_modify and content.lower() == "keep":
                pass
            else:
                data["repeat_raw"] = content

            creation["step"] = 7
            hint = f"\n*Current: {data.get('duration_raw', '')}*" if is_modify else ""
            await message.channel.send(
                f"**Step 7/9:** How long should the poll stay open for voting?\n"
                f"*(e.g., \"24 hours\", \"2 days\", \"48 hours\")*{hint}"
            )

        elif step == 7:
            # Duration
            if is_modify and content.lower() == "keep":
                pass
            else:
                duration_hours = self._parse_duration(content)
                if duration_hours is None:
                    await message.channel.send("I couldn't understand that duration. Please try again. (e.g., \"24 hours\", \"2 days\")")
                    return
                data["duration_raw"] = content
                data["duration_hours"] = duration_hours

            creation["step"] = 8
            hint = f"\n*Current: {data.get('vote_threshold', 0)}*" if is_modify else ""
            await message.channel.send(
                f"**Step 8/9:** Minimum votes for an option to count in results?\n"
                f"*(Enter a number, e.g., \"3\" means options with fewer than 3 votes are excluded. Use \"0\" for no minimum.)*{hint}"
            )

        elif step == 8:
            # Vote threshold
            if is_modify and content.lower() == "keep":
                pass
            else:
                try:
                    threshold = int(content)
                    if threshold < 0:
                        raise ValueError
                    data["vote_threshold"] = threshold
                except ValueError:
                    await message.channel.send("Please enter a valid number (0 or higher).")
                    return

            # Show confirmation
            creation["step"] = 9
            await self._show_confirmation(message.channel, data)

        elif step == 9:
            # Confirmation - strip any unicode whitespace/formatting characters
            cleaned = re.sub(r'[^\w]', '', content.lower())
            print(f"[DEBUG] Confirmation input: {repr(message.content)} -> cleaned: {repr(cleaned)}")
            if cleaned in ("yes", "y", "confirm"):
                await self._finalize_poll(message, creation)
                del self.active_creations[key]
            elif cleaned in ("no", "n", "cancel"):
                del self.active_creations[key]
                await message.channel.send("Poll creation cancelled.")
            else:
                await message.channel.send("Please type **yes** to confirm or **no** to cancel.")

    async def _ask_send_time(self, channel, data, is_modify):
        """Helper to ask the send time question."""
        hint = f"\n*Current: {data.get('send_time_raw', '')}*" if is_modify else ""
        await channel.send(
            f"**Step 5/9:** When should the poll be sent?\n"
            f"*(e.g., \"Monday at 9am EST\", \"tomorrow at 3pm\", \"in 2 hours\")*{hint}"
        )

    async def _show_confirmation(self, channel, data):
        """Show a summary of the poll for confirmation."""
        options_list = [o.strip() for o in data.get("options_raw", "").split(",") if o.strip()]
        options_display = ""
        for i, opt in enumerate(options_list):
            options_display += f"  {NUMBER_EMOJIS[i]} {opt}\n"

        duration = data.get("duration_hours")
        if not duration:
            duration = self._parse_duration(data.get("duration_raw", "24 hours"))
            data["duration_hours"] = duration

        repeat_text = data.get("repeat_raw", "none")
        if repeat_text.lower() in ("none", "no"):
            repeat_text = "No (one-time poll)"

        # Format send time with Discord timestamp
        send_time_display = data.get("send_time_raw", "?")
        if data.get("send_time_parsed"):
            try:
                parsed_dt = datetime.fromisoformat(data["send_time_parsed"])
                send_time_display = to_discord_timestamp(parsed_dt, "F")
            except (ValueError, TypeError):
                pass

        post_channel = f"<#{data['post_channel_id']}>" if data.get("post_channel_id") else "This channel"

        embed = discord.Embed(
            title="Poll Summary — Confirm?",
            color=discord.Color.gold(),
        )
        embed.add_field(name="Question", value=data.get("question", "?"), inline=False)
        embed.add_field(name="Options", value=options_display or "?", inline=False)
        embed.add_field(name="Ping", value=data.get("ping_target", "?"), inline=True)
        embed.add_field(name="Post In", value=post_channel, inline=True)
        embed.add_field(name="Send Time", value=send_time_display, inline=True)
        embed.add_field(name="Repeat", value=repeat_text, inline=True)
        embed.add_field(name="Duration", value=data.get("duration_raw", "?"), inline=True)
        embed.add_field(name="Vote Threshold", value=str(data.get("vote_threshold", 0)), inline=True)

        await channel.send(embed=embed)
        await channel.send("Type **yes** to confirm or **no** to cancel.")

    async def _finalize_poll(self, message, creation):
        """Create the poll from collected data and schedule it."""
        data = creation["data"]

        # Build options with emojis
        options_list = [o.strip() for o in data.get("options_raw", "").split(",") if o.strip()]
        options = []
        for i, label in enumerate(options_list):
            options.append({"label": label, "emoji": NUMBER_EMOJIS[i]})

        # Parse recurrence
        repeat_raw = data.get("repeat_raw", "none")
        recurrence = parse_recurrence(repeat_raw)
        recurring = recurrence is not None

        # Parse duration
        duration_hours = data.get("duration_hours")
        if not duration_hours:
            duration_hours = self._parse_duration(data.get("duration_raw", "24 hours")) or 24

        # Parse timezone
        tz = data.get("timezone", "US/Eastern")

        # Determine post channel (default to the channel where setup happened)
        post_channel_id = data.get("post_channel_id", creation["channel_id"])

        # Determine poll ID
        modify_id = creation.get("modify_id")
        if modify_id:
            poll_id = modify_id
            for prefix in ["poll_send_", "poll_resolve_"]:
                try:
                    self.bot.scheduler.remove_job(f"{prefix}{poll_id}")
                except Exception:
                    pass
        else:
            poll_id = str(uuid.uuid4())

        poll = {
            "id": poll_id,
            "guild_id": creation["guild_id"],
            "channel_id": creation["channel_id"],
            "post_channel_id": post_channel_id,
            "creator_id": creation["creator_id"],
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

        print(f"[Polls] Poll {poll_id[:8]} {('modified' if modify_id else 'created')}: "
              f"question='{data['question']}', send_time={data.get('send_time_parsed')}, "
              f"tz={tz}, recurring={recurring}")
        self._register_send_job(poll_id, poll)

        action = "modified" if modify_id else "created"
        await message.channel.send(
            f"Poll {action} and scheduled! ID: `{poll_id[:8]}`\n"
            f"Poll will be posted in <#{post_channel_id}>.\n"
            f"Use `/events list` to see all scheduled polls."
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
