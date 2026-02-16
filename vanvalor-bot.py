import discord
from discord.ext import commands
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import os

load_dotenv()
BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")

# Set up discord intents
intents = discord.Intents.default()
intents.message_content = True
intents.reactions = True
intents.guild_scheduled_events = True

# Set up bot with command prefix (kept for legacy reminder commands)
bot = commands.Bot(command_prefix='$', intents=intents)

# Set up scheduler
scheduler = AsyncIOScheduler()
bot.scheduler = scheduler

# Ensure data directory exists
os.makedirs("data", exist_ok=True)


async def load_extensions():
    await bot.load_extension("cogs.reminders")
    await bot.load_extension("cogs.polls")


@bot.tree.command(name="help", description="Show how to use the Vanvalor bot")
async def help_command(interaction: discord.Interaction):
    embed = discord.Embed(
        title="Vanvalor Bot - Help",
        description="A bot to help schedule D&D games for busy adults across multiple time zones.",
        color=discord.Color.purple(),
    )

    embed.add_field(
        name="Scheduled Polls",
        value=(
            "`/schedule poll` - Create a new scheduled poll\n"
            "`/schedule cancel` - Cancel poll creation in progress\n\n"
            "The bot will walk you through 9 steps:\n"
            "1. Poll question\n"
            "2. Response options (comma-separated)\n"
            "3. Who to ping\n"
            "4. Which channel to post in\n"
            "5. When to send the poll\n"
            "6. Repeat schedule (or none)\n"
            "7. How long voting stays open\n"
            "8. Minimum votes per option\n"
            "9. Confirm and schedule"
        ),
        inline=False,
    )

    embed.add_field(
        name="Poll Lifecycle",
        value=(
            "1. Poll posts automatically at the scheduled time with reaction emojis\n"
            "2. Members react to vote for their preferred options\n"
            "3. When voting ends, results are announced with rankings\n"
            "4. Options below the vote threshold are excluded\n"
            "5. If there's a tie, a 30-minute tiebreaker poll runs automatically\n"
            "6. The winning option is created as a Discord server event\n"
            "7. If recurring, the poll re-posts on the next scheduled date"
        ),
        inline=False,
    )

    embed.add_field(
        name="Manage Polls",
        value=(
            "`/events list` - View all scheduled polls\n"
            "`/events delete <id>` - Delete a poll\n"
            "`/events modify <id>` - Edit a poll (type \"keep\" to skip a step)\n"
            "`/events clone <id>` - Copy a poll as a starting point for a new one"
        ),
        inline=False,
    )

    embed.add_field(
        name="Reminders",
        value=(
            "`$remind <text>` - Add a reminder\n"
            "`$list` - View all reminders\n"
            "`$delete <number>` - Delete a reminder by number"
        ),
        inline=False,
    )

    embed.add_field(
        name="Tips",
        value=(
            "- All times are shown in your local timezone automatically\n"
            "- Use natural language for times (e.g., \"Monday at 9am EST\", \"in 2 hours\")\n"
            "- You can run multiple polls at once\n"
            "- Poll IDs are shown as 8-character codes in `/events list`"
        ),
        inline=False,
    )

    await interaction.response.send_message(embed=embed)


@bot.event
async def on_ready():
    print(f'We have logged in as {bot.user}')
    # Sync slash commands to each guild for instant availability
    for guild in bot.guilds:
        bot.tree.copy_global_to(guild=guild)
        await bot.tree.sync(guild=guild)
        print(f"Slash commands synced to {guild.name}.")
    # Start scheduler
    if not scheduler.running:
        scheduler.start()
        print("Scheduler started.")


import asyncio

async def main():
    async with bot:
        await load_extensions()
        await bot.start(BOT_TOKEN)

asyncio.run(main())
