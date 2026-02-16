# Vanvalor

A Discord bot to help schedule D&D games for 5 very busy adults across multiple time zones.

## Features

### Scheduled Polls
Create recurring polls that automatically post, collect votes, and announce results.

- **Automated scheduling** - Polls post at a set time (e.g., every Monday at 9am) and close after a configurable duration
- **Reaction-based voting** - Members react with number emojis to vote for their preferred times
- **Vote thresholds** - Options that don't meet a minimum number of votes are excluded from results
- **Tiebreaker polls** - If the top options tie, a 30-minute tiebreaker poll runs automatically
- **Event creation** - The winning option is automatically created as a Discord server event
- **Cross-timezone support** - All times display in each user's local timezone using Discord's timestamp formatting
- **Channel targeting** - Set up polls in a bot channel but have them post to a campaign channel
- **Multiple polls** - Run as many simultaneous polls as you need

### Reminders
A simple shared reminder list for the group.

## Commands

| Command | Description |
|---------|-------------|
| `/schedule poll` | Create a new scheduled poll (guided 9-step setup) |
| `/schedule cancel` | Cancel poll creation in progress |
| `/events list` | View all scheduled polls with status and IDs |
| `/events delete <id>` | Delete a scheduled poll |
| `/events modify <id>` | Edit an existing poll |
| `/events clone <id>` | Copy a poll as a starting point for a new one |
| `/help` | Show bot usage information |
| `$remind <text>` | Add a reminder |
| `$list` | View all reminders |
| `$delete <number>` | Delete a reminder by number |

## Setup

1. Clone the repository
2. Create a virtual environment and install dependencies:
   ```bash
   python -m venv venv
   venv/Scripts/activate   # Windows
   source venv/bin/activate # macOS/Linux
   pip install -r requirements.txt
   ```
3. Create a `.env` file with your bot token:
   ```
   DISCORD_BOT_TOKEN=your_token_here
   ```
4. Run the bot:
   ```bash
   python vanvalor-bot.py
   ```

## Required Bot Permissions
- Send Messages
- Add Reactions
- Read Message History
- Manage Events
- Mention Everyone

## Required Intents
- Message Content (privileged)
- Guild Reactions
- Guild Scheduled Events
