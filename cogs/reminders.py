import discord
from discord.ext import commands
import json
import os

DATA_PATH = "data/reminder_list.json"


class Reminders(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.reminder_list = []
        self.load_list()

    def save_list(self):
        with open(DATA_PATH, "w") as f:
            json.dump(self.reminder_list, f)
        print("List Saved!")

    def load_list(self):
        try:
            with open(DATA_PATH, "r") as f:
                content = f.read()
                if content:
                    self.reminder_list = json.loads(content)
                    print("List Loaded!")
                else:
                    print("Empty file, starting fresh")
        except FileNotFoundError:
            print("No saved list found, starting fresh")

    def format_list(self):
        if not self.reminder_list:
            return "\nNo reminders yet! You're all caught up."
        output = "\nYour reminders:"
        for i, reminder in enumerate(self.reminder_list, 1):
            output += f"\n{i}. {reminder['reminder']}"
        return output

    @commands.command(name="remind")
    async def remind(self, ctx, *, text: str):
        await ctx.send(f'Reminder set: {text}')
        self.reminder_list.append({"reminder": text})
        self.save_list()

    @commands.command(name="list")
    async def list_reminders(self, ctx):
        await ctx.send('Here are your reminders:')
        await ctx.send(self.format_list())

    @commands.command(name="delete")
    async def delete_reminder(self, ctx, index: int):
        idx = index - 1
        if 0 <= idx < len(self.reminder_list):
            del self.reminder_list[idx]
            self.save_list()
            await ctx.send('Reminder deleted.')
        else:
            await ctx.send('Invalid index. Please provide a valid reminder number to delete.')

    @delete_reminder.error
    async def delete_error(self, ctx, error):
        if isinstance(error, (commands.MissingRequiredArgument, commands.BadArgument)):
            await ctx.send('Invalid index. Please provide a valid reminder number to delete.')


async def setup(bot):
    await bot.add_cog(Reminders(bot))
