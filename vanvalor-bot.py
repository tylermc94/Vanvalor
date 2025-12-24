import discord
import json
from config import vanvalor_bot_token

#set up global variables

reminder_list = []

#set up functions

def save_list(reminder_list):
    """Save list to JSON File"""
    #create json file
    with open("reminder_list.json", "w") as final:
        json.dump(reminder_list, final)  
    print("List Saved!")

def load_list(reminder_list):
    """Load from JSON File"""
    try:
        with open("reminder_list.json", "r") as final:
            content = final.read()
            if content:  # Only try to load if file isn't empty
                reminder_list.clear()
                loaded_list = json.loads(content)
                for reminder in loaded_list:
                    reminder_list.append(reminder)
                print("List Loaded!")
            else:
                print("Empty file, starting fresh")
    except FileNotFoundError:
        print("No saved list found, starting fresh")

def view_list(reminder_list):
    """Show all current tasks"""
    list_output = ""
    if len(reminder_list) == 0:
        list_output = "\nNo reminders yet! You're all caught up."
    else:
        list_output += "\nYour reminders:"
        for i, reminder in enumerate(reminder_list, 1):
            list_output += f"\n{i}. {reminder["reminder"]}"
    return list_output

#set up discord intents
intents = discord.Intents.default()
intents.message_content = True

#set up discord client
client = discord.Client(intents=intents)

#set up event handlers
@client.event
async def on_ready():
    global reminder_list
    print(f'We have logged in as {client.user}')
    load_list(reminder_list)

#set up message handler
@client.event
async def on_message(message):
    global reminder_list
    if message.author == client.user:
        return

    if message.content.startswith('$remind'):
        message_content = message.content[8:]
        await message.channel.send(f'Reminder set: {message_content}')
        reminder_list.append({"reminder": message_content})
        save_list(reminder_list)

    if message.content.startswith('$list'):
        await message.channel.send('Here are your reminders:')
        list_output = view_list(reminder_list)
        await message.channel.send(list_output)

client.run(vanvalor_bot_token)