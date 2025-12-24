import discord
from config import vanvalor_bot_token

reminder_list = ""

#set up discord intents
intents = discord.Intents.default()
intents.message_content = True

#set up discord client
client = discord.Client(intents=intents)

#set up event handlers
@client.event
async def on_ready():
    print(f'We have logged in as {client.user}')

#set up message handler
@client.event
async def on_message(message):
    global reminder_list
    if message.author == client.user:
        return

    if message.content.startswith('$remind'):
        message_content = message.content[8:]
        await message.channel.send(f'Reminder: {message_content}')
        reminder_list += f"{message_content}\n"

    if message.content.startswith('$list'):
        await message.channel.send('Here are your reminders:')
        await message.channel.send(reminder_list)

client.run(vanvalor_bot_token)