import discord
import json
from config import vanvalor_bot_token
from datetime import datetime, timedelta

#set up global variables

reminder_list = []
active_polls = {}

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

def delete_reminder(reminder_list, index):
    """Delete a reminder by index"""
    if 0 <= index < len(reminder_list):
        del reminder_list[index]
        print("Reminder deleted!")
        return True
    else:
        print("Invalid index, cannot delete reminder.")
        return False

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
    
    userid = message.author.id

    if userid in active_polls:
    # Block other commands while creating poll
        if message.content.startswith('$'):
            await message.channel.send("You're in the middle of creating a poll! Finish that first.")
            return

    if userid in active_polls:
    #resume poll creation
        poll_data = active_polls[userid]
        step = poll_data["step"]

        if step == "waiting_for_event_name":
            poll_data["event_name"] = message.content
            poll_data["step"] = "waiting_for_times"
            await message.channel.send('Please provide the possible times for the event, separated by commas.')

        elif step == "waiting_for_times":
            poll_data["times"] = [time.strip() for time in message.content.split(',')]
            poll_data["step"] = "waiting_for_participants"
            await message.channel.send('Please provide the participants for the event, separated by commas.')

        elif step == "waiting_for_participants":
            poll_data["participants"] = [participant.strip() for participant in message.content.split(',')]
            poll_data["step"] = "waiting_for_endtime"
            await message.channel.send('Please provide the end time for the poll (e.g., in hours).')

        elif step == "waiting_for_endtime":
            poll_data["endtime"] = message.content
            
            # Create and send the poll
            poll = discord.Poll(
                question=poll_data["event_name"],
                duration=timedelta(hours=24)  # Default duration; can be modified based on endtime input
            )
    
            for time in poll_data["times"]:
                poll.add_answer(text=time)
    
            await message.channel.send(poll=poll)
    
            # Clean up
            del active_polls[userid]
            await message.channel.send('Poll created successfully!')

    if message.content.startswith('$remind'):
        message_content = message.content[8:]
        await message.channel.send(f'Reminder set: {message_content}')
        reminder_list.append({"reminder": message_content})
        save_list(reminder_list)

    if message.content.startswith('$list'):
        await message.channel.send('Here are your reminders:')
        list_output = view_list(reminder_list)
        await message.channel.send(list_output)

    if message.content.startswith('$delete'):
        print("Delete command received")
        try:
            index = int(message.content[8:]) - 1
            if delete_reminder(reminder_list, index):
                save_list(reminder_list)
                await message.channel.send('Reminder deleted.')
            else:
                await message.channel.send('Invalid index. Please provide a valid reminder number to delete.')
        except (ValueError, IndexError):
            await message.channel.send('Invalid index. Please provide a valid reminder number to delete.')

    if message.content.startswith('$poll'):
        active_polls[userid] = {
            "step": "waiting_for_event_name",
            "event_name": None,
            "times": None,
            "participants": None,
            "endtime": None,
        }
        await message.channel.send('What is the name of the event?')


client.run(vanvalor_bot_token)