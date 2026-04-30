"""One-time helper: create a Telethon user-account session.

Run this ONCE on the server (or locally and upload the .session file).
You will be prompted for your phone number and the SMS code Telegram sends.
"""
import os

from dotenv import load_dotenv
from telethon.sync import TelegramClient

load_dotenv()

API_ID = int(os.environ["TELEGRAM_API_ID"])
API_HASH = os.environ["TELEGRAM_API_HASH"]
SESSION_NAME = os.getenv("SESSION_NAME", "session")

print(f"Creating Telethon session: {SESSION_NAME}.session")
print("You'll be asked for your phone number and SMS code.")
print()

with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
    me = client.get_me()
    is_bot = getattr(me, "bot", False)
    print()
    print(f"Logged in as: {me.first_name} (@{me.username}) id={me.id}")
    print(f"Is bot account: {is_bot}")
    if is_bot:
        print()
        print("ERROR: This is a BOT account. The cargo monitor needs a USER account.")
        print("Delete the session file and run again, entering a phone number this time.")
    else:
        print()
        print("SUCCESS — you can now start the bot service.")
