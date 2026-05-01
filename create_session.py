"""One-time helper: create a Telethon user-account session.

Run this ONCE on the server (or locally and upload the .session file).
You will be prompted for your phone number and the SMS code Telegram sends.

Tip: Telegram normally delivers the login code via the Telegram app.
If you OPEN the message in Telegram, the code is INVALIDATED for security.
Read the code from the lock-screen / notification, OR press
"Did not receive code?" in the app and request SMS delivery.
"""
import os
import sys

from dotenv import load_dotenv
from telethon.sync import TelegramClient
from telethon.errors import (
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    SessionPasswordNeededError,
)

load_dotenv()

API_ID = int(os.environ["TELEGRAM_API_ID"])
API_HASH = os.environ["TELEGRAM_API_HASH"]
SESSION_NAME = os.getenv("SESSION_NAME", "session")

print(f"Creating Telethon session: {SESSION_NAME}.session")
print(f"Using API_ID={API_ID}")
print()
print("HINT: read the code from a Telegram notification (lock screen).")
print("DO NOT open the Telegram chat that contains the code — it will be invalidated.")
print("If the code is not arriving, press 'Did not receive code?' in Telegram and")
print("choose 'Send via SMS'.")
print()

phone = input("Enter your phone number (e.g. +998901234567): ").strip()
if not phone.startswith("+"):
    print("ERROR: phone must start with + and country code")
    sys.exit(1)

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
client.connect()

if client.is_user_authorized():
    me = client.get_me()
    print()
    print(f"Already authorized as: {me.first_name} (@{me.username}) id={me.id}")
    print(f"Is bot account: {getattr(me, 'bot', False)}")
    client.disconnect()
    sys.exit(0)

# First request: Telegram delivers via the Telegram app (SentCodeTypeApp).
# WARNING: opening the Telegram chat that contains the code invalidates it.
sent = client.send_code_request(phone)
print(f"Code sent via: {sent.type.__class__.__name__}")
print()
print("Choose delivery method:")
print("  [1] Use code from Telegram app (do NOT open the chat)")
print("  [2] Resend code as SMS (recommended)")
choice = input("Choice [1/2]: ").strip()
if choice == "2":
    sent = client.send_code_request(phone, force_sms=True)
    print(f"Code resent via: {sent.type.__class__.__name__}")
print()

# Allow up to 5 attempts before giving up
signed_in = False
for attempt in range(1, 6):
    code = input(f"[{attempt}/5] Enter code: ").strip().replace(" ", "")
    if not code:
        print("  -> empty input, try again")
        continue
    try:
        client.sign_in(phone=phone, code=code)
        signed_in = True
        break
    except PhoneCodeInvalidError:
        print("  -> Invalid code (wrong digits). Try again.")
    except PhoneCodeExpiredError:
        print("  -> Code expired (already invalidated, e.g. by opening the chat).")
        resend = input("     Resend? [s=SMS / a=app / n=abort]: ").strip().lower()
        if resend == "n":
            break
        try:
            sent = client.send_code_request(phone, force_sms=(resend == "s"))
            print(f"     New code sent via: {sent.type.__class__.__name__}")
        except Exception as exc:
            print(f"     Resend failed: {exc}")
            break
    except SessionPasswordNeededError:
        password = input("2FA password: ")
        client.sign_in(password=password)
        signed_in = True
        break

if not signed_in:
    print("Too many failed attempts. Aborting.")
    client.disconnect()
    sys.exit(1)

me = client.get_me()
is_bot = getattr(me, "bot", False)
print()
print(f"Logged in as: {me.first_name} (@{me.username}) id={me.id}")
print(f"Is bot account: {is_bot}")

if is_bot:
    print()
    print("ERROR: This is a BOT account. The cargo monitor needs a USER account.")
    print("Delete session.session and run again with a phone number.")
    sys.exit(2)

print()
print("SUCCESS — session.session is ready. Start the bot service:")
print("  sudo systemctl start cargo-bot")
client.disconnect()
