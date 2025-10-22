import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
PROVIDER_TOKEN = os.getenv("PROVIDER_TOKEN", "")
BASE_URL = os.getenv("BASE_URL", "")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "")
SESSION_PRICE = int(os.getenv("SESSION_PRICE", "5000"))  # minor units
CURRENCY = "EUR"
TZ = os.getenv("TZ", "Europe/Stockholm")

# Google integrations
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")  # paste JSON here (single line)
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "")          # or path to creds json
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")                          # target spreadsheet ID
SHEET_WORKSHEET_NAME = os.getenv("SHEET_WORKSHEET_NAME", "Form Responses")  # tab name
GOOGLE_CALENDAR_ID = os.getenv("GOOGLE_CALENDAR_ID", "")                    # calendar id (email-like)
MEETING_DURATION_MIN = int(os.getenv("MEETING_DURATION_MIN", "60"))
USE_GOOGLE_CALENDAR = os.getenv("USE_GOOGLE_CALENDAR", "1") in ("1","true","True","yes")

GOOGLE_OAUTH_CLIENT_JSON=os.getenv('GOOGLE_OAUTH_CLIENT_JSON','')
GOOGLE_OAUTH_TOKEN_JSON=os.getenv('GOOGLE_OAUTH_TOKEN_JSON','')
OAUTH_TOKEN_FILE=os.getenv('OAUTH_TOKEN_FILE','token.json')
