from datetime import datetime
import pytz
from typing import List
from config import TZ

# Define preferred local start times for sessions (used as candidates)
from datetime import time
SLOTS_LOCAL = [time(10,0), time(14,0), time(18,0)]

def fmt(dt:datetime) -> str:
    return dt.strftime("%a, %d %b %Y %H:%M (%Z)")
