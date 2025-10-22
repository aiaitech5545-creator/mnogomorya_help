from typing import List, Dict, Tuple
import json, os, pytz
from datetime import datetime, timedelta
from config import (GOOGLE_SERVICE_ACCOUNT_JSON, GOOGLE_CREDENTIALS_FILE, GOOGLE_SHEET_ID, SHEET_WORKSHEET_NAME, GOOGLE_CALENDAR_ID, MEETING_DURATION_MIN, TZ, USE_GOOGLE_CALENDAR, GOOGLE_OAUTH_CLIENT_JSON, GOOGLE_OAUTH_TOKEN_JSON, OAUTH_TOKEN_FILE)
from booking import SLOTS_LOCAL
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials as UserCredentials
from google_auth_oauthlib.flow import InstalledAppFlow
import gspread
from googleapiclient.discovery import build

SCOPES=[
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/calendar.readonly",
    "https://www.googleapis.com/auth/userinfo.email",
    "openid",
]

_creds=None
_gs_client=None
_cal=None

def _load_user_oauth_creds():
    creds=None
    if GOOGLE_OAUTH_TOKEN_JSON:
        try:
            creds=UserCredentials.from_authorized_user_info(json.loads(GOOGLE_OAUTH_TOKEN_JSON), SCOPES)
            if creds and creds.valid:
                return creds
        except Exception:
            pass
    if OAUTH_TOKEN_FILE and os.path.exists(OAUTH_TOKEN_FILE):
        try:
            creds=UserCredentials.from_authorized_user_file(OAUTH_TOKEN_FILE, SCOPES)
            if creds and creds.valid:
                return creds
        except Exception:
            pass
    if GOOGLE_OAUTH_CLIENT_JSON:
        client_cfg=json.loads(GOOGLE_OAUTH_CLIENT_JSON)
        flow=InstalledAppFlow.from_client_config(client_cfg, SCOPES)
        creds=flow.run_local_server(open_browser=True, port=8080, prompt="consent")
        if OAUTH_TOKEN_FILE:
            with open(OAUTH_TOKEN_FILE,"w",encoding="utf-8") as f:
                f.write(creds.to_json())
        return creds
    return None

def _load_service_account_creds():
    if GOOGLE_SERVICE_ACCOUNT_JSON:
        info=json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    if GOOGLE_CREDENTIALS_FILE and os.path.exists(GOOGLE_CREDENTIALS_FILE):
        return service_account.Credentials.from_service_account_file(GOOGLE_CREDENTIALS_FILE, scopes=SCOPES)
    return None

def _load_creds():
    global _creds
    if _creds: return _creds
    creds=_load_user_oauth_creds()
    if not creds:
        creds=_load_service_account_creds()
    if not creds:
        raise RuntimeError("Google auth not configured. Provide OAuth client (GOOGLE_OAUTH_CLIENT_JSON) or service account.")
    _creds=creds
    return _creds

def _gs():
    global _gs_client
    if _gs_client: return _gs_client
    creds=_load_creds()
    _gs_client=gspread.Client(auth=creds)
    _gs_client.session=gspread.authorize(creds).session
    return _gs_client

def _calendar():
    global _cal
    if _cal: return _cal
    creds=_load_creds()
    _cal=build("calendar","v3",credentials=creds, cache_discovery=False)
    return _cal

def append_form_to_sheet(row:Dict[str,str])->None:
    if not GOOGLE_SHEET_ID: return
    client=_gs()
    sh=client.open_by_key(GOOGLE_SHEET_ID)
    try:
        ws=sh.worksheet(SHEET_WORKSHEET_NAME)
    except gspread.WorksheetNotFound:
        ws=sh.add_worksheet(title=SHEET_WORKSHEET_NAME, rows=1000, cols=20)
        ws.append_row(["Timestamp","User ID","Username","Full Name","Position","Ship Type","Experience","Questions","Email","Telegram"])
    values=[
        datetime.utcnow().isoformat(timespec="seconds")+"Z",
        row.get("user_id",""),
        row.get("username",""),
        row.get("full_name",""),
        row.get("position",""),
        row.get("ship_type",""),
        row.get("experience",""),
        row.get("questions",""),
        row.get("email",""),
        row.get("telegram",""),
    ]
    ws.append_row(values)

def _overlap(a_start,a_end,b_start,b_end)->bool:
    return not (a_end<=b_start or b_end<=a_start)

def available_slots_from_calendar(days:int=7):
    if not (USE_GOOGLE_CALENDAR and GOOGLE_CALENDAR_ID):
        return []
    tz=pytz.timezone(TZ)
    now=datetime.now(tz)
    # Build candidate starts
    candidates=[]
    start_date=now.date()
    for d in range(days):
        day=start_date+timedelta(days=d)
        for t in SLOTS_LOCAL:
            dt=tz.localize(datetime.combine(day,t))
            if dt>now: candidates.append(dt)
    if not candidates: return []
    time_min=candidates[0].astimezone(pytz.UTC).isoformat()
    time_max=(candidates[-1]+timedelta(minutes=MEETING_DURATION_MIN)).astimezone(pytz.UTC).isoformat()
    cal=_calendar()
    fb=cal.freebusy().query(body={"timeMin":time_min,"timeMax":time_max,"timeZone":TZ,"items":[{"id":GOOGLE_CALENDAR_ID}]}).execute()
    busy=fb.get("calendars",{}).get(GOOGLE_CALENDAR_ID,{}).get("busy",[])
    busy_intervals=[]
    for b in busy:
        bs=datetime.fromisoformat(b["start"].replace("Z","+00:00")).astimezone(tz)
        be=datetime.fromisoformat(b["end"].replace("Z","+00:00")).astimezone(tz)
        busy_intervals.append((bs,be))
    ok=[]
    dur=timedelta(minutes=MEETING_DURATION_MIN)
    for start in candidates:
        end=start+dur
        if not any(_overlap(start,end,b0,b1) for (b0,b1) in busy_intervals):
            ok.append(start)
    return ok
