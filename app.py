import os
import sys
import json
import ssl
import asyncio
import socket
import time
from functools import wraps
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timedelta, date
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

from aiohttp import web

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiogram.exceptions import TelegramBadRequest

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy import text

from dateutil import tz
from dotenv import load_dotenv

# Google Sheets
import gspread
from google.oauth2.service_account import Credentials as SheetsCreds

# Google Calendar (optional)
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials as CalCreds

# ============================================================
# ENV
# ============================================================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DATABASE_URL_ENV = os.getenv("DATABASE_URL", "")
BASE_URL = os.getenv("BASE_URL", "")  # e.g. https://xxxx.up.railway.app
ADMIN_IDS = {int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()}

TZ_NAME = os.getenv("TZ", "Europe/Stockholm")
SLOT_MINUTES = int(os.getenv("SLOT_MINUTES", "60"))
PRICE_USD = os.getenv("PRICE_USD", "75")

AUTO_SLOTS_DAYS_AHEAD = int(os.getenv("AUTO_SLOTS_DAYS_AHEAD", "30"))
SHOW_DAYS_AHEAD = int(os.getenv("SHOW_DAYS_AHEAD", "7"))
SLOTS_DATE_PAGE_SIZE = int(os.getenv("SLOTS_DATE_PAGE_SIZE", "7"))

WORK_START_HOUR = int(os.getenv("WORK_START_HOUR", "13"))
WORK_END_HOUR = int(os.getenv("WORK_END_HOUR", "17"))

# IMPORTANT: for Railway webhook mode, default should be 0 (False)
SKIP_AUTO_WEBHOOK = os.getenv("SKIP_AUTO_WEBHOOK", "0") in ("1", "true", "True")

# Google Sheets
GSPREAD_SA_JSON = os.getenv("GSPREAD_SERVICE_ACCOUNT_JSON", "")
GSPREAD_SHEET_ID = os.getenv("GSPREAD_SHEET_ID", "")

# Google Calendar (optional)
GCAL_SA_JSON = os.getenv("GCAL_SERVICE_ACCOUNT_JSON", "")
GCAL_CALENDAR_ID = os.getenv("GCAL_CALENDAR_ID", "")  # recommend: set explicit calendar id, not "primary"

# Cache TTL
DATES_CACHE_TTL_SEC = int(os.getenv("DATES_CACHE_TTL_SEC", "60"))
TIMES_CACHE_TTL_SEC = int(os.getenv("TIMES_CACHE_TTL_SEC", "30"))


def mask_token(t: str, keep: int = 8) -> str:
    if not t:
        return "EMPTY"
    return t[:keep] + "..." + t[-4:] if len(t) > keep + 4 else t


print("==== DIAG: startup ====")
print("Python:", sys.version)
try:
    import aiogram  # noqa

    print("Aiogram:", aiogram.__version__)
except Exception:
    print("Aiogram: unknown")
print("BOT_TOKEN:", mask_token(BOT_TOKEN))
print("BASE_URL:", BASE_URL or "EMPTY")
print("DATABASE_URL set:", bool(DATABASE_URL_ENV))
print("GSPREAD_SHEET_ID set:", bool(GSPREAD_SHEET_ID))
print("GCAL enabled:", bool(GCAL_SA_JSON))
print("GCAL_CALENDAR_ID:", GCAL_CALENDAR_ID or "EMPTY")
print("SKIP_AUTO_WEBHOOK:", SKIP_AUTO_WEBHOOK)
print("TZ:", TZ_NAME)
print("AUTO_SLOTS_DAYS_AHEAD:", AUTO_SLOTS_DAYS_AHEAD, "SHOW_DAYS_AHEAD:", SHOW_DAYS_AHEAD)
print("========================")

if not BOT_TOKEN or ":" not in BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN отсутствует или неверен (получи у @BotFather).")
if not DATABASE_URL_ENV:
    raise RuntimeError("DATABASE_URL отсутствует (подключи PostgreSQL на Railway).")


# ============================================================
# DB URL normalize + DNS debug
# ============================================================
def normalize_database_url(raw: str) -> str:
    if not raw:
        return raw
    if raw.startswith("postgres://"):
        raw = "postgresql+asyncpg://" + raw[len("postgres://") :]
    if raw.startswith("postgresql://") and "+asyncpg" not in raw:
        raw = raw.replace("postgresql://", "postgresql+asyncpg://", 1)

    u = urlparse(raw)
    q = dict(parse_qsl(u.query or "", keep_blank_values=True))
    # remove ssl query params; we set ssl via connect_args
    for k in list(q.keys()):
        if k.lower().startswith("ssl"):
            q.pop(k, None)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q), u.fragment))


def debug_db_dns(url: str):
    p = urlparse(url)
    host, port = p.hostname, p.port
    print(f"[DB DEBUG] URL={url}")
    print(f"[DB DEBUG] HOST={host} PORT={port}")
    try:
        ip = socket.gethostbyname(host)
        print(f"[DB DEBUG] DNS OK -> {host} -> {ip}")
    except Exception as e:
        print(f"[DB DEBUG] DNS FAIL for {host}: {e}")


DATABASE_URL = normalize_database_url(DATABASE_URL_ENV)
debug_db_dns(DATABASE_URL)


# ============================================================
# Aiogram & DB engine/session
# ============================================================
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# Railway PG proxy often needs TLS and may be self-signed
SSL_CTX = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    future=True,
    pool_pre_ping=True,
    connect_args={"ssl": SSL_CTX},
)
Session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def _db_self_test():
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
    print("DB SELF-TEST: OK")


SCHEMA_STMTS = [
    """
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      tg_id BIGINT UNIQUE NOT NULL,
      username TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS slots (
      id SERIAL PRIMARY KEY,
      start_utc TIMESTAMPTZ NOT NULL,
      end_utc   TIMESTAMPTZ NOT NULL,
      is_booked BOOLEAN NOT NULL DEFAULT false
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_slots_start_utc_unique
    ON slots(start_utc)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_slots_is_booked_start
    ON slots(is_booked, start_utc)
    """,
]


async def _db_init_schema():
    async with engine.begin() as conn:
        for stmt in SCHEMA_STMTS:
            await conn.execute(text(stmt))
    print("DB INIT: OK (schema ensured)")


# ============================================================
# Time utils
# ============================================================
WELCOME = (
    "👋 Привет! Добро пожаловать. Этот бот поможет быстро записаться на консультацию — просто отвечай на его вопросы.\n\n"
    f"⏱ Продолжительность: {SLOT_MINUTES} минут.\n"
    f"💵 Стоимость консультации — ${PRICE_USD}.\n\n"
    "Сначала пройдём короткую анкету, затем выберем время 👇"
)


def _tzinfo():
    return tz.gettz(TZ_NAME)


def human_dt(dt_utc: datetime) -> str:
    return dt_utc.astimezone(_tzinfo()).strftime("%d %b %Y, %H:%M")


def _cutoff_utc(days_ahead: int = SHOW_DAYS_AHEAD) -> datetime:
    now_local = datetime.now(_tzinfo())
    return (now_local + timedelta(days=days_ahead)).astimezone(tz.UTC)


async def notify_admins(text_msg: str):
    if not ADMIN_IDS:
        return
    for aid in ADMIN_IDS:
        try:
            await bot.send_message(aid, f"⚠️ {text_msg}")
        except Exception:
            pass


async def safe_edit(msg: Message, text_msg: str, kb: Optional[InlineKeyboardMarkup]):
    try:
        await msg.edit_text(text_msg)
        await msg.edit_reply_markup(reply_markup=kb)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            return
        raise


# ============================================================
# Slots generator
# ============================================================
def _localize(dt_naive: datetime) -> datetime:
    return dt_naive.replace(tzinfo=_tzinfo())


def _to_utc(dt_local: datetime) -> datetime:
    return dt_local.astimezone(tz.UTC)


def _is_weekday(d: date) -> bool:
    return d.weekday() < 5


async def ensure_slots_for_range(days_ahead: int):
    if days_ahead <= 0:
        return
    today_local = datetime.now(_tzinfo()).date()
    last_date = today_local + timedelta(days=days_ahead)

    async with Session() as s:
        for d in (today_local + timedelta(days=i) for i in range((last_date - today_local).days + 1)):
            if not _is_weekday(d):
                continue
            for hour in range(WORK_START_HOUR, WORK_END_HOUR):
                start_local = _localize(datetime(d.year, d.month, d.day, hour, 0, 0))
                end_local = start_local + timedelta(minutes=SLOT_MINUTES)
                start_utc = _to_utc(start_local)
                end_utc = _to_utc(end_local)
                await s.execute(
                    text(
                        """
                        INSERT INTO slots(start_utc, end_utc, is_booked)
                        VALUES (:s, :e, false)
                        ON CONFLICT (start_utc) DO NOTHING
                        """
                    ),
                    {"s": start_utc, "e": end_utc},
                )
        await s.commit()
    print(f"AUTO-SLOTS: ensured next {days_ahead} days (weekdays {WORK_START_HOUR}:00–{WORK_END_HOUR}:00, {SLOT_MINUTES} min).")


async def auto_slots_loop():
    while True:
        try:
            await ensure_slots_for_range(AUTO_SLOTS_DAYS_AHEAD)
        except Exception as e:
            print("AUTO-SLOTS loop warn:", repr(e))
        await asyncio.sleep(6 * 3600)


# ============================================================
# Google Sheets (lazy init, SYNC only)
# ============================================================
_sheet = None


def get_sheet():
    global _sheet
    if _sheet is None:
        if not GSPREAD_SA_JSON or not GSPREAD_SHEET_ID:
            raise RuntimeError("Google Sheets не настроен (нет GSPREAD_*).")
        sa_info = json.loads(GSPREAD_SA_JSON)
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = SheetsCreds.from_service_account_info(sa_info, scopes=scopes)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(GSPREAD_SHEET_ID)
        ws = sh.sheet1

        headers = [
            "timestamp",
            "tg_id",
            "tg_username",
            "name",
            "phone",
            "ship_type",
            "position",
            "experience",
            "topic",
            "slot_start_local",
            "slot_end_local",
            "payment_method",
            "gcal_event_id",
        ]
        try:
            first = ws.row_values(1)
            if not first:
                ws.append_row(headers)
        except Exception:
            ws.append_row(headers)

        _sheet = ws
    return _sheet


def append_row_sync(row: list):
    ws = get_sheet()
    try:
        ws.append_row(row)
        return
    except Exception:
        time.sleep(2)
        ws.append_row(row)


# ============================================================
# Google Calendar (optional, SYNC only)
# ============================================================
_gcal = None


def get_calendar():
    global _gcal
    if _gcal is None:
        if not GCAL_SA_JSON:
            raise RuntimeError("Google Calendar не настроен (нет GCAL_SERVICE_ACCOUNT_JSON).")
        sa_info = json.loads(GCAL_SA_JSON)
        scopes = ["https://www.googleapis.com/auth/calendar"]
        creds = CalCreds.from_service_account_info(sa_info, scopes=scopes)
        _gcal = build("calendar", "v3", credentials=creds, cache_discovery=False)
    return _gcal


def to_rfc3339(dt_utc: datetime) -> str:
    return dt_utc.replace(tzinfo=tz.UTC).isoformat().replace("+00:00", "Z")


def create_calendar_event_sync(start_utc: datetime, end_utc: datetime, summary: str, description: str) -> str:
    service = get_calendar()
    ev = {
        "summary": summary,
        "description": description,
        "start": {"dateTime": to_rfc3339(start_utc), "timeZone": "UTC"},
        "end": {"dateTime": to_rfc3339(end_utc), "timeZone": "UTC"},
    }
    created = service.events().insert(calendarId=GCAL_CALENDAR_ID, body=ev).execute()
    return created.get("id", "") or ""


# ============================================================
# FSM
# ============================================================
class Form(StatesGroup):
    name = State()
    tg_username = State()
    phone = State()
    ship_type = State()
    position = State()
    experience = State()
    topic = State()
    waiting_slot = State()
    payment_method = State()


# ============================================================
# Caching
# ============================================================
_dates_cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
_times_cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}


def _cache_key_dates() -> str:
    return f"{TZ_NAME}:{SHOW_DAYS_AHEAD}"


def _dates_cache_get() -> Optional[List[Dict[str, Any]]]:
    key = _cache_key_dates()
    item = _dates_cache.get(key)
    if not item:
        return None
    ts, data = item
    if (datetime.utcnow().timestamp() - ts) > DATES_CACHE_TTL_SEC:
        _dates_cache.pop(key, None)
        return None
    return data


def _dates_cache_set(data: List[Dict[str, Any]]):
    _dates_cache[_cache_key_dates()] = (datetime.utcnow().timestamp(), data)


def _times_cache_get(date_str: str) -> Optional[List[Dict[str, Any]]]:
    item = _times_cache.get(date_str)
    if not item:
        return None
    ts, data = item
    if (datetime.utcnow().timestamp() - ts) > TIMES_CACHE_TTL_SEC:
        _times_cache.pop(date_str, None)
        return None
    return data


def _times_cache_set(date_str: str, data: List[Dict[str, Any]]):
    _times_cache[date_str] = (datetime.utcnow().timestamp(), data)


# ============================================================
# Fast queries
# ============================================================
async def fetch_available_dates_counts(session: AsyncSession) -> List[Dict[str, Any]]:
    cached = _dates_cache_get()
    if cached is not None:
        return cached

    cutoff = _cutoff_utc()
    q = text(
        f"""
        SELECT
            (start_utc AT TIME ZONE '{TZ_NAME}')::date AS local_date,
            COUNT(*) AS cnt
        FROM slots
        WHERE is_booked = false
          AND start_utc > NOW()
          AND start_utc < :cutoff
        GROUP BY 1
        ORDER BY 1
        """
    )
    rows = (await session.execute(q, {"cutoff": cutoff})).mappings().all()
    data = [{"local_date": r["local_date"], "count": int(r["cnt"])} for r in rows]
    _dates_cache_set(data)
    return data


async def get_free_slots_for_local_date(session: AsyncSession, date_str: str) -> List[dict]:
    cached = _times_cache_get(date_str)
    if cached is not None:
        return cached

    y, m, d = map(int, date_str.split("-"))
    tzinfo_ = _tzinfo()
    start_local = datetime(y, m, d, 0, 0, 0, tzinfo=tzinfo_)
    end_local = start_local + timedelta(days=1)
    start_utc = start_local.astimezone(tz.UTC)
    end_utc = end_local.astimezone(tz.UTC)

    q = text(
        """
        SELECT id, start_utc, end_utc
        FROM slots
        WHERE is_booked = false
          AND start_utc >= :s
          AND start_utc <  :e
          AND start_utc <  :cutoff
        ORDER BY start_utc ASC
        """
    )
    rows = (await session.execute(q, {"s": start_utc, "e": end_utc, "cutoff": _cutoff_utc()})).mappings().all()
    data = [dict(r) for r in rows]
    _times_cache_set(date_str, data)
    return data


# ============================================================
# UI builders
# ============================================================
def build_dates_kb(all_days: List[Dict[str, Any]], page: int) -> Tuple[str, InlineKeyboardMarkup]:
    total = len(all_days)
    if total == 0:
        return ("Свободных дат в ближайшие дни нет. Напишите желаемое время — постараюсь подстроиться.", InlineKeyboardMarkup(inline_keyboard=[]))

    limit = SLOTS_DATE_PAGE_SIZE
    start = page * limit
    end = min(start + limit, total)
    days = all_days[start:end]

    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for i, dct in enumerate(days, start=1):
        dt_txt = datetime.strptime(str(dct["local_date"]), "%Y-%m-%d").strftime("%d %b, %a")
        row.append(InlineKeyboardButton(text=f"📅 {dt_txt} ({dct['count']})", callback_data=f"date:{dct['local_date']}"))
        if i % 2 == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    nav: List[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="« Назад", callback_data=f"dates:{page-1}"))
    if end < total:
        nav.append(InlineKeyboardButton(text="Вперёд »", callback_data=f"dates:{page+1}"))
    if nav:
        rows.append(nav)

    return (f"Выберите дату (показаны ближайшие {SHOW_DAYS_AHEAD} дней): {start+1}–{end} из {total}", InlineKeyboardMarkup(inline_keyboard=rows))


def build_times_kb(slots: List[Dict[str, Any]], date_str: str) -> Tuple[str, InlineKeyboardMarkup]:
    if not slots:
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« К датам", callback_data="dates:0")]])
        return ("На этот день слотов нет. Выберите другую дату.", kb)

    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for i, sl in enumerate(slots, start=1):
        row.append(InlineKeyboardButton(text=human_dt(sl["start_utc"]), callback_data=f"slot:{sl['id']}"))
        if i % 2 == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    rows.append([InlineKeyboardButton(text="↻ Обновить", callback_data=f"refresh:{date_str}"), InlineKeyboardButton(text="« К датам", callback_data="dates:0")])
    return ("Выберите время:", InlineKeyboardMarkup(inline_keyboard=rows))


# ============================================================
# Guard
# ============================================================
def _form_completed_guard(func):
    @wraps(func)
    async def wrapper(event: Any, state: FSMContext, *args, **kwargs):
        st = await state.get_state()
        if st not in (Form.waiting_slot, Form.payment_method):
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📝 Начать анкету", callback_data="form:start")]])
            if isinstance(event, Message):
                await event.answer("Сначала, пожалуйста, пройдите короткую анкету.", reply_markup=kb)
            else:
                try:
                    await event.message.answer("Сначала, пожалуйста, пройдите короткую анкету.", reply_markup=kb)
                except Exception:
                    pass
                try:
                    await event.answer()
                except Exception:
                    pass
            return
        return await func(event, state)

    return wrapper


# ============================================================
# Handlers
# ============================================================
@dp.message(CommandStart())
async def on_start(m: Message, state: FSMContext):
    async with Session() as s:
        await s.execute(
            text("INSERT INTO users(tg_id, username) VALUES (:tg,:un) ON CONFLICT (tg_id) DO NOTHING"),
            {"tg": m.from_user.id, "un": m.from_user.username},
        )
        await s.commit()
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📝 Начать анкету", callback_data="form:start")]])
    await m.answer(WELCOME, reply_markup=kb)


@dp.callback_query(F.data == "form:start")
async def start_form(cq: CallbackQuery, state: FSMContext):
    await state.set_state(Form.name)
    await cq.message.answer("Как вас зовут? (только имя)")
    await cq.answer()


@dp.message(Form.name)
async def form_name(m: Message, state: FSMContext):
    await state.update_data(name=(m.text or "").strip())
    await state.set_state(Form.tg_username)
    await m.answer("Ваш ник в Telegram (например, @username)? Если не знаете — отправьте '-'.")


@dp.message(Form.tg_username)
async def form_tg(m: Message, state: FSMContext):
    txt = (m.text or "").strip()
    if txt == "-" or not txt:
        un = m.from_user.username or ""
        txt = ("@" + un) if un else "-"
    else:
        if txt != "-" and not txt.startswith("@"):
            txt = "@" + txt
    await state.update_data(tg_username=txt)
    await state.set_state(Form.phone)
    await m.answer("Номер мобильного (необязательно). Если хотите пропустить — отправьте '-'.")


@dp.message(Form.phone)
async def form_phone(m: Message, state: FSMContext):
    phone = (m.text or "").strip()
    await state.update_data(phone=None if phone == "-" else phone)
    await state.set_state(Form.ship_type)
    await m.answer("Тип судна, на котором вы работаете?")


@dp.message(Form.ship_type)
async def form_ship(m: Message, state: FSMContext):
    await state.update_data(ship_type=(m.text or "").strip())
    await state.set_state(Form.position)
    await m.answer("Ваша должность?")


@dp.message(Form.position)
async def form_position(m: Message, state: FSMContext):
    await state.update_data(position=(m.text or "").strip())
    await state.set_state(Form.experience)
    await m.answer("Опыт работы в должности (сколько лет/мес.)?")


@dp.message(Form.experience)
async def form_experience(m: Message, state: FSMContext):
    await state.update_data(experience=(m.text or "").strip())
    await state.set_state(Form.topic)
    await m.answer("Что хотели бы обсудить на консультации?")


@dp.message(Form.topic)
async def form_topic(m: Message, state: FSMContext):
    await state.update_data(topic=(m.text or "").strip())
    await state.set_state(Form.waiting_slot)

    async with Session() as s:
        all_days = await fetch_available_dates_counts(s)
    text_msg, kb = build_dates_kb(all_days, page=0)
    await m.answer("Спасибо! Теперь выберите удобную дату 👇")
    await m.answer(text_msg, reply_markup=kb)


@dp.message(Command("book"))
@_form_completed_guard
async def cmd_book(m: Message, state: FSMContext):
    async with Session() as s:
        all_days = await fetch_available_dates_counts(s)
    text_msg, kb = build_dates_kb(all_days, page=0)
    await m.answer(text_msg, reply_markup=kb)


@dp.callback_query(F.data.startswith("dates:"))
@_form_completed_guard
async def cb_dates_paged(cq: CallbackQuery, state: FSMContext):
    try:
        page = int(cq.data.split(":")[1])
    except Exception:
        page = 0
    async with Session() as s:
        all_days = await fetch_available_dates_counts(s)
    text_msg, kb = build_dates_kb(all_days, page=page)
    await safe_edit(cq.message, text_msg, kb)
    await cq.answer()


@dp.callback_query(F.data.startswith("date:"))
@_form_completed_guard
async def cb_date_pick(cq: CallbackQuery, state: FSMContext):
    date_str = cq.data.split(":", 1)[1]
    async with Session() as s:
        slots = await get_free_slots_for_local_date(s, date_str)
    text_msg, kb = build_times_kb(slots, date_str)
    await safe_edit(cq.message, text_msg, kb)
    await cq.answer()


@dp.callback_query(F.data.startswith("refresh:"))
@_form_completed_guard
async def cb_refresh_times(cq: CallbackQuery, state: FSMContext):
    date_str = cq.data.split(":", 1)[1]
    async with Session() as s:
        _times_cache.pop(date_str, None)
        slots = await get_free_slots_for_local_date(s, date_str)
    text_msg, kb = build_times_kb(slots, date_str)
    await safe_edit(cq.message, text_msg, kb)
    await cq.answer("Обновлено")


@dp.callback_query(F.data.startswith("slot:"))
@_form_completed_guard
async def choose_slot(cq: CallbackQuery, state: FSMContext):
    slot_id = int(cq.data.split(":", 1)[1])

    async with Session() as s:
        upd = await s.execute(
            text(
                """
                UPDATE slots
                SET is_booked = true
                WHERE id=:id AND is_booked=false
                RETURNING start_utc, end_utc
                """
            ),
            {"id": slot_id},
        )
        row = upd.first()
        if not row:
            await cq.answer("Увы, слот уже занят.", show_alert=True)
            return
        start_utc, end_utc = row
        await s.commit()

    await state.update_data(
        slot_start_local=human_dt(start_utc),
        slot_end_local=human_dt(end_utc),
        slot_start_utc=start_utc,
        slot_end_utc=end_utc,
    )

    # invalidate caches
    _dates_cache.clear()
    try:
        day_key = start_utc.astimezone(_tzinfo()).strftime("%Y-%m-%d")
        _times_cache.pop(day_key, None)
    except Exception:
        pass

    await state.set_state(Form.payment_method)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🇷🇺 Картой из РФ", callback_data="pay:ru")],
            [InlineKeyboardButton(text="🌍 Иностранная карта", callback_data="pay:intl")],
        ]
    )
    await safe_edit(cq.message, "Выберите способ оплаты (для учёта в заявке):", kb)
    await cq.answer()


@dp.callback_query(F.data.startswith("pay:"))
async def payment_pick(cq: CallbackQuery, state: FSMContext):
    pm = "Карта РФ" if cq.data.endswith("ru") else "Иностранная карта"
    data = await state.update_data(payment_method=pm)

    start_utc = data.get("slot_start_utc")
    end_utc = data.get("slot_end_utc")

    # Calendar (optional) in executor
    gcal_event_id = ""
    if GCAL_SA_JSON and GCAL_CALENDAR_ID and start_utc and end_utc:
        try:
            tg_u = (data.get("tg_username") or "").lstrip("@")
            summary = f"Консультация с {data.get('name')} (@{tg_u})"
            description = (
                f"Тема: {data.get('topic')}\n"
                f"Тип судна: {data.get('ship_type')}\n"
                f"Должность: {data.get('position')}\n"
                f"Опыт: {data.get('experience')}\n"
                f"Контакт: {data.get('phone') or '-'}\n"
                f"Способ оплаты: {data.get('payment_method')}"
            )
            loop = asyncio.get_event_loop()
            gcal_event_id = await loop.run_in_executor(
                None, lambda: create_calendar_event_sync(start_utc, end_utc, summary, description)
            )
        except Exception as e:
            print("WARN: Calendar insert failed:", repr(e))
            await notify_admins(f"Calendar insert failed: {repr(e)}")

    # Sheets in executor ✅ MAIN FIX
    try:
        if not (GSPREAD_SA_JSON and GSPREAD_SHEET_ID):
            print("INFO: Sheets not configured; skipping append.")
        else:
            now = datetime.utcnow().isoformat()
            row = [
                now,
                str(cq.from_user.id),
                data.get("tg_username") or ("@" + (cq.from_user.username or "")),
                data.get("name"),
                data.get("phone") or "",
                data.get("ship_type"),
                data.get("position"),
                data.get("experience"),
                data.get("topic"),
                data.get("slot_start_local"),
                data.get("slot_end_local"),
                data.get("payment_method"),
                gcal_event_id or "",
            ]
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, lambda: append_row_sync(row))
            print("SHEETS: append OK")
    except Exception as e:
        print("WARN: Sheets append failed:", repr(e))
        await notify_admins(f"Sheets append failed: {repr(e)}")

    await state.clear()
    await safe_edit(cq.message, "Спасибо! Заявка сохранена. Я свяжусь с вами для подтверждения. 🙌", None)
    await cq.answer()


# ============================================================
# Admin
# ============================================================
@dp.message(Command("admin"))
async def admin_menu(m: Message):
    if m.from_user.id not in ADMIN_IDS:
        return
    await m.answer(
        "Админ команды:\n"
        "/addslot YYYY-MM-DD HH:MM — добавить один слот\n"
        "/autofill — сгенерировать слоты на ближайшие дни (AUTO_SLOTS_DAYS_AHEAD)\n"
        "/testsheet — записать тестовую строку в Google Sheet\n"
    )


@dp.message(Command("autofill"))
async def cmd_autofill(m: Message):
    if m.from_user.id not in ADMIN_IDS:
        return
    await ensure_slots_for_range(AUTO_SLOTS_DAYS_AHEAD)
    await m.answer(f"Готово! Слоты проверены на {AUTO_SLOTS_DAYS_AHEAD} дней вперёд.")


@dp.message(Command("testsheet"))
async def testsheet(m: Message):
    if m.from_user.id not in ADMIN_IDS:
        return
    try:
        if not (GSPREAD_SA_JSON and GSPREAD_SHEET_ID):
            await m.answer("⚠️ Sheets не настроен (нет GSPREAD_* env).")
            return
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: append_row_sync(["test", datetime.utcnow().isoformat()]))
        await m.answer("✅ Тестовая строка записана в таблицу.")
    except Exception as e:
        await m.answer(f"⚠️ Ошибка Google Sheets: {repr(e)}")


# ============================================================
# Webhook / Server (Railway)
# ============================================================
async def on_startup():
    await _db_self_test()
    await _db_init_schema()
    await ensure_slots_for_range(AUTO_SLOTS_DAYS_AHEAD)
    asyncio.create_task(auto_slots_loop())

    if SKIP_AUTO_WEBHOOK:
        print("INFO: SKIP_AUTO_WEBHOOK=1 — пропускаю setWebhook (поставь вручную через Telegram API).")
        return

    if not BASE_URL:
        print("WARN: BASE_URL пустой — не могу поставить webhook.")
        await notify_admins("BASE_URL пустой — бот не сможет поставить webhook автоматически.")
        return

    try:
        await bot.set_webhook(url=f"{BASE_URL}/webhook", allowed_updates=["message", "callback_query"])
        print("Webhook set to", f"{BASE_URL}/webhook")
    except Exception as e:
        print("WARN: set_webhook failed:", repr(e))
        await notify_admins(f"set_webhook failed: {repr(e)}")


async def on_shutdown():
    try:
        await bot.delete_webhook()
    except Exception:
        pass


async def main():
    app = web.Application()

    SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path="/webhook")
    setup_application(app, dp, bot=bot)

    async def health_handler(request):
        return web.Response(text="ok")

    app.router.add_get("/", health_handler)

    await on_startup()

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
    await site.start()
    print("Webhook server started")

    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())
