import os
import sys
import json
import ssl
import asyncio
import socket
from typing import Optional, List
from datetime import datetime, timedelta, date
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy import text

from dateutil import tz
from dotenv import load_dotenv

# Google Sheets (optional)
import gspread
from google.oauth2.service_account import Credentials as SheetsCreds

# Google Calendar (optional)
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials as CalCreds


# =========================
# ENV & BASIC DIAG
# =========================
load_dotenv()

def mask_token(t: str, keep=8):
    if not t:
        return "EMPTY"
    return t[:keep] + "..." + t[-4:] if len(t) > keep + 4 else t

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DATABASE_URL_ENV = os.getenv("DATABASE_URL", "")
BASE_URL = os.getenv("BASE_URL", "")
ADMIN_IDS = {int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()}
TZ_NAME = os.getenv("TZ", "Europe/Stockholm")
SLOT_MINUTES = int(os.getenv("SLOT_MINUTES", "60"))
PRICE_USD = os.getenv("PRICE_USD", "75")
SKIP_AUTO_WEBHOOK = os.getenv("SKIP_AUTO_WEBHOOK", "1") in ("1", "true", "True")

# Автосоздание слотов (создаём на 30 дней вперёд, но показываем пользователю только 14 дней)
AUTO_SLOTS_DAYS_AHEAD = int(os.getenv("AUTO_SLOTS_DAYS_AHEAD", "30"))
WORK_START_HOUR = int(os.getenv("WORK_START_HOUR", "13"))
WORK_END_HOUR = int(os.getenv("WORK_END_HOUR", "17"))  # последний стартовый час = WORK_END_HOUR-1

# Ограничение отображения дат
SHOW_DAYS_AHEAD = int(os.getenv("SHOW_DAYS_AHEAD", "14"))
SLOTS_DATE_PAGE_SIZE = int(os.getenv("SLOTS_DATE_PAGE_SIZE", "7"))

# Google Sheets
GSPREAD_SA_JSON = os.getenv("GSPREAD_SERVICE_ACCOUNT_JSON", "")
GSPREAD_SHEET_ID = os.getenv("GSPREAD_SHEET_ID", "")

# Google Calendar
GCAL_SA_JSON = os.getenv("GCAL_SERVICE_ACCOUNT_JSON", GSPREAD_SA_JSON or "")
GCAL_CALENDAR_ID = os.getenv("GCAL_CALENDAR_ID", "primary")

print("==== DIAG: startup ====")
print("Python:", sys.version)
try:
    import aiogram
    print("Aiogram:", aiogram.__version__)
except Exception:
    print("Aiogram: unknown")
print("BOT_TOKEN:", mask_token(BOT_TOKEN))
print("BASE_URL:", BASE_URL or "EMPTY")
print("DATABASE_URL set:", bool(DATABASE_URL_ENV))
try:
    u0 = urlparse(DATABASE_URL_ENV or "")
    print("DB scheme(raw):", u0.scheme or "EMPTY")
    print("DB host(raw):", u0.hostname or "EMPTY")
except Exception as e:
    print("DIAG urlparse failed:", e)
print("GSPREAD_SHEET_ID set:", bool(GSPREAD_SHEET_ID))
print("GCAL_CALENDAR_ID:", GCAL_CALENDAR_ID)
print("SKIP_AUTO_WEBHOOK:", SKIP_AUTO_WEBHOOK)
print("========================")

if not BOT_TOKEN or ":" not in BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN отсутствует или неверен (получи у @BotFather).")
if not DATABASE_URL_ENV:
    raise RuntimeError("DATABASE_URL отсутствует (подключи PostgreSQL на Railway).")


# =========================
# DB DEBUG / NORMALIZE
# =========================
def normalize_database_url(raw: str) -> str:
    if not raw:
        return raw
    if raw.startswith("postgres://"):
        raw = "postgresql+asyncpg://" + raw[len("postgres://"):]
    if raw.startswith("postgresql://") and "+asyncpg" not in raw:
        raw = raw.replace("postgresql://", "postgresql+asyncpg://", 1)
    u = urlparse(raw)
    q = dict(parse_qsl(u.query or "", keep_blank_values=True))
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


# =========================
# Aiogram & DB engine/session
# =========================
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# Railway PG proxy: нужен TLS, цепочка может быть self-signed
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
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        print("DB SELF-TEST: OK")
    except Exception as e:
        print("DB SELF-TEST: FAILED ->", repr(e))
        raise


# =========================
# DB schema init (ensure tables)
# =========================
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
    CREATE TABLE IF NOT EXISTS bookings (
      id SERIAL PRIMARY KEY,
      user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      slot_id INTEGER NOT NULL REFERENCES slots(id) ON DELETE CASCADE,
      status  TEXT NOT NULL DEFAULT 'requested',
      paid    BOOLEAN NOT NULL DEFAULT false
    )
    """,
]

async def _db_init_schema():
    try:
        async with engine.begin() as conn:
            for stmt in SCHEMA_STMTS:
                await conn.execute(text(stmt))
        print("DB INIT: OK (schema ensured)")
    except Exception as e:
        print("DB INIT: FAILED ->", repr(e))
        raise


# =========================
# AUTO-SLOTS (weekdays 13:00–17:00 local)
# =========================
def _localize(dt_naive: datetime) -> datetime:
    tzinfo = tz.gettz(TZ_NAME)
    return dt_naive.replace(tzinfo=tzinfo)

def _to_utc(dt_local: datetime) -> datetime:
    return dt_local.astimezone(tz.UTC)

def _is_weekday(d: date) -> bool:
    return d.weekday() < 5  # Mon..Fri

async def ensure_slots_for_range(days_ahead: int):
    if days_ahead <= 0:
        return
    today_local = datetime.now(tz.gettz(TZ_NAME)).date()
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
                    text("""
                        INSERT INTO slots(start_utc, end_utc, is_booked)
                        VALUES (:s, :e, false)
                        ON CONFLICT (start_utc) DO NOTHING
                    """),
                    {"s": start_utc, "e": end_utc},
                )
        await s.commit()
    print(f"AUTO-SLOTS: ensured next {days_ahead} days (weekdays {WORK_START_HOUR}:00–{WORK_END_HOUR}:00, {SLOT_MINUTES} min).")

async def auto_slots_loop():
    while True:
        try:
            await ensure_slots_for_range(AUTO_SLOTS_DAYS_AHEAD)
        except Exception as e:
            print("AUTO-SLOTS loop warn:", e)
        await asyncio.sleep(6 * 3600)


# =========================
# UI texts
# =========================
WELCOME = (
    "👋 Привет! Добро пожаловать. Этот бот поможет быстро записаться на консультацию — просто отвечай на его вопросы.\n\n"
    "⏱ Продолжительность: 45 минут.\n"
    "💡 Советую заранее продумать темы, которые хотел бы обсудить.\n"
    f"💵 Стоимость консультации — ${PRICE_USD}.\n\n"
    "Сначала пройдём короткую анкету, затем выберем время 👇"
)


# =========================
# Google Sheets (lazy init)
# =========================
_sheet = None
def get_sheet():
    global _sheet
    if _sheet is None:
        if not GSPREAD_SA_JSON or not GSPREAD_SHEET_ID:
            raise RuntimeError("Google Sheets не настроен (нет GSPREAD_*).")
        sa_info = json.loads(GSPREAD_SA_JSON)
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.readonly",
        ]
        creds = SheetsCreds.from_service_account_info(sa_info, scopes=scopes)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(GSPREAD_SHEET_ID)
        ws = sh.sheet1
        headers = [
            "timestamp", "tg_id", "tg_username", "name", "phone",
            "ship_type", "position", "experience", "topic",
            "slot_start_local", "slot_end_local", "payment_method", "gcal_event_id"
        ]
        try:
            first = ws.row_values(1)
            if not first:
                ws.append_row(headers)
        except Exception:
            ws.append_row(headers)
        _sheet = ws
    return _sheet


# =========================
# Google Calendar (lazy init)
# =========================
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

def create_calendar_event_sync(start_utc, end_utc, summary, description):
    try:
        service = get_calendar()
        ev = {
            "summary": summary,
            "description": description,
            "start": {"dateTime": to_rfc3339(start_utc), "timeZone": "UTC"},
            "end": {"dateTime": to_rfc3339(end_utc), "timeZone": "UTC"},
        }
        created = service.events().insert(calendarId=GCAL_CALENDAR_ID, body=ev).execute()
        return created.get("id")
    except Exception as e:
        print("WARN: Calendar insert failed:", e)
        return ""


# =========================
# FSM
# =========================
class Form(StatesGroup):
    name = State()
    tg_username = State()
    phone = State()
    ship_type = State()
    position = State()
    experience = State()
    topic = State()
    waiting_slot = State()   # анкета собрана — ждём выбора слота
    payment_method = State()


# =========================
# DB helpers + date/time windows
# =========================
def human_dt(dt_utc: datetime) -> str:
    tzinfo = tz.gettz(TZ_NAME)
    return dt_utc.astimezone(tzinfo).strftime("%d %b %Y, %H:%M")

def _cutoff_utc(days_ahead: int = SHOW_DAYS_AHEAD) -> datetime:
    now_local = datetime.now(tz.gettz(TZ_NAME))
    cutoff_local = now_local + timedelta(days=days_ahead)
    return cutoff_local.astimezone(tz.UTC)

# (оставлено как вспомогательное; основной флоу — по датам)
async def get_free_slots(session: AsyncSession) -> List[dict]:
    q = text("""
        SELECT id, start_utc, end_utc
        FROM slots
        WHERE is_booked = false AND start_utc > NOW()
        ORDER BY start_utc ASC
        LIMIT 12
    """)
    rows = (await session.execute(q)).mappings().all()
    return [dict(r) for r in rows]

# === Список доступных дат и слоты в выбранный день (ограничены ближайшими 14 днями) ===
async def count_available_dates(session: AsyncSession) -> int:
    cutoff = _cutoff_utc()
    q = text(f"""
        SELECT COUNT(*) FROM (
            SELECT (start_utc AT TIME ZONE '{TZ_NAME}')::date AS local_date
            FROM slots
            WHERE is_booked = false
              AND start_utc > NOW()
              AND start_utc < :cutoff
            GROUP BY 1
        ) t
    """)
    return (await session.execute(q, {"cutoff": cutoff})).scalar_one()

async def get_available_dates_page(session: AsyncSession, limit: int, offset: int) -> List[dict]:
    cutoff = _cutoff_utc()
    q = text(f"""
        SELECT
            (start_utc AT TIME ZONE '{TZ_NAME}')::date AS local_date,
            COUNT(*) AS cnt
        FROM slots
        WHERE is_booked = false
          AND start_utc > NOW()
          AND start_utc < :cutoff
        GROUP BY 1
        ORDER BY 1
        LIMIT :limit OFFSET :offset
    """)
    rows = (await session.execute(q, {"cutoff": cutoff, "limit": limit, "offset": offset})).mappings().all()
    return [{"local_date": r["local_date"], "count": r["cnt"]} for r in rows]

def _local_midnight_bounds(date_str: str):
    y, m, d = map(int, date_str.split("-"))
    tzinfo = tz.gettz(TZ_NAME)
    start_local = datetime(y, m, d, 0, 0, 0, tzinfo=tzinfo)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(tz.UTC), end_local.astimezone(tz.UTC)

async def get_free_slots_for_local_date(session: AsyncSession, date_str: str) -> List[dict]:
    start_utc, end_utc = _local_midnight_bounds(date_str)
    cutoff = _cutoff_utc()
    if start_utc >= cutoff:
        return []
    q = text("""
        SELECT id, start_utc, end_utc
        FROM slots
        WHERE is_booked = false
          AND start_utc >= :s
          AND start_utc <  :e
          AND start_utc <  :cutoff
        ORDER BY start_utc ASC
    """)
    rows = (await session.execute(q, {"s": start_utc, "e": end_utc, "cutoff": cutoff})).mappings().all()
    return [dict(r) for r in rows]

async def ensure_user(session: AsyncSession, tg_id: int, username: Optional[str]) -> int:
    row = (await session.execute(text("SELECT id FROM users WHERE tg_id=:tg"), {"tg": tg_id})).scalar()
    if row:
        return row
    uid = (await session.execute(
        text("INSERT INTO users(tg_id, username) VALUES (:tg,:un) RETURNING id"),
        {"tg": tg_id, "un": username}
    )).scalar_one()
    return uid


# =========================
# UI flows: выбор даты → выбор времени
# =========================
async def show_dates(target: Message, page: int = 0):
    async with Session() as s:
        total = await count_available_dates(s)
        if total == 0:
            await target.answer("Свободных дат в ближайшие 14 дней нет. Напишите желаемое время — постараюсь подстроиться.")
            return
        limit = SLOTS_DATE_PAGE_SIZE
        offset = page * limit
        days = await get_available_dates_page(s, limit=limit, offset=offset)

    rows = []
    row = []
    for i, d in enumerate(days, start=1):
        dt_txt = datetime.strptime(str(d["local_date"]), "%Y-%m-%d").strftime("%d %b, %a")
        text_btn = f"📅 {dt_txt} ({d['count']})"
        row.append(InlineKeyboardButton(text=text_btn, callback_data=f"date:{d['local_date']}"))
        if i % 2 == 0:
            rows.append(row); row = []
    if row:
        rows.append(row)

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="« Назад", callback_data=f"dates:{page-1}"))
    if offset + limit < total:
        nav.append(InlineKeyboardButton(text="Вперёд »", callback_data=f"dates:{page+1}"))
    if nav:
        rows.append(nav)

    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    cur_from = offset + 1
    cur_to = min(offset + limit, total)
    await target.answer(f"Выберите дату на ближайшие 14 дней ({cur_from}–{cur_to} из {total}):", reply_markup=kb)

async def show_times_for_date(target: Message, date_str: str):
    today_local = datetime.now(tz.gettz(TZ_NAME)).date()
    max_date = today_local + timedelta(days=SHOW_DAYS_AHEAD)
    picked = datetime.strptime(date_str, "%Y-%m-%d").date()
    if picked >= max_date:
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« К датам", callback_data="dates:0")]])
        await target.answer("Выбранная дата вне ближайших 14 дней. Пожалуйста, выберите другую.", reply_markup=kb)
        return

    async with Session() as s:
        slots = await get_free_slots_for_local_date(s, date_str)

    if not slots:
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« К датам", callback_data="dates:0")]])
        await target.answer("На этот день слотов нет. Выберите другую дату.", reply_markup=kb)
        return

    rows, row = [], []
    for i, sl in enumerate(slots, start=1):
        text_btn = human_dt(sl["start_utc"])
        row.append(InlineKeyboardButton(text=text_btn, callback_data=f"slot:{sl['id']}"))
        if i % 2 == 0:
            rows.append(row); row = []
    if row:
        rows.append(row)

    rows.append([InlineKeyboardButton(text="« К датам", callback_data="dates:0")])

    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await target.answer("Выберите время:", reply_markup=kb)


# =========================
# Handlers
# =========================
@dp.message(CommandStart())
async def on_start(m: Message, state: FSMContext):
    async with Session() as s:
        await ensure_user(s, m.from_user.id, m.from_user.username)
        await s.commit()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Начать анкету", callback_data="form:start")],
    ])
    await m.answer(WELCOME, reply_markup=kb)

@dp.callback_query(F.data == "form:start")
async def start_form(cq: CallbackQuery, state: FSMContext):
    await state.set_state(Form.name)
    await cq.message.answer("Как вас зовут? (только имя)")
    await cq.answer()

@dp.message(Form.name)
async def form_name(m: Message, state: FSMContext):
    await state.update_data(name=m.text.strip())
    await state.set_state(Form.tg_username)
    await m.answer("Ваш ник в Telegram (например, @username)?")

@dp.message(Form.tg_username)
async def form_tg(m: Message, state: FSMContext):
    await state.update_data(tg_username=m.text.strip())
    await state.set_state(Form.phone)
    await m.answer("Номер мобильного (необязательно). Если хотите пропустить — отправьте '-'")

@dp.message(Form.phone)
async def form_phone(m: Message, state: FSMContext):
    phone = m.text.strip()
    await state.update_data(phone=None if phone == '-' else phone)
    await state.set_state(Form.ship_type)
    await m.answer("Тип судна, на котором вы работаете?")

@dp.message(Form.ship_type)
async def form_ship(m: Message, state: FSMContext):
    await state.update_data(ship_type=m.text.strip())
    await state.set_state(Form.position)
    await m.answer("Ваша должность?")

@dp.message(Form.position)
async def form_position(m: Message, state: FSMContext):
    await state.update_data(position=m.text.strip())
    await state.set_state(Form.experience)
    await m.answer("Опыт работы в должности (сколько лет/мес.)?")

@dp.message(Form.experience)
async def form_experience(m: Message, state: FSMContext):
    await state.update_data(experience=m.text.strip())
    await state.set_state(Form.topic)
    await m.answer("Что хотели бы обсудить на консультации?")

@dp.message(Form.topic)
async def form_topic(m: Message, state: FSMContext):
    await state.update_data(topic=m.text.strip())
    # Анкета собрана — сразу открываем выбор дат и блокируем доступ к /book, пока не дойдём до выбора времени
    await state.set_state(Form.waiting_slot)
    await m.answer("Спасибо! Теперь выберите удобную дату 👇")
    await show_dates(m, page=0)

# Старт выбора дат/времени допускаем только если анкета собрана (Form.waiting_slot)
def _form_completed_guard(func):
    async def wrapper(event, state: FSMContext, *args, **kwargs):
        st = await state.get_state()
        if st != Form.waiting_slot:
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📝 Начать анкету", callback_data="form:start")],
            ])
            if isinstance(event, Message):
                await event.answer("Сначала, пожалуйста, пройдите короткую анкету.", reply_markup=kb)
            else:
                await event.message.answer("Сначала, пожалуйста, пройдите короткую анкету.", reply_markup=kb)
                await event.answer()
            return
        return await func(event, state, *args, **kwargs)
    return wrapper

@dp.message(Command("book"))
@_form_completed_guard
async def cmd_book(m: Message, state: FSMContext):
    await show_dates(m, page=0)

@dp.callback_query(F.data == "book")
@_form_completed_guard
async def cb_book(cq: CallbackQuery, state: FSMContext):
    await show_dates(cq.message, page=0)
    await cq.answer()

@dp.callback_query(F.data.startswith("dates:"))
@_form_completed_guard
async def cb_dates_paged(cq: CallbackQuery, state: FSMContext):
    try:
        page = int(cq.data.split(":")[1])
    except Exception:
        page = 0
    await show_dates(cq.message, page=page)
    await cq.answer()

@dp.callback_query(F.data.startswith("date:"))
@_form_completed_guard
async def cb_date_pick(cq: CallbackQuery, state: FSMContext):
    date_str = cq.data.split(":")[1]  # YYYY-MM-DD
    await show_times_for_date(cq.message, date_str)
    await cq.answer()

@dp.callback_query(F.data.startswith("slot:"))
@_form_completed_guard
async def choose_slot(cq: CallbackQuery, state: FSMContext):
    slot_id = int(cq.data.split(":")[1])
    async with Session() as s:
        slot_row = (await s.execute(text("SELECT start_utc, end_utc FROM slots WHERE id=:id"), {"id": slot_id})).first()
        upd = await s.execute(text("UPDATE slots SET is_booked = true WHERE id=:id AND is_booked=false RETURNING id"), {"id": slot_id})
        if not upd.first():
            await cq.answer("Увы, слот уже занят.", show_alert=True); return
        await s.commit()
    if slot_row:
        start_utc, end_utc = slot_row
        await state.update_data(
            slot_start_local=human_dt(start_utc),
            slot_end_local=human_dt(end_utc),
            slot_start_utc=start_utc,
            slot_end_utc=end_utc
        )
    await state.set_state(Form.payment_method)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🇷🇺 Картой из РФ", callback_data="pay:ru")],
        [InlineKeyboardButton(text="🌍 Иностранная карта", callback_data="pay:intl")],
    ])
    await cq.message.answer("Выберите способ оплаты (для учёта в заявке):", reply_markup=kb)
    await cq.answer()

@dp.callback_query(F.data.startswith("pay:"))
async def payment_pick(cq: CallbackQuery, state: FSMContext):
    pm = "Карта РФ" if cq.data.endswith("ru") else "Иностранная карта"
    data = await state.update_data(payment_method=pm)

    # Calendar (sync API in thread)
    gcal_event_id = ""
    try:
        start_utc = data.get("slot_start_utc")
        end_utc = data.get("slot_end_utc")
        if start_utc and end_utc and GCAL_SA_JSON:
            summary = f"Консультация с {data.get('name')} (@{(data.get('tg_username') or '').lstrip('@')})"
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
        print("WARN: Calendar creation failed:", e)

    # Sheets append (optional)
    try:
        if GSPREAD_SA_JSON and GSPREAD_SHEET_ID:
            ws = get_sheet()
            now = datetime.utcnow().isoformat()
            ws.append_row([
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
                gcal_event_id or ""
            ])
    except Exception as e:
        print("WARN: Sheets append failed:", e)

    await state.clear()
    await cq.message.answer("Спасибо! Заявка сохранена. Я свяжусь с вами для подтверждения. 🙌")
    await cq.answer()


# ---- Admin helpers
@dp.message(Command("admin"))
async def admin_menu(m: Message):
    if m.from_user.id not in ADMIN_IDS:
        return
    await m.answer(
        "Админ команды:\n"
        "/addslot YYYY-MM-DD HH:MM — добавить один слот\n"
        "/autofill — сгенерировать слоты на ближайшие дни (AUTO_SLOTS_DAYS_AHEAD)\n"
        "/testsheet — записать тестовую строку в Google Sheet\n"
        "/book — (после анкеты) открыть выбор даты\n"
    )

@dp.message(Command("addslot"))
async def addslot(m: Message):
    if m.from_user.id not in ADMIN_IDS:
        return
    try:
        parts = m.text.split()
        dt_local = datetime.strptime(parts[1] + " " + parts[2], "%Y-%m-%d %H:%M")
        local_tz = tz.gettz(TZ_NAME)
        dt_local = dt_local.replace(tzinfo=local_tz)
        dt_utc = dt_local.astimezone(tz.UTC)
        dt_utc_end = dt_utc + timedelta(minutes=SLOT_MINUTES)
    except Exception:
        await m.answer("Неверный формат. Пример: /addslot 2025-10-25 15:00")
        return
    async with Session() as s:
        await s.execute(
            text("""
                 INSERT INTO slots(start_utc, end_utc, is_booked)
                 VALUES (:s,:e,false)
                 ON CONFLICT (start_utc) DO NOTHING
            """),
            {"s": dt_utc, "e": dt_utc_end}
        )
        await s.commit()
    await m.answer(f"Слот добавлен: {dt_local.strftime('%d %b %Y, %H:%M')} ({SLOT_MINUTES} мин)")

@dp.message(Command("autofill"))
async def cmd_autofill(m: Message):
    if m.from_user.id not in ADMIN_IDS:
        return
    await ensure_slots_for_range(AUTO_SLOTS_DAYS_AHEAD)
    await m.answer(f"Готово! Созданы/проверены слоты на {AUTO_SLOTS_DAYS_AHEAD} дней вперёд (будни {WORK_START_HOUR}:00–{WORK_END_HOUR}:00).")

@dp.message(Command("testsheet"))
async def testsheet(m: Message):
    if m.from_user.id not in ADMIN_IDS:
        return
    try:
        ws = get_sheet()
        ws.append_row(["test", datetime.utcnow().isoformat()])
        await m.answer("✅ Тестовая строка записана в таблицу.")
    except Exception as e:
        await m.answer(f"⚠️ Ошибка Google Sheets: {e}")


# =========================
# Webhook / Server
# =========================
async def on_startup():
    await _db_self_test()
    await _db_init_schema()
    await ensure_slots_for_range(AUTO_SLOTS_DAYS_AHEAD)
    asyncio.create_task(auto_slots_loop())

    if SKIP_AUTO_WEBHOOK:
        print("INFO: SKIP_AUTO_WEBHOOK=1 — пропускаю setWebhook (поставь вручную через Telegram API).")
        return
    if BASE_URL:
        try:
            await bot.set_webhook(url=f"{BASE_URL}/webhook", allowed_updates=["message","callback_query"])
            print("Webhook set to", f"{BASE_URL}/webhook")
        except Exception as e:
            print("WARN: set_webhook failed:", e)

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
