import os
import json
import asyncio
from datetime import datetime, timedelta
from urllib.parse import urlparse

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy import text

from dateutil import tz
from dotenv import load_dotenv

import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ------------------------------
# 1. LOAD ENV
# ------------------------------
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
BASE_URL = os.getenv("BASE_URL")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_IDS = {int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()}
TZ = os.getenv("TZ", "Europe/Stockholm")
SLOT_MINUTES = int(os.getenv("SLOT_MINUTES", "60"))
PRICE_USD = os.getenv("PRICE_USD", "75")

GSPREAD_JSON = os.getenv("GSPREAD_SERVICE_ACCOUNT_JSON")
GSPREAD_SHEET_ID = os.getenv("GSPREAD_SHEET_ID")

GCAL_JSON = os.getenv("GCAL_SERVICE_ACCOUNT_JSON", GSPREAD_JSON)
GCAL_CALENDAR_ID = os.getenv("GCAL_CALENDAR_ID", "primary")

SKIP_AUTO_WEBHOOK = os.getenv("SKIP_AUTO_WEBHOOK", "0") == "1"

assert BOT_TOKEN and DATABASE_URL and BASE_URL, "❌ Missing required env vars"

# ------------------------------
# 2. DATABASE CONNECTION FIX
# ------------------------------
# Ensure proper asyncpg scheme
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://")

# Add sslmode=require if not present
if "sslmode" not in DATABASE_URL:
    DATABASE_URL += "?sslmode=require"

# --- Normalize DATABASE_URL for SQLAlchemy + asyncpg (жёсткая версия) ---
import re
_raw = os.getenv("DATABASE_URL", "")

# 1) схема под asyncpg
if _raw.startswith("postgres://"):
    _raw = _raw.replace("postgres://", "postgresql+asyncpg://", 1)
elif _raw.startswith("postgresql://") and "+asyncpg" not in _raw:
    _raw = _raw.replace("postgresql://", "postgresql+asyncpg://", 1)

# 2) вырезаем sslmode=... где бы он ни был (и чистим лишние ?/&)
_raw = re.sub(r'([?&])sslmode=[^&]*(&)?', lambda m: (m.group(1) if m.group(2) else ''), _raw)
_raw = _raw.replace('?&', '?').rstrip('?').rstrip('&')

# 3) добавляем ssl=true, если его нет
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
u = urlparse(_raw)
q = dict(parse_qsl(u.query or "", keep_blank_values=True))
if q.get("ssl") not in ("true", "1"):
    q["ssl"] = "true"
DATABASE_URL = urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q), u.fragment))

engine = create_async_engine(DATABASE_URL, echo=False, future=True)
Session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# ------------------------------
# 3. GOOGLE CLIENTS
# ------------------------------
def get_sheets_ws():
    sa_info = json.loads(GSPREAD_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(GSPREAD_SHEET_ID)
    ws = sh.sheet1
    if not ws.row_values(1):
        ws.append_row([
            "timestamp", "tg_id", "tg_username", "name", "phone",
            "ship_type", "position", "experience", "topic",
            "slot_start_local", "slot_end_local", "payment_method"
        ])
    return ws

def get_calendar_service():
    sa_info = json.loads(GCAL_JSON)
    scopes = ["https://www.googleapis.com/auth/calendar"]
    creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
    return build("calendar", "v3", credentials=creds)

# ------------------------------
# 4. BOT
# ------------------------------
bot = Bot(token=BOT_TOKEN, default=ParseMode.HTML)
dp = Dispatcher()

# ------------------------------
# 5. FSM FORM
# ------------------------------
class Form(StatesGroup):
    name = State()
    username = State()
    phone = State()
    ship_type = State()
    position = State()
    experience = State()
    topic = State()
    waiting_slot = State()
    payment_method = State()

# ------------------------------
# 6. HELPERS
# ------------------------------
def human_dt(dt_utc: datetime) -> str:
    tzinfo = tz.gettz(TZ)
    return dt_utc.astimezone(tzinfo).strftime("%d %b %Y, %H:%M")

async def get_free_slots(session: AsyncSession):
    q = text("""
        SELECT id, start_utc, end_utc FROM slots
        WHERE is_booked = false AND start_utc > NOW()
        ORDER BY start_utc LIMIT 10
    """)
    rows = (await session.execute(q)).mappings().all()
    return [dict(r) for r in rows]

async def ensure_user(session: AsyncSession, tg_id: int, username: str):
    row = (await session.execute(text("SELECT id FROM users WHERE tg_id=:tg"), {"tg": tg_id})).scalar()
    if not row:
        await session.execute(text("INSERT INTO users(tg_id, username) VALUES (:tg,:un)"),
                              {"tg": tg_id, "un": username})
        await session.commit()

# ------------------------------
# 7. COMMANDS
# ------------------------------
@dp.message(CommandStart())
async def on_start(m: Message, state: FSMContext):
    async with Session() as s:
        await ensure_user(s, m.from_user.id, m.from_user.username)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Начать анкету", callback_data="form:start")],
        [InlineKeyboardButton(text="🗓 Выбрать слот", callback_data="book")]
    ])

    await m.answer(
        "👋 Привет! Добро пожаловать. Этот бот поможет быстро записаться на консультацию — просто отвечай на его вопросы.\n\n"
        "⏱ Продолжительность: 45 минут.\n"
        "💡 Советую заранее продумать темы, которые хотел бы обсудить.\n"
        f"💵 Стоимость консультации — ${PRICE_USD}.\n\n"
        "До скорой встречи!",
        reply_markup=kb
    )

@dp.callback_query(F.data == "form:start")
async def form_start(cq: CallbackQuery, state: FSMContext):
    await state.set_state(Form.name)
    await cq.message.answer("Как вас зовут?")
    await cq.answer()

@dp.message(Form.name)
async def form_name(m: Message, state: FSMContext):
    await state.update_data(name=m.text.strip())
    await state.set_state(Form.username)
    await m.answer("Ваш ник в Telegram (например, @username)?")

@dp.message(Form.username)
async def form_username(m: Message, state: FSMContext):
    await state.update_data(username=m.text.strip())
    await state.set_state(Form.phone)
    await m.answer("Ваш номер телефона (необязательно, можно '-' )")

@dp.message(Form.phone)
async def form_phone(m: Message, state: FSMContext):
    phone = m.text.strip()
    await state.update_data(phone=None if phone == '-' else phone)
    await state.set_state(Form.ship_type)
    await m.answer("Тип судна, на котором вы работаете?")

@dp.message(Form.ship_type)
async def form_ship_type(m: Message, state: FSMContext):
    await state.update_data(ship_type=m.text.strip())
    await state.set_state(Form.position)
    await m.answer("Ваша должность?")

@dp.message(Form.position)
async def form_position(m: Message, state: FSMContext):
    await state.update_data(position=m.text.strip())
    await state.set_state(Form.experience)
    await m.answer("Опыт работы в должности (в годах/месяцах)?")

@dp.message(Form.experience)
async def form_experience(m: Message, state: FSMContext):
    await state.update_data(experience=m.text.strip())
    await state.set_state(Form.topic)
    await m.answer("Что хотели бы обсудить на консультации?")

@dp.message(Form.topic)
async def form_topic(m: Message, state: FSMContext):
    await state.update_data(topic=m.text.strip())
    async with Session() as s:
        slots = await get_free_slots(s)
    if not slots:
        await m.answer("Пока нет свободных слотов. Напишите своё желаемое время.")
        return
    rows, row = [], []
    for i, sl in enumerate(slots, start=1):
        btn = InlineKeyboardButton(text=human_dt(sl["start_utc"]), callback_data=f"slot:{sl['id']}")
        row.append(btn)
        if i % 2 == 0:
            rows.append(row)
            row = []
    if row: rows.append(row)
    await state.set_state(Form.waiting_slot)
    await m.answer("Выберите удобное время:", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

@dp.callback_query(F.data.startswith("slot:"))
async def on_slot(cq: CallbackQuery, state: FSMContext):
    slot_id = int(cq.data.split(":")[1])
    async with Session() as s:
        slot = (await s.execute(text("SELECT start_utc, end_utc FROM slots WHERE id=:id"), {"id": slot_id})).first()
        await s.execute(text("UPDATE slots SET is_booked=true WHERE id=:id"), {"id": slot_id})
        await s.commit()

    if not slot:
        await cq.message.answer("Этот слот уже занят 😢")
        return

    start_utc, end_utc = slot
    await state.update_data(slot_start_local=human_dt(start_utc), slot_end_local=human_dt(end_utc))
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🇷🇺 Картой из РФ", callback_data="pay:ru")],
        [InlineKeyboardButton(text="🌍 Иностранная карта", callback_data="pay:intl")]
    ])
    await state.set_state(Form.payment_method)
    await cq.message.answer("Выберите способ оплаты:", reply_markup=kb)

@dp.callback_query(F.data.startswith("pay:"))
async def on_pay(cq: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    data["payment_method"] = "Карта РФ" if cq.data.endswith("ru") else "Иностранная карта"

    ws = get_sheets_ws()
    ws.append_row([
        datetime.utcnow().isoformat(),
        cq.from_user.id,
        data.get("username"),
        data.get("name"),
        data.get("phone"),
        data.get("ship_type"),
        data.get("position"),
        data.get("experience"),
        data.get("topic"),
        data.get("slot_start_local"),
        data.get("slot_end_local"),
        data.get("payment_method"),
    ])

    # Create event in Google Calendar
    try:
        start = datetime.strptime(data["slot_start_local"], "%d %b %Y, %H:%M")
        end = datetime.strptime(data["slot_end_local"], "%d %b %Y, %H:%M")
        service = get_calendar_service()
        event = {
            "summary": f"Консультация с {data.get('name')}",
            "description": f"Тема: {data.get('topic')}",
            "start": {"dateTime": start.isoformat(), "timeZone": TZ},
            "end": {"dateTime": end.isoformat(), "timeZone": TZ},
        }
        service.events().insert(calendarId=GCAL_CALENDAR_ID, body=event).execute()
    except Exception as e:
        print("Calendar error:", e)

    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, f"🆕 Новая заявка от {data.get('name')} ({data.get('username')})")
        except:
            pass

    await state.clear()
    await cq.message.answer("Спасибо! Ваша заявка сохранена. Я свяжусь с вами для подтверждения. 🙌")
    await cq.answer()

# ------------------------------
# 8. HEALTHCHECK & STARTUP
# ------------------------------
async def healthcheck(request):
    return web.Response(text="ok")

async def main():
    app = web.Application()
    app.router.add_get("/", healthcheck)
    SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path="/webhook")
    setup_application(app, dp, bot=bot)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
    await site.start()
    print("==== DIAG: startup ====")
    print(f"Python: {os.sys.version}")
    print(f"Aiogram: {dp.__class__.__module__}")
    print(f"BOT_TOKEN: {BOT_TOKEN[:9]}...")
    print(f"BASE_URL: {BASE_URL}")
    print(f"DATABASE_URL set: {bool(DATABASE_URL)}")
    print("========================")
    if not SKIP_AUTO_WEBHOOK:
        try:
            await bot.set_webhook(f"{BASE_URL}/webhook")
            print("Webhook set ✅")
        except Exception as e:
            print("Webhook set failed ❌", e)
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
