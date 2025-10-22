import os
import sys
import json
import asyncio
from typing import Optional, List
from datetime import datetime, timedelta
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

# Google Sheets
import gspread
from google.oauth2.service_account import Credentials as GCreds

# Google Calendar
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials as CCreds


# =========================
# Environment & DIAG
# =========================
load_dotenv()

def mask_token(t: str, keep=8):
    if not t:
        return "EMPTY"
    return t[:keep] + "..." + t[-4:] if len(t) > keep + 4 else t

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
BASE_URL = os.getenv("BASE_URL", "")
ADMIN_IDS = {int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()}
TZ_NAME = os.getenv("TZ", "Europe/Stockholm")
SLOT_MINUTES = int(os.getenv("SLOT_MINUTES", "60"))
PRICE_USD = os.getenv("PRICE_USD", "75")
SKIP_AUTO_WEBHOOK = os.getenv("SKIP_AUTO_WEBHOOK", "0") == "1"

# Google Sheets
GSPREAD_SA_JSON = os.getenv("GSPREAD_SERVICE_ACCOUNT_JSON", "")
GSPREAD_SHEET_ID = os.getenv("GSPREAD_SHEET_ID", "")

# Google Calendar
GCAL_SA_JSON = os.getenv("GCAL_SERVICE_ACCOUNT_JSON", GSPREAD_SA_JSON or "")
GCAL_CALENDAR_ID = os.getenv("GCAL_CALENDAR_ID", "primary")

# --- Normalize DATABASE_URL for SQLAlchemy + asyncpg + SSL ---
_raw = DATABASE_URL or ""
if _raw.startswith("postgres://"):
    _raw = _raw.replace("postgres://", "postgresql+asyncpg://", 1)
elif _raw.startswith("postgresql://") and "+asyncpg" not in _raw:
    _raw = _raw.replace("postgresql://", "postgresql+asyncpg://", 1)

# purge old bad params if any
for bad in ("ssl=true", "sslmode=true"):
    if bad in _raw:
        # remove &bad or ?bad
        _raw = _raw.replace("&" + bad, "")
        _raw = _raw.replace("?" + bad, "?")

# ensure sslmode=require
u = urlparse(_raw)
q = dict(parse_qsl(u.query or "", keep_blank_values=True))
valid_modes = {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}
if "sslmode" not in q or q.get("sslmode") not in valid_modes:
    q["sslmode"] = "require"
DATABASE_URL = urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q), u.fragment))

# --- DIAG ---
print("==== DIAG: startup ====")
print("Python:", sys.version)
try:
    import aiogram
    print("Aiogram:", aiogram.__version__)
except Exception:
    print("Aiogram: unknown")
print("BOT_TOKEN:", mask_token(BOT_TOKEN))
print("BASE_URL:", BASE_URL or "EMPTY")
print("DATABASE_URL set:", bool(DATABASE_URL))
try:
    u = urlparse(DATABASE_URL or "")
    q = dict(parse_qsl(u.query or "", keep_blank_values=True))
    ssl_flag = (q.get("ssl") in ("true", "1")) or (q.get("sslmode") in {"require", "verify-ca", "verify-full"})
    print("DB scheme:", u.scheme or "EMPTY")
    print("DB host:", u.hostname or "EMPTY")
    print("DB ssl?", ssl_flag, "| sslmode:", q.get("sslmode"))
except Exception as e:
    print("DIAG urlparse failed:", e)
print("GSPREAD_SHEET_ID set:", bool(GSPREAD_SHEET_ID))
print("GCAL_CALENDAR_ID:", GCAL_CALENDAR_ID)
print("========================")

if not BOT_TOKEN or ":" not in BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN отсутствует или неверен (вставь из @BotFather).")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL отсутствует (подключи PostgreSQL на Railway).")
# BASE_URL может быть пуст — тогда просто не ставим вебхук автоматически


# =========================
# Aiogram & DB setup
# =========================
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

engine = create_async_engine(DATABASE_URL, echo=False, future=True)
Session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


# =========================
# Texts
# =========================
WELCOME = (
    "👋 Привет! Добро пожаловать. Этот бот поможет быстро записаться на консультацию — просто отвечай на его вопросы.\n\n"
    "⏱ Продолжительность: 45 минут.\n"
    "💡 Советую заранее продумать темы, которые хотел бы обсудить.\n"
    f"💵 Стоимость консультации — ${PRICE_USD}.\n\n"
    "До скорой встречи!"
)


# =========================
# Google Sheets helpers
# =========================
_sheet = None
def get_sheet():
    global _sheet
    if _sheet is None:
        if not GSPREAD_SA_JSON or not GSPREAD_SHEET_ID:
            print("WARN: Google Sheets не настроен (нет GSPREAD_*).")
            raise RuntimeError("Sheets not configured")
        sa_info = json.loads(GSPREAD_SA_JSON)
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.readonly",
        ]
        creds = GCreds.from_service_account_info(sa_info, scopes=scopes)
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
# Google Calendar helpers
# =========================
_gcal = None
def get_calendar():
    global _gcal
    if _gcal is None:
        if not GCAL_SA_JSON:
            print("WARN: Google Calendar не настроен (нет GCAL_SERVICE_ACCOUNT_JSON).")
            raise RuntimeError("Calendar not configured")
        sa_info = json.loads(GCAL_SA_JSON)
        scopes = ["https://www.googleapis.com/auth/calendar"]
        creds = CCreds.from_service_account_info(sa_info, scopes=scopes)
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
# FSM (form)
# =========================
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


# =========================
# DB utils
# =========================
def human_dt(dt_utc: datetime) -> str:
    tzinfo = tz.gettz(TZ_NAME)
    return dt_utc.astimezone(tzinfo).strftime("%d %b %Y, %H:%M")

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

async def reserve_slot(session: AsyncSession, slot_id: int, user_id: int) -> Optional[int]:
    q1 = text("UPDATE slots SET is_booked = true WHERE id = :id AND is_booked = false RETURNING id")
    res = await session.execute(q1, {"id": slot_id})
    row = res.first()
    if not row:
        return None
    q2 = text("""
        INSERT INTO bookings(user_id, slot_id, status, paid)
        VALUES (:uid, :sid, 'requested', false)
        RETURNING id
    """)
    bid = (await session.execute(q2, {"uid": user_id, "sid": slot_id})).scalar_one()
    return bid

async def ensure_user(session: AsyncSession, tg_id: int, username: Optional[str]) -> int:
    q = text("SELECT id FROM users WHERE tg_id = :tg")
    row = (await session.execute(q, {"tg": tg_id})).scalar()
    if row:
        return row
    ins = text("INSERT INTO users(tg_id, username) VALUES (:tg,:un) RETURNING id")
    uid = (await session.execute(ins, {"tg": tg_id, "un": username})).scalar_one()
    return uid


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
        [InlineKeyboardButton(text="🗓 Выбрать слот", callback_data="book")],
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
    await state.set_state(Form.waiting_slot)
    await m.answer("Спасибо! Теперь выберите удобное время: /book")

@dp.message(Command("book"))
async def cmd_book(m: Message, state: FSMContext):
    await show_slots(m)

@dp.callback_query(F.data == "book")
async def cb_book(cq: CallbackQuery, state: FSMContext):
    await show_slots(cq.message)
    await cq.answer()

async def show_slots(target: Message):
    async with Session() as s:
        slots = await get_free_slots(s)
    if not slots:
        await target.answer("Свободных слотов пока нет. Напишите желаемое время — постараюсь подстроиться.")
        return
    rows, row = [], []
    for i, sl in enumerate(slots, start=1):
        text_btn = human_dt(sl["start_utc"]) + f" ({SLOT_MINUTES} мин)"
        row.append(InlineKeyboardButton(text=text_btn, callback_data=f"slot:{sl['id']}"))
        if i % 2 == 0:
            rows.append(row); row = []
    if row: rows.append(row)
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await target.answer("Выберите удобное время:", reply_markup=kb)

@dp.callback_query(F.data.startswith("slot:"))
async def choose_slot(cq: CallbackQuery, state: FSMContext):
    slot_id = int(cq.data.split(":")[1])
    async with Session() as s:
        uid = await ensure_user(s, cq.from_user.id, cq.from_user.username)
        slot_row = (await s.execute(text("SELECT start_utc, end_utc FROM slots WHERE id=:id"), {"id": slot_id})).first()
        booking_id = await reserve_slot(s, slot_id, uid)
        if not booking_id:
            await cq.answer("Увы, слот уже занят.", show_alert=True)
            return
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

    # Создание события в Google Calendar
    gcal_event_id = ""
    try:
        start_utc = data.get("slot_start_utc")
        end_utc = data.get("slot_end_utc")
        if start_utc and end_utc:
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
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(admin_id, f"Ошибка Google Calendar: {e}")
            except Exception:
                pass

    # Запись в Google Sheets
    try:
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
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(admin_id, f"Не удалось записать в Google Sheets: {e}")
            except Exception:
                pass

    # Уведомление в админку (если задано)
    summary_msg = (
        "🆕 Заявка на консультацию\n"
        f"Имя: <b>{data.get('name')}</b>\n"
        f"TG: {data.get('tg_username')}\n"
        f"Тел: {data.get('phone') or '-'}\n"
        f"Судно: {data.get('ship_type')}\n"
        f"Должность: {data.get('position')}\n"
        f"Опыт: {data.get('experience')}\n"
        f"Тема: {data.get('topic')}\n"
        f"Слот: {data.get('slot_start_local')} — {data.get('slot_end_local')}\n"
        f"Оплата: {data.get('payment_method')}\n"
        f"GCAL: {'создано ✅' if gcal_event_id else '—'}\n"
    )
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, summary_msg)
        except Exception:
            pass

    await state.clear()
    await cq.message.answer("Спасибо! Заявка сохранена. Я свяжусь с вами для подтверждения. 🙌")
    await cq.answer()

# Admin
@dp.message(Command("admin"))
async def admin_menu(m: Message):
    if m.from_user.id not in ADMIN_IDS:
        return
    await m.answer(
        "Админ: /addslot YYYY-MM-DD HH:MM — добавить слот (длительность берётся из SLOT_MINUTES).\n"
        "Пример: /addslot 2025-10-25 15:00"
    )

from dateutil import tz as _tz

@dp.message(Command("addslot"))
async def addslot(m: Message):
    if m.from_user.id not in ADMIN_IDS:
        return
    try:
        parts = m.text.split()
        dt_local = datetime.strptime(parts[1] + " " + parts[2], "%Y-%m-%d %H:%M")
        local_tz = _tz.gettz(TZ_NAME)
        dt_local = dt_local.replace(tzinfo=local_tz)
        dt_utc = dt_local.astimezone(_tz.UTC)
        dt_utc_end = dt_utc + timedelta(minutes=SLOT_MINUTES)
    except Exception:
        await m.answer("Неверный формат. Пример: /addslot 2025-10-25 15:00")
        return
    async with Session() as s:
        await s.execute(text("INSERT INTO slots(start_utc, end_utc, is_booked) VALUES (:s,:e,false)"),
                        {"s": dt_utc, "e": dt_utc_end})
        await s.commit()
    await m.answer(f"Слот добавлен: {dt_local.strftime('%d %b %Y, %H:%M')} ({SLOT_MINUTES} мин)")


# =========================
# Webhook server
# =========================
async def on_startup():
    if SKIP_AUTO_WEBHOOK:
        print("INFO: SKIP_AUTO_WEBHOOK=1 — пропускаю setWebhook (вебхук уже можно ставить вручную).")
        return
    if BASE_URL:
        try:
            await bot.set_webhook(url=f"{BASE_URL}/webhook", allowed_updates=["message", "callback_query"])
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
