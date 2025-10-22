import asyncio
import re
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, LabeledPrice, PreCheckoutQuery, ContentType
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiohttp import web

# импортируем настройки и модули
from config import (
    BOT_TOKEN, PROVIDER_TOKEN, BASE_URL, ADMIN_CHAT_ID,
    SESSION_PRICE, CURRENCY, MEETING_DURATION_MIN
)
from storage import init_db, upsert_user, save_form, is_slot_taken, reserve_slot, mark_paid, latest_reserved_booking
from booking import fmt
from google_integration import available_slots_from_calendar, append_form_to_sheet

# -------------------------------------------

class Form(StatesGroup):
    lastname = State()
    firstname = State()
    patronymic = State()
    position = State()
    shiptype = State()
    experience = State()
    questions = State()
    email = State()
    telegram = State()

from aiogram.client.default import DefaultBotProperties

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))

dp = Dispatcher()

WELCOME = (
    "Привет! Я бот для записи на консультацию.\n\n"
    "Команды:\n"
    "/anketa — заполнить анкету\n"
    "/book — выбрать слот\n"
    "/help — помощь\n"
)

# -------------------------------------------

@dp.message(Command("start"))
async def cmd_start(m: Message, state: FSMContext):
    await upsert_user(m.from_user.id, m.from_user.username or "", m.from_user.first_name or "", m.from_user.last_name or "")
    await m.answer(WELCOME)

@dp.message(Command("help"))
async def cmd_help(m: Message):
    await m.answer(WELCOME)

# -------------------------------------------
# Анкета

@dp.message(Command("anketa"))
async def start_form(m: Message, state: FSMContext):
    await state.clear()
    await state.set_state(Form.lastname)
    await m.answer("Введите вашу <b>фамилию</b>:")

@dp.message(Form.lastname)
async def form_lastname(m: Message, state: FSMContext):
    await state.update_data(lastname=m.text.strip())
    await state.set_state(Form.firstname)
    await m.answer("Введите ваше <b>имя</b>:")

@dp.message(Form.firstname)
async def form_firstname(m: Message, state: FSMContext):
    await state.update_data(firstname=m.text.strip())
    await state.set_state(Form.patronymic)
    await m.answer("Введите ваше <b>отчество</b> (если нет — поставьте '-'):")

@dp.message(Form.patronymic)
async def form_patronymic(m: Message, state: FSMContext):
    await state.update_data(patronymic=m.text.strip())
    await state.set_state(Form.position)
    await m.answer("Укажите вашу <b>текущую должность</b> на судне:")

@dp.message(Form.position)
async def form_position(m: Message, state: FSMContext):
    await state.update_data(position=m.text.strip())
    await state.set_state(Form.shiptype)
    await m.answer("Укажите <b>тип судна</b> (например: танкер, балкер, контейнеровоз):")

@dp.message(Form.shiptype)
async def form_shiptype(m: Message, state: FSMContext):
    await state.update_data(shiptype=m.text.strip())
    await state.set_state(Form.experience)
    await m.answer("Ваш <b>опыт работы</b> (в годах или кратко):")

@dp.message(Form.experience)
async def form_experience(m: Message, state: FSMContext):
    await state.update_data(experience=m.text.strip())
    await state.set_state(Form.questions)
    await m.answer("Какие <b>вопросы</b> вы хотели бы обсудить на консультации?")

@dp.message(Form.questions)
async def form_questions(m: Message, state: FSMContext):
    await state.update_data(questions=m.text.strip())
    await state.set_state(Form.email)
    await m.answer("Введите ваш <b>e-mail</b>:")

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

@dp.message(Form.email)
async def form_email(m: Message, state: FSMContext):
    email = m.text.strip()
    if not EMAIL_RE.match(email):
        await m.answer("Похоже, это не e-mail. Пример: name@example.com\nПопробуйте ещё раз:")
        return
    await state.update_data(email=email)
    await state.set_state(Form.telegram)
    preset = m.from_user.username
    hint = f" (например, @{preset})" if preset else ""
    await m.answer(f"Введите ваш <b>ник в Telegram</b>{hint}:")

@dp.message(Form.telegram)
async def form_telegram(m: Message, state: FSMContext):
    tg = m.text.strip()
    if not re.match(r"^@[A-Za-z0-9_]{5,32}$", tg):
        preset = m.from_user.username
        example = f"@{preset}" if preset else "@your_username"
        await m.answer(f"Ник должен начинаться с @ и содержать 5–32 символа (латиница, цифры, _). Пример: {example}\nПопробуйте ещё раз:")
        return

    data = await state.get_data()
    lastname = data.get("lastname", "")
    firstname = data.get("firstname", "")
    patronymic = data.get("patronymic", "")
    position = data.get("position", "")
    shiptype = data.get("shiptype", "")
    experience = data.get("experience", "")
    questions = data.get("questions", "")
    email = data.get("email", "")

    full_name = " ".join(x for x in [lastname, firstname, patronymic] if x and x != "-")
    topic = (
        f"Должность: {position}\n"
        f"Тип судна: {shiptype}\n"
        f"Опыт: {experience}\n"
        f"Вопросы: {questions}\n"
        f"Telegram: {tg}"
    )

    await save_form(m.from_user.id, full_name, email, "", topic)

    try:
        append_form_to_sheet({
            "user_id": str(m.from_user.id),
            "username": f"@{m.from_user.username}" if m.from_user.username else "",
            "full_name": full_name,
            "position": position,
            "ship_type": shiptype,
            "experience": experience,
            "questions": questions,
            "email": email,
            "telegram": tg,
        })
    except Exception:
        await m.answer("Анкета сохранена локально, но не удалось отправить в Google Sheets.")

    preview = (
        f"<b>ФИО:</b> {full_name or '—'}\n"
        f"<b>Должность:</b> {position or '—'}\n"
        f"<b>Тип судна:</b> {shiptype or '—'}\n"
        f"<b>Опыт:</b> {experience or '—'}\n"
        f"<b>Вопросы:</b> {questions or '—'}\n"
        f"<b>E-mail:</b> {email or '—'}\n"
        f"<b>Telegram:</b> {tg or '—'}\n\n"
        f"Длительность консультации — {MEETING_DURATION_MIN // 60} час.\n"
        "Теперь можно выбрать слот: /book"
    )

    await m.answer(preview)

# -------------------------------------------
# Слоты и оплата

def slots_keyboard():
    kb = InlineKeyboardBuilder()
    for dt in available_slots_from_calendar(days=7):
        iso = dt.isoformat()
        kb.button(text=fmt(dt), callback_data=f"slot:{iso}")
    kb.adjust(1)
    return kb.as_markup()

@dp.message(Command("book"))
async def cmd_book(m: Message):
    txt = f"Выберите удобное время (сеанс длится {MEETING_DURATION_MIN // 60} час):"
    await m.answer(txt, reply_markup=slots_keyboard())

@dp.callback_query(F.data.startswith("slot:"))
async def pick_slot(cq: CallbackQuery):
    iso = cq.data.split(":", 1)[1]
    if await is_slot_taken(iso):
        await cq.answer("Увы, слот уже занят. Выберите другой.", show_alert=True)
        await cq.message.edit_reply_markup(reply_markup=slots_keyboard())
        return
    booking_id = await reserve_slot(cq.from_user.id, iso)
    prices = [LabeledPrice(label="Сессия консультации", amount=SESSION_PRICE)]
    await cq.message.answer_invoice(
        title="Запись на консультацию",
        description=f"Слот: {iso} (длительность: {MEETING_DURATION_MIN // 60} час)",
        payload=f"booking:{booking_id}",
        provider_token=PROVIDER_TOKEN,
        currency=CURRENCY,
        prices=prices
    )
    await cq.answer()

@dp.pre_checkout_query()
async def pre_checkout(pre: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre.id, ok=True)

@dp.message(F.content_type == ContentType.SUCCESSFUL_PAYMENT)
async def successful_payment(m: Message):
    payload = m.successful_payment.invoice_payload
    if payload.startswith("booking:"):
        booking_id = int(payload.split(":", 1)[1])
        await mark_paid(booking_id)
        await m.answer("Оплата получена! До встречи на консультации.\nЕсли нужно перенести — напишите здесь.")
        if ADMIN_CHAT_ID:
            text = (
                f"Новая оплаченная консультация!\n"
                f"Пользователь: @{m.from_user.username} ({m.from_user.id})\n"
                f"Сумма: {m.successful_payment.total_amount / 100:.2f} {m.successful_payment.currency}"
            )
            try:
                await bot.send_message(int(ADMIN_CHAT_ID), text)
            except Exception:
                pass

# -------------------------------------------
# Запуск

async def on_startup(app: web.Application):
    await init_db()

def build_app():
    app = web.Application()
    app.on_startup.append(on_startup)
    return app

async def run_polling():
    await init_db()
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(run_polling())
