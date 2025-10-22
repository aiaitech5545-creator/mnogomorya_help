# Telegram Consult Bot — Railway + Google Sheets + Google Calendar (aiogram v3)

Бот для записи на консультации:
- 👋 Приветствие (профессиональное — вариант 2): длительность **45 мин**, стоимость **$75**.
- 📝 Анкета в порядке: имя → @username → телефон (опц.) → тип судна → должность → опыт → тема.
- 🗓 Выбор слота из **PostgreSQL**.
- 💳 В конце — выбор способа оплаты: **🇷🇺 Картой из РФ / 🌍 Иностранная карта** (только фиксируем).
- 📊 Всё сохраняется в **Google Sheets** (Service Account).
- 📆 Автосоздание события в **Google Calendar** после выбора способа оплаты.
- 🚄 Готов для деплоя на **Railway** (Dockerfile, requirements, миграции, `.env.example`).

## Переменные окружения
Скопируйте из `.env.example` в Railway → Variables и заполните:

```
BOT_TOKEN=
ADMIN_IDS=211779388

TZ=Europe/Stockholm
BASE_URL=https://<your-app>.up.railway.app
PORT=8080
SLOT_MINUTES=60
PRICE_USD=75

DATABASE_URL=postgresql+asyncpg://user:pass@host:5432/dbname

# Google Sheets
GSPREAD_SERVICE_ACCOUNT_JSON={"type":"service_account", ...}
GSPREAD_SHEET_ID=<spreadsheet_id>

# Google Calendar
GCAL_SERVICE_ACCOUNT_JSON={"type":"service_account", ...}   # можно тот же JSON, что и для Sheets
GCAL_CALENDAR_ID=primary_or_calendar_id                     # id календаря, с которым поделились
```

> Важно: поделитесь вашим календарем с `client_email` сервис-аккаунта (Редактор). Для личного календаря можно указать `GCAL_CALENDAR_ID=primary`, если это календарь аккаунта, к которому относится сервис-аккаунт.

## Команды
- `/start` — приветствие и меню
- `/admin` — справка
- `/addslot YYYY-MM-DD HH:MM` — добавить слот (длительность берется из `SLOT_MINUTES`)

## Деплой на Railway — шаги
1. Загрузите репозиторий на GitHub.
2. На Railway: **New Project → Deploy from Repo**.
3. В **Networking** → **Generate Service Domain** с `Target port: 8080` → скопируйте домен и вставьте в `BASE_URL`.
4. В **Variables** добавьте переменные из `.env.example` (со значениями).
5. Добавьте **PostgreSQL** (New → Database / Provision PostgreSQL).
6. Выполните SQL из `migrations/001_init.sql` в консоли БД.
7. Нажмите **Redeploy**.
8. В Telegram откройте бота → `/start` → пройдите анкету → выберите слот → укажите метод оплаты — заявка попадет в Google Sheets и создастся событие в Google Calendar.

## Лицензия
MIT
