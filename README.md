# Telegram Consult Bot — Railway Edition (aiogram v3)

Функции:
- Приветствие (45 мин, $75).
- Анкета: имя → @username → телефон (необ.) → тип судна → должность → опыт → тема.
- Выбор слота из БД.
- В конце способ оплаты: 🇷🇺 карта РФ / 🌍 иностранная карта (фиксируем).
- Запись в Google Sheets + событие в Google Calendar (если настроены).
- Готов для Railway (Dockerfile, миграции, .env.example).

## Запуск на Railway
1) Залей репозиторий в GitHub → Railway → New Project → Deploy from Repo.
2) В Variables внеси значения из `.env.example`.
3) Networking → Public Networking → Target Port = `8080`.
4) Deploy. Открой домен `https://<твой>.up.railway.app/` — должно вернуть `ok`.
5) Выполни миграции в БД (один раз) через `psql`, вставив SQL из `migrations/001_init.sql`.
6) Вебхук (в браузере):
   - Сброс: `https://api.telegram.org/bot<TOKEN>/deleteWebhook?drop_pending_updates=true`
   - Установка: `https://api.telegram.org/bot<TOKEN>/setWebhook?url=https://<домен>.up.railway.app/webhook`
   - Проверка: `https://api.telegram.org/bot<TOKEN>/getWebhookInfo`
7) В Telegram:
   - Добавь слоты: `/addslot 2025-10-25 15:00`
   - Старт: `/start`

> Примечание по БД: если `DATABASE_URL` содержит `sslmode=require` — это нормально.
> Код автоматически удалит `sslmode` и включит SSL для asyncpg через `connect_args={"ssl": True}`.
