# Telegram Consultation Bot — Sheets + Calendar

## Новое
- Отправка анкет в **Google Sheets**.
- Слоты для записи берутся из **Google Calendar**: бот показывает только свободные окна (по вашим фиксированным времени SLOTS_LOCAL), учитывая занятость календаря.

## Настройка Google
1) В Google Cloud Console создайте **Service Account** и выдайте ему роль доступа к Sheets/Calendar (или просто создайте ключ).
2) Получите JSON ключ и:
   - Либо вставьте целиком в переменную окружения **GOOGLE_SERVICE_ACCOUNT_JSON** (одной строкой),
   - Либо загрузите файл и укажите путь в **GOOGLE_CREDENTIALS_FILE**.
3) **Поделитесь** нужным календарём и таблицей с адресом сервис-аккаунта (вида `name@project.iam.gserviceaccount.com`) с правом:
   - Calendar: `Просмотр сведений обо всех событиях` (Reader) достаточно.
   - Sheets: `Редактор` (Editor), чтобы бот мог добавлять строки.
4) Заполните переменные окружения:
```
GOOGLE_SERVICE_ACCOUNT_JSON=...  # или GOOGLE_CREDENTIALS_FILE=/app/creds.json
GOOGLE_SHEET_ID=...              # ID таблицы (из URL)
SHEET_WORKSHEET_NAME=Form Responses
GOOGLE_CALENDAR_ID=you@example.com  # ID календаря (обычно email)
MEETING_DURATION_MIN=60
USE_GOOGLE_CALENDAR=1
```
5) Фиксированные часы для кандидатов слотов меняются в `booking.py` → `SLOTS_LOCAL`.

## Как это работает
- При завершении `/anketa` данные сохраняются локально в SQLite и отправляются в Sheets (если настроено).
- При `/book` бот строит список слотов на 7 дней вперёд, но **фильтрует** их через FreeBusy API календаря — занятые окна не показываются.

## Запуск
```bash
pip install -r requirements.txt
python app.py
```


## Использовать обычный Google Календарь (OAuth, без сервис-аккаунта)
1. В Google Cloud Console создайте **OAuth Client** типа *Desktop App* и скачайте `client_secret_*.json`.
2. Откройте `.env` и вставьте содержимое этого файла одной строкой в `GOOGLE_OAUTH_CLIENT_JSON=` (или укажите путь для сохранения токена `OAUTH_TOKEN_FILE=token.json`).
3. Поставьте `GOOGLE_CALENDAR_ID=primary` (или email нужного календаря).
4. Запустите `python app.py` — откроется браузер, войдите в Google и разрешите доступ. Токен сохранится в `token.json`.
5. Для деплоя можете скопировать содержимое `token.json` в переменную окружения `GOOGLE_OAUTH_TOKEN_JSON`.

