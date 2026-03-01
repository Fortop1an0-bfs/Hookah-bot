# 🌿 HookahFlow Bot

Telegram-бот для сохранения рецептов кальянных миксов и проверки наличия табаков в магазине [SmokyArt](https://smokyart.ru) (Владивосток, Металлургическая д1).

## ✨ Возможности

- **Парсинг миксов из сообщений** — пересылай любой пост с рецептом, бот сам распознает бренды, вкусы и проценты
- **Проверка наличия** — автоматически проверяет каждый табак на сайте магазина по всем граммовкам
- **Распознавание фото** — пришли фото с банками табака, бот прочитает надписи через AI (Groq Vision)
- **Нормализация названий** — бот знает что "ваниль" у MustHave = "Vanilla Cream", "груша" = "Mad Pear" и т.д. (через Groq LLM)
- **Хранение миксов** — каждый микс получает уникальный код, можно переименовывать
- **Inline-навигация** — список миксов с кнопками, состав со ссылками на товары, обновление наличия одной кнопкой

## 📋 Команды бота

| Команда | Описание |
|---------|----------|
| `/mixes` | Список последних миксов с кнопками |
| `/check` | Статус последнего микса |
| `/update` | Обновить наличие всех табаков |
| `/tobaccos` | Все табаки в базе со ссылками |
| `/sync` | Полная синхронизация с сайтом |
| `/sync MustHave` | Синхронизация конкретного бренда |
| `/help` | Справка |

## 🏗️ Стек

- **Python 3.11**
- **aiogram 3.x** — Telegram Bot API
- **SQLAlchemy 2.0 + asyncpg** — async ORM
- **PostgreSQL 15** (Docker)
- **Playwright** — парсинг сайта smokyart.ru
- **Groq API** — нормализация названий (llama-3.3-70b) + распознавание фото (llama-4-scout)

## 🗄️ Структура базы данных

```
tobaccos       — все известные табаки с наличием и граммовками (JSON)
mixes          — сохранённые миксы с кодом, названием, составом
mix_tobaccos   — состав каждого микса (табак + процент)
```

## 🚀 Установка и запуск

### 1. Клонирование

```bash
git clone https://github.com/eduard03072000-png/Hookah-bot.git
cd Hookah-bot
```

### 2. Виртуальное окружение

```bash
python -m venv venv
venv\Scripts\activate       # Windows
# source venv/bin/activate  # Linux/Mac
pip install -r requirements.txt
playwright install chromium
```

### 3. PostgreSQL через Docker

```bash
docker-compose up -d
```

### 4. Переменные окружения

Создай файл `.env` в корне проекта:

```env
BOT_TOKEN=ваш_токен_от_BotFather
GROQ_API_KEY=gsk_ваш_ключ_от_groq.com

DB_HOST=localhost
DB_PORT=5432
DB_NAME=hookah_db
DB_USER=hookah
DB_PASSWORD=hookah123
```

**Получение ключей:**
- Telegram Bot Token → [@BotFather](https://t.me/BotFather)
- Groq API Key → [console.groq.com](https://console.groq.com)

### 5. Миграции базы данных

```bash
python migrate_v2.py
python migrate_v3.py
```

### 6. Запуск

```bash
python -m bot.main
```

## 📁 Структура проекта

```
Hookah_bot/
├── bot/
│   ├── handlers/
│   │   ├── commands.py       # /mixes, /check, /update, inline-кнопки
│   │   └── mix_handler.py    # Обработка сообщений с миксами и фото
│   ├── parsers/
│   │   ├── message_parser.py # Парсинг текста сообщения → табаки
│   │   └── site_parser.py    # Парсинг smokyart.ru + Groq API
│   ├── services/
│   │   ├── mix_service.py    # CRUD миксов, дедупликация
│   │   └── tobacco_service.py# CRUD табаков, синхронизация
│   └── main.py
├── db/
│   ├── database.py           # Подключение к БД
│   └── models.py             # SQLAlchemy модели
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

## 🔄 Как работает парсинг

### Текстовые сообщения

Поддерживаемые форматы:

```
# Формат 1: бренд + вкус + %
MustHave огуречный лимонад 50%
Jent follar 30%
Muassel кактус 20%

# Формат 2: бренд в контексте, вкусы отдельно
Взял все от MustHave
Ваниль 30%
Груша 60%
Орех пекан 10%

# Формат 3: граммовки вместо процентов
Overdose Самаркандская дыня 40
Overdose Сахарный арбуз 40
Overdose Мята 20
```

### Нормализация через Groq

Русские названия → фирменные английские:
- `MustHave ваниль` → поиск `Must Have Vanilla Cream`
- `MustHave груша` → поиск `Must Have Mad Pear`  
- `MustHave огуречный лимонад` → поиск `Must Have Cucunade`

### Проверка наличия

1. Groq генерирует 3 варианта поискового запроса
2. Playwright ищет на smokyart.ru через `/search?search=...`
3. Фильтрация: только `tabak-*` URL (не кальяны, не аксессуары)
4. Парсинг наличия на Металлургической д1 для каждой граммовки

## 🛠️ Утилиты

```bash
python truncate.py    # Очистить все таблицы
python migrate_v2.py  # Миграция: variants JSON, tobaccos_summary
python migrate_v3.py  # Миграция: mixes.title
```

## 📝 Лицензия

MIT
