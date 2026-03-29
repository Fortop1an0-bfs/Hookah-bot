"""
Трекер использования API. Хранит статистику в usage_stats.json.
"""

import json
import os
from datetime import datetime, date

STATS_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../usage_stats.json"))

# Лимиты бесплатных планов
GROQ_DAILY_LIMIT = 500_000    # токенов/день (llama-3.3-70b)
GROQ_RPM_LIMIT = 30           # запросов/минуту
GEMINI_DAILY_TOKENS = 1_000_000  # токенов/день (gemini-2.0-flash)
GEMINI_DAILY_REQUESTS = 1_500    # запросов/день


def _load() -> dict:
    today = str(date.today())
    try:
        with open(STATS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Сбрасываем дневную статистику если новый день
        if data.get("date") != today:
            data["date"] = today
            data["groq"]["tokens_today"] = 0
            data["groq"]["calls_today"] = 0
            data["gemini"]["tokens_today"] = 0
            data["gemini"]["calls_today"] = 0
            _save(data)
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        data = _default(today)
        _save(data)
    return data


def _default(today: str) -> dict:
    return {
        "date": today,
        "groq": {
            "tokens_today": 0,
            "calls_today": 0,
            "tokens_total": 0,
            "calls_total": 0,
            "last_used": None,
        },
        "gemini": {
            "tokens_today": 0,
            "calls_today": 0,
            "tokens_total": 0,
            "calls_total": 0,
            "last_used": None,
        },
    }


def _save(data: dict):
    os.makedirs(os.path.dirname(os.path.abspath(STATS_FILE)), exist_ok=True)
    with open(STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def record_groq(tokens: int):
    data = _load()
    data["groq"]["tokens_today"] += tokens
    data["groq"]["calls_today"] += 1
    data["groq"]["tokens_total"] += tokens
    data["groq"]["calls_total"] += 1
    data["groq"]["last_used"] = datetime.now().strftime("%d.%m %H:%M")
    _save(data)


def record_gemini(tokens: int):
    data = _load()
    data["gemini"]["tokens_today"] += tokens
    data["gemini"]["calls_today"] += 1
    data["gemini"]["tokens_total"] += tokens
    data["gemini"]["calls_total"] += 1
    data["gemini"]["last_used"] = datetime.now().strftime("%d.%m %H:%M")
    _save(data)


def get_stats() -> dict:
    return _load()


def reset_stats():
    _save(_default(str(date.today())))
