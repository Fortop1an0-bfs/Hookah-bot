"""
Генерирует рецепты миксов через Groq / Gemini.
Передаёт AI реальные вкусы из БД (только в наличии).
"""

import os
import re
import json
import random
import logging
import aiohttp
from bot.services.usage_tracker import record_groq, record_gemini
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, distinct
from db.models import Tobacco

logger = logging.getLogger(__name__)

GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"

FALLBACK_BRANDS = [
    "MustHave", "Jent", "Overdose", "Darkside", "Muassel",
    "Spectrum", "Hook", "Starline", "Adalya", "Burn",
]


async def get_instock_tobaccos(session: AsyncSession) -> list[tuple[str, str]]:
    """Возвращает [(brand, flavor), ...] только тех что в наличии."""
    result = await session.execute(
        select(Tobacco.brand, Tobacco.flavor)
        .where(Tobacco.in_stock == True)
        .order_by(Tobacco.brand, Tobacco.flavor)
    )
    return [(row[0], row[1]) for row in result.fetchall()]


async def get_known_brands(session: AsyncSession) -> list[str]:
    result = await session.execute(
        select(distinct(Tobacco.brand)).order_by(Tobacco.brand)
    )
    brands = [row[0] for row in result.fetchall() if row[0]]
    return brands if brands else FALLBACK_BRANDS


def _build_prompt(tobaccos: list[tuple[str, str]], count: int) -> str:
    """
    Если есть реальные вкусы — передаём их AI.
    Если нет — передаём только бренды.
    """
    if tobaccos:
        # Группируем по брендам, берём случайную выборку до 120 позиций
        by_brand: dict[str, list[str]] = {}
        for brand, flavor in tobaccos:
            by_brand.setdefault(brand, []).append(flavor)

        # Если слишком много — берём случайные вкусы из каждого бренда
        sampled: list[tuple[str, str]] = []
        for brand, flavors in by_brand.items():
            take = min(len(flavors), max(3, 120 // len(by_brand)))
            sampled.extend((brand, f) for f in random.sample(flavors, take))

        tobaccos_str = "\n".join(f"- {b} {f}" for b, f in sampled)
        return f"""Ты опытный кальянщик. Придумай {count} уникальных, вкусных и гармоничных миксов.

ВАЖНО: используй ТОЛЬКО табаки из списка ниже — именно с такими названиями вкусов!

Доступные табаки (в наличии в магазине):
{tobaccos_str}

Правила:
- 2-4 компонента, проценты суммируются в 100%
- Бренд и вкус пиши ТОЧНО как в списке выше
- Придумай короткое красивое название каждому миксу
- Делай разнообразные миксы: фруктовые, ягодные, освежающие, десертные

Формат — строго JSON массив без пояснений:
[
  {{
    "title": "Название микса",
    "recipe": "Бренд Вкус 40%\\nБренд Вкус 35%\\nБренд Вкус 25%"
  }},
  ...
]"""
    else:
        # Фолбэк — только бренды
        brands_str = ", ".join(FALLBACK_BRANDS)
        return f"""Ты опытный кальянщик. Придумай {count} уникальных миксов.

Доступные бренды: {brands_str}

Правила:
- 2-4 компонента, проценты суммируются в 100%
- Придумай короткое красивое название каждому миксу

Формат — строго JSON массив:
[
  {{
    "title": "Название микса",
    "recipe": "Бренд Вкус 40%\\nБренд Вкус 35%\\nБренд Вкус 25%"
  }},
  ...
]"""


async def _call_groq(prompt: str) -> str | None:
    api_key = os.getenv("GROQ_API_KEY", "")
    if not api_key:
        return None
    try:
        async with aiohttp.ClientSession() as http:
            async with http.post(
                GROQ_API_URL,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "llama-3.1-8b-instant",
                    "messages": [
                        {"role": "system", "content": "Ты эксперт-кальянщик. Отвечай только JSON без пояснений."},
                        {"role": "user", "content": prompt},
                    ],
                    "max_tokens": 2000,
                    "temperature": 0.85,
                },
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                data = await resp.json()
                if "choices" not in data:
                    logger.warning(f"Groq: нет choices: {data}")
                    return None
                tokens = data.get("usage", {}).get("total_tokens", 0)
                record_groq(tokens)
                return data["choices"][0]["message"]["content"].strip()
    except Exception as e:
        logger.warning(f"Groq research ошибка: {e}")
        return None


async def _call_gemini(prompt: str) -> str | None:
    api_key = os.getenv("GOOGLE_API_KEY", "")
    if not api_key:
        return None
    try:
        async with aiohttp.ClientSession() as http:
            async with http.post(
                f"{GEMINI_API_URL}?key={api_key}",
                headers={"Content-Type": "application/json"},
                json={
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {
                        "temperature": 0.85,
                        "maxOutputTokens": 2000,
                        "responseMimeType": "application/json",
                    },
                },
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                data = await resp.json()
                if "candidates" not in data:
                    logger.warning(f"Gemini: нет candidates: {data}")
                    return None
                tokens = data.get("usageMetadata", {}).get("totalTokenCount", 0)
                record_gemini(tokens)
                return data["candidates"][0]["content"]["parts"][0]["text"].strip()
    except Exception as e:
        logger.warning(f"Gemini research ошибка: {e}")
        return None


async def generate_mixes(session: AsyncSession, count: int = 5) -> list[dict]:
    """
    Генерирует миксы на основе реальных вкусов из БД (только в наличии).
    """
    tobaccos = await get_instock_tobaccos(session)
    prompt = _build_prompt(tobaccos, count)

    raw = await _call_groq(prompt)
    if not raw:
        logger.info("Groq недоступен, пробуем Gemini...")
        raw = await _call_gemini(prompt)

    if not raw:
        logger.error("Оба AI недоступны для генерации миксов")
        return []

    try:
        cleaned = re.sub(r"```json|```", "", raw).strip()
        mixes = json.loads(cleaned)
        if not isinstance(mixes, list):
            raise ValueError("Ожидался JSON массив")
        logger.info(f"AI сгенерировал {len(mixes)} миксов (запрошено {count})")
        return mixes
    except (json.JSONDecodeError, ValueError) as e:
        logger.error(f"Ошибка парсинга JSON от AI: {e}\nRaw: {raw[:300]}")
        return []
