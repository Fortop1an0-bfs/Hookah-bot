"""
Ищет реальные рецепты миксов в интернете.
Источники: Telegram каналы + тематические сайты.
Извлечение: сначала regex (без токенов), потом AI если нужно.
"""

import re
import os
import json
import logging
import aiohttp
from playwright.async_api import Page
from bot.services.usage_tracker import record_groq, record_gemini

logger = logging.getLogger(__name__)

GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"

TELEGRAM_CHANNELS = [
    ("vanyazabeygroop", "https://t.me/s/vanyazabeygroop"),
    ("musthavetobacco",  "https://t.me/s/musthavetobacco"),
    ("kalyanich",        "https://t.me/s/kalyanich604819"),
    ("hookahxmix",       "https://t.me/s/hookahxmix"),
]

WEBSITES = [
    ("ohookah.ru",       "https://ohookah.ru/mix"),
    ("nn-kalyan.ru",     "https://nn-kalyan.ru/100-vkusnyx-miksov-dlya-vashego-kalyana/"),
    ("kalyan-expert.ru", "https://kalyan-expert.ru/kalyan-mix.html"),
    ("hookah.ru",        "https://hookah.ru/mix/"),
]

# Минимум символов чтобы считать страницу загруженной
MIN_PAGE_LEN = 200


# ─── Загрузка страницы ─────────────────────────────────────────────────────────

async def _get_page_text(page: Page, url: str, wait_sec: int = 3) -> str | None:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=35000)
        await page.wait_for_timeout(wait_sec * 1000)

        for selector in ['button:has-text("Принять")', 'button:has-text("Согласен")',
                          'button:has-text("Закрыть")', 'button:has-text("OK")',
                          'button:has-text("Понятно")']:
            try:
                btn = page.locator(selector).first
                if await btn.is_visible(timeout=500):
                    await btn.click()
                    await page.wait_for_timeout(200)
            except Exception:
                pass

        text = await page.inner_text("body")
        if not text or len(text) < MIN_PAGE_LEN:
            logger.warning(f"{url}: страница пустая ({len(text or '')} симв.)")
            return None
        logger.debug(f"{url}: {len(text)} символов")
        return text[:12000]
    except Exception as e:
        logger.warning(f"Не удалось загрузить {url}: {e}")
        return None


# ─── Специальный парсер ohookah.ru ────────────────────────────────────────────

def _parse_ohookah_card(card_text: str) -> dict | None:
    """
    Парсит одну карточку ohookah.ru.
    Формат: 'Brand\nFlavor\n(40%)\n \nBrand2\nFlavor2\n(25%)...'
    """
    # Ищем секцию состава
    if "Состав" not in card_text:
        return None

    # Название — текст до "Сохранить микс"
    title = ""
    if "Сохранить микс" in card_text:
        before = card_text.split("Сохранить микс")[0]
        lines = [l.strip() for l in before.split("\n") if l.strip()]
        # Убираем строки с процентами в начале, берём последнюю содержательную
        for line in reversed(lines):
            if not re.match(r'^\d+%$', line) and len(line) > 2:
                title = line
                break

    # Секция состава
    consist_part = card_text.split("Состав")[-1]
    lines = [l.strip() for l in consist_part.split("\n") if l.strip() and l.strip() != "\xa0"]

    # Парсим тройки: Brand, Flavor, (XX%)
    recipe_parts = []
    i = 0
    while i < len(lines) - 1:
        pct_match = re.search(r'\((\d+)%\)', lines[i])
        if pct_match:
            pct = pct_match.group(1)
            # Ищем бренд и вкус перед процентом
            if i >= 2:
                brand = lines[i - 2].strip(" :")
                flavor = lines[i - 1].strip()
                if brand and flavor and not re.search(r'\d+%', brand):
                    recipe_parts.append(f"{brand} {flavor} {pct}%")
            elif i >= 1:
                brand = lines[i - 1].strip(" :")
                if brand and not re.search(r'\d+%', brand):
                    recipe_parts.append(f"{brand} {pct}%")
        i += 1

    if len(recipe_parts) < 2:
        return None

    return {
        "title": title or "Микс с ohookah.ru",
        "recipe": "\n".join(recipe_parts),
        "source": "ohookah.ru",
    }


async def _parse_ohookah(page: Page, max_pages: int = 3) -> list[dict]:
    """Парсит ohookah.ru/mix напрямую через DOM — не требует AI."""
    results = []
    try:
        for p_num in range(1, max_pages + 1):
            url = "https://ohookah.ru/mix" if p_num == 1 else f"https://ohookah.ru/mix?page={p_num}"
            await page.goto(url, wait_until="domcontentloaded", timeout=35000)
            await page.wait_for_timeout(3000)

            cards = await page.query_selector_all("[class*='mix__item'], [class*='mix-item'], .mix__list > div, .mixes__list > div")
            if not cards:
                # Запасной вариант — все [class*='mix'] кроме первых 2 (хедер)
                all_cards = await page.query_selector_all("[class*='mix']")
                cards = all_cards[2:]

            logger.info(f"ohookah.ru стр{p_num}: {len(cards)} карточек")

            page_results = 0
            for card in cards[:60]:
                try:
                    card_text = await card.inner_text()
                    mix = _parse_ohookah_card(card_text)
                    if mix:
                        results.append(mix)
                        page_results += 1
                except Exception:
                    pass

            logger.info(f"ohookah.ru стр{p_num}: распарсили {page_results} миксов")
            if page_results == 0:
                break

    except Exception as e:
        logger.warning(f"ohookah.ru парсер: {e}")

    logger.info(f"ohookah.ru итого: {len(results)} миксов")
    return results


# ─── Regex извлечение (без AI) ─────────────────────────────────────────────────

def _regex_extract(text: str, source: str) -> list[dict]:
    """
    Извлекает рецепты из текста по паттернам вида 'Бренд Вкус XX%'.
    Не требует API.
    """
    results = []
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    recipe_lines = []
    title_candidate = ""

    for line in lines:
        has_pct = bool(re.search(r'\b\d{1,3}\s*%', line))
        # Строка выглядит как компонент микса: содержит % и не слишком длинная
        if has_pct and 5 < len(line) < 120:
            recipe_lines.append(re.sub(r'\s+', ' ', line).strip())
        else:
            # Если накопили рецепт — сохраняем
            if len(recipe_lines) >= 2:
                # Берём ближайший предшествующий короткий текст как название
                title = title_candidate[:60] if title_candidate else f"Микс ({source})"
                recipe = "\n".join(recipe_lines[:5])
                results.append({"title": title, "recipe": recipe, "source": source})
            recipe_lines = []
            # Короткая строка без процентов = потенциальное название
            if 3 < len(line) < 80 and not any(w in line.lower() for w in
                    ["купить", "цена", "руб", "₽", "доставка", "скидка", "подписка",
                     "©", "http", "instagram", "telegram", "vk.com"]):
                title_candidate = line

    # Последний накопленный рецепт
    if len(recipe_lines) >= 2:
        title = title_candidate[:60] if title_candidate else f"Микс ({source})"
        results.append({"title": title, "recipe": "\n".join(recipe_lines[:5]), "source": source})

    logger.info(f"{source}: regex нашёл {len(results)} рецептов")
    return results


# ─── AI извлечение (запасной вариант) ─────────────────────────────────────────

async def _ai_extract(text: str, source: str) -> list[dict]:
    """Передаёт текст в AI для извлечения рецептов. Использует быструю модель."""
    prompt = f"""Из текста извлеки рецепты миксов для кальяна.
Ищи строки с брендом, вкусом и процентами. Если процентов нет — раздели поровну.

JSON массив (или [] если нет рецептов):
[{{"title": "Название", "recipe": "Бренд Вкус 40%\\nБренд Вкус 60%"}}]

Текст:
{text[:6000]}"""

    raw = await _call_groq_fast(prompt)
    if not raw:
        raw = await _call_gemini(prompt)
    if not raw:
        return []

    try:
        cleaned = re.sub(r"```json|```", "", raw).strip()
        result = json.loads(cleaned)
        if not isinstance(result, list):
            return []
        valid = [r for r in result if r.get("title") and r.get("recipe")]
        for item in valid:
            item["source"] = source
        logger.info(f"{source}: AI извлёк {len(valid)} рецептов")
        return valid
    except Exception as e:
        logger.warning(f"AI парсинг ошибка ({source}): {e}")
        return []


async def _call_groq_fast(prompt: str) -> str | None:
    """Использует llama-3.1-8b-instant — лимит 500К токенов/день."""
    api_key = os.getenv("GROQ_API_KEY", "")
    if not api_key:
        return None
    try:
        async with aiohttp.ClientSession() as http:
            async with http.post(
                GROQ_API_URL,
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json={
                    "model": "llama-3.1-8b-instant",   # 500K TPD, быстрая
                    "messages": [
                        {"role": "system", "content": "Извлекай рецепты. Только JSON."},
                        {"role": "user", "content": prompt},
                    ],
                    "max_tokens": 2000,
                    "temperature": 0.1,
                },
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                data = await resp.json()
                if "choices" not in data:
                    logger.warning(f"Groq fast: {data.get('error', {}).get('message', '')[:80]}")
                    return None
                tokens = data.get("usage", {}).get("total_tokens", 0)
                record_groq(tokens)
                return data["choices"][0]["message"]["content"].strip()
    except Exception as e:
        logger.warning(f"Groq fast ошибка: {e}")
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
                    "generationConfig": {"temperature": 0.1, "maxOutputTokens": 2000,
                                          "responseMimeType": "application/json"},
                },
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                data = await resp.json()
                if "candidates" not in data:
                    logger.warning(f"Gemini: {data.get('error', {}).get('message', '')[:80]}")
                    return None
                tokens = data.get("usageMetadata", {}).get("totalTokenCount", 0)
                record_gemini(tokens)
                return data["candidates"][0]["content"]["parts"][0]["text"].strip()
    except Exception as e:
        logger.warning(f"Gemini ошибка: {e}")
        return None


# ─── Главная функция ───────────────────────────────────────────────────────────

async def search_internet_mixes(page: Page, count: int = 15) -> list[dict]:
    """
    Ищет рецепты в Telegram каналах и на сайтах.
    Сначала regex (без AI), потом AI если regex не дал результата.
    """
    all_results = []

    # 1. ohookah.ru — специальный DOM-парсер
    logger.info("Парсю ohookah.ru через DOM...")
    ohookah = await _parse_ohookah(page)
    if ohookah:
        all_results.extend(ohookah)
        logger.info(f"ohookah.ru: {len(ohookah)} рецептов")

    # 2. Telegram каналы
    for source_name, url in TELEGRAM_CHANNELS:
        if len(all_results) >= count:
            break
        logger.info(f"TG {source_name}: {url}")
        text = await _get_page_text(page, url, wait_sec=2)
        if not text:
            continue

        # Сначала regex
        recipes = _regex_extract(text, source_name)
        if not recipes:
            # Потом AI
            recipes = await _ai_extract(text, source_name)
        all_results.extend(recipes)

    # 3. Сайты
    for source_name, url in WEBSITES:
        if len(all_results) >= count:
            break
        logger.info(f"Сайт {source_name}: {url}")
        text = await _get_page_text(page, url, wait_sec=3)
        if not text:
            continue

        recipes = _regex_extract(text, source_name)
        if not recipes:
            recipes = await _ai_extract(text, source_name)
        all_results.extend(recipes)

    logger.info(f"Итого рецептов из интернета: {len(all_results)}")
    return all_results
