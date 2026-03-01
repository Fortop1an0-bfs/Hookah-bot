"""
Парсер сайта smokyart.ru через встроенный поиск.
Логика:
1. Groq API нормализует название табака (любой формат -> поисковые запросы)
2. Ищем на сайте через /search?search=<запрос>
3. Фильтруем только табаки (URL содержит 'tabak-')
4. На карточках сразу видно наличие по Металлургической
"""

import re
import os
import json
import logging
import base64
import aiohttp
from dataclasses import dataclass
from typing import Optional
from playwright.async_api import async_playwright, Page

logger = logging.getLogger(__name__)

GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"

NON_TOBACCO_KEYWORDS = [
    'ароматизатор', 'жидкость', 'испаритель', 'картридж', 'вейп',
    'одноразов', 'уголь', 'чаша', 'кальян', 'мундштук', 'шланг', 'колба',
    'blur constructor', 'narcoz', 'арома'
]


@dataclass
class TobaccoStockInfo:
    brand: str
    flavor: str
    full_name: str
    in_stock: bool
    grams: Optional[str]
    url: str


def extract_grams_from_name(name: str) -> Optional[str]:
    matches = re.findall(r'(\d+)\s*(?:гр|г|g|gr)\.?', name, re.IGNORECASE)
    if matches:
        return ', '.join(f"{m} гр" for m in dict.fromkeys(matches))
    return None


def check_metall_stock_from_text(text: str) -> Optional[bool]:
    pattern = re.compile(
        r'Металлургическая.*?д\s*1[\s\n]*(Есть|Нет|В наличии|Нет в наличии)',
        re.IGNORECASE | re.DOTALL
    )
    match = pattern.search(text)
    if match:
        return match.group(1).strip().lower() in ('есть', 'в наличии')
    return None


def split_brand_flavor(full_name: str) -> tuple[str, str]:
    name = re.sub(r'^Табак\s+', '', full_name, flags=re.IGNORECASE).strip()
    name = re.sub(r'\s*\d+\s*(?:гр|г|g|gr)\.?\s*$', '', name, flags=re.IGNORECASE).strip()
    parts = name.split(' ', 1)
    return parts[0], parts[1] if len(parts) > 1 else ''


async def normalize_tobacco_with_groq(brand: str, flavor: str) -> list[str]:
    """
    Groq API нормализует название -> 3 поисковых запроса от точного к общему.
    """
    api_key = os.getenv("GROQ_API_KEY", "")
    if not api_key:
        logger.warning("GROQ_API_KEY не задан, используем базовый поиск")
        return [f"{brand} {flavor}", brand, flavor.split()[0]]

    prompt = f"""Ты эксперт по кальянным табакам. Тебе дано название табака из Telegram-сообщения.
Бренд: {brand}
Вкус: {flavor}

Задача: придумай 3 поисковых запроса для поиска этого табака.
ВАЖНО: вкусы часто пишут по-русски, но у брендов есть фирменные английские названия. Ищи именно их!

Примеры для Must Have:
- "ваниль" -> ["Must Have Vanilla Cream", "Vanilla Cream", "Must Have ваниль"]
- "груша" -> ["Must Have Mad Pear", "Mad Pear", "Must Have груша"]
- "орех пекан" -> ["Must Have Maple Pecan", "Maple Pecan", "Must Have Pecan"]
- "огуречный лимонад" -> ["Must Have Cucunade", "Cucunade", "Must Have огурец"]
- "ягоды холод" -> ["Must Have Berry Holls", "Berry Holls", "Must Have berry"]

Примеры для других брендов:
- "Muassel кактус" -> ["Muassel кактус", "Muassel cactus", "кактус"]
- "Darkside кола" -> ["Darkside Cola", "Dark Side Cola", "кола"]
- "Jent фисташки" -> ["Jent Pistachio", "Pistachio", "Jent фиста"]
- "Hook малиновый" -> ["Hook малиновый", "Hook Raspberry", "малиновый"]
- "Hook чёрная смородина" -> ["Hook чёрная смородина", "Hook Blackcurrant", "чёрная смородина"]

Отвечай ТОЛЬКО JSON массивом из 3 строк, без пояснений:
["запрос1", "запрос2", "запрос3"]"""

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                GROQ_API_URL,
                headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
                json={
                    "model": "llama-3.3-70b-versatile",
                    "messages": [
                        {"role": "system", "content": "Ты эксперт по кальянным табакам. Отвечай только JSON массивом без пояснений."},
                        {"role": "user", "content": prompt}
                    ],
                    "max_tokens": 100,
                    "temperature": 0
                },
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                data = await resp.json()
                text = data["choices"][0]["message"]["content"].strip()
                text = re.sub(r"```json|```", "", text).strip()
                queries = json.loads(text)
                logger.info(f"Groq: '{brand} {flavor}' -> {queries}")
                return queries
    except Exception as e:
        logger.warning(f"Groq API ошибка: {e}, используем базовый поиск")
        return [f"{brand} {flavor}", brand, flavor.split()[0]]


async def identify_tobaccos_from_image(image_url: str) -> list[dict]:
    """
    Отправляет фото из Telegram поста в Groq vision модель.
    Возвращает список найденных табаков: [{"brand": ..., "flavor": ...}, ...]
    """
    api_key = os.getenv("GROQ_API_KEY", "")
    if not api_key:
        return []

    prompt = """На фото банки/упаковки табака для кальяна.
Прочитай точно что написано на каждой банке: название бренда и название вкуса.
НАЗВАНИЯ ЧИТАЙ ТОЧНО С БАНКИ - не переводи и не догадывай!

Примеры:
- Банка Must Have с надписью "VANILLA CREAM" -> {"brand": "Must Have", "flavor": "Vanilla Cream"}
- Банка Must Have с надписью "ГРАНАТ" -> {"brand": "Must Have", "flavor": "гранат"}
- Банка Hook с надписью "Малиновый" -> {"brand": "Hook", "flavor": "малиновый"}

Ответь ONLY JSON массивом, без пояснений:
[{"brand": "...", "flavor": "..."}, ...]
Если табаков нет — []"""

    VISION_MODELS = [
        "llama-3.2-90b-vision-preview",
        "llama-3.2-11b-vision-preview",
        "meta-llama/llama-4-scout-17b-16e-instruct",
    ]

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(image_url) as img_resp:
                if img_resp.status != 200:
                    logger.warning(f"Groq vision: не удалось скачать фото, status={img_resp.status}")
                    return []
                img_data = await img_resp.read()
                img_b64 = base64.b64encode(img_data).decode()
                content_type = img_resp.headers.get('Content-Type', 'image/jpeg')

            for model in VISION_MODELS:
                try:
                    async with session.post(
                        GROQ_API_URL,
                        headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
                        json={
                            "model": model,
                            "messages": [{
                                "role": "user",
                                "content": [
                                    {"type": "image_url", "image_url": {"url": f"data:{content_type};base64,{img_b64}"}},
                                    {"type": "text", "text": prompt}
                                ]
                            }],
                            "max_tokens": 300,
                            "temperature": 0
                        },
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as resp:
                        data = await resp.json()
                        if "choices" not in data:
                            logger.warning(f"Groq vision {model}: нет choices, ответ: {data}")
                            continue
                        text = data["choices"][0]["message"]["content"].strip()
                        text = re.sub(r"```json|```", "", text).strip()
                        result = json.loads(text)
                        logger.info(f"Groq vision ({model}) распознал с фото: {result}")
                        return result
                except json.JSONDecodeError as e:
                    logger.warning(f"Groq vision {model}: не JSON: {e}")
                    continue
                except Exception as e:
                    logger.warning(f"Groq vision {model}: ошибка: {e}")
                    continue

        logger.warning("Groq vision: все модели недоступны")
        return []
    except Exception as e:
        logger.warning(f"Groq vision: критическая ошибка: {e}")
        return []


async def search_tobacco_on_site(page: Page, query: str) -> list[TobaccoStockInfo]:
    """Ищет табак на сайте, возвращает только табаки (фильтр по URL)."""
    url = f"https://smokyart.ru/search?search={query}"
    results = []

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(2000)

        try:
            age_btn = page.locator('button:has-text("ДА МНЕ 18 ЛЕТ")')
            if await age_btn.count() > 0:
                await age_btn.click()
                await page.wait_for_timeout(500)
        except Exception:
            pass

        cards = await page.query_selector_all('.product-thumb')
        if not cards:
            logger.warning(f"По запросу '{query}' ничего не найдено")
            return []

        for card in cards:
            try:
                name_el = await card.query_selector('.product-thumb__name')
                if not name_el:
                    continue
                full_name = (await name_el.inner_text()).strip()

                link_el = await card.query_selector('a')
                product_url = await link_el.get_attribute('href') if link_el else ''
                product_url = product_url.split('?')[0] if product_url else ''

                # Фильтр: только табаки
                if 'tabak-' not in product_url.lower():
                    continue
                if any(kw in full_name.lower() for kw in NON_TOBACCO_KEYWORDS):
                    continue

                grams = extract_grams_from_name(full_name)
                card_text = await card.inner_text()
                in_stock = check_metall_stock_from_text(card_text)
                if in_stock is None:
                    if 'отсутствует в г. владивосток' in card_text.lower():
                        in_stock = False
                    elif 'в наличии' in card_text.lower():
                        in_stock = True

                brand, flavor = split_brand_flavor(full_name)
                results.append(TobaccoStockInfo(
                    brand=brand, flavor=flavor, full_name=full_name,
                    in_stock=in_stock if in_stock is not None else False,
                    grams=grams, url=product_url
                ))
            except Exception as e:
                logger.error(f"Ошибка парсинга карточки: {e}")

        logger.info(f"Запрос '{query}': найдено {len(results)} товаров")
    except Exception as e:
        logger.error(f"Ошибка поиска '{query}': {e}")

    return results


async def parse_all_brands(brands: list[str]) -> list[TobaccoStockInfo]:
    """Парсит несколько брендов через поиск."""
    all_results = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = await context.new_page()
        for brand in brands:
            logger.info(f"Парсим бренд: {brand}")
            results = await search_tobacco_on_site(page, brand)
            all_results.extend(results)
        await browser.close()
    return all_results


async def check_single_tobacco(brand: str, flavor: str) -> list[TobaccoStockInfo]:
    """
    Проверяет наличие табака: Groq нормализует -> ищем на сайте.
    Возвращает ВСЕ найденные граммовки.
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = await context.new_page()
        try:
            queries = await normalize_tobacco_with_groq(brand, flavor)
            results = []
            for query in queries:
                results = await search_tobacco_on_site(page, query)
                if results:
                    logger.info(f"Найдено {len(results)} граммовок по запросу: '{query}'")
                    break
            return results
        finally:
            await browser.close()
