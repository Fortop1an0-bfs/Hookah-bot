"""
Парсер раздела готовых миксов на smokyart.ru.
Страница: https://smokyart.ru/smesi-dlya-kalyana
"""

import re
import logging
from dataclasses import dataclass
from typing import Optional
from playwright.async_api import async_playwright, Page

logger = logging.getLogger(__name__)

BASE_URL = "https://smokyart.ru/smesi-dlya-kalyana"


@dataclass
class ParsedStoreMix:
    name: str
    brand: Optional[str]
    flavor: Optional[str]
    mix_type: Optional[str]
    grams: Optional[str]
    price: Optional[int]
    url: str
    image_url: Optional[str]
    in_stock: Optional[bool]


def _parse_name(full_name: str) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Разбирает название вида:
    'Смесь "Летим", Райский манго, Medium, 25 гр.'
    -> brand='Летим', flavor='Райский манго', mix_type='Medium', grams='25 гр'
    """
    name = re.sub(r'^Смесь\s+', '', full_name, flags=re.IGNORECASE).strip()

    # Бренд в кавычках
    brand = None
    m = re.match(r'^["\u00ab\u201c]([^"\u00bb\u201d]+)["\u00bb\u201d],?\s*', name)
    if m:
        brand = m.group(1).strip()
        name = name[m.end():]

    # Граммовка
    grams = None
    m = re.search(r'(\d+\s*гр\.?)$', name, re.IGNORECASE)
    if m:
        grams = m.group(1).strip().rstrip('.')
        name = name[:m.start()].strip().rstrip(',').strip()

    # Тип смеси
    mix_type = None
    TYPE_WORDS = ['medium', 'strong', 'light', 'ultra light', 'soft', 'hard', 'classic', 'original']
    for t in TYPE_WORDS:
        pattern = re.compile(r',?\s*' + re.escape(t) + r'\s*,?', re.IGNORECASE)
        if pattern.search(name):
            mix_type = t.title()
            name = pattern.sub(',', name).strip().strip(',').strip()
            break

    flavor = name.strip().strip(',').strip() if name.strip() else None
    return brand, flavor, mix_type, grams


def _parse_stock(card_text: str) -> Optional[bool]:
    pattern = re.compile(
        r'Металлургическая.*?д\s*1[^<\n]*?(Есть|Нет)',
        re.IGNORECASE | re.DOTALL
    )
    m = pattern.search(card_text)
    if m:
        return m.group(1).lower() == 'есть'
    return None


async def _parse_page(page: Page, url: str) -> list[ParsedStoreMix]:
    results = []
    try:
        await page.goto(url, wait_until='domcontentloaded', timeout=30000)
        await page.wait_for_timeout(2000)

        # Закрываем попап 18+
        try:
            btn = page.locator('button:has-text("ДА МНЕ 18")')
            if await btn.count() > 0:
                await btn.click()
                await page.wait_for_timeout(500)
        except Exception:
            pass

        cards = await page.query_selector_all('.product-thumb')
        logger.info(f"Страница {url}: {len(cards)} карточек")

        for card in cards:
            try:
                name_el = await card.query_selector('.product-thumb__name')
                if not name_el:
                    continue
                full_name = (await name_el.inner_text()).strip()
                product_url = await name_el.get_attribute('href') or ''

                img_el = await card.query_selector('.product-thumb__image img')
                image_url = await img_el.get_attribute('src') if img_el else None

                price_el = await card.query_selector('.product-thumb__price')
                price = None
                if price_el:
                    price_text = await price_el.inner_text()
                    m = re.search(r'(\d+)', price_text.replace(' ', ''))
                    if m:
                        price = int(m.group(1))

                card_text = await card.inner_text()
                in_stock = _parse_stock(card_text)

                brand, flavor, mix_type, grams = _parse_name(full_name)

                results.append(ParsedStoreMix(
                    name=full_name,
                    brand=brand,
                    flavor=flavor,
                    mix_type=mix_type,
                    grams=grams,
                    price=price,
                    url=product_url,
                    image_url=image_url,
                    in_stock=in_stock,
                ))
            except Exception as e:
                logger.error(f"Ошибка парсинга карточки: {e}")

    except Exception as e:
        logger.error(f"Ошибка загрузки страницы {url}: {e}")

    return results


async def parse_all_store_mixes(on_progress=None) -> list[ParsedStoreMix]:
    """Парсит все страницы раздела миксов магазина."""
    all_results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        page = await context.new_page()

        try:
            # Определяем количество страниц
            await page.goto(BASE_URL, wait_until='domcontentloaded', timeout=30000)
            await page.wait_for_timeout(2000)

            try:
                btn = page.locator('button:has-text("ДА МНЕ 18")')
                if await btn.count() > 0:
                    await btn.click()
                    await page.wait_for_timeout(500)
            except Exception:
                pass

            last_page = 1
            pagination = await page.query_selector_all('.pagination li a')
            for a in pagination:
                text = (await a.inner_text()).strip()
                if text.isdigit():
                    last_page = max(last_page, int(text))

            logger.info(f"Всего страниц: {last_page}")

            for p_num in range(1, last_page + 1):
                url = BASE_URL if p_num == 1 else f"{BASE_URL}?page={p_num}"
                if on_progress:
                    await on_progress(f"📄 Парсю страницу {p_num}/{last_page}...")

                mixes = await _parse_page(page, url)
                all_results.extend(mixes)
                logger.info(f"Страница {p_num}: +{len(mixes)}, итого {len(all_results)}")

        finally:
            await browser.close()

    return all_results
