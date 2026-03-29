"""
Полный парсер каталога табаков с smokyart.ru.
Обходит все бренды -> все граммовки -> все страницы.
Фильтрует по наличию на Металлургической д1.
"""

import re
import logging
from dataclasses import dataclass
from typing import Optional
from playwright.async_api import async_playwright, Page

logger = logging.getLogger(__name__)

SITE_BASE = "https://xn----8sbwhjmsbq.xn--p1ai"
CATALOG_URL = f"{SITE_BASE}/smesi-dlya-kalyana"


@dataclass
class CatalogTobacco:
    brand: str
    flavor: str
    full_name: str
    grams: Optional[str]
    price: Optional[int]
    url: str
    image_url: Optional[str]
    in_stock: bool      # наличие на Металлургической д1


def _check_metall_stock(text: str) -> bool:
    m = re.search(
        r'Металлургическая.*?д\s*1[^<\n]*(Есть|Нет)',
        text, re.IGNORECASE | re.DOTALL
    )
    if m:
        return m.group(1).lower() == 'есть'
    # Запасной — общее наличие
    return 'в наличии в 1 магазине' in text.lower()


def _extract_grams(name: str) -> Optional[str]:
    m = re.search(r'(\d+)\s*(?:гр|г|g|gr)\.?', name, re.IGNORECASE)
    return f"{m.group(1)} гр" if m else None


def _extract_price(text: str) -> Optional[int]:
    m = re.search(r'(\d[\d\s]+)\s*₽', text)
    if m:
        return int(m.group(1).replace(' ', ''))
    return None


def _split_brand_flavor(full_name: str, brand_hint: str) -> tuple[str, str]:
    """Убирает граммовку и вычленяет вкус."""
    name = re.sub(r'\s*\d+\s*(?:гр|г|g|gr)\.?\s*$', '', full_name, flags=re.IGNORECASE).strip()
    name = re.sub(r'^Табак\s+', '', name, flags=re.IGNORECASE).strip()
    # Если начинается с бренда — убираем его
    if name.lower().startswith(brand_hint.lower()):
        flavor = name[len(brand_hint):].strip().strip(',').strip()
    else:
        flavor = name
    return brand_hint, flavor if flavor else name


async def _dismiss_age(page: Page):
    try:
        btn = page.locator('button:has-text("ДА МНЕ 18")')
        if await btn.count() > 0:
            await btn.click()
            await page.wait_for_timeout(500)
    except Exception:
        pass


async def _get_brand_urls(page: Page) -> list[tuple[str, str]]:
    """Возвращает [(brand_name, url), ...] для всех брендов табака."""
    await page.goto(CATALOG_URL, wait_until='domcontentloaded', timeout=30000)
    await page.wait_for_timeout(2000)
    await _dismiss_age(page)

    links = await page.query_selector_all('a')
    brands = []
    seen = set()
    for a in links:
        href = await a.get_attribute('href') or ''
        text = (await a.inner_text()).strip()
        # Только прямые подкатегории вида /smesi-dlya-kalyana/tabak-XXX
        if re.search(r'/smesi-dlya-kalyana/tabak-[^/]+$', href) and href not in seen:
            seen.add(href)
            brand_name = re.sub(r'^Табак\s+', '', text, flags=re.IGNORECASE).strip()
            if brand_name and not brand_name.startswith('('):
                brands.append((brand_name, href))

    logger.info(f"Найдено брендов: {len(brands)}")
    return brands


async def _get_subcat_urls(page: Page, brand_url: str) -> list[str]:
    """Возвращает URL всех подкатегорий (граммовок) бренда."""
    await page.goto(brand_url, wait_until='domcontentloaded', timeout=30000)
    await page.wait_for_timeout(1500)
    await _dismiss_age(page)

    links = await page.query_selector_all('a')
    subcats = []
    seen = set()
    prefix = brand_url.rstrip('/')
    for a in links:
        href = await a.get_attribute('href') or ''
        if href.startswith(prefix + '/') and href not in seen:
            seen.add(href)
            subcats.append(href)

    # Если подкатегорий нет — сам бренд и есть листинг
    return subcats if subcats else [brand_url]


async def _parse_listing(page: Page, url: str, brand_name: str) -> list[CatalogTobacco]:
    """Парсит одну страницу листинга, возвращает табаки."""
    results = []
    try:
        await page.goto(url, wait_until='domcontentloaded', timeout=30000)
        await page.wait_for_timeout(1500)
        await _dismiss_age(page)

        cards = await page.query_selector_all('.product-thumb')
        for card in cards:
            try:
                name_el = await card.query_selector('.product-thumb__name')
                if not name_el:
                    continue
                full_name = (await name_el.inner_text()).strip()
                product_url = (await name_el.get_attribute('href') or '').split('?')[0]

                # Только табаки
                if 'tabak-' not in product_url.lower():
                    continue

                card_text = await card.inner_text()
                in_stock = _check_metall_stock(card_text)

                img_el = await card.query_selector('img')
                image_url = await img_el.get_attribute('src') if img_el else None

                grams = _extract_grams(full_name)
                price = _extract_price(card_text)
                brand, flavor = _split_brand_flavor(full_name, brand_name)

                if not flavor:
                    continue

                results.append(CatalogTobacco(
                    brand=brand,
                    flavor=flavor,
                    full_name=full_name,
                    grams=grams,
                    price=price,
                    url=product_url,
                    image_url=image_url,
                    in_stock=in_stock,
                ))
            except Exception as e:
                logger.error(f"Ошибка карточки: {e}")

    except Exception as e:
        logger.error(f"Ошибка страницы {url}: {e}")

    return results


async def _get_last_page(page: Page) -> int:
    pagination = await page.query_selector_all('.pagination li a')
    last = 1
    for a in pagination:
        text = (await a.inner_text()).strip()
        if text.isdigit():
            last = max(last, int(text))
    return last


async def parse_full_catalog(on_progress=None) -> list[CatalogTobacco]:
    """
    Парсит весь каталог табаков.
    Возвращает список CatalogTobacco со всеми позициями.
    """
    all_results: list[CatalogTobacco] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        page = await context.new_page()

        try:
            brands = await _get_brand_urls(page)

            for idx, (brand_name, brand_url) in enumerate(brands):
                if on_progress:
                    await on_progress(
                        f"🔍 Бренд {idx+1}/{len(brands)}: <b>{brand_name}</b>\n"
                        f"Найдено: {len(all_results)} табаков"
                    )

                subcats = await _get_subcat_urls(page, brand_url)
                logger.info(f"{brand_name}: {len(subcats)} подкатегорий")

                for subcat_url in subcats:
                    # Первая страница уже загружена через _get_subcat_urls
                    items = await _parse_listing(page, subcat_url, brand_name)
                    all_results.extend(items)

                    # Пагинация внутри подкатегории
                    last_page = await _get_last_page(page)
                    for p_num in range(2, last_page + 1):
                        paged_url = f"{subcat_url}?page={p_num}"
                        items = await _parse_listing(page, paged_url, brand_name)
                        all_results.extend(items)

        finally:
            await browser.close()

    # Дедупликация по URL
    seen = set()
    unique = []
    for item in all_results:
        if item.url not in seen:
            seen.add(item.url)
            unique.append(item)

    logger.info(f"Каталог спарсен: {len(unique)} уникальных позиций")
    return unique
