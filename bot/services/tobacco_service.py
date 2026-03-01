import asyncio
import logging
from datetime import datetime
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from db.models import Tobacco
from bot.parsers.site_parser import check_single_tobacco, parse_all_brands

logger = logging.getLogger(__name__)


async def update_tobacco_stock(session: AsyncSession, brand: str, flavor: str) -> Optional[bool]:
    """
    Проверяет наличие табака на сайте и обновляет запись в БД.
    Возвращает True/False/None (не найдено на сайте).
    """
    logger.info(f"Проверяем наличие: {brand} {flavor}")
    info = await check_single_tobacco(brand, flavor)

    result = await session.execute(
        select(Tobacco).where(
            Tobacco.brand.ilike(brand),
            Tobacco.flavor.ilike(flavor)
        )
    )
    tobacco = result.scalar_one_or_none()

    if not tobacco:
        return None

    if info:
        tobacco.in_stock = info.in_stock
        tobacco.grams = info.grams
        tobacco.last_checked = datetime.utcnow()
        await session.commit()
        logger.info(f"{brand} {flavor}: {'есть' if info.in_stock else 'нет'} ({info.grams})")
        return info.in_stock
    else:
        logger.warning(f"{brand} {flavor}: не найдено на сайте")
        return None


async def sync_brand_tobaccos(session: AsyncSession, brand: str):
    """
    Полная синхронизация всех табаков бренда с сайтом.
    Парсит все товары бренда и обновляет/создаёт записи в БД.
    """
    from bot.parsers.site_parser import parse_brand_page
    from playwright.async_api import async_playwright

    logger.info(f"Синхронизация бренда: {brand}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = await context.new_page()

        try:
            infos = await parse_brand_page(page, brand)
        finally:
            await browser.close()

    updated = 0
    created = 0

    for info in infos:
        result = await session.execute(
            select(Tobacco).where(
                Tobacco.brand.ilike(info.brand),
                Tobacco.flavor.ilike(info.flavor)
            )
        )
        tobacco = result.scalar_one_or_none()

        if tobacco:
            tobacco.in_stock = info.in_stock
            tobacco.grams = info.grams
            tobacco.last_checked = datetime.utcnow()
            updated += 1
        else:
            tobacco = Tobacco(
                brand=info.brand,
                flavor=info.flavor,
                full_name=info.full_name,
                in_stock=info.in_stock,
                grams=info.grams,
                last_checked=datetime.utcnow()
            )
            session.add(tobacco)
            created += 1

    await session.commit()
    logger.info(f"Бренд '{brand}': обновлено {updated}, создано {created}")
    return updated, created


async def sync_all_brands(session: AsyncSession):
    """Полная синхронизация всех брендов с сайтом"""
    from bot.parsers.site_parser import BRAND_URL_MAP

    brands = [b for b, url in BRAND_URL_MAP.items() if url is not None]
    total_updated = 0
    total_created = 0

    for brand in brands:
        try:
            updated, created = await sync_brand_tobaccos(session, brand)
            total_updated += updated
            total_created += created
        except Exception as e:
            logger.error(f"Ошибка синхронизации бренда '{brand}': {e}")

    logger.info(f"Полная синхронизация завершена: обновлено {total_updated}, создано {total_created}")
    return total_updated, total_created


async def get_all_tobaccos(session: AsyncSession) -> list[Tobacco]:
    result = await session.execute(
        select(Tobacco).order_by(Tobacco.brand, Tobacco.flavor)
    )
    return result.scalars().all()


async def get_tobaccos_by_brand(session: AsyncSession, brand: str) -> list[Tobacco]:
    result = await session.execute(
        select(Tobacco).where(Tobacco.brand.ilike(f"%{brand}%")).order_by(Tobacco.flavor)
    )
    return result.scalars().all()
