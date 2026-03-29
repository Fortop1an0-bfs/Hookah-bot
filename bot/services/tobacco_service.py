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
    Синхронизация бренда: ищет все табаки через поиск на сайте.
    """
    from bot.parsers.site_parser import parse_all_brands
    from datetime import timezone

    logger.info(f"Синхронизация бренда: {brand}")
    infos = await parse_all_brands([brand])

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

        variants = [{"grams": info.grams, "url": info.url, "in_stock": info.in_stock}]

        if tobacco:
            tobacco.in_stock = info.in_stock
            tobacco.variants = variants
            tobacco.updated_at = datetime.now(timezone.utc)
            updated += 1
        else:
            session.add(Tobacco(
                brand=info.brand,
                flavor=info.flavor,
                full_name=info.full_name,
                in_stock=info.in_stock,
                variants=variants,
            ))
            created += 1

    await session.commit()
    logger.info(f"Бренд '{brand}': обновлено {updated}, создано {created}")
    return updated, created


async def sync_all_brands(session: AsyncSession):
    """Полная синхронизация всех известных брендов с сайтом."""
    from bot.parsers.message_parser import BRAND_ALIASES

    brands = sorted(set(BRAND_ALIASES.values()))
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


async def sync_from_catalog(session: AsyncSession, on_progress=None) -> tuple[int, int]:
    """
    Парсит полный каталог табаков с сайта и обновляет таблицу tobaccos.
    Группирует граммовки одного табака в variants JSON.
    Возвращает (updated, created).
    """
    from bot.parsers.catalog_parser import parse_full_catalog
    from collections import defaultdict
    from datetime import timezone

    items = await parse_full_catalog(on_progress=on_progress)
    logger.info(f"Каталог получен: {len(items)} позиций, начинаю запись в БД")

    # Группируем по (brand, flavor) — разные граммовки = варианты одного табака
    grouped: dict[tuple[str, str], list] = defaultdict(list)
    for item in items:
        key = (item.brand.strip(), item.flavor.strip())
        grouped[key].append(item)

    updated = 0
    created = 0

    for (brand, flavor), variants_list in grouped.items():
        # Строим список вариантов
        variants = [
            {
                "grams": v.grams,
                "price": v.price,
                "url": v.url,
                "image_url": v.image_url,
                "in_stock": v.in_stock,
            }
            for v in variants_list
        ]
        # Табак считается в наличии если хоть один вариант есть
        in_stock = any(v.in_stock for v in variants_list)
        full_name = variants_list[0].full_name

        res = await session.execute(
            select(Tobacco).where(
                Tobacco.brand.ilike(brand),
                Tobacco.flavor.ilike(flavor),
            )
        )
        existing = res.scalar_one_or_none()

        if existing:
            existing.in_stock = in_stock
            existing.variants = variants
            existing.full_name = full_name
            existing.updated_at = datetime.now(timezone.utc)
            updated += 1
        else:
            session.add(Tobacco(
                brand=brand,
                flavor=flavor,
                full_name=full_name,
                in_stock=in_stock,
                variants=variants,
            ))
            created += 1

    await session.commit()
    logger.info(f"sync_from_catalog завершён: обновлено {updated}, создано {created}")
    return updated, created


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
