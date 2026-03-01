import uuid
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_
from db.models import Tobacco, Mix, MixTobacco
from bot.parsers.message_parser import ParsedMix, ParsedTobacco


async def get_or_create_tobacco(session: AsyncSession, brand: str, flavor: str) -> Tobacco:
    """Находит или создаёт запись табака"""
    result = await session.execute(
        select(Tobacco).where(
            and_(
                Tobacco.brand.ilike(brand),
                Tobacco.flavor.ilike(flavor)
            )
        )
    )
    tobacco = result.scalar_one_or_none()

    if not tobacco:
        tobacco = Tobacco(
            brand=brand,
            flavor=flavor,
            full_name=f"{brand} {flavor}"
        )
        session.add(tobacco)
        await session.flush()

    return tobacco


async def get_or_create_mix(session: AsyncSession, parsed_mix: ParsedMix, source_channel: str = None) -> tuple[Mix, bool]:
    """
    Ищет существующий микс с таким же текстом сообщения.
    Если найден — возвращает его (is_new=False).
    Если нет — создаёт новый (is_new=True).
    """
    original_text = parsed_mix.original_text.strip() if parsed_mix.original_text else ""

    # Проверяем дубль по тексту сообщения
    if original_text:
        result = await session.execute(
            select(Mix).where(Mix.original_text == original_text)
        )
        existing = result.scalar_one_or_none()
        if existing:
            return existing, False

    # Составляем краткое описание состава
    parts = []
    for pt in parsed_mix.tobaccos:
        pct = f" {int(pt.percentage)}%" if pt.percentage else ""
        parts.append(f"{pt.raw_brand} {pt.raw_flavor}{pct}")
    summary = ", ".join(parts)

    # Создаём новый микс
    mix = Mix(
        code=str(uuid.uuid4())[:8].upper(),
        original_text=original_text,
        source_channel=source_channel,
        tobaccos_summary=summary
    )
    session.add(mix)
    await session.flush()

    for pt in parsed_mix.tobaccos:
        tobacco = await get_or_create_tobacco(session, pt.raw_brand, pt.raw_flavor)
        session.add(MixTobacco(
            mix_id=mix.id,
            tobacco_id=tobacco.id,
            raw_brand=pt.raw_brand,
            raw_flavor=pt.raw_flavor,
            percentage=pt.percentage
        ))

    await session.commit()
    await session.refresh(mix)
    return mix, True


async def update_mix_availability(session: AsyncSession, mix_id: int) -> Mix:
    """
    Обновляет доступность микса на основе наличия табаков.
    Всегда обновляет updated_at.
    """
    result = await session.execute(select(Mix).where(Mix.id == mix_id))
    mix = result.scalar_one_or_none()
    if not mix:
        return None

    result = await session.execute(
        select(MixTobacco).where(MixTobacco.mix_id == mix_id)
    )
    mix_tobaccos = result.scalars().all()

    missing = []
    all_available = True

    for mt in mix_tobaccos:
        if mt.tobacco_id:
            result = await session.execute(
                select(Tobacco).where(Tobacco.id == mt.tobacco_id)
            )
            tobacco = result.scalar_one_or_none()
            if tobacco and tobacco.in_stock is False:
                missing.append(f"{mt.raw_brand} {mt.raw_flavor}")
                all_available = False
            elif tobacco and tobacco.in_stock is None:
                all_available = False
        else:
            missing.append(f"{mt.raw_brand} {mt.raw_flavor} (не найден)")
            all_available = False

    mix.is_available = all_available
    mix.missing_tobaccos = ', '.join(missing) if missing else None
    mix.updated_at = datetime.utcnow()  # явно обновляем время

    await session.commit()
    await session.refresh(mix)
    return mix


async def get_recent_mixes(session: AsyncSession, limit: int = 10) -> list[Mix]:
    """Последние N миксов по дате создания"""
    result = await session.execute(
        select(Mix).order_by(Mix.origin_date.desc()).limit(limit)
    )
    return result.scalars().all()
