"""
Агент-исследователь: ищет миксы через AI + интернет, проверяет по БД, сохраняет доступные.
"""

import logging
from dataclasses import dataclass, field
from typing import Callable, Awaitable, Optional

from playwright.async_api import async_playwright
from sqlalchemy import select, func

from bot.agents.web_researcher import generate_mixes
from bot.agents.internet_searcher import search_internet_mixes
from bot.parsers.message_parser import parse_mix_from_text, BRAND_ALIASES
from bot.services.mix_service import get_or_create_mix, update_mix_availability
from db.database import async_session
from db.models import Tobacco

logger = logging.getLogger(__name__)

ProgressCallback = Callable[[str], Awaitable[None]]

MAX_ATTEMPTS = 60
BATCH_SIZE = 8


@dataclass
class MixResult:
    title: str
    recipe: str
    saved: bool
    mix_code: Optional[str] = None
    missing: Optional[str] = None
    error: Optional[str] = None
    source: str = "AI"


@dataclass
class ResearchReport:
    target: int
    results: list[MixResult] = field(default_factory=list)
    attempts: int = 0

    @property
    def saved_count(self) -> int:
        return sum(1 for r in self.results if r.saved)

    @property
    def saved(self) -> list[MixResult]:
        return [r for r in self.results if r.saved]


def _normalize_brand(brand: str) -> str:
    lower = brand.lower().strip()
    for alias, canonical in BRAND_ALIASES.items():
        if alias.lower() == lower:
            return canonical
    return brand.strip()


async def _check_db_tobacco(session, brand: str, flavor: str) -> Optional[Tobacco]:
    """Ищет табак в БД: точное совпадение → нечёткое."""
    norm_brand = _normalize_brand(brand)

    # 1. Точный поиск
    res = await session.execute(
        select(Tobacco).where(
            Tobacco.brand.ilike(norm_brand),
            Tobacco.flavor.ilike(flavor),
        )
    )
    t = res.scalar_one_or_none()
    if t:
        return t

    # 2. Бренд точный, вкус содержит
    res = await session.execute(
        select(Tobacco).where(
            Tobacco.brand.ilike(norm_brand),
            Tobacco.flavor.ilike(f"%{flavor}%"),
        )
    )
    results = res.scalars().all()
    if results:
        return min(results, key=lambda t: len(t.flavor))

    # 3. Оба содержат
    res = await session.execute(
        select(Tobacco).where(
            Tobacco.brand.ilike(f"%{norm_brand}%"),
            Tobacco.flavor.ilike(f"%{flavor}%"),
        )
    )
    results = res.scalars().all()
    if results:
        return min(results, key=lambda t: len(t.flavor))

    return None


async def _process_mix(mix_data: dict, idx: int) -> MixResult:
    title = mix_data.get("title", f"Микс {idx}")
    recipe = mix_data.get("recipe", "")
    source = mix_data.get("source", "AI")

    parsed = parse_mix_from_text(recipe)
    if not parsed or not parsed.tobaccos:
        return MixResult(title=title, recipe=recipe, saved=False,
                         error="Не распарсился", source=source)

    all_available = True
    missing_list = []

    async with async_session() as session:
        for pt in parsed.tobaccos:
            try:
                tobacco = await _check_db_tobacco(session, pt.raw_brand, pt.raw_flavor)
                if tobacco is None:
                    all_available = False
                    missing_list.append(f"{pt.raw_brand} {pt.raw_flavor} (нет в каталоге)")
                    logger.debug(f"Не найден в БД: '{pt.raw_brand}' '{pt.raw_flavor}'")
                elif not tobacco.in_stock:
                    all_available = False
                    missing_list.append(f"{tobacco.brand} {tobacco.flavor}")
                    logger.debug(f"Нет в наличии: '{tobacco.brand}' '{tobacco.flavor}'")
            except Exception as e:
                logger.error(f"Ошибка проверки {pt.raw_brand} {pt.raw_flavor}: {e}")
                all_available = False
                missing_list.append(f"{pt.raw_brand} {pt.raw_flavor}")

    if not all_available:
        return MixResult(title=title, recipe=recipe, saved=False,
                         missing=", ".join(missing_list), source=source)

    # Всё есть — сохраняем
    source_label = f"🌐 {source}" if source != "AI" else "🤖 Research Agent"
    async with async_session() as session:
        mix, _ = await get_or_create_mix(session, parsed, source_channel=source_label)
        mix.title = title
        await session.commit()
        await update_mix_availability(session, mix.id)

    logger.info(f"✅ [{mix.code}] {title} (источник: {source})")
    return MixResult(title=title, recipe=recipe, saved=True,
                     mix_code=mix.code, source=source)


async def run_research(
    count: int = 5,
    on_progress: Optional[ProgressCallback] = None,
    internet_only: bool = False,
) -> ResearchReport:
    """
    Ищет миксы двумя способами:
    1. Парсит реальные рецепты с hookah.ru и других сайтов
    2. AI генерирует из реальных вкусов в наличии (если internet_only=False)
    Проверка наличия — через таблицу tobaccos (без лишних запросов на сайт).
    """
    # Проверяем что БД не пуста
    async with async_session() as session:
        total_db = await session.scalar(select(func.count()).select_from(Tobacco))
        instock_db = await session.scalar(
            select(func.count()).select_from(Tobacco).where(Tobacco.in_stock == True)
        )

    if not total_db:
        if on_progress:
            await on_progress(
                "⚠️ <b>База табаков пуста!</b>\n"
                "Сначала запусти /sync чтобы загрузить каталог."
            )
        return ResearchReport(target=count)

    report = ResearchReport(target=count)
    internet_queue: list[dict] = []
    internet_fetched = False

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            ignore_https_errors=True,
        )
        page = await context.new_page()

        try:
            # Шаг 1: забираем реальные рецепты из интернета
            if on_progress:
                await on_progress(
                    f"🌐 Ищу рецепты в интернете...\n"
                    f"<i>В наличии: {instock_db} табаков из {total_db}</i>"
                )

            internet_queue = await search_internet_mixes(page, count=count * 3)
            internet_fetched = True

            if on_progress:
                await on_progress(
                    f"🌐 Найдено {len(internet_queue)} рецептов в интернете\n"
                    f"🤖 Запускаю AI генерацию...\n"
                    f"<i>В наличии: {instock_db} табаков из {total_db}</i>"
                )

            # Шаг 2: основной цикл — чередуем интернет и AI
            while report.saved_count < count and report.attempts < MAX_ATTEMPTS:
                need = count - report.saved_count

                # Сначала обрабатываем интернет-рецепты
                while internet_queue and report.saved_count < count:
                    mix_data = internet_queue.pop(0)
                    report.attempts += 1
                    title = mix_data.get("title", f"Микс {report.attempts}")
                    source = mix_data.get("source", "интернет")

                    if on_progress:
                        await on_progress(
                            f"🔍 Найдено <b>{report.saved_count}/{count}</b>\n"
                            f"🌐 Проверяю рецепт с <i>{source}</i>: {title[:40]}"
                        )

                    result = await _process_mix(mix_data, report.attempts)
                    report.results.append(result)

                if report.saved_count >= count:
                    break

                # Если интернет-рецепты закончились — AI генерирует (если не internet_only)
                if internet_only:
                    logger.info("Интернет-рецепты закончились, internet_only=True — останавливаемся")
                    break

                batch_size = min(BATCH_SIZE, need + 3)

                if on_progress:
                    await on_progress(
                        f"🤖 Найдено <b>{report.saved_count}/{count}</b>\n"
                        f"Генерирую {batch_size} вариантов из реальных табаков..."
                    )

                async with async_session() as session:
                    mixes_data = await generate_mixes(session, batch_size)

                if not mixes_data:
                    logger.error("AI не вернул миксы, останавливаемся")
                    break

                for mix_data in mixes_data:
                    if report.saved_count >= count or report.attempts >= MAX_ATTEMPTS:
                        break

                    report.attempts += 1
                    title = mix_data.get("title", f"Микс {report.attempts}")

                    if on_progress:
                        await on_progress(
                            f"🤖 Найдено <b>{report.saved_count}/{count}</b>\n"
                            f"Проверяю: <i>{title}</i>"
                        )

                    result = await _process_mix(mix_data, report.attempts)
                    report.results.append(result)

        finally:
            await browser.close()

    return report
