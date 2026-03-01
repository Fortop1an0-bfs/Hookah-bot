from aiogram import Router, F, Bot
from aiogram.types import Message
from bot.handlers.commands import build_mix_keyboard, vld_time
from db.database import async_session
from db.models import Tobacco
from bot.parsers.message_parser import parse_mix_from_text
from bot.services.mix_service import get_or_create_mix, update_mix_availability
from bot.parsers.site_parser import check_single_tobacco, identify_tobaccos_from_image, TobaccoStockInfo
from sqlalchemy import select
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)
router = Router()


async def get_photo_url(message: Message, bot: Bot) -> str | None:
    if message.photo:
        file = await bot.get_file(message.photo[-1].file_id)
        return f"https://api.telegram.org/file/bot{bot.token}/{file.file_path}"
    return None


@router.message(F.text | F.photo | F.forward_from | F.forward_from_chat)
async def handle_message(message: Message, bot: Bot):
    # Текст из сообщения или подписи к фото
    text = message.text or message.caption or ""

    # Сначала пробуем распознать текст
    parsed = parse_mix_from_text(text) if text else None
    has_text_tobaccos = parsed and parsed.tobaccos

    # Vision запускаем ТОЛЬКО если текст ничего не дал
    photo_tobaccos = []
    if not has_text_tobaccos and message.photo:
        photo_url = await get_photo_url(message, bot)
        if photo_url:
            photo_tobaccos = await identify_tobaccos_from_image(photo_url)

    if not has_text_tobaccos and not photo_tobaccos:
        return

    source = None
    if message.forward_from_chat:
        source = message.forward_from_chat.title or str(message.forward_from_chat.id)
    elif message.forward_from:
        source = message.forward_from.full_name

    status_msg = await message.reply("🔍 Распознал микс, проверяю наличие на сайте...")

    async with async_session() as session:
        if parsed and parsed.tobaccos:
            mix, is_new = await get_or_create_mix(session, parsed, source_channel=source)
            tobaccos_to_check = [(pt.raw_brand, pt.raw_flavor) for pt in parsed.tobaccos]
            # Сохраняем граммовку из парсера если есть
            for pt in parsed.tobaccos:
                if pt.grams:
                    logger.info(f"Указана граммовка: {pt.raw_brand} {pt.raw_flavor} = {pt.grams}")
        else:
            from bot.parsers.message_parser import ParsedMix, ParsedTobacco
            fake_tobaccos = [
                ParsedTobacco(raw_brand=t["brand"], raw_flavor=t["flavor"], percentage=0)
                for t in photo_tobaccos
            ]
            parsed = ParsedMix(tobaccos=fake_tobaccos, original_text=text or "[фото]")
            mix, is_new = await get_or_create_mix(session, parsed, source_channel=source)
            tobaccos_to_check = [(t["brand"], t["flavor"]) for t in photo_tobaccos]

        if not is_new:
            await status_msg.edit_text(
                f"🔄 Микс <code>{mix.code}</code> уже есть в базе, обновляю наличие...",
                parse_mode="HTML"
            )

        # Результаты проверки: brand+flavor -> список граммовок
        # {(brand, flavor): [TobaccoStockInfo, ...]}
        check_results: dict[tuple, list[TobaccoStockInfo]] = {}

        for raw_brand, raw_flavor in tobaccos_to_check:
            try:
                variants = await check_single_tobacco(raw_brand, raw_flavor)
                check_results[(raw_brand, raw_flavor)] = variants

                # Сохраняем в БД — берём первую граммовку как основную запись
                result = await session.execute(
                    select(Tobacco).where(
                        Tobacco.brand.ilike(raw_brand),
                        Tobacco.flavor.ilike(raw_flavor)
                    )
                )
                tobacco = result.scalar_one_or_none()

                if tobacco and variants:
                    any_in_stock = any(v.in_stock for v in variants)
                    tobacco.in_stock = any_in_stock
                    tobacco.variants = [
                        {"grams": v.grams, "url": v.url, "in_stock": v.in_stock}
                        for v in variants
                    ]
                    tobacco.updated_at = datetime.now(timezone.utc)
                    await session.commit()

                    status_icon = '✅' if any_in_stock else '❌'
                    logger.info(f"{raw_brand} {raw_flavor}: {status_icon} граммовок={len(variants)}")
                elif not variants:
                    logger.warning(f"Не найдено на сайте: {raw_brand} {raw_flavor}")

            except Exception as e:
                logger.error(f"Ошибка проверки {raw_brand} {raw_flavor}: {e}")
                check_results[(raw_brand, raw_flavor)] = []

        mix = await update_mix_availability(session, mix.id)

    # --- Формируем сообщение ---
    icon = "🌿" if is_new else "🔄"
    verb = "сохранён" if is_new else "обновлён"
    lines = [f"{icon} <b>Микс {verb}!</b> Код: <code>{mix.code}</code>\n"]

    for pt in parsed.tobaccos:
        pct = f" — {int(pt.percentage)}%" if pt.percentage else ""
        variants = check_results.get((pt.raw_brand, pt.raw_flavor), [])

        if not variants:
            lines.append(f"❓ <b>{pt.raw_brand} {pt.raw_flavor}</b>{pct} — нет в магазине")
            continue

        # Группируем граммовки по наличию
        in_stock = [v for v in variants if v.in_stock]
        out_stock = [v for v in variants if not v.in_stock]

        # Название берём от первого варианта
        site_name = variants[0].full_name
        lines.append(f"\n<b>{pt.raw_brand} {pt.raw_flavor}</b>{pct}")

        if in_stock:
            grams_links = ' | '.join(
                f'<a href="{v.url}">{v.grams or "?"}</a>' for v in in_stock
            )
            lines.append(f"  ✅ Есть: {grams_links}")

        if out_stock:
            grams_list = ', '.join(v.grams or "?" for v in out_stock)
            lines.append(f"  ❌ Нет: {grams_list}")

    lines.append("")
    if mix.is_available is True:
        lines.append("✅ <b>Всё есть на Металлургической д1!</b>")
    elif mix.is_available is False:
        lines.append("❌ <b>Чего-то не хватает</b> на Металлургической д1")
    else:
        lines.append("⏳ Наличие не удалось проверить")

    if not is_new and mix.updated_at:
        lines.append(f"\n🕐 Обновлено: {vld_time(mix.updated_at)} (Влад.)")

    await status_msg.edit_text(
        '\n'.join(lines),
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=build_mix_keyboard(mix.code)
    )
