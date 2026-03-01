from aiogram import Router, F, Bot
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from db.database import async_session
from db.models import Mix, MixTobacco, Tobacco
from bot.services.mix_service import get_recent_mixes, update_mix_availability
from bot.services.tobacco_service import sync_brand_tobaccos, sync_all_brands
from bot.parsers.site_parser import check_single_tobacco
from sqlalchemy import select
from datetime import datetime, timezone, timedelta
import logging

logger = logging.getLogger(__name__)
router = Router()

VLD_TZ = timezone(timedelta(hours=10))


class RenameState(StatesGroup):
    waiting_for_title = State()


def vld_time(dt: datetime | None) -> str:
    if not dt:
        return "—"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(VLD_TZ).strftime('%d.%m %H:%M')


def mix_status_icon(mix: Mix) -> str:
    if mix.is_available is True:
        return "✅"
    elif mix.is_available is False:
        return "❌"
    return "⏳"


def mix_display_name(mix: Mix) -> str:
    """Показывает title если задан, иначе tobaccos_summary."""
    return mix.title or mix.tobaccos_summary or mix.code


def build_mix_keyboard(mix_code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="📋 Состав", callback_data=f"mix:{mix_code}"),
        InlineKeyboardButton(text="🔄 Обновить", callback_data=f"recheck:{mix_code}"),
    ]])


def build_mix_detail_keyboard(mix_code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔄 Обновить наличие", callback_data=f"recheck:{mix_code}"),
            InlineKeyboardButton(text="✏️ Переименовать", callback_data=f"rename:{mix_code}"),
        ],
        [
            InlineKeyboardButton(text="◀️ Все миксы", callback_data="mixes:0"),
        ]
    ])


def build_mixes_list_keyboard(mixes: list, page: int = 0, page_size: int = 5) -> InlineKeyboardMarkup:
    buttons = []
    start = page * page_size
    chunk = mixes[start:start + page_size]

    for mix in chunk:
        icon = mix_status_icon(mix)
        name = mix_display_name(mix)
        label = f"{icon} {mix.code} — {name[:35]}"
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"mix:{mix.code}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"mixes:{page - 1}"))
    if start + page_size < len(mixes):
        nav.append(InlineKeyboardButton(text="Вперёд ▶️", callback_data=f"mixes:{page + 1}"))
    if nav:
        buttons.append(nav)

    return InlineKeyboardMarkup(inline_keyboard=buttons)


async def _get_all_mixes() -> list[Mix]:
    async with async_session() as session:
        result = await session.execute(
            select(Mix).order_by(Mix.origin_date.desc()).limit(50)
        )
        return result.scalars().all()


async def _render_mixes_page(mixes: list, page: int) -> tuple[str, InlineKeyboardMarkup]:
    page_size = 5
    start = page * page_size
    chunk = mixes[start:start + page_size]
    total = len(mixes)

    lines = [f"📋 <b>Миксы</b> ({start + 1}–{min(start + page_size, total)} из {total}):\n"]
    for mix in chunk:
        icon = mix_status_icon(mix)
        name = mix_display_name(mix)
        date = vld_time(mix.origin_date)
        upd = vld_time(mix.updated_at)
        # Если есть своё название — показываем его жирным, под ним состав
        if mix.title:
            lines.append(
                f"{icon} <code>{mix.code}</code>  {date}\n"
                f"   <b>{mix.title}</b>\n"
                f"   {mix.tobaccos_summary or '—'}\n"
                f"   🕐 {upd} (Влад.)\n"
            )
        else:
            lines.append(
                f"{icon} <code>{mix.code}</code>  {date}\n"
                f"   {name}\n"
                f"   🕐 {upd} (Влад.)\n"
            )

    keyboard = build_mixes_list_keyboard(mixes, page)
    return '\n'.join(lines), keyboard


# ─── Команды ───────────────────────────────────────────────

@router.message(Command("mixes"))
async def cmd_mixes(message: Message):
    mixes = await _get_all_mixes()
    if not mixes:
        await message.answer("Миксов пока нет. Перешли мне сообщение с рецептом!")
        return
    text, keyboard = await _render_mixes_page(mixes, 0)
    await message.answer(text, parse_mode="HTML", reply_markup=keyboard)


@router.callback_query(F.data.startswith("mixes:"))
async def cb_mixes_page(callback: CallbackQuery):
    page = int(callback.data.split(":", 1)[1])
    mixes = await _get_all_mixes()
    if not mixes:
        await callback.answer("Миксов нет", show_alert=True)
        return
    text, keyboard = await _render_mixes_page(mixes, page)
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    except Exception:
        pass
    await callback.answer()


@router.callback_query(F.data.startswith("mix:"))
async def cb_mix_detail(callback: CallbackQuery):
    mix_code = callback.data.split(":", 1)[1]
    async with async_session() as session:
        result = await session.execute(select(Mix).where(Mix.code == mix_code))
        mix = result.scalar_one_or_none()
        if not mix:
            await callback.answer("Микс не найден", show_alert=True)
            return

        result = await session.execute(
            select(MixTobacco).where(MixTobacco.mix_id == mix.id).order_by(MixTobacco.id)
        )
        entries = result.scalars().all()

        tobacco_ids = [e.tobacco_id for e in entries if e.tobacco_id]
        tobaccos = {}
        if tobacco_ids:
            res = await session.execute(select(Tobacco).where(Tobacco.id.in_(tobacco_ids)))
            for t in res.scalars().all():
                tobaccos[t.id] = t

    icon = mix_status_icon(mix)
    # Заголовок: своё название или код
    if mix.title:
        header = f"{icon} <b>{mix.title}</b>  <code>{mix.code}</code>"
    else:
        header = f"{icon} <b>Микс <code>{mix.code}</code></b>"

    lines = [header, ""]

    for entry in entries:
        pct = f" — {int(entry.percentage)}%" if entry.percentage else ""
        t = tobaccos.get(entry.tobacco_id)

        if t and t.variants:
            lines.append(f"<b>{entry.raw_brand} {entry.raw_flavor}</b>{pct}")
            for v in t.variants:
                grams = v.get("grams") or "?"
                url = v.get("url", "")
                s = "✅" if v.get("in_stock") else "❌"
                lines.append(f"  {s} <a href=\"{url}\">{grams}</a>" if url else f"  {s} {grams}")
        elif t:
            s = "✅" if t.in_stock else ("❌" if t.in_stock is False else "⏳")
            lines.append(f"{s} <b>{entry.raw_brand} {entry.raw_flavor}</b>{pct}")
        else:
            lines.append(f"❓ <b>{entry.raw_brand} {entry.raw_flavor}</b>{pct} — не найден")

    lines.append(f"\n🕐 Проверено: {vld_time(mix.updated_at)} (Влад.)")

    try:
        await callback.message.edit_text(
            '\n'.join(lines), parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=build_mix_detail_keyboard(mix_code)
        )
    except Exception:
        await callback.message.answer(
            '\n'.join(lines), parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=build_mix_detail_keyboard(mix_code)
        )
    await callback.answer()


# ─── Переименование ────────────────────────────────────────

@router.callback_query(F.data.startswith("rename:"))
async def cb_rename_start(callback: CallbackQuery, state: FSMContext):
    mix_code = callback.data.split(":", 1)[1]
    await state.set_state(RenameState.waiting_for_title)
    await state.update_data(mix_code=mix_code)

    async with async_session() as session:
        result = await session.execute(select(Mix).where(Mix.code == mix_code))
        mix = result.scalar_one_or_none()

    current = f"\nТекущее: <b>{mix.title}</b>" if mix and mix.title else ""
    await callback.message.answer(
        f"✏️ Введи новое название для микса <code>{mix_code}</code>{current}\n\n"
        f"Или /cancel чтобы отменить.",
        parse_mode="HTML"
    )
    await callback.answer()


@router.message(StateFilter(RenameState.waiting_for_title))
async def process_rename(message: Message, state: FSMContext):
    data = await state.get_data()
    mix_code = data.get("mix_code")
    new_title = message.text.strip()

    if not mix_code:
        await state.clear()
        return

    async with async_session() as session:
        result = await session.execute(select(Mix).where(Mix.code == mix_code))
        mix = result.scalar_one_or_none()
        if not mix:
            await message.answer("Микс не найден.")
            await state.clear()
            return

        mix.title = new_title
        mix.updated_at = datetime.now(timezone.utc)
        await session.commit()

    await state.clear()
    await message.answer(
        f"✅ Микс <code>{mix_code}</code> переименован в <b>{new_title}</b>",
        parse_mode="HTML",
        reply_markup=build_mix_detail_keyboard(mix_code)
    )


@router.message(Command("cancel"), StateFilter(RenameState.waiting_for_title))
async def cmd_cancel_rename(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Переименование отменено.")


# ─── Остальные команды ─────────────────────────────────────

@router.callback_query(F.data.startswith("recheck:"))
async def cb_recheck(callback: CallbackQuery):
    mix_code = callback.data.split(":", 1)[1]
    await callback.answer("🔄 Обновляю...")

    async with async_session() as session:
        result = await session.execute(select(Mix).where(Mix.code == mix_code))
        mix = result.scalar_one_or_none()
        if not mix:
            await callback.message.answer("Микс не найден")
            return
        result = await session.execute(select(MixTobacco).where(MixTobacco.mix_id == mix.id))
        entries = result.scalars().all()

    try:
        await callback.message.edit_text(
            f"🔄 Обновляю наличие для <code>{mix_code}</code>...", parse_mode="HTML"
        )
    except Exception:
        pass

    async with async_session() as session:
        for entry in entries:
            try:
                variants = await check_single_tobacco(entry.raw_brand, entry.raw_flavor)
                res = await session.execute(
                    select(Tobacco).where(
                        Tobacco.brand.ilike(entry.raw_brand),
                        Tobacco.flavor.ilike(entry.raw_flavor)
                    )
                )
                t = res.scalar_one_or_none()
                if t and variants:
                    t.in_stock = any(v.in_stock for v in variants)
                    t.variants = [{"grams": v.grams, "url": v.url, "in_stock": v.in_stock} for v in variants]
                    t.updated_at = datetime.now(timezone.utc)
                    await session.commit()
            except Exception as e:
                logger.error(f"recheck {entry.raw_brand} {entry.raw_flavor}: {e}")

        mix_obj = await update_mix_availability(session, mix.id)

    icon = mix_status_icon(mix_obj)
    name = mix_display_name(mix_obj)
    status = "Все есть!" if mix_obj.is_available else f"Не хватает: {mix_obj.missing_tobaccos or '?'}"
    try:
        await callback.message.edit_text(
            f"{icon} <b>{name}</b>  <code>{mix_code}</code>\n{status}\n🕐 {vld_time(mix_obj.updated_at)} (Влад.)",
            parse_mode="HTML",
            reply_markup=build_mix_detail_keyboard(mix_code)
        )
    except Exception:
        pass


@router.message(Command("check"))
async def cmd_check(message: Message):
    async with async_session() as session:
        mixes = await get_recent_mixes(session, limit=1)
        if not mixes:
            await message.answer("Миксов нет.")
            return
        mix = await update_mix_availability(session, mixes[0].id)

    icon = mix_status_icon(mix)
    name = mix_display_name(mix)
    status = "все есть!" if mix.is_available else f"не хватает: {mix.missing_tobaccos or '?'}"
    await message.answer(
        f"{icon} <b>{name}</b>  <code>{mix.code}</code>: {status}",
        parse_mode="HTML",
        reply_markup=build_mix_keyboard(mix.code)
    )


@router.message(Command("tobaccos"))
async def cmd_tobaccos(message: Message):
    async with async_session() as session:
        result = await session.execute(select(Tobacco).order_by(Tobacco.brand, Tobacco.flavor))
        tobaccos = result.scalars().all()

    if not tobaccos:
        await message.answer("База пуста.")
        return

    lines = ["🗂 <b>Все табаки:</b>\n"]
    current_brand = None
    for t in tobaccos:
        if t.brand != current_brand:
            current_brand = t.brand
            lines.append(f"\n<b>{t.brand}</b>")
        if t.variants:
            parts = []
            for v in t.variants:
                grams = v.get("grams") or "?"
                url = v.get("url", "")
                s = "✅" if v.get("in_stock") else "❌"
                parts.append(f'{s} <a href="{url}">{grams}</a>' if url else f"{s} {grams}")
            lines.append(f"  {t.flavor}: {' | '.join(parts)}")
        else:
            s = "✅" if t.in_stock else ("❌" if t.in_stock is False else "⏳")
            lines.append(f"  {s} {t.flavor}")

    text = '\n'.join(lines)
    for i in range(0, len(text), 4000):
        await message.answer(text[i:i + 4000], parse_mode="HTML", disable_web_page_preview=True)


@router.message(Command("update"))
async def cmd_update(message: Message):
    async with async_session() as session:
        result = await session.execute(select(Tobacco).order_by(Tobacco.brand))
        tobaccos = result.scalars().all()

    if not tobaccos:
        await message.answer("База пуста.")
        return

    msg = await message.answer(f"🔄 Обновляю {len(tobaccos)} табаков...")
    updated, failed = 0, 0

    async with async_session() as session:
        for tobacco in tobaccos:
            try:
                variants = await check_single_tobacco(tobacco.brand, tobacco.flavor)
                res = await session.execute(select(Tobacco).where(Tobacco.id == tobacco.id))
                t = res.scalar_one_or_none()
                if t and variants:
                    t.in_stock = any(v.in_stock for v in variants)
                    t.variants = [{"grams": v.grams, "url": v.url, "in_stock": v.in_stock} for v in variants]
                    t.updated_at = datetime.now(timezone.utc)
                    await session.commit()
                    updated += 1
                else:
                    failed += 1
            except Exception as e:
                logger.error(f"update {tobacco.brand} {tobacco.flavor}: {e}")
                failed += 1

        mixes_res = await session.execute(select(Mix))
        for mix in mixes_res.scalars().all():
            await update_mix_availability(session, mix.id)

    await msg.edit_text(f"✅ Готово! Обновлено: {updated} | Не найдено: {failed}")


@router.message(Command("sync"))
async def cmd_sync(message: Message):
    args = message.text.split()
    if len(args) > 1:
        brand_name = ' '.join(args[1:])
        KNOWN_BRANDS = [
            "MustHave", "Muassel", "Jent", "Darkside", "Spectrum", "Overdose",
            "Burn Black", "Starline", "Adalya", "Endorphin", "Brusko", "Chaba",
            "Chabacco", "Satyr", "Snobless", "Trofimoff", "Morpheus", "Husky",
            "Joy", "DEUS", "Dogma", "DUFT", "НАШ", "Душа", "Сарма"
        ]
        matched = next((b for b in KNOWN_BRANDS if b.lower() == brand_name.lower()), None)
        if not matched:
            await message.answer(f"Бренд <code>{brand_name}</code> не найден.", parse_mode="HTML")
            return
        msg = await message.answer(f"🔄 <b>{matched}</b>...", parse_mode="HTML")
        async with async_session() as session:
            updated, created = await sync_brand_tobaccos(session, matched)
        await msg.edit_text(f"✅ <b>{matched}</b>: обновлено {updated}, добавлено {created}", parse_mode="HTML")
    else:
        msg = await message.answer("🔄 Синхронизирую все бренды...")
        async with async_session() as session:
            updated, created = await sync_all_brands(session)
        await msg.edit_text(f"✅ Готово! Обновлено: {updated}, добавлено: {created}")


@router.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer("""
🌿 <b>MixCheck Bot</b>

Перешли сообщение с миксом или фото с банками — проверю наличие на Металлургической д1.

<b>Команды:</b>
/mixes — список миксов (нажми на микс чтобы открыть состав)
/check — статус последнего микса
/update — обновить наличие всех табаков
/tobaccos — все табаки со ссылками
/sync — синхронизация с сайтом
/help — справка
""", parse_mode="HTML")
