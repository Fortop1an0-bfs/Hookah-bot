import html
import logging

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from bot.agents.research_agent import run_research, MAX_ATTEMPTS
from bot.handlers.commands import build_mix_keyboard

logger = logging.getLogger(__name__)
router = Router()


@router.message(Command("research"))
async def cmd_research(message: Message):
    args = message.text.split()

    # /research [N] [web]
    count = 5
    internet_only = False
    for arg in args[1:]:
        if arg.lower() == "web":
            internet_only = True
        else:
            try:
                count = max(1, min(int(arg), 15))
            except ValueError:
                pass

    mode_label = "🌐 только интернет" if internet_only else "🤖 AI + интернет"
    status_msg = await message.answer(
        f"🔍 Ищу <b>{count}</b> миксов в наличии ({mode_label})...",
        parse_mode="HTML",
    )

    async def on_progress(text: str):
        try:
            await status_msg.edit_text(text, parse_mode="HTML")
        except Exception:
            pass

    try:
        report = await run_research(count=count, on_progress=on_progress, internet_only=internet_only)
    except Exception as e:
        logger.error(f"Research agent error: {e}", exc_info=True)
        await status_msg.edit_text(f"❌ Ошибка агента: {html.escape(str(e))}", parse_mode="HTML")
        return

    saved = report.saved_count

    if saved == 0:
        await status_msg.edit_text(
            f"😕 Агент проверил {report.attempts} миксов — ни один не оказался полностью в наличии.\n\n"
            f"Попробуй позже или пополни ассортимент магазина.",
            parse_mode="HTML",
        )
        return

    # Итоговое сообщение
    if saved >= count:
        header = f"✅ Агент нашёл <b>{saved}</b> миксов в наличии!"
    else:
        header = f"🔍 Агент нашёл <b>{saved} из {count}</b> — больше не удалось подобрать за {report.attempts} попыток."

    await status_msg.edit_text(header, parse_mode="HTML")

    # Карточка для каждого найденного микса
    for r in report.saved:
        compact_recipe = r.recipe.replace("\n", "\n   ")
        source_line = f"\n📌 Источник: <i>{html.escape(r.source)}</i>" if r.source else ""
        await message.answer(
            f"🌿 <b>{html.escape(r.title)}</b>  <code>{r.mix_code}</code>\n"
            f"   <i>{html.escape(compact_recipe)}</i>"
            f"{source_line}",
            parse_mode="HTML",
            reply_markup=build_mix_keyboard(r.mix_code),
        )
