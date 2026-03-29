import asyncio
import logging
import os
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.types import BotCommand

from db.database import init_db
from bot.handlers import mix_handler, commands, research_handler

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def main():
    bot = Bot(
        token=os.getenv("BOT_TOKEN"),
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp = Dispatcher(storage=MemoryStorage())

    # Регистрируем роутеры
    dp.include_router(commands.router)
    dp.include_router(research_handler.router)
    dp.include_router(mix_handler.router)

    # Инициализируем БД
    logger.info("Инициализация БД...")
    await init_db()
    logger.info("БД готова!")

    # Устанавливаем меню команд в Telegram
    await bot.set_my_commands([
        BotCommand(command="mixes",    description="📋 Список сохранённых миксов"),
        BotCommand(command="research", description="🤖 Агент ищет миксы по наличию"),
        BotCommand(command="check",    description="🔍 Статус последнего микса"),
        BotCommand(command="update",   description="🔄 Обновить наличие всех табаков"),
        BotCommand(command="tobaccos", description="🗂 Все табаки со ссылками"),
        BotCommand(command="sync",       description="⚙️ Синхронизация с сайтом"),
        BotCommand(command="syncstore",  description="🏪 Спарсить миксы магазина"),
        BotCommand(command="storemixes", description="🏪 Миксы магазина в наличии"),
        BotCommand(command="usage",      description="📊 Использование API"),
        BotCommand(command="help",      description="❓ Справка"),
    ])
    logger.info("Меню команд установлено")

    logger.info("Запуск бота...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
