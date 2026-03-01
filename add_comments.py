"""
Добавляет комментарии ко всем таблицам и колонкам в БД.
Запускать из корня проекта: python add_comments.py
"""
import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

COMMENTS = """
COMMENT ON TABLE tobaccos IS 'Все известные табаки. Пополняется из миксов и с сайта smokyart.ru';
COMMENT ON TABLE mixes IS 'Миксы для кальяна, распознанные из пересланных сообщений в Telegram';
COMMENT ON TABLE mix_tobaccos IS 'Состав миксов: какие табаки входят в каждый микс и в каком проценте';

COMMENT ON COLUMN tobaccos.id IS 'Уникальный идентификатор табака';
COMMENT ON COLUMN tobaccos.brand IS 'Бренд табака в нормализованном виде. Пример: MustHave, Jent, Darkside';
COMMENT ON COLUMN tobaccos.flavor IS 'Вкус табака. Пример: Berry Holls, follar, инжирный';
COMMENT ON COLUMN tobaccos.full_name IS 'Полное название: бренд + вкус. Пример: MustHave Berry Holls';
COMMENT ON COLUMN tobaccos.in_stock IS 'Наличие на Металлургической д1. True=есть, False=нет, NULL=не проверялось';
COMMENT ON COLUMN tobaccos.grams IS 'Доступные граммовки через запятую. Пример: 50 гр, 125 гр';
COMMENT ON COLUMN tobaccos.last_checked IS 'Дата последней проверки наличия на сайте smokyart.ru';
COMMENT ON COLUMN tobaccos.created_at IS 'Дата первого появления табака в системе';

COMMENT ON COLUMN mixes.id IS 'Уникальный идентификатор микса';
COMMENT ON COLUMN mixes.code IS 'Короткий уникальный код микса. Пример: A1B2C3D4';
COMMENT ON COLUMN mixes.original_text IS 'Оригинальный текст сообщения из Telegram';
COMMENT ON COLUMN mixes.source_channel IS 'Название канала откуда пришло сообщение';
COMMENT ON COLUMN mixes.is_available IS 'True=все табаки есть, False=чего-то не хватает, NULL=не проверялось';
COMMENT ON COLUMN mixes.missing_tobaccos IS 'Перечень отсутствующих табаков через запятую';
COMMENT ON COLUMN mixes.created_at IS 'Дата сохранения микса в систему';
COMMENT ON COLUMN mixes.checked_at IS 'Дата последней проверки наличия табаков для этого микса';

COMMENT ON COLUMN mix_tobaccos.id IS 'Уникальный идентификатор записи';
COMMENT ON COLUMN mix_tobaccos.mix_id IS 'Ссылка на микс из таблицы mixes';
COMMENT ON COLUMN mix_tobaccos.tobacco_id IS 'Ссылка на табак. NULL если табак не распознан в базе';
COMMENT ON COLUMN mix_tobaccos.raw_brand IS 'Бренд как написано в оригинальном сообщении. Пример: musthave, МХ';
COMMENT ON COLUMN mix_tobaccos.raw_flavor IS 'Вкус как написано в оригинальном сообщении';
COMMENT ON COLUMN mix_tobaccos.percentage IS 'Процент табака в миксе. Сумма по миксу должна быть 100';
"""

async def main():
    conn = await asyncpg.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        user=os.getenv("DB_USER", "hookah"),
        password=os.getenv("DB_PASSWORD", "hookah123"),
        database=os.getenv("DB_NAME", "hookah_db"),
    )

    for statement in COMMENTS.strip().split(";"):
        statement = statement.strip()
        if statement:
            await conn.execute(statement)
            print(f"✅ {statement[:80]}...")

    await conn.close()
    print("\n🎉 Все комментарии добавлены!")

if __name__ == "__main__":
    asyncio.run(main())
