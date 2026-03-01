"""
Миграция: добавление mixes.title
Запуск: python migrate_v3.py
"""
import asyncio, asyncpg, os
from dotenv import load_dotenv
load_dotenv()

async def main():
    conn = await asyncpg.connect(
        host=os.getenv("DB_HOST", "localhost"), port=int(os.getenv("DB_PORT", 5432)),
        user=os.getenv("DB_USER", "hookah"), password=os.getenv("DB_PASSWORD", "hookah123"),
        database=os.getenv("DB_NAME", "hookah_db"),
    )
    await conn.execute("ALTER TABLE mixes ADD COLUMN IF NOT EXISTS title VARCHAR(200)")
    await conn.execute("COMMENT ON COLUMN mixes.title IS 'Пользовательское название микса, заданное вручную'")
    print("✅ Колонка mixes.title добавлена")
    await conn.close()

asyncio.run(main())
