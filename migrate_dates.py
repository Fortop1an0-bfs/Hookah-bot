"""
Миграция: переименование и добавление колонок origin_date / updated_at
Запускать: python migrate_dates.py
"""
import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

SQL = """
-- Таблица tobaccos: переименовываем created_at -> origin_date, добавляем updated_at
ALTER TABLE tobaccos RENAME COLUMN created_at TO origin_date;

ALTER TABLE tobaccos
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();

UPDATE tobaccos SET updated_at = origin_date WHERE updated_at IS NULL;

COMMENT ON COLUMN tobaccos.origin_date IS 'Дата и время первого появления табака в системе';
COMMENT ON COLUMN tobaccos.updated_at IS 'Дата и время последнего обновления (проверка наличия, изменение статуса)';

-- Таблица mixes: переименовываем created_at -> origin_date, checked_at -> updated_at
ALTER TABLE mixes RENAME COLUMN created_at TO origin_date;
ALTER TABLE mixes RENAME COLUMN checked_at TO updated_at;

UPDATE mixes SET updated_at = origin_date WHERE updated_at IS NULL;

COMMENT ON COLUMN mixes.origin_date IS 'Дата и время первого сохранения микса в систему';
COMMENT ON COLUMN mixes.updated_at IS 'Дата и время последнего обновления строки (проверка наличия, изменение статуса)';
"""

async def main():
    conn = await asyncpg.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        user=os.getenv("DB_USER", "hookah"),
        password=os.getenv("DB_PASSWORD", "hookah123"),
        database=os.getenv("DB_NAME", "hookah_db"),
    )

    for statement in SQL.strip().split(";"):
        stmt = statement.strip()
        if not stmt or stmt.startswith("--"):
            continue
        try:
            await conn.execute(stmt)
            print(f"✅ {stmt[:80]}")
        except Exception as e:
            print(f"⚠️  {stmt[:60]} -> {e}")

    await conn.close()
    print("\n🎉 Миграция завершена!")

if __name__ == "__main__":
    asyncio.run(main())
