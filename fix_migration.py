"""
Проверяет текущие колонки в БД и применяет нужные изменения.
"""
import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

async def main():
    conn = await asyncpg.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        user=os.getenv("DB_USER", "hookah"),
        password=os.getenv("DB_PASSWORD", "hookah123"),
        database=os.getenv("DB_NAME", "hookah_db"),
    )

    # Смотрим текущие колонки
    for table in ['mixes', 'tobaccos']:
        cols = await conn.fetch("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = $1 ORDER BY ordinal_position
        """, table)
        print(f"\n{table}: {[r['column_name'] for r in cols]}")

    print("\n--- Применяю миграцию ---\n")

    steps = [
        # mixes: created_at -> origin_date
        ("Переименовываем mixes.created_at -> origin_date",
         "ALTER TABLE mixes RENAME COLUMN created_at TO origin_date"),
        # mixes: checked_at -> updated_at
        ("Переименовываем mixes.checked_at -> updated_at",
         "ALTER TABLE mixes RENAME COLUMN checked_at TO updated_at"),
        # mixes: updated_at NOT NULL с дефолтом
        ("Устанавливаем DEFAULT для mixes.updated_at",
         "ALTER TABLE mixes ALTER COLUMN updated_at SET DEFAULT NOW()"),
        ("Заполняем NULL в mixes.updated_at",
         "UPDATE mixes SET updated_at = origin_date WHERE updated_at IS NULL"),

        # tobaccos: created_at -> origin_date
        ("Переименовываем tobaccos.created_at -> origin_date",
         "ALTER TABLE tobaccos RENAME COLUMN created_at TO origin_date"),
        # tobaccos: добавляем updated_at
        ("Добавляем tobaccos.updated_at",
         "ALTER TABLE tobaccos ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()"),
        ("Заполняем tobaccos.updated_at",
         "UPDATE tobaccos SET updated_at = origin_date WHERE updated_at IS NULL"),

        # tobaccos: url
        ("Добавляем tobaccos.url",
         "ALTER TABLE tobaccos ADD COLUMN IF NOT EXISTS url VARCHAR(500)"),

        # Комментарии
        ("Комментарий mixes.origin_date",
         "COMMENT ON COLUMN mixes.origin_date IS 'Дата первого сохранения микса'"),
        ("Комментарий mixes.updated_at",
         "COMMENT ON COLUMN mixes.updated_at IS 'Дата последнего обновления'"),
        ("Комментарий tobaccos.origin_date",
         "COMMENT ON COLUMN tobaccos.origin_date IS 'Дата первого появления табака'"),
        ("Комментарий tobaccos.updated_at",
         "COMMENT ON COLUMN tobaccos.updated_at IS 'Дата последнего обновления'"),
        ("Комментарий tobaccos.url",
         "COMMENT ON COLUMN tobaccos.url IS 'Ссылка на страницу товара на smokyart.ru'"),
    ]

    for desc, sql in steps:
        try:
            await conn.execute(sql)
            print(f"✅ {desc}")
        except Exception as e:
            print(f"⚠️  {desc}: {e}")

    # Показываем итоговые колонки
    print("\n--- Итоговые колонки ---")
    for table in ['mixes', 'tobaccos']:
        cols = await conn.fetch("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = $1 ORDER BY ordinal_position
        """, table)
        print(f"{table}: {[r['column_name'] for r in cols]}")

    await conn.close()
    print("\n🎉 Готово!")

if __name__ == "__main__":
    asyncio.run(main())
