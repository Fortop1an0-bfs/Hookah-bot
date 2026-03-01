"""
Миграция: добавление variants (JSON) в tobaccos, tobaccos_summary в mixes,
удаление устаревших last_checked, grams, url из tobaccos.
Запуск: python migrate_v2.py
"""
import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

SQL_STEPS = [
    ("Добавляем tobaccos.variants (JSON)",
     "ALTER TABLE tobaccos ADD COLUMN IF NOT EXISTS variants JSONB"),

    ("Добавляем mixes.tobaccos_summary",
     "ALTER TABLE mixes ADD COLUMN IF NOT EXISTS tobaccos_summary TEXT"),

    ("Удаляем устаревший last_checked",
     "ALTER TABLE tobaccos DROP COLUMN IF EXISTS last_checked"),

    ("Удаляем устаревший grams (теперь в variants)",
     "ALTER TABLE tobaccos DROP COLUMN IF EXISTS grams"),

    ("Удаляем устаревший url (теперь в variants)",
     "ALTER TABLE tobaccos DROP COLUMN IF EXISTS url"),

    ("Индекс на mix_tobaccos.mix_id",
     "CREATE INDEX IF NOT EXISTS idx_mix_tobaccos_mix_id ON mix_tobaccos(mix_id)"),

    ("Комментарий tobaccos.variants",
     "COMMENT ON COLUMN tobaccos.variants IS 'JSON массив граммовок: [{grams, url, in_stock}]'"),

    ("Комментарий mixes.tobaccos_summary",
     "COMMENT ON COLUMN mixes.tobaccos_summary IS 'Краткий состав микса: MustHave Ваниль 30%, Груша 60%'"),
]

async def main():
    conn = await asyncpg.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        user=os.getenv("DB_USER", "hookah"),
        password=os.getenv("DB_PASSWORD", "hookah123"),
        database=os.getenv("DB_NAME", "hookah_db"),
    )

    print("Текущие колонки tobaccos:")
    cols = await conn.fetch(
        "SELECT column_name FROM information_schema.columns WHERE table_name='tobaccos' ORDER BY ordinal_position"
    )
    print([r['column_name'] for r in cols])

    print("\nТекущие колонки mixes:")
    cols = await conn.fetch(
        "SELECT column_name FROM information_schema.columns WHERE table_name='mixes' ORDER BY ordinal_position"
    )
    print([r['column_name'] for r in cols])

    print("\n--- Применяю миграцию ---")
    for desc, sql in SQL_STEPS:
        try:
            await conn.execute(sql)
            print(f"✅ {desc}")
        except Exception as e:
            print(f"⚠️  {desc}: {e}")

    print("\n--- Итог tobaccos ---")
    cols = await conn.fetch(
        "SELECT column_name FROM information_schema.columns WHERE table_name='tobaccos' ORDER BY ordinal_position"
    )
    print([r['column_name'] for r in cols])

    print("\n--- Итог mixes ---")
    cols = await conn.fetch(
        "SELECT column_name FROM information_schema.columns WHERE table_name='mixes' ORDER BY ordinal_position"
    )
    print([r['column_name'] for r in cols])

    await conn.close()
    print("\n🎉 Готово!")

if __name__ == "__main__":
    asyncio.run(main())
