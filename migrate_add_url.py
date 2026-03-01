"""
Добавляет колонку url в таблицу tobaccos (если не существует)
и обновляет комментарии.
Запускать из корня проекта: python migrate_add_url.py
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

    # Добавляем колонку url если не существует
    await conn.execute("""
        ALTER TABLE tobaccos 
        ADD COLUMN IF NOT EXISTS url VARCHAR(500);
    """)
    print("✅ Колонка url добавлена")

    await conn.execute("""
        COMMENT ON COLUMN tobaccos.url IS 'Ссылка на страницу товара на smokyart.ru';
    """)
    print("✅ Комментарий к url добавлен")

    await conn.close()
    print("🎉 Готово!")

if __name__ == "__main__":
    asyncio.run(main())
