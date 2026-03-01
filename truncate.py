import asyncio, asyncpg, os
from dotenv import load_dotenv
load_dotenv()

async def main():
    conn = await asyncpg.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=int(os.getenv('DB_PORT', 5432)),
        user=os.getenv('DB_USER', 'hookah'),
        password=os.getenv('DB_PASSWORD', 'hookah123'),
        database=os.getenv('DB_NAME', 'hookah_db')
    )
    await conn.execute('TRUNCATE TABLE mix_tobaccos, mixes, tobaccos RESTART IDENTITY CASCADE')
    print('Таблицы очищены')
    await conn.close()

asyncio.run(main())
