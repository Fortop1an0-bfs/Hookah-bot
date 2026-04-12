"""
Equipment Scraper — Кальяны, чаши, угли, калауды
Источники:
  - alibaba-market.ru: кальяны (cat=1), угли (cat=6)
  - Статический список чаш и HMD со ссылками на фото

Таблица: scraper.ali_equipment
"""
import asyncio, asyncpg, httpx, json, logging, sys, re

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s',
                    handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger(__name__)

DB_DSN   = 'postgresql://hookah:hookah123@localhost:5432/hookah_db'
BASE_URL = 'https://alibaba-market.ru'
CITY_ID  = 4   # Владивосток — там есть кальяны
HEADERS  = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

# ── SCHEMA ───────────────────────────────────────────────────────────────────

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS scraper.ali_equipment (
    id          SERIAL PRIMARY KEY,
    source_id   INT,                       -- ID на alibaba-market.ru (если есть)
    type        TEXT NOT NULL,             -- hookah | bowl | coal | hmd
    name        TEXT NOT NULL,
    brand       TEXT DEFAULT '',
    line        TEXT DEFAULT '',
    price       INT,
    image_url   TEXT DEFAULT '',
    in_stock    BOOLEAN DEFAULT FALSE,
    extra       JSONB DEFAULT '{}',        -- цвет, форма, объём и т.п.
    scraped_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(source_id, type)
);
CREATE INDEX IF NOT EXISTS idx_equip_type  ON scraper.ali_equipment(type);
CREATE INDEX IF NOT EXISTS idx_equip_name  ON scraper.ali_equipment USING gin(to_tsvector('russian', name));
"""

# ── STATIC DATA — чаши и HMD (фото с сайтов производителей) ────────────────

STATIC_BOWLS = [
    {"name":"Убивашка",           "brand":"Generic",      "image_url":"https://kalyan.ru/upload/iblock/ubivashka.jpg"},
    {"name":"Фанел",              "brand":"Generic",      "image_url":"https://kalyan.ru/upload/iblock/phunnel.jpg"},
    {"name":"Oblako Bowl M",      "brand":"Oblako",       "image_url":"https://oblako.pro/images/bowl_m.jpg"},
    {"name":"Oblako Bowl PHUNNEL","brand":"Oblako",       "image_url":"https://oblako.pro/images/phunnel.jpg"},
    {"name":"Alpaca Bowl Rook",   "brand":"Alpaca",       "image_url":"https://alpacabowls.com/images/rook.jpg"},
    {"name":"Alpaca Bowl Genie",  "brand":"Alpaca",       "image_url":"https://alpacabowls.com/images/genie.jpg"},
    {"name":"Fremium Bowl",       "brand":"Fremium",      "image_url":"https://kalyan.ru/upload/iblock/fremium.jpg"},
    {"name":"Triton Bowl",        "brand":"Triton",       "image_url":"https://kalyan.ru/upload/iblock/triton.jpg"},
    {"name":"Vessel Bowl",        "brand":"Vessel",       "image_url":"https://kalyan.ru/upload/iblock/vessel.jpg"},
    {"name":"Labrax Bowl",        "brand":"Labrax",       "image_url":"https://kalyan.ru/upload/iblock/labrax.jpg"},
    {"name":"Harmony Bowl",       "brand":"Harmony",      "image_url":"https://kalyan.ru/upload/iblock/harmony.jpg"},
    {"name":"Wookah Bowl",        "brand":"Wookah",       "image_url":"https://wookah.com/media/bowl.jpg"},
    {"name":"Amy Bowl",           "brand":"Amy Deluxe",   "image_url":"https://kalyan.ru/upload/iblock/amy_bowl.jpg"},
    {"name":"Moze Glaze Bowl",    "brand":"Moze",         "image_url":"https://kalyan.ru/upload/iblock/moze.jpg"},
]

STATIC_HMD = [
    {"name":"Kaloud Lotus",       "brand":"Kaloud",       "image_url":"https://kaloud.com/cdn/shop/products/lotus.jpg"},
    {"name":"Kaloud Lotus+",      "brand":"Kaloud",       "image_url":"https://kaloud.com/cdn/shop/products/lotus_plus.jpg"},
    {"name":"Kaloud Lotus 2",     "brand":"Kaloud",       "image_url":"https://kaloud.com/cdn/shop/products/lotus2.jpg"},
    {"name":"Provost HMD",        "brand":"Provost",      "image_url":"https://kalyan.ru/upload/iblock/provost.jpg"},
    {"name":"Ignis HMD",          "brand":"Ignis",        "image_url":"https://kalyan.ru/upload/iblock/ignis.jpg"},
    {"name":"Mig HMD",            "brand":"Mig",          "image_url":"https://kalyan.ru/upload/iblock/mig_hmd.jpg"},
    {"name":"Oblako HMD",         "brand":"Oblako",       "image_url":"https://oblako.pro/images/hmd.jpg"},
    {"name":"Kaloud Nimbus",      "brand":"Kaloud",       "image_url":"https://kaloud.com/cdn/shop/products/nimbus.jpg"},
]

# Форма колбы — справочник
FLASK_SHAPES = ["Классика", "Египет", "Колба", "Цилиндр", "Граната", "Шар", "Конус"]
FLASK_COLORS = ["Чёрный", "Прозрачный", "Синий", "Зелёный", "Красный", "Белый",
                "Золотой", "Розовый", "Бронза", "Серебро", "Фиолетовый"]

# ── SCRAPING ─────────────────────────────────────────────────────────────────

async def fetch_category(client: httpx.AsyncClient, cat_id: int) -> list:
    """Fetch all products of a category (tries multiple cities)."""
    city_ids = [4, 10, 13, 17, 20]
    for city_id in city_ids:
        all_items = []
        offset = 0
        while True:
            url = f"{BASE_URL}/api/products/category/{cat_id}/products?cityId={city_id}&limit=200&offset={offset}"
            try:
                r = await client.get(url, headers=HEADERS, timeout=30)
                if r.status_code != 200: break
                data = r.json()
                if not data: break
                all_items.extend(data)
                if len(data) < 200: break
                offset += 200
            except Exception as e:
                log.warning(f"Error fetching cat {cat_id} city {city_id}: {e}")
                break
        if all_items:
            log.info(f"Cat {cat_id} city {city_id}: {len(all_items)} items")
            return all_items
    return []


async def upsert_equipment(conn, eq_type: str, items: list):
    count = 0
    for p in items:
        imgs = p.get('images', [])
        img_url = (BASE_URL + imgs[0]['url']) if imgs else ''
        brand = ''
        if isinstance(p.get('brand'), dict):
            brand = p['brand'].get('name', '')
        elif isinstance(p.get('brand'), str):
            brand = p['brand']
        await conn.execute("""
            INSERT INTO scraper.ali_equipment
                (source_id, type, name, brand, line, price, image_url, in_stock, extra)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT (source_id, type) DO UPDATE SET
                name=EXCLUDED.name, brand=EXCLUDED.brand, price=EXCLUDED.price,
                image_url=EXCLUDED.image_url, in_stock=EXCLUDED.in_stock, scraped_at=NOW()
        """,
            p['id'], eq_type, p['name'],
            brand, p.get('line',''), p.get('price'),
            img_url, bool(p.get('stores',[])),
            json.dumps({"sku": p.get('sku'), "one_c_id": p.get('one_c_id')})
        )
        count += 1
    return count


async def upsert_static(conn, eq_type: str, items: list):
    count = 0
    for item in items:
        existing = await conn.fetchval(
            "SELECT id FROM scraper.ali_equipment WHERE type=$1 AND name=$2",
            eq_type, item['name']
        )
        if not existing:
            await conn.execute("""
                INSERT INTO scraper.ali_equipment (type, name, brand, image_url, in_stock)
                VALUES ($1,$2,$3,$4,true)
            """, eq_type, item['name'], item.get('brand',''), item.get('image_url',''))
            count += 1
    return count


async def main():
    log.info("Connecting to DB...")
    pool = await asyncpg.create_pool(DB_DSN)
    async with pool.acquire() as conn:
        await conn.execute(CREATE_TABLE)
        log.info("Table created/verified")

    async with httpx.AsyncClient() as client:
        # 1. Кальяны
        log.info("Scraping hookahs (cat=1)...")
        hookahs = await fetch_category(client, 1)
        if hookahs:
            async with pool.acquire() as conn:
                n = await upsert_equipment(conn, 'hookah', hookahs)
            log.info(f"Hookahs: {n} upserted")

        # 2. Угли
        log.info("Scraping coals (cat=6)...")
        coals = await fetch_category(client, 6)
        if coals:
            async with pool.acquire() as conn:
                n = await upsert_equipment(conn, 'coal', coals)
            log.info(f"Coals: {n} upserted")

    # 3. Статические чаши и HMD
    async with pool.acquire() as conn:
        n = await upsert_static(conn, 'bowl', STATIC_BOWLS)
        log.info(f"Bowls: {n} new static items")
        n = await upsert_static(conn, 'hmd', STATIC_HMD)
        log.info(f"HMD: {n} new static items")

    # 4. Итог
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT type, COUNT(*) FROM scraper.ali_equipment GROUP BY type ORDER BY type"
        )
        log.info("=== RESULT ===")
        for r in rows:
            log.info(f"  {r['type']:10s}: {r['count']} items")

    await pool.close()
    log.info("Done!")

if __name__ == '__main__':
    asyncio.run(main())
