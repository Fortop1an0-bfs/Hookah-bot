"""
Alibaba-market.ru scraper
Scrapes: hookah tobaccos with prices, availability per store/city
API: /api/products/category/2/products?cityId={id}&limit=500&offset={n}
Stores to PostgreSQL schema `scraper`
"""
import asyncio
import logging
import sys
from datetime import datetime
from typing import Optional

import asyncpg
import httpx

sys.stdout.reconfigure(encoding='utf-8')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('alibaba_scraper.log', encoding='utf-8'),
    ]
)
log = logging.getLogger(__name__)

DB_DSN    = 'postgresql://hookah:hookah123@localhost:5432/hookah_db'
DB_SCHEMA = 'scraper'
BASE_URL  = 'https://alibaba-market.ru'
CATEGORY_ID = 2   # Табак для кальяна
PAGE_LIMIT  = 200
DELAY       = 0.5  # секунды между запросами

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json',
    'Referer': 'https://alibaba-market.ru/',
}

# ─── SCHEMA ──────────────────────────────────────────────────────────────────

CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS ali_cities (
    id          INT PRIMARY KEY,
    name        TEXT NOT NULL,
    scraped_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ali_stores (
    id              INT PRIMARY KEY,
    city_id         INT REFERENCES ali_cities(id),
    name            TEXT NOT NULL,
    full_name       TEXT,
    address         TEXT,
    phone           TEXT,
    schedule        TEXT,
    is_discounter   BOOLEAN DEFAULT FALSE,
    store_2gisid    TEXT,
    scraped_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ali_products (
    id                      INT PRIMARY KEY,
    sku                     BIGINT,
    one_c_id                TEXT UNIQUE,
    name                    TEXT NOT NULL,
    brand_id                INT,
    brand_name              TEXT,
    category_id             INT,
    line                    TEXT,
    country                 TEXT,
    weight                  TEXT,
    price                   INT,
    price_before_discount   INT,
    has_discount            BOOLEAN DEFAULT FALSE,
    rating                  NUMERIC(3,1),
    is_bestseller           BOOLEAN DEFAULT FALSE,
    is_premium              BOOLEAN DEFAULT FALSE,
    is_new                  BOOLEAN DEFAULT FALSE,
    description             TEXT,
    short_description       TEXT,
    htreviews_id            TEXT,
    image_url               TEXT,
    in_stock                BOOLEAN DEFAULT FALSE,
    stores_count            INT DEFAULT 0,
    total_amount            INT DEFAULT 0,
    scraped_at              TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ali_product_availability (
    id          SERIAL PRIMARY KEY,
    product_id  INT REFERENCES ali_products(id) ON DELETE CASCADE,
    store_id    INT REFERENCES ali_stores(id) ON DELETE CASCADE,
    city_id     INT REFERENCES ali_cities(id),
    amount      INT DEFAULT 0,
    scraped_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(product_id, store_id)
);

CREATE INDEX IF NOT EXISTS idx_ali_products_brand     ON ali_products(brand_id);
CREATE INDEX IF NOT EXISTS idx_ali_products_stock     ON ali_products(in_stock);
CREATE INDEX IF NOT EXISTS idx_ali_products_price     ON ali_products(price);
CREATE INDEX IF NOT EXISTS idx_ali_avail_product      ON ali_product_availability(product_id);
CREATE INDEX IF NOT EXISTS idx_ali_avail_store        ON ali_product_availability(store_id);
"""

ADD_COMMENTS = """
COMMENT ON TABLE ali_cities IS 'Города присутствия сети Alibaba Market';
COMMENT ON TABLE ali_stores IS 'Магазины сети Alibaba Market';
COMMENT ON TABLE ali_products IS 'Табаки для кальяна из каталога alibaba-market.ru';
COMMENT ON TABLE ali_product_availability IS 'Наличие и количество товара в каждом магазине';

COMMENT ON COLUMN ali_products.sku               IS 'Внутренний артикул товара';
COMMENT ON COLUMN ali_products.one_c_id          IS 'ID в системе 1С';
COMMENT ON COLUMN ali_products.brand_name        IS 'Название бренда (Tangiers, Satyr и др.)';
COMMENT ON COLUMN ali_products.line              IS 'Линейка товара (Noir, Classic и др.)';
COMMENT ON COLUMN ali_products.weight            IS 'Фасовка (25г, 100г, 250г)';
COMMENT ON COLUMN ali_products.price             IS 'Текущая цена в рублях';
COMMENT ON COLUMN ali_products.price_before_discount IS 'Цена до скидки в рублях';
COMMENT ON COLUMN ali_products.has_discount      IS 'Есть ли скидка на товар';
COMMENT ON COLUMN ali_products.rating            IS 'Рейтинг товара на сайте (1.0–5.0)';
COMMENT ON COLUMN ali_products.is_bestseller     IS 'Флаг бестселлера';
COMMENT ON COLUMN ali_products.is_premium        IS 'Флаг премиум товара';
COMMENT ON COLUMN ali_products.is_new            IS 'Флаг нового товара';
COMMENT ON COLUMN ali_products.htreviews_id      IS 'Ссылка на отзывы htreviews.org (напр. htr71747)';
COMMENT ON COLUMN ali_products.image_url         IS 'URL основного изображения товара';
COMMENT ON COLUMN ali_products.in_stock          IS 'Есть ли товар хотя бы в одном магазине';
COMMENT ON COLUMN ali_products.stores_count      IS 'В скольки магазинах есть в наличии';
COMMENT ON COLUMN ali_products.total_amount      IS 'Суммарное количество единиц во всех магазинах';

COMMENT ON COLUMN ali_stores.is_discounter       IS 'Является ли магазин дискаунтером';
COMMENT ON COLUMN ali_stores.store_2gisid        IS 'ID магазина в 2GIS';
COMMENT ON COLUMN ali_product_availability.amount IS 'Количество единиц товара в магазине';
"""


# ─── HTTP ────────────────────────────────────────────────────────────────────

async def fetch_json(client: httpx.AsyncClient, url: str, retries=3) -> Optional[list | dict]:
    for attempt in range(retries):
        try:
            await asyncio.sleep(DELAY)
            r = await client.get(url, headers=HEADERS, timeout=30, follow_redirects=True)
            if r.status_code == 200 and r.text.strip():
                return r.json()
            log.warning(f"HTTP {r.status_code} for {url}")
            await asyncio.sleep(2 ** attempt)
        except Exception as e:
            log.warning(f"Error {url}: {e}")
            await asyncio.sleep(2 ** attempt)
    return None


# ─── SCRAPING ────────────────────────────────────────────────────────────────

async def get_cities(client: httpx.AsyncClient) -> list[dict]:
    data = await fetch_json(client, f'{BASE_URL}/api/cities')
    return data or []


async def upsert_cities(pool: asyncpg.Pool, cities: list[dict]):
    async with pool.acquire() as conn:
        for c in cities:
            await conn.execute("""
                INSERT INTO ali_cities (id, name)
                VALUES ($1, $2)
                ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name, scraped_at=NOW()
            """, c['id'], c['name'])
    log.info(f"Upserted {len(cities)} cities")


async def upsert_store(conn: asyncpg.Connection, store: dict):
    await conn.execute("""
        INSERT INTO ali_stores (id, city_id, name, full_name, address, phone, schedule, is_discounter, store_2gisid)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT (id) DO UPDATE SET
            full_name=EXCLUDED.full_name, address=EXCLUDED.address,
            phone=EXCLUDED.phone, schedule=EXCLUDED.schedule, scraped_at=NOW()
    """,
        store['id'], store.get('city_id'), store.get('name',''),
        store.get('full_name'), store.get('address'), store.get('phone'),
        store.get('schedule'), bool(store.get('is_discounter', 0)),
        store.get('store_2gisid')
    )


async def scrape_city_products(client: httpx.AsyncClient, pool: asyncpg.Pool,
                                city_id: int, city_name: str) -> set[int]:
    """Scrape all products for a city, upsert products+stores+availability. Returns set of product IDs."""
    all_products: dict[int, dict] = {}
    offset = 0

    while True:
        url = (f'{BASE_URL}/api/products/category/{CATEGORY_ID}/products'
               f'?cityId={city_id}&limit={PAGE_LIMIT}&offset={offset}')
        data = await fetch_json(client, url)

        if not data or not isinstance(data, list) or len(data) == 0:
            break

        new_count = 0
        for p in data:
            pid = p['id']
            if pid not in all_products:
                all_products[pid] = p
                new_count += 1

        log.info(f"  [{city_name}] offset={offset}: +{len(data)} items ({new_count} new), total={len(all_products)}")
        offset += PAGE_LIMIT

        if len(data) < PAGE_LIMIT:
            break
        if new_count == 0:
            log.info(f"  [{city_name}] No new products, stopping")
            break

    log.info(f"  [{city_name}] Total unique products: {len(all_products)}")

    # Upsert all products + stores + availability
    async with pool.acquire() as conn:
        for p in all_products.values():
            # Main product
            images = p.get('images') or []
            image_url = images[0]['url'] if images else None
            brand = p.get('brand') or {}

            await conn.execute("""
                INSERT INTO ali_products (
                    id, sku, one_c_id, name, brand_id, brand_name, category_id,
                    line, country, weight, price, price_before_discount, has_discount,
                    rating, is_bestseller, is_premium, is_new,
                    description, short_description, htreviews_id, image_url
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)
                ON CONFLICT (id) DO UPDATE SET
                    name=EXCLUDED.name, price=EXCLUDED.price,
                    price_before_discount=EXCLUDED.price_before_discount,
                    has_discount=EXCLUDED.has_discount, rating=EXCLUDED.rating,
                    is_bestseller=EXCLUDED.is_bestseller, is_premium=EXCLUDED.is_premium,
                    is_new=EXCLUDED.is_new, scraped_at=NOW()
            """,
                p['id'], p.get('sku'), p.get('one_c_id'), p['name'],
                brand.get('id'), brand.get('name'), p.get('category_id'),
                p.get('line'), p.get('country'), p.get('weight'),
                p.get('price'), p.get('price_before_discount'),
                bool(p.get('before_discount', False)),
                p.get('rating'), bool(p.get('bestseller', False)),
                bool(p.get('premium', False)), bool(p.get('new', False)),
                p.get('description'), p.get('short_description'),
                p.get('htreviews_id'), image_url
            )

            # Stores + availability
            stores = p.get('stores') or []
            for store in stores:
                store['city_id'] = city_id
                await upsert_store(conn, store)

                pivot = store.get('pivot') or {}
                amount = pivot.get('amount', 0) or 0
                await conn.execute("""
                    INSERT INTO ali_product_availability (product_id, store_id, city_id, amount)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (product_id, store_id) DO UPDATE SET
                        amount=EXCLUDED.amount, scraped_at=NOW()
                """, p['id'], store['id'], city_id, amount)

    return set(all_products.keys())


async def update_stock_summary(pool: asyncpg.Pool):
    """Update in_stock, stores_count, total_amount for all products"""
    log.info("Updating stock summary...")
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE ali_products p SET
                in_stock     = (sub.total_amount > 0),
                stores_count = sub.stores_count,
                total_amount = sub.total_amount
            FROM (
                SELECT
                    product_id,
                    COUNT(*) FILTER (WHERE amount > 0) AS stores_count,
                    COALESCE(SUM(amount), 0)           AS total_amount
                FROM ali_product_availability
                GROUP BY product_id
            ) sub
            WHERE p.id = sub.product_id
        """)
    log.info("Stock summary updated")


# ─── MAIN ────────────────────────────────────────────────────────────────────

async def main():
    log.info("Connecting to DB...")
    init_conn = await asyncpg.connect(DB_DSN)
    await init_conn.execute(f'CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}')
    await init_conn.close()

    pool = await asyncpg.create_pool(
        DB_DSN, min_size=2, max_size=5,
        server_settings={'search_path': DB_SCHEMA}
    )

    log.info("Creating tables...")
    async with pool.acquire() as conn:
        await conn.execute(CREATE_TABLES)
        await conn.execute(ADD_COMMENTS)

    async with httpx.AsyncClient() as client:
        # 1. Города
        cities = await get_cities(client)
        log.info(f"Cities: {[c['name'] for c in cities]}")
        await upsert_cities(pool, cities)

        # 2. По каждому городу скрапим товары
        all_product_ids: set[int] = set()
        for city in cities:
            log.info(f"\n{'='*50}")
            log.info(f"Scraping city: {city['name']} (id={city['id']})")
            ids = await scrape_city_products(client, pool, city['id'], city['name'])
            all_product_ids |= ids

        log.info(f"\nTotal unique products across all cities: {len(all_product_ids)}")

        # 3. Обновляем сводку наличия
        await update_stock_summary(pool)

    await pool.close()
    log.info("Done!")


if __name__ == '__main__':
    asyncio.run(main())
