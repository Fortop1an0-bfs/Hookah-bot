"""
Backfill image_url for htr_tobaccos and logo_url for htr_brands.
Fetches each brand page once (~30-50 requests total) and extracts:
  - brand logo from .object_image img
  - tobacco thumbnails from a.tobacco_list_item_image img (keyed by href = url_path)

Run on server:
  cd /opt/hookah/scraper && python fix_images.py
"""
import asyncio
import sys
import asyncpg
import httpx
from bs4 import BeautifulSoup

sys.stdout.reconfigure(encoding='utf-8')

DB_DSN = 'postgresql://hookah:hookah123@localhost:5432/hookah_db'
BASE_URL = 'https://htreviews.org'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept-Language': 'ru-RU,ru;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
}


async def main():
    pool = await asyncpg.create_pool(DB_DSN, server_settings={'search_path': 'scraper'})

    # Ensure columns exist
    async with pool.acquire() as conn:
        await conn.execute("""
            ALTER TABLE htr_brands ADD COLUMN IF NOT EXISTS logo_url TEXT;
            ALTER TABLE htr_tobaccos ADD COLUMN IF NOT EXISTS image_url TEXT;
        """)
    print("Columns ensured.")

    # Get all brands from DB
    async with pool.acquire() as conn:
        brands = await conn.fetch('SELECT id, slug FROM htr_brands ORDER BY slug')
    print(f"Found {len(brands)} brands in DB.")

    total_logos = 0
    total_images = 0

    async with httpx.AsyncClient(follow_redirects=True, timeout=20) as client:
        for brand in brands:
            await asyncio.sleep(1.0)
            try:
                r = await client.get(f'{BASE_URL}/tobaccos/{brand["slug"]}', headers=HEADERS)
            except Exception as e:
                print(f"  ERROR fetching {brand['slug']}: {e}")
                continue

            if r.status_code != 200:
                print(f"  {brand['slug']}: HTTP {r.status_code}")
                continue

            soup = BeautifulSoup(r.text, 'lxml')

            # Brand logo from .object_image img (the main brand photo)
            logo_img = soup.select_one('.object_image img')
            logo_url = logo_img['src'] if logo_img and logo_img.get('src') else None

            # Tobacco thumbnails: a.tobacco_list_item_image → href (url_path) + img src
            tob_images = {}
            for a in soup.select('a.tobacco_list_item_image[href]'):
                href = a['href'].split('?')[0].split('#')[0]
                # Normalize to relative path
                if href.startswith('https://htreviews.org'):
                    href = href[len('https://htreviews.org'):]
                img = a.find('img')
                if img and img.get('src') and href:
                    tob_images[href] = img['src']

            async with pool.acquire() as conn:
                if logo_url:
                    await conn.execute(
                        'UPDATE htr_brands SET logo_url=$1 WHERE id=$2',
                        logo_url, brand['id']
                    )
                    total_logos += 1

                updated = 0
                for url_path, img_url in tob_images.items():
                    result = await conn.execute(
                        'UPDATE htr_tobaccos SET image_url=$1 WHERE url_path=$2 AND image_url IS NULL',
                        img_url, url_path
                    )
                    # result is like "UPDATE N"
                    n = int(result.split()[-1])
                    updated += n

                total_images += updated

            print(f"  {brand['slug']}: logo={'✓' if logo_url else '✗'}, "
                  f"page_images={len(tob_images)}, db_updated={updated}")

    await pool.close()
    print(f"\nDone! Logos updated: {total_logos}, Tobacco images updated: {total_images}")

    # Final stats
    init_conn = await asyncpg.connect(DB_DSN)
    r1 = await init_conn.fetchval("SELECT COUNT(*) FROM scraper.htr_brands WHERE logo_url IS NOT NULL")
    r2 = await init_conn.fetchval("SELECT COUNT(*) FROM scraper.htr_tobaccos WHERE image_url IS NOT NULL")
    r3 = await init_conn.fetchval("SELECT COUNT(*) FROM scraper.htr_tobaccos")
    await init_conn.close()
    print(f"Brands with logo: {r1}/{len(brands)}")
    print(f"Tobaccos with image: {r2}/{r3}")


if __name__ == '__main__':
    asyncio.run(main())
