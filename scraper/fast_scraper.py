"""
Fast parallel HTReviews scraper.
Uses asyncio.Semaphore for concurrent requests (10 at once).
Skips reviews to focus on tobacco metadata (name, rating, strength, tags).
"""
import asyncio, re, sys, logging
from datetime import datetime, date
from typing import Optional

import asyncpg
import httpx
from bs4 import BeautifulSoup

sys.stdout.reconfigure(encoding='utf-8')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

DB_DSN = 'postgresql://hookah:hookah123@localhost:5432/hookah_db'
BASE_URL = 'https://htreviews.org'
CONCURRENCY = 8
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept-Language': 'ru-RU,ru;q=0.9',
}


# ── HELPERS ──────────────────────────────────────────────────────────────────

def parse_number(text):
    if not text: return None
    text = text.strip().replace('\xa0','').replace(' ','')
    m = re.search(r'[\d.,]+[kкKК]?', text)
    if not m: return None
    val = m.group()
    if val.endswith(('k','к','K','К')): return int(float(val[:-1])*1000)
    try: return int(float(val.replace(',','.')))
    except: return None

def parse_rating(text):
    if not text: return None
    m = re.search(r'(\d+[.,]\d+|\d+)', text.strip())
    return float(m.group().replace(',','.')) if m else None

def parse_date(text):
    if not text: return None
    for fmt in ('%d.%m.%Y','%Y-%m-%d'):
        try: return datetime.strptime(text.strip(), fmt).date()
        except: pass
    return None


# ── FETCH ─────────────────────────────────────────────────────────────────────

async def fetch(client, url, sem, retries=2):
    async with sem:
        for attempt in range(retries):
            try:
                await asyncio.sleep(0.15)
                r = await client.get(url, headers=HEADERS, timeout=15, follow_redirects=True)
                if r.status_code == 200: return r.text
                if r.status_code == 404: return None
            except Exception as e:
                log.warning(f"Fetch error {url}: {e}")
                await asyncio.sleep(1)
    return None


# ── PARSE BRAND PAGE ─────────────────────────────────────────────────────────

def _extract_links(html, brand_slug, depth):
    soup = BeautifulSoup(html, 'lxml')
    result = set()
    pat = rf'^(?:https://htreviews\.org)?/tobaccos/{re.escape(brand_slug)}(?:/[^/?#]+){{{depth}}}$'
    for a in soup.find_all('a', href=True):
        href = re.sub(r'^https://htreviews\.org', '', a['href'].split('?')[0].split('#')[0])
        if re.match(pat, href):
            result.add(href)
    return list(result)

def get_tobacco_urls(html, brand_slug):
    return _extract_links(html, brand_slug, 2)

def get_line_urls(html, brand_slug):
    return _extract_links(html, brand_slug, 1)

def parse_brand_name(html, slug):
    soup = BeautifulSoup(html, 'lxml')
    h1 = soup.find('h1')
    return h1.get_text(strip=True) if h1 else slug


async def get_all_tobacco_urls(client, sem, brand_slug, brand_html):
    """Fetch brand + line pages with 2 sort orders for ~2x tobacco coverage.

    HTReviews shows only first 20 tobaccos in static HTML per page.
    ?r=position&s=rating&d=asc gives a *different* set of 20 (lowest-rated first).
    Combining both passes ~doubles coverage from 31% to 70% per line.
    """
    urls = set(get_tobacco_urls(brand_html, brand_slug))
    line_slugs = get_line_urls(brand_html, brand_slug)
    log.info(f"  Lines: {len(line_slugs)}, direct tobaccos from brand page: {len(urls)}")

    # Brand page alternate sort (lowest-rated first → different 20 tobaccos)
    alt_html = await fetch(client, f'{BASE_URL}/tobaccos/{brand_slug}?r=position&s=rating&d=asc', sem)
    if alt_html:
        alt_urls = set(get_tobacco_urls(alt_html, brand_slug))
        added = alt_urls - urls
        urls.update(alt_urls)
        if added:
            log.info(f"  Brand alt-sort: +{len(added)} more tobaccos")

    # Each line — default + alternate sort
    for line_url in line_slugs:
        line_html = await fetch(client, BASE_URL + line_url, sem)
        if line_html:
            new = set(get_tobacco_urls(line_html, brand_slug))
            urls.update(new)
            log.info(f"  Line {line_url}: +{len(new)} tobaccos")
        alt_line_html = await fetch(client, f'{BASE_URL}{line_url}?r=position&s=rating&d=asc', sem)
        if alt_line_html:
            alt_new = set(get_tobacco_urls(alt_line_html, brand_slug))
            added2 = alt_new - urls
            urls.update(alt_new)
            if added2:
                log.info(f"  Line {line_url} (alt): +{len(added2)} more")

    return list(urls)


# ── PARSE TOBACCO PAGE ────────────────────────────────────────────────────────

def parse_tobacco(html, url_path):
    soup = BeautifulSoup(html, 'lxml')
    d = {'url_path': url_path}

    h1 = soup.find('h1')
    d['name'] = h1.get_text(strip=True) if h1 else url_path.split('/')[-1]

    # htreviews_id from data-id
    wrapper = soup.find(class_='object_wrapper', attrs={'data-id': True})
    if wrapper:
        tid = wrapper.get('data-id','')
        if tid.isdigit() and int(tid) > 100:
            d['htreviews_id'] = int(tid)
    if 'htreviews_id' not in d:
        for el in soup.find_all(string=re.compile(r'htr\d{4,}')):
            m = re.search(r'htr(\d+)', str(el))
            if m: d['htreviews_id'] = int(m.group(1)); break

    # Info rows
    for row in soup.find_all(class_='object_info_item'):
        spans = row.find_all('span')
        if len(spans) < 2: continue
        label = spans[0].get_text(strip=True).rstrip('?').strip().lower()
        value = spans[-1].get_text(strip=True)
        if not value: continue
        if 'крепость официальн' in label: d['strength_official'] = value
        elif 'крепость по оценк' in label or 'крепость пользов' in label: d['strength_user'] = value
        elif 'статус' in label: d['status'] = value
        elif 'стран' in label: d['country'] = value
        elif 'добавлен' in label: d['added_to_site'] = parse_date(value)

    # Tags
    tags = list(dict.fromkeys(
        el.get_text(strip=True)
        for el in soup.find_all(class_='object_card_tag')
        if el.get_text(strip=True) and len(el.get_text(strip=True)) < 50
    ))
    d['flavor_tags'] = tags or None

    # Rating
    sg = soup.find(class_='score_graphic')
    if sg:
        rated = sg.find(attrs={'data-rating': True})
        if rated: d['avg_rating'] = parse_rating(rated.get('data-rating'))
        for stat in sg.find_all(attrs={'data-hover-title': True}):
            title = stat.get('data-hover-title','').lower()
            span = stat.find('span')
            val = parse_number(span.get_text(strip=True)) if span else None
            if val is None: continue
            if 'оценк' in title: d['total_ratings'] = val
            elif 'просмотр' in title: d['total_views'] = val

    # Reviews count
    rev_h2 = soup.find('h2', string=re.compile(r'Отзывы'))
    if rev_h2:
        m = re.search(r'\((\d+)\)', rev_h2.get_text())
        if m: d['total_reviews'] = int(m.group(1))

    # pct_recommend
    rec = soup.find(string=re.compile(r'Покурили\s*бы\s*снова|Рекоменд', re.I))
    if rec:
        parent = rec.find_parent()
        if parent:
            pct = parent.find(string=re.compile(r'\d+\s*%'))
            if not pct and parent.parent:
                pct = parent.parent.find(string=re.compile(r'\d+\s*%'))
            if pct:
                m = re.search(r'(\d+)', pct)
                if m: d['pct_recommend'] = int(m.group(1))

    # Image
    img = soup.select_one('.object_image img')
    if img and img.get('src'): d['image_url'] = img['src']

    return d


# ── DB UPSERT ────────────────────────────────────────────────────────────────

async def upsert_brand(conn, slug, name):
    return await conn.fetchval("""
        INSERT INTO scraper.htr_brands (slug, name)
        VALUES ($1, $2)
        ON CONFLICT (slug) DO UPDATE SET name=EXCLUDED.name, scraped_at=NOW()
        RETURNING id
    """, slug, name)

async def upsert_line(conn, brand_id, line_slug):
    return await conn.fetchval("""
        INSERT INTO scraper.htr_lines (brand_id, slug, name)
        VALUES ($1, $2, $2)
        ON CONFLICT (brand_id, slug) DO UPDATE SET name=EXCLUDED.name
        RETURNING id
    """, brand_id, line_slug)

async def upsert_tobacco(conn, data, brand_id, line_id):
    htid = data.get('htreviews_id')
    if not htid: return None
    parts = data['url_path'].strip('/').split('/')
    slug = parts[-1] if parts else data['url_path']
    return await conn.fetchval("""
        INSERT INTO scraper.htr_tobaccos (
            htreviews_id, brand_id, line_id, slug, name, url_path,
            strength_official, strength_user, status, country,
            flavor_tags, added_to_site, avg_rating, total_ratings,
            total_reviews, total_views, pct_recommend, image_url
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
        ON CONFLICT (htreviews_id) DO UPDATE SET
            name=EXCLUDED.name, avg_rating=EXCLUDED.avg_rating,
            total_ratings=EXCLUDED.total_ratings, total_reviews=EXCLUDED.total_reviews,
            total_views=EXCLUDED.total_views, flavor_tags=EXCLUDED.flavor_tags,
            strength_official=EXCLUDED.strength_official, strength_user=EXCLUDED.strength_user,
            status=EXCLUDED.status, pct_recommend=EXCLUDED.pct_recommend,
            image_url=COALESCE(EXCLUDED.image_url, htr_tobaccos.image_url),
            scraped_at=NOW()
        RETURNING id
    """, htid, brand_id, line_id, slug, data.get('name', slug), data['url_path'],
        data.get('strength_official'), data.get('strength_user'), data.get('status'),
        data.get('country'), data.get('flavor_tags'), data.get('added_to_site'),
        data.get('avg_rating'), data.get('total_ratings', 0),
        data.get('total_reviews', 0), data.get('total_views', 0),
        data.get('pct_recommend'), data.get('image_url'))


# ── SCRAPE ONE TOBACCO ────────────────────────────────────────────────────────

async def scrape_one_tobacco(client, pool, sem, url_path, brand_id):
    parts = url_path.strip('/').split('/')
    if len(parts) < 4: return
    line_slug = parts[2]

    html = await fetch(client, BASE_URL + url_path, sem)
    if not html: return

    data = parse_tobacco(html, url_path)
    if not data.get('htreviews_id'):
        log.warning(f"No ID: {url_path}")
        return

    async with pool.acquire() as conn:
        line_id = await upsert_line(conn, brand_id, line_slug)
        await upsert_tobacco(conn, data, brand_id, line_id)

    log.info(f"  ✓ {data.get('name','?')} htr={data['htreviews_id']} ⭐{data.get('avg_rating','–')}")


# ── MAIN ─────────────────────────────────────────────────────────────────────

# Brands found on /tobaccos/brands page — ordered by importance
PRIORITY_BRANDS = [
    'darkside','musthave','sebero','black-burn','spectrum','brusko',
    'chabacco','sarma','trofimoffs','starline','overdose','adalya',
    'banger','duft','bonche','original-virginia','hit','leteam',
    'fumari','al-fakher','tangiers','satyr','jookah','cobra',
    'hooligan','nash', 'wto',
]

# Extra brand slugs to try directly (not on /tobaccos/brands page)
# These are known to exist on htreviews.org but aren't listed on brands page
EXTRA_BRANDS = [
    'darkside','musthave','sebero','black-burn','spectrum','brusko',
    'chabacco','sarma','trofimoffs','starline','overdose','adalya',
    'banger','duft','bonche','hit','leteam','fumari','al-fakher',
    'hooligan','nash','jookah','cobra','bonche','overdosed','brume',
    'burn','element','true-passion','must',
]

async def main():
    pool = await asyncpg.create_pool(DB_DSN, min_size=3, max_size=15)
    sem = asyncio.Semaphore(CONCURRENCY)

    async with httpx.AsyncClient() as client:
        # Get all brand slugs
        html = await fetch(client, f'{BASE_URL}/tobaccos/brands', sem)
        if not html:
            log.error("Can't fetch brands page"); return
        soup = BeautifulSoup(html, 'lxml')
        all_slugs = set()
        for a in soup.find_all('a', href=True):
            clean = a['href'].split('?')[0].split('#')[0]
            m = re.match(r'^(?:https://htreviews\.org)?/tobaccos/([a-z0-9\-]+)$', clean)
            if m and m.group(1) not in ('brands','lines','new'):
                all_slugs.add(m.group(1))
        log.info(f"Total brands: {len(all_slugs)}")

        # Combine: priority extras first, then brands page list
        extra_set = set(EXTRA_BRANDS)
        page_set = set(all_slugs)
        ordered = list(EXTRA_BRANDS)  # try all extra brands first
        ordered += [b for b in all_slugs if b not in extra_set]  # then remainder from page
        # deduplicate preserving order
        seen = set(); ordered = [b for b in ordered if not (b in seen or seen.add(b))]
        log.info(f"Total to scrape: {len(ordered)} (page:{len(page_set)} + extra:{len(extra_set)})")

        for brand_slug in ordered:
            try:
                log.info(f"\n{'='*40} {brand_slug}")
                html = await fetch(client, f'{BASE_URL}/tobaccos/{brand_slug}', sem)
                if not html: continue

                name = parse_brand_name(html, brand_slug)
                async with pool.acquire() as conn:
                    brand_id = await upsert_brand(conn, brand_slug, name)
                log.info(f"Brand: {name} (id={brand_id})")

                # Get tobacco URLs — brand + all lines, 2 sort orders each (~2x coverage)
                urls = await get_all_tobacco_urls(client, sem, brand_slug, html)
                log.info(f"  Total URLs: {len(urls)}")
                if not urls: continue

                # Scrape all tobaccos in this brand CONCURRENTLY (chunks of CONCURRENCY)
                for i in range(0, len(urls), CONCURRENCY):
                    chunk = urls[i:i+CONCURRENCY]
                    tasks = [scrape_one_tobacco(client, pool, sem, u, brand_id) for u in chunk]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for r in results:
                        if isinstance(r, Exception):
                            log.error(f"  Task error: {r}")
                log.info(f"  Done: {brand_slug}")
            except Exception as e:
                log.error(f"Brand {brand_slug} failed: {e}", exc_info=True)
                continue

    await pool.close()
    log.info("ALL DONE")

if __name__ == '__main__':
    asyncio.run(main())
