"""
HTReviews.org full scraper
Scrapes: brands, lines, tobaccos, reviews
Stores data in PostgreSQL on 198.13.184.39
"""
import asyncio
import re
import sys
import time
import logging
from datetime import datetime, date
from typing import Optional

import asyncpg
import httpx
from bs4 import BeautifulSoup

sys.stdout.reconfigure(encoding='utf-8')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('scraper.log', encoding='utf-8'),
    ]
)
log = logging.getLogger(__name__)

DB_DSN = 'postgresql://hookah:hookah123@localhost:5432/hookah_db'
DB_SCHEMA = 'scraper'
BASE_URL = 'https://htreviews.org'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept-Language': 'ru-RU,ru;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
}

REVIEWS_HEADERS = {**HEADERS, 'HX-Request': 'true', 'HX-Target': 'reviews_list'}

DELAY = 1.0  # seconds between requests (be polite)


# ─── DB SCHEMA ──────────────────────────────────────────────────────────────

CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS htr_brands (
    id              SERIAL PRIMARY KEY,
    slug            TEXT UNIQUE NOT NULL,
    name            TEXT NOT NULL,
    country         TEXT,
    founded_year    INT,
    website         TEXT,
    description     TEXT,
    avg_rating      NUMERIC(3,2),
    total_ratings   INT DEFAULT 0,
    total_reviews   INT DEFAULT 0,
    total_views     INT DEFAULT 0,
    pct_recommend   INT,
    scraped_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS htr_lines (
    id              SERIAL PRIMARY KEY,
    brand_id        INT REFERENCES htr_brands(id) ON DELETE CASCADE,
    slug            TEXT NOT NULL,
    name            TEXT NOT NULL,
    strength_official TEXT,
    strength_user   TEXT,
    description     TEXT,
    status          TEXT,
    flavor_count    INT DEFAULT 0,
    avg_rating      NUMERIC(3,2),
    total_ratings   INT DEFAULT 0,
    total_reviews   INT DEFAULT 0,
    total_views     INT DEFAULT 0,
    scraped_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(brand_id, slug)
);

CREATE TABLE IF NOT EXISTS htr_tobaccos (
    id                  SERIAL PRIMARY KEY,
    htreviews_id        INT UNIQUE NOT NULL,
    brand_id            INT REFERENCES htr_brands(id) ON DELETE CASCADE,
    line_id             INT REFERENCES htr_lines(id) ON DELETE SET NULL,
    slug                TEXT NOT NULL,
    name                TEXT NOT NULL,
    url_path            TEXT UNIQUE,
    description         TEXT,
    strength_official   TEXT,
    strength_user       TEXT,
    status              TEXT,
    country             TEXT,
    flavor_tags         TEXT[],
    added_to_site       DATE,
    avg_rating          NUMERIC(3,2),
    total_ratings       INT DEFAULT 0,
    total_reviews       INT DEFAULT 0,
    total_views         INT DEFAULT 0,
    pct_recommend       INT,
    reviews_scraped     BOOLEAN DEFAULT FALSE,
    scraped_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS htr_reviewers (
    id              SERIAL PRIMARY KEY,
    htreviews_uid   INT UNIQUE NOT NULL,
    username        TEXT NOT NULL,
    profile_url     TEXT,
    reputation      INT,
    scraped_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS htr_reviews (
    id              SERIAL PRIMARY KEY,
    htreviews_rid   INT UNIQUE NOT NULL,
    tobacco_id      INT REFERENCES htr_tobaccos(id) ON DELETE CASCADE,
    reviewer_id     INT REFERENCES htr_reviewers(id) ON DELETE SET NULL,
    rating          NUMERIC(2,1),
    review_text     TEXT,
    reviewed_at     DATE,
    reviewed_at_raw TEXT,
    likes           INT DEFAULT 0,
    dislikes        INT DEFAULT 0,
    counts_for_rating BOOLEAN DEFAULT TRUE,
    scraped_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_htr_tobaccos_brand ON htr_tobaccos(brand_id);
CREATE INDEX IF NOT EXISTS idx_htr_tobaccos_line ON htr_tobaccos(line_id);
CREATE INDEX IF NOT EXISTS idx_htr_tobaccos_rating ON htr_tobaccos(avg_rating DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_htr_reviews_tobacco ON htr_reviews(tobacco_id);
CREATE INDEX IF NOT EXISTS idx_htr_reviews_reviewer ON htr_reviews(reviewer_id);
CREATE INDEX IF NOT EXISTS idx_htr_reviews_date ON htr_reviews(reviewed_at DESC NULLS LAST);
"""


# ─── HTTP CLIENT ────────────────────────────────────────────────────────────

async def fetch(client: httpx.AsyncClient, url: str, headers=None, retries=3) -> Optional[str]:
    h = {**HEADERS, **(headers or {})}
    for attempt in range(retries):
        try:
            await asyncio.sleep(DELAY)
            resp = await client.get(url, headers=h, timeout=20, follow_redirects=True)
            if resp.status_code == 200:
                return resp.text
            elif resp.status_code == 404:
                return None
            log.warning(f"HTTP {resp.status_code} for {url}")
            await asyncio.sleep(2 ** attempt)
        except Exception as e:
            log.warning(f"Error fetching {url}: {e}")
            await asyncio.sleep(2 ** attempt)
    return None


# ─── PARSERS ────────────────────────────────────────────────────────────────

def parse_number(text: str) -> Optional[int]:
    if not text:
        return None
    text = text.strip().replace('\xa0', '').replace(' ', '')
    m = re.search(r'[\d.,]+[kкKК]?', text)
    if not m:
        return None
    val = m.group()
    if val.endswith(('k', 'к', 'K', 'К')):
        return int(float(val[:-1]) * 1000)
    return int(float(val.replace(',', '.')))


def parse_rating(text: str) -> Optional[float]:
    if not text:
        return None
    m = re.search(r'(\d+[.,]\d+|\d+)', text.strip())
    return float(m.group().replace(',', '.')) if m else None


def parse_date(text: str) -> Optional[date]:
    """Parse dates like '06.12.2024' or '2024-12-06'"""
    if not text:
        return None
    for fmt in ('%d.%m.%Y', '%Y-%m-%d'):
        try:
            return datetime.strptime(text.strip(), fmt).date()
        except ValueError:
            pass
    return None


def parse_brand_page(html: str, slug: str) -> dict:
    """Parse brand page: /tobaccos/{brand}"""
    soup = BeautifulSoup(html, 'lxml')
    result = {'slug': slug, 'lines': [], 'tobaccos': []}

    # Brand name (h1 or title)
    h1 = soup.find('h1')
    result['name'] = h1.get_text(strip=True) if h1 else slug

    # Country, founded year, website
    for row in soup.select('.brand_info_row, .info_row, [class*="brand_detail"]'):
        label = row.find(class_=re.compile('label|key'))
        val = row.find(class_=re.compile('value|val'))
        if label and val:
            ltext = label.get_text(strip=True).lower()
            vtext = val.get_text(strip=True)
            if 'стран' in ltext or 'country' in ltext:
                result['country'] = vtext
            elif 'год' in ltext or 'founded' in ltext:
                m = re.search(r'\d{4}', vtext)
                if m:
                    result['founded_year'] = int(m.group())
            elif 'сайт' in ltext or 'site' in ltext or 'web' in ltext:
                result['website'] = vtext

    # Overall rating
    rating_el = soup.find(class_=re.compile('rating_value|avg_rating|object_rating'))
    if rating_el:
        result['avg_rating'] = parse_rating(rating_el.get_text())

    # Stats (ratings count, reviews, views)
    stats = soup.select('[class*="stat"], [class*="count"]')
    for s in stats:
        text = s.get_text(strip=True)
        val = parse_number(text)
        if val and val > 0:
            parent_text = s.parent.get_text(strip=True).lower() if s.parent else ''
            if 'рейтинг' in parent_text or 'оценк' in parent_text:
                result['total_ratings'] = val
            elif 'отзыв' in parent_text or 'review' in parent_text:
                result['total_reviews'] = val
            elif 'просмотр' in parent_text or 'view' in parent_text:
                result['total_views'] = val

    # Tobacco IDs from data-id attributes - filter for real tobacco IDs (5-6 digits)
    tobacco_elements = soup.select('[data-id]')
    for el in tobacco_elements:
        tid = el.get('data-id', '')
        if tid.isdigit() and len(tid) >= 4:
            result['tobaccos'].append({'htreviews_id': int(tid)})

    # Description
    desc_el = soup.find(class_=re.compile('description|about|brand_desc'))
    if desc_el:
        result['description'] = desc_el.get_text(strip=True)

    # % recommend
    rec_el = soup.find(string=re.compile(r'\d+\s*%'))
    if rec_el:
        m = re.search(r'(\d+)\s*%', rec_el)
        if m:
            result['pct_recommend'] = int(m.group(1))

    return result


def parse_tobacco_page(html: str, url_path: str) -> dict:
    """Parse individual tobacco page: /tobaccos/{brand}/{line}/{slug}"""
    soup = BeautifulSoup(html, 'lxml')
    result = {'url_path': url_path}

    # Name
    h1 = soup.find('h1')
    result['name'] = h1.get_text(strip=True) if h1 else url_path.split('/')[-1]

    # HTReviews ID: class=object_wrapper has data-id
    wrapper = soup.find(class_='object_wrapper', attrs={'data-id': True})
    if wrapper:
        tid = wrapper.get('data-id', '')
        if tid.isdigit() and int(tid) > 100:
            result['htreviews_id'] = int(tid)

    # Fallback: look for htrXXXX string
    if 'htreviews_id' not in result:
        for el in soup.find_all(string=re.compile(r'htr\d{4,}')):
            m = re.search(r'htr(\d+)', str(el))
            if m:
                result['htreviews_id'] = int(m.group(1))
                break

    # Description
    desc = soup.find(class_=re.compile('object_description|tobacco_desc'))
    if desc:
        result['description'] = desc.get_text(strip=True)

    # Info items: class=object_info_item
    # Structure: <span>Label?</span><span>Label</span><div>?</div><span>Value</span>
    for row in soup.find_all(class_='object_info_item'):
        spans = row.find_all('span', recursive=False)
        if not spans:
            spans = row.find_all('span')
        if len(spans) < 2:
            continue
        label = spans[0].get_text(strip=True).rstrip('?').strip().lower()
        value = spans[-1].get_text(strip=True)
        if not value:
            continue
        if 'крепость официальн' in label:
            result['strength_official'] = value
        elif 'крепость по оценк' in label or 'крепость пользов' in label:
            result['strength_user'] = value
        elif 'статус' in label:
            result['status'] = value
        elif 'стран' in label:
            result['country'] = value
        elif 'добавлен' in label:
            result['added_to_site'] = parse_date(value)

    # Flavor tags: only leaf tags (class=object_card_tag), deduplicated
    tags = list(dict.fromkeys(
        el.get_text(strip=True)
        for el in soup.find_all(class_='object_card_tag')
        if el.get_text(strip=True) and len(el.get_text(strip=True)) < 50
    ))
    result['flavor_tags'] = tags if tags else None

    # Rating: score_graphic > div[data-rating]
    sg = soup.find(class_='score_graphic')
    if sg:
        rated_div = sg.find(attrs={'data-rating': True})
        if rated_div:
            result['avg_rating'] = parse_rating(rated_div.get('data-rating'))

        # Stats inside score_graphic: Оценки / Отзывы / Просмотры
        for stat in sg.find_all(attrs={'data-hover-title': True}):
            title = stat.get('data-hover-title', '').lower()
            span = stat.find('span')
            val = parse_number(span.get_text(strip=True)) if span else None
            if val is None:
                continue
            if 'оценк' in title:
                result['total_ratings'] = val
            elif 'просмотр' in title:
                result['total_views'] = val

    # Total reviews: from h2 "Отзывы (N)"
    rev_h2 = soup.find('h2', string=re.compile(r'Отзывы'))
    if not rev_h2:
        rev_h2 = soup.find(string=re.compile(r'Отзывы\s*\(\d+\)'))
    if rev_h2:
        m = re.search(r'\((\d+)\)', rev_h2 if isinstance(rev_h2, str) else rev_h2.get_text())
        if m:
            result['total_reviews'] = int(m.group(1))

    # % recommend: look for "Покурили бы снова" or similar
    rec_el = soup.find(string=re.compile(r'Покурили\s*бы\s*снова|Рекоменд', re.I))
    if rec_el:
        parent = rec_el.find_parent()
        if parent:
            pct_el = parent.find(string=re.compile(r'\d+\s*%'))
            if not pct_el:
                pct_el = parent.parent.find(string=re.compile(r'\d+\s*%')) if parent.parent else None
            if pct_el:
                m = re.search(r'(\d+)', pct_el)
                if m:
                    result['pct_recommend'] = int(m.group(1))

    return result


def parse_reviews_page(html: str) -> list[dict]:
    """Parse HTMX reviews block"""
    soup = BeautifulSoup(html, 'lxml')
    reviews = []

    for wrapper in soup.select('.reviews_item_wrapper'):
        review_el = wrapper.find(class_='reviews_item')
        if not review_el:
            continue

        review = {}

        # Review ID and user ID
        rid = review_el.get('data-id')
        uid = review_el.get('data-user')
        if rid and rid.isdigit():
            review['htreviews_rid'] = int(rid)
        if uid and uid.isdigit():
            review['htreviews_uid'] = int(uid)

        # Username
        username_el = review_el.select_one('.reviews_item_content_top a span span, .reviews_item_content_top a p span')
        if not username_el:
            username_el = review_el.select_one('.reviews_item_content_top a')
        review['username'] = username_el.get_text(strip=True) if username_el else None

        # Profile URL
        profile_link = review_el.select_one('.reviews_item_content_top a[href*="/profile/"]')
        if profile_link:
            review['profile_url'] = profile_link.get('href', '')

        # Reputation
        rep_el = review_el.select_one('.reviews_item_content_top span')
        if rep_el:
            rep_text = rep_el.get_text(strip=True)
            m = re.search(r'^(\d+)', rep_text)
            if m:
                review['reputation'] = int(m.group(1))

        # Date
        date_el = review_el.select_one('.date_info')
        if date_el:
            review['reviewed_at_raw'] = date_el.get('data-before', '')
            review['reviewed_at'] = parse_date(
                re.search(r'\d{2}\.\d{2}\.\d{4}', date_el.get('data-before', '') or '').group()
                if re.search(r'\d{2}\.\d{2}\.\d{4}', date_el.get('data-before', '') or '') else None
            )

        # Rating score
        score_el = review_el.select_one('.reviews_item_score span')
        if score_el:
            review['rating'] = parse_rating(score_el.get_text())

        # Review text
        content_el = review_el.select_one('[data-type="content"]')
        if content_el:
            review['review_text'] = content_el.get_text(strip=True)

        # Likes / dislikes
        relations = review_el.select_one('.reviews_item_relations')
        if relations:
            like_el = relations.select_one('[data-type="1"]')
            dislike_el = relations.select_one('[data-type="0"]')
            if like_el:
                review['likes'] = int(like_el.get('data-value', 0))
            if dislike_el:
                review['dislikes'] = int(dislike_el.get('data-value', 0))

        # Counts for rating?
        newbie = review_el.select_one('.newbie_review')
        review['counts_for_rating'] = newbie is None

        if review.get('htreviews_rid'):
            reviews.append(review)

    return reviews


# ─── SCRAPING LOGIC ─────────────────────────────────────────────────────────

async def get_all_brand_slugs(client: httpx.AsyncClient) -> list[str]:
    """Get all brand slugs from /tobaccos/brands"""
    html = await fetch(client, f'{BASE_URL}/tobaccos/brands')
    if not html:
        return []
    soup = BeautifulSoup(html, 'lxml')
    slugs = set()
    for a in soup.find_all('a', href=True):
        href = a['href']
        m = re.match(r'^(?:https://htreviews\.org)?/tobaccos/([a-z0-9\-]+)$', href)
        if m:
            s = m.group(1)
            if s not in ('brands', 'lines', 'new'):
                slugs.add(s)
    log.info(f"Found {len(slugs)} brand slugs")
    return list(slugs)


async def scrape_brand(client: httpx.AsyncClient, pool: asyncpg.Pool, slug: str):
    """Scrape a brand page and upsert brand + tobacco stubs"""
    html = await fetch(client, f'{BASE_URL}/tobaccos/{slug}')
    if not html:
        log.warning(f"No HTML for brand: {slug}")
        return None

    data = parse_brand_page(html, slug)

    async with pool.acquire() as conn:
        brand_id = await conn.fetchval("""
            INSERT INTO htr_brands (slug, name, country, founded_year, website, description,
                                     avg_rating, total_ratings, total_reviews, total_views, pct_recommend)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            ON CONFLICT (slug) DO UPDATE SET
                name=EXCLUDED.name, country=EXCLUDED.country, avg_rating=EXCLUDED.avg_rating,
                total_ratings=EXCLUDED.total_ratings, total_reviews=EXCLUDED.total_reviews,
                total_views=EXCLUDED.total_views, scraped_at=NOW()
            RETURNING id
        """, slug, data.get('name', slug), data.get('country'),
            data.get('founded_year'), data.get('website'), data.get('description'),
            data.get('avg_rating'), data.get('total_ratings', 0),
            data.get('total_reviews', 0), data.get('total_views', 0),
            data.get('pct_recommend'))

    log.info(f"Brand: {data.get('name', slug)} (id={brand_id}), tobaccos found: {len(data.get('tobaccos', []))}")
    return brand_id, data.get('tobaccos', [])


async def get_tobacco_ids_for_brand(client: httpx.AsyncClient, brand_slug: str) -> list[int]:
    """Get all tobacco htreviews IDs from brand page"""
    html = await fetch(client, f'{BASE_URL}/tobaccos/{brand_slug}')
    if not html:
        return []
    soup = BeautifulSoup(html, 'lxml')
    ids = set()
    for el in soup.select('[data-id]'):
        tid = el.get('data-id', '')
        if tid.isdigit() and int(tid) > 1000:
            ids.add(int(tid))
    return list(ids)


async def get_tobacco_urls_for_brand(client: httpx.AsyncClient, brand_slug: str) -> list[str]:
    """Get tobacco URL paths from brand page"""
    html = await fetch(client, f'{BASE_URL}/tobaccos/{brand_slug}')
    if not html:
        return []
    soup = BeautifulSoup(html, 'lxml')
    urls = set()
    for a in soup.find_all('a', href=True):
        href = a['href'].split('?')[0].split('#')[0]  # strip query/fragment
        if re.match(rf'^/tobaccos/{brand_slug}/[^/]+/[^/]+$', href):
            urls.add(href)
        elif re.match(rf'^https://htreviews\.org/tobaccos/{brand_slug}/[^/]+/[^/]+$', href):
            urls.add(re.sub(r'^https://htreviews\.org', '', href))
    return list(urls)


async def scrape_tobacco_page(client: httpx.AsyncClient, pool: asyncpg.Pool,
                               url_path: str, brand_id: int) -> Optional[int]:
    """Scrape individual tobacco page and upsert"""
    parts = url_path.strip('/').split('/')
    # parts: ['tobaccos', 'brand', 'line', 'slug']
    if len(parts) < 4:
        return None
    brand_slug, line_slug, tobacco_slug = parts[1], parts[2], parts[3]

    html = await fetch(client, f'{BASE_URL}{url_path}')
    if not html:
        return None

    data = parse_tobacco_page(html, url_path)

    # Get or create line
    async with pool.acquire() as conn:
        line_id = await conn.fetchval("""
            INSERT INTO htr_lines (brand_id, slug, name)
            VALUES ($1, $2, $3)
            ON CONFLICT (brand_id, slug) DO UPDATE SET name=EXCLUDED.name
            RETURNING id
        """, brand_id, line_slug, line_slug)

        # Upsert tobacco
        htid = data.get('htreviews_id')
        if not htid:
            # Try to get from URL data-id — fallback: skip
            log.warning(f"No htreviews_id for {url_path}")
            return None

        tobacco_id = await conn.fetchval("""
            INSERT INTO htr_tobaccos (
                htreviews_id, brand_id, line_id, slug, name, url_path,
                description, strength_official, strength_user, status, country,
                flavor_tags, added_to_site, avg_rating, total_ratings, total_reviews,
                total_views, pct_recommend
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
            ON CONFLICT (htreviews_id) DO UPDATE SET
                name=EXCLUDED.name, avg_rating=EXCLUDED.avg_rating,
                total_ratings=EXCLUDED.total_ratings, total_reviews=EXCLUDED.total_reviews,
                total_views=EXCLUDED.total_views, flavor_tags=EXCLUDED.flavor_tags,
                strength_official=EXCLUDED.strength_official, strength_user=EXCLUDED.strength_user,
                status=EXCLUDED.status, pct_recommend=EXCLUDED.pct_recommend, scraped_at=NOW()
            RETURNING id
        """, htid, brand_id, line_id, tobacco_slug, data.get('name', tobacco_slug),
            url_path, data.get('description'), data.get('strength_official'),
            data.get('strength_user'), data.get('status'), data.get('country'),
            data.get('flavor_tags'), data.get('added_to_site'),
            data.get('avg_rating'), data.get('total_ratings', 0),
            data.get('total_reviews', 0), data.get('total_views', 0),
            data.get('pct_recommend'))

    log.info(f"  Tobacco: {data.get('name')} (htr_id={htid}, db_id={tobacco_id})")
    return tobacco_id


async def scrape_all_reviews(client: httpx.AsyncClient, pool: asyncpg.Pool,
                              tobacco_db_id: int, htreviews_id: int):
    """Scrape all reviews for a tobacco via HTMX endpoint with pagination"""
    offset = 0
    total_scraped = 0
    seen_ids = set()

    while True:
        url = (f'{BASE_URL}/htmx/load/reviews_object'
               f'?id={htreviews_id}&object=tobacco&sortBy=created&direction=desc&offset={offset}')
        html = await fetch(client, url, headers=REVIEWS_HEADERS)
        if not html or len(html) < 200:
            break

        reviews = parse_reviews_page(html)
        if not reviews:
            break

        # Check if we're getting new reviews
        new_ids = {r['htreviews_rid'] for r in reviews if 'htreviews_rid' in r}
        if new_ids.issubset(seen_ids):
            break  # All reviews already seen - pagination exhausted
        seen_ids |= new_ids

        # Upsert reviewers and reviews
        async with pool.acquire() as conn:
            for rev in reviews:
                if 'htreviews_uid' in rev and rev.get('username'):
                    reviewer_id = await conn.fetchval("""
                        INSERT INTO htr_reviewers (htreviews_uid, username, profile_url, reputation)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (htreviews_uid) DO UPDATE SET
                            username=EXCLUDED.username, reputation=EXCLUDED.reputation
                        RETURNING id
                    """, rev['htreviews_uid'], rev.get('username', 'unknown'),
                        rev.get('profile_url'), rev.get('reputation'))
                else:
                    reviewer_id = None

                if 'htreviews_rid' in rev:
                    await conn.execute("""
                        INSERT INTO htr_reviews (
                            htreviews_rid, tobacco_id, reviewer_id, rating,
                            review_text, reviewed_at, reviewed_at_raw,
                            likes, dislikes, counts_for_rating
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                        ON CONFLICT (htreviews_rid) DO UPDATE SET
                            rating=EXCLUDED.rating, review_text=EXCLUDED.review_text,
                            likes=EXCLUDED.likes, dislikes=EXCLUDED.dislikes
                    """, rev['htreviews_rid'], tobacco_db_id, reviewer_id,
                        rev.get('rating'), rev.get('review_text'),
                        rev.get('reviewed_at'), rev.get('reviewed_at_raw'),
                        rev.get('likes', 0), rev.get('dislikes', 0),
                        rev.get('counts_for_rating', True))

            total_scraped += len(reviews)

        log.info(f"    Reviews offset={offset}: +{len(reviews)} (total={total_scraped})")
        offset += 30

        if len(reviews) < 25:  # Last page
            break

    # Mark tobacco as reviews_scraped
    async with pool.acquire() as conn:
        await conn.execute(
            'UPDATE htr_tobaccos SET reviews_scraped=TRUE WHERE id=$1', tobacco_db_id)

    return total_scraped


# ─── MAIN ────────────────────────────────────────────────────────────────────

async def main():
    log.info("Connecting to DB...")
    # Create schema first (before pool, without search_path set)
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

    async with httpx.AsyncClient() as client:
        # Step 1: Get all brand slugs
        brand_slugs = await get_all_brand_slugs(client)
        log.info(f"Total brands: {len(brand_slugs)}")

        for brand_slug in brand_slugs:
            log.info(f"\n{'='*50}")
            log.info(f"Processing brand: {brand_slug}")

            # Step 2: Scrape brand page
            brand_result = await scrape_brand(client, pool, brand_slug)
            if not brand_result:
                continue
            brand_id, _ = brand_result

            # Step 3: Get all tobacco URLs for this brand
            tobacco_urls = await get_tobacco_urls_for_brand(client, brand_slug)
            log.info(f"  Found {len(tobacco_urls)} tobacco URLs for {brand_slug}")

            for url_path in tobacco_urls:
                # Step 4: Scrape individual tobacco page
                tobacco_db_id = await scrape_tobacco_page(client, pool, url_path, brand_id)
                if not tobacco_db_id:
                    continue

                # Step 5: Scrape all reviews for this tobacco
                # Get htreviews_id from DB
                async with pool.acquire() as conn:
                    row = await conn.fetchrow(
                        'SELECT htreviews_id, reviews_scraped FROM htr_tobaccos WHERE id=$1',
                        tobacco_db_id)
                    if row and not row['reviews_scraped']:
                        htid = row['htreviews_id']
                        n = await scrape_all_reviews(client, pool, tobacco_db_id, htid)
                        log.info(f"    Total reviews scraped: {n}")

    await pool.close()
    log.info("Done!")


if __name__ == '__main__':
    asyncio.run(main())
