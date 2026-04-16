"""
HookahLab — FastAPI Backend
Port: 8082
"""
from fastapi import FastAPI, Request, Header, HTTPException, Depends
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import asyncpg, hashlib, secrets, json, re, httpx, os, bcrypt as _bcrypt
from typing import Optional

# ── TRANSLITERATION ──────────────────────────────────────────────────────────

_RU_EN = {
    'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','ё':'yo','ж':'zh','з':'z',
    'и':'i','й':'y','к':'k','л':'l','м':'m','н':'n','о':'o','п':'p','р':'r',
    'с':'s','т':'t','у':'u','ф':'f','х':'kh','ц':'ts','ч':'ch','ш':'sh',
    'щ':'sch','ъ':'','ы':'y','ь':'','э':'e','ю':'yu','я':'ya',
}
_EN_RU = {
    'darkside':'даркрсайд','tangiers':'танжерс','satyr':'сатир','must':'маст',
    'adalya':'адалья','al fakher':'аль фахер','jookah':'джукан',
    'supernova':'супернова','blueberry':'черника','strawberry':'клубника',
    'mango':'манго','peach':'персик','grape':'виноград','mint':'мята',
    'watermelon':'арбуз','lemon':'лимон','orange':'апельсин','apple':'яблоко',
    'raspberry':'малина','cherry':'вишня','vanilla':'ваниль',
}
# Словарь русских названий брендов/вкусов → английские
_RU_BRAND_DICT = {
    # Бренды
    'даркрсайд': 'darkside', 'даркрсайт': 'darkside',
    'танжерс': 'tangiers', 'тангирс': 'tangiers',
    'сатир': 'satyr', 'маст': 'must', 'мает': 'must',
    'адалья': 'adalya', 'адалиа': 'adalya',
    'аль': 'al', 'фахер': 'fakher',
    'соус': 'sauce', 'одиссей': 'odissey',
    'облако': 'oblako', 'матрикс': 'matrix',
    'бондс': 'bonds', 'бонд': 'bond',
    'элита': 'element', 'джарир': 'jарير',
    'кесма': 'kesma', 'трабзон': 'trabzon',
    'хулиган': 'hooligan', 'пираты': 'pirates',
    # Вкусы и линейки
    'суперновa': 'supernova', 'супернова': 'supernova',
    'черника': 'blueberry', 'клубника': 'strawberry',
    'малина': 'raspberry', 'вишня': 'cherry',
    'персик': 'peach', 'манго': 'mango',
    'арбуз': 'watermelon', 'дыня': 'melon',
    'лимон': 'lemon', 'апельсин': 'orange',
    'яблоко': 'apple', 'груша': 'pear',
    'виноград': 'grape', 'слива': 'plum',
    'мята': 'mint', 'мятный': 'mint',
    'холод': 'cold', 'холодок': 'ice',
    'ваниль': 'vanilla', 'карамель': 'caramel',
    'кофе': 'coffee', 'шоколад': 'chocolate',
    'банан': 'banana', 'ананас': 'pineapple',
    'кокос': 'coconut', 'персик': 'peach',
    'медитация': 'meditation', 'тропик': 'tropic',
    'нуар': 'noir', 'кор': 'core', 'бейс': 'base',
    'шот': 'shot', 'нуль': 'null',
}

def translit_ru_en(text: str) -> str:
    """Транслитерация русского текста в латиницу"""
    res = ''
    for ch in text.lower():
        res += _RU_EN.get(ch, ch)
    return res

def expand_query(q: str) -> list[str]:
    """Возвращает список вариантов запроса: оригинал + транслитерация + словарный перевод"""
    q = q.strip()
    variants = {q}

    has_cyrillic = any('\u0400' <= c <= '\u04ff' for c in q)

    if has_cyrillic:
        # 1. Обычная транслитерация целиком
        translit = translit_ru_en(q)
        if translit != q:
            variants.add(translit)

        # 2. Пословарный перевод: заменяем каждое слово по словарю брендов
        words = q.lower().split()
        translated_words = []
        any_replaced = False
        for w in words:
            if w in _RU_BRAND_DICT:
                translated_words.append(_RU_BRAND_DICT[w])
                any_replaced = True
            else:
                # Транслитерируем слово
                translated_words.append(translit_ru_en(w))
        if any_replaced:
            variants.add(' '.join(translated_words))

        # 3. Версия только из словарных замен (без других слов)
        dict_words = [_RU_BRAND_DICT[w] for w in words if w in _RU_BRAND_DICT]
        if dict_words:
            variants.add(' '.join(dict_words))
    else:
        # Запрос на английском — ищем русский эквивалент
        ql = q.lower()
        if ql in _EN_RU:
            variants.add(_EN_RU[ql])

    return list(variants)[:4]  # максимум 4 варианта

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

try:
    app.mount("/static", StaticFiles(directory="static"), name="static")
except Exception:
    pass

DB_DSN   = os.environ.get("DB_DSN", "postgresql://hookah:hookah@localhost:5432/hookah_db")
GROQ_KEY = os.environ.get("GROQ_KEY", "")
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "llama-3.3-70b-versatile"
TABAK_OPENAI_SEARCH_URL = os.environ.get("TABAK_OPENAI_SEARCH_URL", "http://127.0.0.1:5051/api/search")

pool = None

AI_QUERY_SYSTEM_PROMPT = """You normalize user search queries for hookah tobacco catalog search.
Return JSON only:
{
  "clean_query": "...",
  "search_query": "...",
  "tags": ["..."],
  "strength_pref": "легкий|средний|крепкий|любой"
}
Rules:
- Keep clean_query short and natural.
- search_query should contain key flavor intent words.
- tags should contain 2-8 short flavor tags in Russian.
- If strength is unknown, set strength_pref to "любой".
"""


def _strength_pref_to_sql_like(pref: str) -> str:
    p = (pref or "").strip().lower()
    if "лег" in p:
        return "лёгк"
    if "креп" in p:
        return "креп"
    if "сред" in p:
        return "средн"
    return ""


def _tokenize_ai_query(text: str) -> list[str]:
    if not text:
        return []
    words = re.findall(r"[a-zA-Zа-яА-ЯёЁ0-9]{2,}", text.lower())
    stop = {
        "и", "или", "для", "под", "очень", "мне", "хочу", "нужно", "как", "что",
        "без", "с", "на", "по", "в", "во", "из", "к", "до", "не", "это", "тот",
    }
    out: list[str] = []
    for w in words:
        if w in stop:
            continue
        if w not in out:
            out.append(w)
    return out[:14]


async def _parse_ai_catalog_query(query: str) -> dict:
    q = (query or "").strip()
    fallback = {
        "clean_query": q,
        "search_query": q,
        "tags": [],
        "strength_pref": "любой",
    }
    if not q or not GROQ_KEY:
        return fallback
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(
                GROQ_URL,
                json={
                    "model": GROQ_MODEL,
                    "messages": [
                        {"role": "system", "content": AI_QUERY_SYSTEM_PROMPT},
                        {"role": "user", "content": q},
                    ],
                    "temperature": 0.1,
                    "max_tokens": 300,
                    "response_format": {"type": "json_object"},
                },
                headers={"Authorization": f"Bearer {GROQ_KEY}"},
            )
        content = (resp.json().get("choices") or [{}])[0].get("message", {}).get("content", "{}")
        parsed = json.loads(content)
        tags = parsed.get("tags") if isinstance(parsed.get("tags"), list) else []
        tags = [str(t).strip() for t in tags if str(t).strip()][:8]
        strength_pref = str(parsed.get("strength_pref") or "любой").strip().lower()
        if strength_pref not in ("легкий", "лёгкий", "средний", "крепкий", "любой"):
            strength_pref = "любой"
        return {
            "clean_query": str(parsed.get("clean_query") or q).strip(),
            "search_query": str(parsed.get("search_query") or q).strip(),
            "tags": tags,
            "strength_pref": strength_pref,
        }
    except Exception:
        return fallback


def _score_strength_label(item: dict) -> int:
    txt = f"{item.get('strength_user') or ''} {item.get('strength_official') or ''}".lower()
    if "креп" in txt:
        return 3
    if "лёг" in txt or "лег" in txt:
        return 1
    return 2


def _clean_tobacco_name(name: str) -> str:
    return re.sub(r"^Табак для кальяна\s*", "", str(name or ""), flags=re.IGNORECASE).strip()


def _extract_brand_name_from_title(title: str) -> tuple[str, str]:
    t = _clean_tobacco_name(title or "")
    t = t.replace('"', "").replace("«", "").replace("»", "").strip()
    left = t.split(",")[0].strip()
    words = left.split()
    if not words:
        return "", t
    if len(words) >= 2 and words[0].lower() in ("must", "dark", "al", "black"):
        brand = f"{words[0]} {words[1]}"
        name = left[len(brand):].strip(" -")
        return brand, (name or left)
    brand = words[0]
    name = left[len(brand):].strip(" -")
    return brand, (name or left)


def _coal_tip_from_heat(heat: str) -> str:
    h = (heat or "").strip()
    if not h:
        return "2 угля стандарт"
    m = re.search(r"Старт:\s*([^;]+)", h, flags=re.IGNORECASE)
    return m.group(1).strip() if m else h


def _strength_from_score(score: float | int | None) -> str:
    try:
        s = float(score or 0)
    except Exception:
        s = 0.0
    if s >= 3.6:
        return "крепкий"
    if s <= 1.8:
        return "лёгкий"
    return "средний"


def _norm_text(v: str) -> str:
    s = re.sub(r"\([^)]*\)", " ", str(v or "").lower())
    s = re.sub(r"\d+(?:[.,]\d+)?\s*(?:г|гр|g|gr)\b", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"[^a-zа-я0-9]+", " ", s, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", s).strip()


def _cabinet_matchers(cabinet_items: list[dict]) -> tuple[list[str], list[tuple[str, str]]]:
    names: list[str] = []
    pairs: list[tuple[str, str]] = []
    for c in cabinet_items or []:
        b = _norm_text(c.get("brand") or "")
        n = _norm_text(_clean_tobacco_name(c.get("name") or ""))
        if n:
            names.append(n)
        if b and n:
            pairs.append((b, n))
    return names, pairs


def _is_in_cabinet(title: str, brand: str, cab_names: list[str], cab_pairs: list[tuple[str, str]]) -> bool:
    cb = _norm_text(brand or "")
    cn = _norm_text(_clean_tobacco_name(title or ""))
    if not cn:
        return False
    if any((n and (n in cn or cn in n)) for n in cab_names):
        return True
    if cb and any((b in cb or cb in b) and (n in cn or cn in n) for b, n in cab_pairs):
        return True
    return False


async def _tabak_openai_search(query: str, mode: str = "mix") -> dict:
    m = (mode or "mix").strip().lower()
    if m not in ("single", "mix"):
        m = "mix"
    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.post(
            TABAK_OPENAI_SEARCH_URL,
            json={"query": query, "mode": m},
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
        return resp.json()


def _sort_ai_ranked_items(ranked: list[dict], sort: str) -> list[dict]:
    if sort == "price_asc":
        ranked.sort(key=lambda x: (x.get("price") is None, x.get("price") or 0))
    elif sort == "price_desc":
        ranked.sort(key=lambda x: (x.get("price") is None, -(x.get("price") or 0)))
    elif sort == "reviews":
        ranked.sort(key=lambda x: (x.get("total_reviews") or 0), reverse=True)
    elif sort == "rating":
        ranked.sort(
            key=lambda x: (
                x.get("_ai_score") or 0,
                x.get("avg_rating") or 0,
                x.get("total_reviews") or 0,
            ),
            reverse=True,
        )
    else:
        ranked.sort(key=lambda x: x.get("_ai_score") or 0, reverse=True)
    return ranked


async def _run_ai_catalog_search(
    query: str,
    in_stock: bool = False,
    strength: str = "",
    cabinet_ids: list[int] | None = None,
) -> tuple[dict, list[dict]]:
    q = (query or "").strip()
    ai = await _parse_ai_catalog_query(q)
    ai_strength_like = _strength_pref_to_sql_like(ai.get("strength_pref") or "")
    ui_strength_like = _strength_pref_to_sql_like(strength)
    strength_like = ui_strength_like or ai_strength_like

    use_cabinet = bool(cabinet_ids)
    cab_ids = [int(x) for x in (cabinet_ids or []) if str(x).isdigit()]
    cab_ids = list(dict.fromkeys(cab_ids))[:400]

    async with pool.acquire() as conn:
        if use_cabinet and cab_ids:
            rows = await conn.fetch(
                """
                SELECT a.id, a.name, a.brand_name as brand, a.line, a.weight,
                       a.price, a.has_discount, a.price_before_discount,
                       a.in_stock, a.stores_count, a.total_amount,
                       a.one_c_id,
                       a.image_url, a.is_bestseller,
                       ht.avg_rating, ht.strength_user, ht.strength_official,
                       ht.flavor_tags, ht.total_reviews, ht.url_path as htr_url
                FROM scraper.ali_products a
                LEFT JOIN scraper.htr_tobaccos ht ON
                    NULLIF(regexp_replace(a.htreviews_id,'[^0-9]','','g'),'')::int = ht.htreviews_id
                WHERE a.id = ANY($1::int[])
                  AND (NOT $2 OR a.in_stock = TRUE)
                  AND ($3 = '' OR ht.strength_user ILIKE '%'||$3||'%' OR ht.strength_official ILIKE '%'||$3||'%')
                LIMIT 2500
                """,
                cab_ids, in_stock, strength_like,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT a.id, a.name, a.brand_name as brand, a.line, a.weight,
                       a.price, a.has_discount, a.price_before_discount,
                       a.in_stock, a.stores_count, a.total_amount,
                       a.one_c_id,
                       a.image_url, a.is_bestseller,
                       ht.avg_rating, ht.strength_user, ht.strength_official,
                       ht.flavor_tags, ht.total_reviews, ht.url_path as htr_url
                FROM scraper.ali_products a
                LEFT JOIN scraper.htr_tobaccos ht ON
                    NULLIF(regexp_replace(a.htreviews_id,'[^0-9]','','g'),'')::int = ht.htreviews_id
                WHERE (NOT $1 OR a.in_stock = TRUE)
                  AND ($2 = '' OR ht.strength_user ILIKE '%'||$2||'%' OR ht.strength_official ILIKE '%'||$2||'%')
                LIMIT 5000
                """,
                in_stock, strength_like,
            )

    terms = _tokenize_ai_query(q)
    for source in (ai.get("clean_query"), ai.get("search_query")):
        terms.extend(_tokenize_ai_query(str(source or "")))
    for tag in (ai.get("tags") or []):
        terms.extend(_tokenize_ai_query(str(tag)))

    uniq_terms: list[str] = []
    for t in terms:
        if t not in uniq_terms:
            uniq_terms.append(t)
    terms = uniq_terms[:18]

    clean_query = str(ai.get("clean_query") or q).lower()
    search_query = str(ai.get("search_query") or clean_query).lower()

    scored: list[tuple[float, dict]] = []
    for r in rows:
        item = dict(r)
        name = str(item.get("name") or "")
        brand = str(item.get("brand") or "")
        line = str(item.get("line") or "")
        tags = " ".join(item.get("flavor_tags") or [])
        text = f"{name} {brand} {line} {tags}".lower()

        score = 0.0
        if clean_query and clean_query in text:
            score += 5.0
        if search_query and search_query in text:
            score += 4.0
        if q.lower() in text:
            score += 3.2

        for t in terms:
            if t in text:
                score += 0.9
                if t in name.lower():
                    score += 0.45
                if t in tags.lower():
                    score += 0.3

        rating = item.get("avg_rating")
        if rating is not None:
            score += float(rating) / 6.0
        score += min(int(item.get("total_reviews") or 0), 350) / 700.0
        if item.get("in_stock"):
            score += 0.45
        if item.get("is_bestseller"):
            score += 0.3

        if strength_like:
            st = f"{item.get('strength_user') or ''} {item.get('strength_official') or ''}".lower()
            if strength_like in st:
                score += 0.7

        if score > 0.5:
            item["_ai_score"] = round(score, 5)
            scored.append((score, item))

    scored.sort(key=lambda x: x[0], reverse=True)
    return ai, [x[1] for x in scored]

@app.on_event("startup")
async def startup():
    global pool
    pool = await asyncpg.create_pool(DB_DSN, min_size=2, max_size=10)
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA_SQL)
        # Убираем FK-ограничения чтобы лайки/комменты работали на всех миксах
        for sql in [
            "ALTER TABLE hl_likes DROP CONSTRAINT IF EXISTS hl_likes_mix_id_fkey",
            "ALTER TABLE hl_saves DROP CONSTRAINT IF EXISTS hl_saves_mix_id_fkey",
            "ALTER TABLE hl_comments DROP CONSTRAINT IF EXISTS hl_comments_mix_id_fkey",
            "ALTER TABLE hl_user_setup ADD COLUMN IF NOT EXISTS coal_size TEXT DEFAULT ''",
            "ALTER TABLE hl_user_setup ADD COLUMN IF NOT EXISTS coal_warmup TEXT DEFAULT ''",
            "ALTER TABLE hl_user_setup ADD COLUMN IF NOT EXISTS bowls_json TEXT DEFAULT '[]'",
            "ALTER TABLE scraper.htr_tobaccos ADD COLUMN IF NOT EXISTS image_url TEXT",
            "ALTER TABLE scraper.htr_brands ADD COLUMN IF NOT EXISTS logo_url TEXT",
        ]:
            await conn.execute(sql)

@app.on_event("shutdown")
async def shutdown():
    if pool:
        await pool.close()

# ── SCHEMA ───────────────────────────────────────────────────────────────────

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS hl_users (
    id          SERIAL PRIMARY KEY,
    username    TEXT UNIQUE NOT NULL,
    email       TEXT UNIQUE NOT NULL,
    pass_hash   TEXT NOT NULL,
    bio         TEXT DEFAULT '',
    avatar      TEXT DEFAULT '🧔',
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS hl_sessions (
    token       TEXT PRIMARY KEY,
    user_id     INT REFERENCES hl_users(id) ON DELETE CASCADE,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS hl_user_setup (
    user_id     INT PRIMARY KEY REFERENCES hl_users(id) ON DELETE CASCADE,
    hookah      TEXT DEFAULT '',
    bowl        TEXT DEFAULT '',
    coal_size   TEXT DEFAULT '',
    coal_warmup TEXT DEFAULT '',
    bowl_type   TEXT DEFAULT '',
    coal        TEXT DEFAULT '',
    foil        TEXT DEFAULT '',
    flask_shape TEXT DEFAULT '',
    flask_color TEXT DEFAULT ''
);
CREATE TABLE IF NOT EXISTS hl_user_tobaccos (
    id          SERIAL PRIMARY KEY,
    user_id     INT REFERENCES hl_users(id) ON DELETE CASCADE,
    ali_id      INT,
    name        TEXT NOT NULL,
    brand       TEXT NOT NULL,
    line        TEXT DEFAULT '',
    weight      TEXT DEFAULT '',
    added_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(user_id, ali_id)
);
CREATE TABLE IF NOT EXISTS hl_mixes (
    id          SERIAL PRIMARY KEY,
    user_id     INT REFERENCES hl_users(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    description TEXT DEFAULT '',
    bowl_type   TEXT DEFAULT 'убивашка',
    bowl_grams  INT DEFAULT 20,
    pack_method TEXT DEFAULT 'секторами',
    coal_tip    TEXT DEFAULT '',
    strength    TEXT DEFAULT 'средний',
    is_public   BOOLEAN DEFAULT TRUE,
    is_llm      BOOLEAN DEFAULT FALSE,
    llm_prompt  TEXT DEFAULT '',
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS hl_mix_items (
    id          SERIAL PRIMARY KEY,
    mix_id      INT REFERENCES hl_mixes(id) ON DELETE CASCADE,
    ali_id      INT,
    tobacco_name TEXT NOT NULL,
    brand       TEXT NOT NULL,
    percentage  INT NOT NULL,
    sort_order  INT DEFAULT 0
);
CREATE TABLE IF NOT EXISTS hl_likes (
    user_id     INT REFERENCES hl_users(id) ON DELETE CASCADE,
    mix_id      INT REFERENCES hl_mixes(id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, mix_id)
);
CREATE TABLE IF NOT EXISTS hl_saves (
    user_id     INT REFERENCES hl_users(id) ON DELETE CASCADE,
    mix_id      INT REFERENCES hl_mixes(id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, mix_id)
);
CREATE TABLE IF NOT EXISTS hl_follows (
    follower_id  INT REFERENCES hl_users(id) ON DELETE CASCADE,
    following_id INT REFERENCES hl_users(id) ON DELETE CASCADE,
    PRIMARY KEY (follower_id, following_id)
);
CREATE TABLE IF NOT EXISTS hl_comments (
    id          SERIAL PRIMARY KEY,
    mix_id      INT REFERENCES hl_mixes(id) ON DELETE CASCADE,
    user_id     INT REFERENCES hl_users(id) ON DELETE CASCADE,
    text        TEXT NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS hl_notifications (
    id           SERIAL PRIMARY KEY,
    user_id      INT REFERENCES hl_users(id) ON DELETE CASCADE,
    type         TEXT NOT NULL,
    from_user_id INT REFERENCES hl_users(id) ON DELETE CASCADE,
    mix_id       INT REFERENCES hl_mixes(id) ON DELETE SET NULL,
    read         BOOLEAN DEFAULT FALSE,
    created_at   TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS hl_mix_ratings (
    user_id     INT REFERENCES hl_users(id) ON DELETE CASCADE,
    mix_id      INT REFERENCES hl_mixes(id) ON DELETE CASCADE,
    rating      INT NOT NULL CHECK(rating BETWEEN 1 AND 5),
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, mix_id)
);
"""

# ── HELPERS ──────────────────────────────────────────────────────────────────

def hash_pw(pw: str) -> str:
    """Hash password with bcrypt (cost=12)."""
    return _bcrypt.hashpw(pw.encode(), _bcrypt.gensalt(12)).decode()

def _verify_pw(pw: str, stored: str) -> bool:
    """Verify password. Supports bcrypt and legacy sha256 (auto-migration on login)."""
    if stored.startswith("$2"):          # bcrypt hash
        return _bcrypt.checkpw(pw.encode(), stored.encode())
    return stored == hashlib.sha256(pw.encode()).hexdigest()  # legacy sha256

def make_token() -> str:
    return secrets.token_hex(32)

async def get_user(authorization: Optional[str] = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        return None
    token = authorization[7:]
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT u.* FROM hl_sessions s JOIN hl_users u ON u.id=s.user_id WHERE s.token=$1", token)
        return dict(row) if row else None

async def req_user(authorization: Optional[str] = Header(None)):
    u = await get_user(authorization)
    if not u:
        raise HTTPException(401, "Не авторизован")
    return u

async def _mix_items(conn, mix_id: int):
    rows = await conn.fetch(
        "SELECT * FROM hl_mix_items WHERE mix_id=$1 ORDER BY sort_order", mix_id)
    return [dict(r) for r in rows]

async def _mix_stats(conn, mix_id: int):
    likes    = await conn.fetchval("SELECT COUNT(*) FROM hl_likes WHERE mix_id=$1", mix_id)
    saves    = await conn.fetchval("SELECT COUNT(*) FROM hl_saves WHERE mix_id=$1", mix_id)
    comments = await conn.fetchval("SELECT COUNT(*) FROM hl_comments WHERE mix_id=$1", mix_id)
    avg_r    = await conn.fetchval("SELECT ROUND(AVG(rating),1) FROM hl_mix_ratings WHERE mix_id=$1", mix_id)
    return {"likes": likes, "saves": saves, "comments": comments, "avg_rating": float(avg_r) if avg_r else None}

# ── SERVE ────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index():
    with open("templates/social.html", "r", encoding="utf-8") as f:
        return f.read()

# ── AUTH ─────────────────────────────────────────────────────────────────────

@app.post("/api/auth/register")
async def register(request: Request):
    d = await request.json()
    username = (d.get("username") or "").strip()
    email    = (d.get("email") or "").strip().lower()
    password = d.get("password") or ""
    if not username or not email or not password:
        raise HTTPException(400, "Заполни все поля")
    if len(username) < 3:
        raise HTTPException(400, "Никнейм минимум 3 символа")
    if len(password) < 6:
        raise HTTPException(400, "Пароль минимум 6 символов")
    async with pool.acquire() as conn:
        if await conn.fetchval("SELECT id FROM hl_users WHERE username=$1 OR email=$2", username, email):
            raise HTTPException(409, "Пользователь уже существует")
        uid = await conn.fetchval(
            "INSERT INTO hl_users (username,email,pass_hash) VALUES ($1,$2,$3) RETURNING id",
            username, email, hash_pw(password))
        await conn.execute("INSERT INTO hl_user_setup (user_id) VALUES ($1)", uid)
        token = make_token()
        await conn.execute("INSERT INTO hl_sessions (token,user_id) VALUES ($1,$2)", token, uid)
    return {"token": token, "username": username, "user_id": uid}

@app.post("/api/auth/login")
async def login(request: Request):
    d = await request.json()
    login_val = (d.get("login") or "").strip().lower()
    password  = d.get("password") or ""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM hl_users WHERE lower(email)=$1 OR lower(username)=$1",
            login_val)
        if not row or not _verify_pw(password, row["pass_hash"]):
            raise HTTPException(401, "Неверный логин или пароль")
        # Auto-migrate legacy sha256 → bcrypt on successful login
        if not row["pass_hash"].startswith("$2"):
            await conn.execute(
                "UPDATE hl_users SET pass_hash=$1 WHERE id=$2",
                hash_pw(password), row["id"])
        token = make_token()
        await conn.execute("INSERT INTO hl_sessions (token,user_id) VALUES ($1,$2)", token, row["id"])
    return {"token": token, "username": row["username"], "user_id": row["id"]}

@app.get("/api/me")
async def get_me(user=Depends(req_user)):
    async with pool.acquire() as conn:
        setup    = await conn.fetchrow("SELECT * FROM hl_user_setup WHERE user_id=$1", user["id"])
        tobaccos = await conn.fetch(
            "SELECT * FROM hl_user_tobaccos WHERE user_id=$1 ORDER BY added_at DESC", user["id"])
        mixes_count = await conn.fetchval(
            "SELECT COUNT(*) FROM hl_mixes WHERE user_id=$1", user["id"])
        followers = await conn.fetchval(
            "SELECT COUNT(*) FROM hl_follows WHERE following_id=$1", user["id"])
        following = await conn.fetchval(
            "SELECT COUNT(*) FROM hl_follows WHERE follower_id=$1", user["id"])
    return {
        "id": user["id"], "username": user["username"],
        "bio": user["bio"], "avatar": user["avatar"],
        "setup": dict(setup) if setup else {},
        "tobaccos": [dict(t) for t in tobaccos],
        "stats": {"mixes": mixes_count, "followers": followers, "following": following},
    }

@app.patch("/api/me")
async def update_me(request: Request, user=Depends(req_user)):
    d = await request.json()
    async with pool.acquire() as conn:
        if "username" in d and d["username"]:
            uname = d["username"].strip()
            if len(uname) < 3 or len(uname) > 30:
                raise HTTPException(400, "Никнейм: 3–30 символов")
            if not re.match(r'^[a-zA-Z0-9_а-яА-ЯёЁ]+$', uname):
                raise HTTPException(400, "Никнейм: только буквы, цифры и _")
            taken = await conn.fetchval(
                "SELECT 1 FROM hl_users WHERE lower(username)=$1 AND id!=$2",
                uname.lower(), user["id"])
            if taken:
                raise HTTPException(409, "Никнейм уже занят")
            await conn.execute("UPDATE hl_users SET username=$1 WHERE id=$2", uname, user["id"])
        if "bio" in d or "avatar" in d:
            await conn.execute(
                "UPDATE hl_users SET bio=COALESCE($1,bio), avatar=COALESCE($2,avatar) WHERE id=$3",
                d.get("bio"), d.get("avatar"), user["id"])
        # map "notes" → "foil" for backward compat
        if "notes" in d: d["foil"] = d.pop("notes")
        setup_fields = {k: d[k] for k in ("hookah","bowl","bowl_type","coal","coal_size","coal_warmup","foil","flask_shape","flask_color","bowls_json") if k in d}
        if setup_fields:
            sets = ", ".join(f"{k}=${i+2}" for i, k in enumerate(setup_fields))
            vals = list(setup_fields.values())
            await conn.execute(
                f"INSERT INTO hl_user_setup (user_id,{','.join(setup_fields)}) VALUES ($1,{','.join(f'${i+2}' for i in range(len(vals)))}) "
                f"ON CONFLICT (user_id) DO UPDATE SET {sets}",
                user["id"], *vals)
    return {"ok": True}

# ── TOBACCOS ─────────────────────────────────────────────────────────────────

@app.get("/api/tobaccos")
async def search_tobaccos(q: str = "", brand: str = "", limit: int = 40):
    async with pool.acquire() as conn:
        if not q.strip():
            # Без запроса — возвращаем топ из alibaba
            rows = await conn.fetch("""
                SELECT a.id, a.name, a.brand_name as brand, a.line, a.weight,
                       a.price, a.price_before_discount, a.has_discount,
                       a.in_stock, a.stores_count, a.total_amount,
                       a.htreviews_id, a.is_bestseller,
                       COALESCE(ht.image_url,
                           CASE WHEN a.image_url LIKE '/%'
                                THEN 'https://alibaba-market.ru' || a.image_url
                                ELSE a.image_url END
                       ) AS image_url,
                       ht.avg_rating, ht.strength_user, ht.strength_official,
                       ht.flavor_tags, ht.total_reviews, ht.url_path as htr_url
                FROM scraper.ali_products a
                LEFT JOIN scraper.htr_tobaccos ht ON
                    NULLIF(regexp_replace(a.htreviews_id,'[^0-9]','','g'),'')::int = ht.htreviews_id
                WHERE ($1 = '' OR a.brand_name ILIKE '%'||$1||'%')
                ORDER BY a.in_stock DESC NULLS LAST, ht.avg_rating DESC NULLS LAST, a.brand_name, a.name
                LIMIT $2
            """, brand, limit)
            return [dict(r) for r in rows]

        # Расширяем запрос транслитерацией
        variants = expand_query(q)

        # Строим tsvector-условие: ищем по каждому варианту запроса
        # plainto_tsquery обрабатывает многословные запросы (AND между словами)
        tsq_conditions_ali = " OR ".join([
            f"to_tsvector('simple', a.name || ' ' || COALESCE(a.brand_name,'')) @@ plainto_tsquery('simple', ${i+3})"
            for i in range(len(variants))
        ])
        params = [brand, limit] + variants

        rows = await conn.fetch(f"""
            WITH ali_matches AS (
                -- Только ali: чистые данные с brand/line/weight полями
                SELECT a.id, a.name, a.brand_name AS brand, a.line, a.weight,
                       a.price,
                       CASE WHEN a.image_url LIKE '/%'
                            THEN 'https://alibaba-market.ru' || a.image_url
                            ELSE a.image_url END AS image_url,
                       a.in_stock, a.htreviews_id, a.is_bestseller,
                       a.one_c_id,
                       -- флейвор = название минус граммовка и скобки с уточнениями
                       lower(trim(regexp_replace(
                           regexp_replace(
                               regexp_replace(a.name,
                                   '^Табак\\s+(для\\s+кальяна\\s+)?', '', 'i'),
                               '\\s*\\([^)]*\\)', '', 'gi'),
                           '\\s+', ' ', 'g'))) AS flavor_key
                FROM scraper.ali_products a
                WHERE ($1 = '' OR a.brand_name ILIKE '%'||$1||'%')
                  AND ({tsq_conditions_ali})
                  AND a.name IS NOT NULL
                ORDER BY a.in_stock DESC NULLS LAST, a.is_bestseller DESC, a.name
                LIMIT 200
            )
            SELECT DISTINCT ON (lower(m.brand), lower(coalesce(m.line,'')), m.flavor_key)
                m.id, 'ali' AS source, m.name, m.brand, m.line, m.weight,
                m.price, COALESCE(ht.image_url, m.image_url) AS image_url, m.in_stock, m.htreviews_id, m.is_bestseller,
                NULL::numeric as has_discount, NULL::int as price_before_discount,
                NULL::int as stores_count, NULL::int as total_amount,
                ht.avg_rating, ht.strength_user, ht.strength_official,
                ht.flavor_tags, ht.total_reviews, ht.url_path as htr_url
            FROM ali_matches m
            LEFT JOIN scraper.htr_tobaccos ht ON
                NULLIF(regexp_replace(COALESCE(m.htreviews_id,''),'[^0-9]','','g'),'')::int = ht.htreviews_id
            ORDER BY lower(m.brand), lower(coalesce(m.line,'')), m.flavor_key,
                     m.in_stock DESC NULLS LAST, m.is_bestseller DESC
            LIMIT $2
        """, *params)
        return [dict(r) for r in rows]

@app.get("/api/brands")
async def get_brands():
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT brand_name as brand,
                   COUNT(*) as total,
                   SUM(CASE WHEN in_stock THEN 1 ELSE 0 END) as in_stock_count,
                   ROUND(AVG(price)) as avg_price
            FROM scraper.ali_products
            GROUP BY brand_name
            ORDER BY in_stock_count DESC, brand_name
        """)
        return [dict(r) for r in rows]

@app.get("/api/tobaccos/{ali_id}/reviews")
async def get_tobacco_reviews(ali_id: int, limit: int = 20):
    async with pool.acquire() as conn:
        product = await conn.fetchrow(
            "SELECT htreviews_id, name FROM scraper.ali_products WHERE id=$1", ali_id)
        if not product or not product["htreviews_id"]:
            return {"reviews": [], "htr_info": None}

        htr_int = re.sub(r"[^0-9]", "", product["htreviews_id"] or "")
        if not htr_int:
            return {"reviews": [], "htr_info": None}

        htr = await conn.fetchrow(
            "SELECT * FROM scraper.htr_tobaccos WHERE htreviews_id=$1", int(htr_int))
        if not htr:
            return {"reviews": [], "htr_info": None}

        rows = await conn.fetch("""
            SELECT r.rating, r.review_text, r.reviewed_at, r.likes,
                   rv.username
            FROM scraper.htr_reviews r
            LEFT JOIN scraper.htr_reviewers rv ON rv.id=r.reviewer_id
            WHERE r.tobacco_id=$1
              AND r.review_text IS NOT NULL AND length(r.review_text) > 15
            ORDER BY r.likes DESC, r.reviewed_at DESC
            LIMIT $2
        """, htr["id"], limit)
        return {"htr_info": dict(htr), "reviews": [dict(r) for r in rows]}

# ── USER TOBACCOS (cabinet) ──────────────────────────────────────────────────

@app.post("/api/cabinet/tobaccos")
async def add_to_cabinet(request: Request, user=Depends(req_user)):
    d = await request.json()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO hl_user_tobaccos (user_id, ali_id, name, brand, line, weight)
            VALUES ($1,$2,$3,$4,$5,$6)
            ON CONFLICT (user_id, ali_id) DO NOTHING
        """, user["id"], d.get("ali_id"), d.get("name",""), d.get("brand",""),
            d.get("line",""), d.get("weight",""))
    return {"ok": True}

@app.delete("/api/cabinet/tobaccos/{ali_id}")
async def remove_from_cabinet(ali_id: int, user=Depends(req_user)):
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM hl_user_tobaccos WHERE user_id=$1 AND ali_id=$2", user["id"], ali_id)
    return {"ok": True}

# ── MIXES ────────────────────────────────────────────────────────────────────

@app.post("/api/mixes")
async def create_mix(request: Request, user=Depends(req_user)):
    d = await request.json()
    name  = (d.get("name") or "Без названия").strip()
    items = d.get("items", [])
    if not items:
        raise HTTPException(400, "Добавь хотя бы один табак")
    async with pool.acquire() as conn:
        mix_id = await conn.fetchval("""
            INSERT INTO hl_mixes (user_id,name,description,bowl_type,bowl_grams,
                                  pack_method,coal_tip,strength,is_public,is_llm,llm_prompt)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING id
        """, user["id"], name, d.get("description",""),
            d.get("bowl_type","убивашка"), d.get("bowl_grams",20),
            d.get("pack_method","секторами"), d.get("coal_tip",""),
            d.get("strength","средний"), d.get("is_public",True),
            d.get("is_llm",False), d.get("llm_prompt",""))
        for i, item in enumerate(items):
            await conn.execute("""
                INSERT INTO hl_mix_items (mix_id,ali_id,tobacco_name,brand,percentage,sort_order)
                VALUES ($1,$2,$3,$4,$5,$6)
            """, mix_id, item.get("ali_id"), item.get("tobacco_name",""),
                item.get("brand",""), item.get("percentage",0), i)
    return {"id": mix_id, "ok": True}

@app.get("/api/mixes")
async def get_my_mixes(user=Depends(req_user)):
    return await _fetch_mixes(user["id"])

@app.get("/api/mixes/{mix_id}")
async def get_mix(mix_id: int):
    async with pool.acquire() as conn:
        mix = await conn.fetchrow(
            "SELECT m.*,u.username FROM hl_mixes m JOIN hl_users u ON u.id=m.user_id WHERE m.id=$1",
            mix_id)
        if not mix:
            raise HTTPException(404, "Микс не найден")
        items = await _mix_items(conn, mix_id)
        stats = await _mix_stats(conn, mix_id)
    return {**dict(mix), "items": items, **stats}

@app.delete("/api/mixes/{mix_id}")
async def delete_mix(mix_id: int, user=Depends(req_user)):
    async with pool.acquire() as conn:
        mix = await conn.fetchrow("SELECT user_id FROM hl_mixes WHERE id=$1", mix_id)
        if not mix or mix["user_id"] != user["id"]:
            raise HTTPException(403, "Нет доступа")
        await conn.execute("DELETE FROM hl_mixes WHERE id=$1", mix_id)
    return {"ok": True}

@app.post("/api/mixes/{mix_id}/like")
async def toggle_like(mix_id: int, user=Depends(req_user)):
    async with pool.acquire() as conn:
        exists = await conn.fetchval(
            "SELECT 1 FROM hl_likes WHERE user_id=$1 AND mix_id=$2", user["id"], mix_id)
        if exists:
            await conn.execute("DELETE FROM hl_likes WHERE user_id=$1 AND mix_id=$2", user["id"], mix_id)
            liked = False
        else:
            await conn.execute("INSERT INTO hl_likes (user_id,mix_id) VALUES ($1,$2)", user["id"], mix_id)
            liked = True
        count = await conn.fetchval("SELECT COUNT(*) FROM hl_likes WHERE mix_id=$1", mix_id)
        if liked:
            owner = await conn.fetchval("SELECT user_id FROM hl_mixes WHERE id=$1", mix_id)
            if owner and owner != user["id"]:
                try:
                    await conn.execute("""
                        INSERT INTO hl_notifications (user_id,type,from_user_id,mix_id)
                        VALUES ($1,'like',$2,$3)
                    """, owner, user["id"], mix_id)
                except Exception:
                    pass
    return {"liked": liked, "count": int(count)}

@app.post("/api/mixes/{mix_id}/save")
async def toggle_save(mix_id: int, user=Depends(req_user)):
    async with pool.acquire() as conn:
        exists = await conn.fetchval(
            "SELECT 1 FROM hl_saves WHERE user_id=$1 AND mix_id=$2", user["id"], mix_id)
        if exists:
            await conn.execute("DELETE FROM hl_saves WHERE user_id=$1 AND mix_id=$2", user["id"], mix_id)
            saved = False
        else:
            await conn.execute("INSERT INTO hl_saves (user_id,mix_id) VALUES ($1,$2)", user["id"], mix_id)
            saved = True
        count = await conn.fetchval("SELECT COUNT(*) FROM hl_saves WHERE mix_id=$1", mix_id)
    return {"saved": saved, "count": count}

# ── FEED ─────────────────────────────────────────────────────────────────────

@app.get("/api/feed")
async def get_feed(offset: int = 0, limit: int = 20):
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT m.id, m.name, m.description, m.bowl_type, m.bowl_grams,
                   m.pack_method, m.coal_tip, m.strength, m.created_at, m.is_llm,
                   u.username, u.avatar,
                   (SELECT COUNT(*) FROM hl_likes    l WHERE l.mix_id=m.id) AS likes,
                   (SELECT COUNT(*) FROM hl_saves    s WHERE s.mix_id=m.id) AS saves,
                   (SELECT COUNT(*) FROM hl_comments c WHERE c.mix_id=m.id) AS comments,
                   COALESCE(
                       (SELECT JSON_AGG(mi ORDER BY mi.sort_order)
                        FROM hl_mix_items mi WHERE mi.mix_id=m.id),
                       '[]'::json
                   ) AS items
            FROM hl_mixes m
            JOIN hl_users u ON u.id=m.user_id
            WHERE m.is_public=TRUE
            ORDER BY m.created_at DESC
            OFFSET $1 LIMIT $2
        """, offset, limit)
        return [dict(row) for row in rows]

@app.get("/api/saved")
async def get_saved(user=Depends(req_user)):
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT m.id, m.name, m.description, m.bowl_type, m.bowl_grams,
                   m.pack_method, m.coal_tip, m.strength, m.created_at,
                   u.username, u.avatar,
                   (SELECT COUNT(*) FROM hl_likes l WHERE l.mix_id=m.id) AS likes,
                   COALESCE(
                       (SELECT JSON_AGG(mi ORDER BY mi.sort_order)
                        FROM hl_mix_items mi WHERE mi.mix_id=m.id),
                       '[]'::json
                   ) AS items
            FROM hl_saves s
            JOIN hl_mixes m ON m.id=s.mix_id
            JOIN hl_users u ON u.id=m.user_id
            WHERE s.user_id=$1
            ORDER BY m.created_at DESC
        """, user["id"])
        return [dict(row) for row in rows]

# ── PROFILE ──────────────────────────────────────────────────────────────────

@app.get("/api/profile/{username}")
async def get_profile(username: str, authorization: Optional[str] = Header(None)):
    viewer = None
    if authorization and authorization.startswith("Bearer "):
        async with pool.acquire() as conn:
            viewer = await conn.fetchrow(
                "SELECT u.id FROM hl_sessions s JOIN hl_users u ON u.id=s.user_id WHERE s.token=$1",
                authorization[7:])
    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM hl_users WHERE lower(username)=$1", username.lower())
        if not user:
            raise HTTPException(404, "Пользователь не найден")
        setup = await conn.fetchrow("SELECT * FROM hl_user_setup WHERE user_id=$1", user["id"])
        tobaccos = await conn.fetch(
            "SELECT * FROM hl_user_tobaccos WHERE user_id=$1 ORDER BY added_at DESC", user["id"])
        followers = await conn.fetchval(
            "SELECT COUNT(*) FROM hl_follows WHERE following_id=$1", user["id"])
        following = await conn.fetchval(
            "SELECT COUNT(*) FROM hl_follows WHERE follower_id=$1", user["id"])
        is_following = False
        if viewer and viewer["id"] != user["id"]:
            is_following = bool(await conn.fetchval(
                "SELECT 1 FROM hl_follows WHERE follower_id=$1 AND following_id=$2",
                viewer["id"], user["id"]))
        mixes = await _fetch_mixes(user["id"])
    return {
        "id": user["id"], "username": user["username"],
        "bio": user["bio"], "avatar": user["avatar"],
        "is_following": is_following,
        "setup": dict(setup) if setup else {},
        "tobaccos": [dict(t) for t in tobaccos],
        "mixes": mixes,
        "stats": {"mixes": len(mixes), "followers": followers, "following": following},
    }

@app.post("/api/follow/{username}")
async def toggle_follow(username: str, user=Depends(req_user)):
    async with pool.acquire() as conn:
        target = await conn.fetchrow("SELECT id FROM hl_users WHERE lower(username)=$1", username.lower())
        if not target or target["id"] == user["id"]:
            raise HTTPException(400, "Нельзя")
        exists = await conn.fetchval(
            "SELECT 1 FROM hl_follows WHERE follower_id=$1 AND following_id=$2",
            user["id"], target["id"])
        if exists:
            await conn.execute(
                "DELETE FROM hl_follows WHERE follower_id=$1 AND following_id=$2",
                user["id"], target["id"])
            return {"following": False}
        await conn.execute(
            "INSERT INTO hl_follows (follower_id,following_id) VALUES ($1,$2)",
            user["id"], target["id"])
        await conn.execute("""
            INSERT INTO hl_notifications (user_id,type,from_user_id)
            VALUES ($1,'follow',$2)
        """, target["id"], user["id"])
        return {"following": True}


# ── COMMENTS ─────────────────────────────────────────────────────────────────

@app.get("/api/mixes/{mix_id}/comments")
async def get_comments(mix_id: int):
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT c.id, c.text, c.created_at,
                   u.username, u.avatar
            FROM hl_comments c
            JOIN hl_users u ON u.id=c.user_id
            WHERE c.mix_id=$1
            ORDER BY c.created_at ASC
        """, mix_id)
        return [dict(r) for r in rows]

@app.post("/api/mixes/{mix_id}/comments")
async def add_comment(mix_id: int, request: Request, user=Depends(req_user)):
    d = await request.json()
    text = (d.get("text") or "").strip()
    if not text or len(text) > 500:
        raise HTTPException(400, "Текст 1–500 символов")
    async with pool.acquire() as conn:
        cid = await conn.fetchval("""
            INSERT INTO hl_comments (mix_id, user_id, text)
            VALUES ($1,$2,$3) RETURNING id
        """, mix_id, user["id"], text)
        owner = await conn.fetchval("SELECT user_id FROM hl_mixes WHERE id=$1", mix_id)
        if owner and owner != user["id"]:
            try:
                await conn.execute("""
                    INSERT INTO hl_notifications (user_id,type,from_user_id,mix_id)
                    VALUES ($1,'comment',$2,$3)
                """, owner, user["id"], mix_id)
            except Exception:
                pass
    return {"id": cid, "ok": True}

@app.delete("/api/comments/{comment_id}")
async def delete_comment(comment_id: int, user=Depends(req_user)):
    async with pool.acquire() as conn:
        c = await conn.fetchrow("SELECT user_id FROM hl_comments WHERE id=$1", comment_id)
        if not c or c["user_id"] != user["id"]:
            raise HTTPException(403, "Нет доступа")
        await conn.execute("DELETE FROM hl_comments WHERE id=$1", comment_id)
    return {"ok": True}

# ── NOTIFICATIONS ─────────────────────────────────────────────────────────────

@app.get("/api/notifications")
async def get_notifications(user=Depends(req_user), limit: int = 30):
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT n.id, n.type, n.read, n.created_at,
                   n.mix_id,
                   u.username as from_username, u.avatar as from_avatar,
                   m.name as mix_name
            FROM hl_notifications n
            JOIN hl_users u ON u.id=n.from_user_id
            LEFT JOIN hl_mixes m ON m.id=n.mix_id
            WHERE n.user_id=$1
            ORDER BY n.created_at DESC
            LIMIT $2
        """, user["id"], limit)
        unread = await conn.fetchval(
            "SELECT COUNT(*) FROM hl_notifications WHERE user_id=$1 AND read=FALSE", user["id"])
        return {"items": [dict(r) for r in rows], "unread": unread}

@app.post("/api/notifications/read-all")
async def mark_notifications_read(user=Depends(req_user)):
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE hl_notifications SET read=TRUE WHERE user_id=$1", user["id"])
    return {"ok": True}

# ── USER SEARCH ───────────────────────────────────────────────────────────────

@app.get("/api/search/users")
async def search_users(q: str = "", limit: int = 20):
    if len(q) < 2:
        return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT u.id, u.username, u.avatar, u.bio,
                   (SELECT COUNT(*) FROM hl_mixes m WHERE m.user_id=u.id) as mixes_count,
                   (SELECT COUNT(*) FROM hl_follows f WHERE f.following_id=u.id) as followers
            FROM hl_users u
            WHERE u.username ILIKE '%' || $1 || '%'
            ORDER BY followers DESC, mixes_count DESC
            LIMIT $2
        """, q, limit)
        return [dict(r) for r in rows]

# ── MIX RATING ────────────────────────────────────────────────────────────────

@app.post("/api/mixes/{mix_id}/rate")
async def rate_mix(mix_id: int, request: Request, user=Depends(req_user)):
    d = await request.json()
    rating = int(d.get("rating", 0))
    if not 1 <= rating <= 5:
        raise HTTPException(400, "Оценка от 1 до 5")
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO hl_mix_ratings (user_id, mix_id, rating)
            VALUES ($1,$2,$3)
            ON CONFLICT (user_id, mix_id) DO UPDATE SET rating=$3, created_at=NOW()
        """, user["id"], mix_id, rating)
        avg = await conn.fetchval(
            "SELECT ROUND(AVG(rating),1) FROM hl_mix_ratings WHERE mix_id=$1", mix_id)
    return {"avg_rating": float(avg) if avg else None, "ok": True}

# ── CATALOG ───────────────────────────────────────────────────────────────────

@app.get("/api/catalog/brands")
async def get_catalog_brands():
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT brand_name FROM scraper.ali_products
            WHERE brand_name IS NOT NULL AND brand_name != ''
            GROUP BY brand_name ORDER BY COUNT(*) DESC LIMIT 300
        """)
        return [r["brand_name"] for r in rows]

@app.get("/api/catalog/lines")
async def get_catalog_lines(brand: str = ""):
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT DISTINCT line FROM scraper.ali_products
            WHERE line IS NOT NULL AND line != ''
              AND ($1 = '' OR brand_name ILIKE '%'||$1||'%')
            ORDER BY line LIMIT 150
        """, brand)
        return [r["line"] for r in rows]

@app.get("/api/catalog")
async def get_catalog(
    q: str = "", brand: str = "", line: str = "", strength: str = "",
    in_stock: bool = False, min_rating: float = 0,
    sort: str = "rating", offset: int = 0, limit: int = 40
):
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT * FROM (
                SELECT DISTINCT ON (lower(brand_name), lower(coalesce(line,'')), flavor_key)
                       a.id, a.name, a.brand_name as brand, a.line, a.weight,
                       a.price, a.has_discount, a.price_before_discount,
                       a.in_stock, a.stores_count, a.total_amount,
                       COALESCE(
                         ht.image_url,
                         CASE WHEN a.image_url LIKE '/%'
                              THEN 'https://alibaba-market.ru' || a.image_url
                              ELSE a.image_url END
                       ) AS image_url,
                       a.is_bestseller,
                       ht.avg_rating, ht.strength_user, ht.strength_official,
                       ht.flavor_tags, ht.total_reviews, ht.url_path as htr_url,
                       hb.logo_url AS brand_logo_url
                FROM (
                    SELECT *,
                      lower(trim(regexp_replace(regexp_replace(name,'\\s*\\(уп.*?\\)','','gi'),'\\s*\\([^)]*\\)','','gi'))) AS flavor_key
                    FROM scraper.ali_products
                    WHERE ($1 = '' OR name ILIKE '%'||$1||'%' OR brand_name ILIKE '%'||$1||'%')
                      AND ($2 = '' OR brand_name ILIKE '%'||$2||'%')
                      AND ($3 = '' OR line ILIKE '%'||$3||'%')
                      AND (NOT $4 OR in_stock = TRUE)
                ) a
                LEFT JOIN scraper.htr_tobaccos ht ON
                    NULLIF(regexp_replace(a.htreviews_id,'[^0-9]','','g'),'')::int = ht.htreviews_id
                LEFT JOIN scraper.htr_brands hb ON hb.id = ht.brand_id
                WHERE ($5 = 0 OR ht.avg_rating >= $5)
                  AND ($6 = '' OR ht.strength_user ILIKE '%'||$6||'%' OR ht.strength_official ILIKE '%'||$6||'%')
                ORDER BY lower(brand_name), lower(coalesce(line,'')), flavor_key,
                         a.in_stock DESC NULLS LAST, a.price ASC NULLS LAST
            ) deduped
            ORDER BY
                CASE WHEN $7='rating'    THEN COALESCE(avg_rating,0) END DESC NULLS LAST,
                CASE WHEN $7='price_asc' THEN price END ASC NULLS LAST,
                CASE WHEN $7='price_desc' THEN price END DESC NULLS LAST,
                CASE WHEN $7='reviews'   THEN COALESCE(total_reviews,0) END DESC NULLS LAST,
                is_bestseller DESC, brand, name
            LIMIT $8 OFFSET $9
        """, q, brand, line, in_stock, min_rating, strength, sort, limit, offset)
        total = await conn.fetchval("""
            SELECT COUNT(*) FROM scraper.ali_products a
            LEFT JOIN scraper.htr_tobaccos ht ON
                NULLIF(regexp_replace(a.htreviews_id,'[^0-9]','','g'),'')::int = ht.htreviews_id
            WHERE ($1='' OR a.name ILIKE '%'||$1||'%' OR a.brand_name ILIKE '%'||$1||'%')
              AND ($2='' OR a.brand_name ILIKE '%'||$2||'%')
              AND ($3='' OR a.line ILIKE '%'||$3||'%')
              AND (NOT $4 OR a.in_stock=TRUE)
              AND ($5=0 OR ht.avg_rating>=$5)
        """, q, brand, line, in_stock, min_rating)
        return {"items": [dict(r) for r in rows], "total": total}


@app.get("/api/catalog/ai-search")
async def ai_catalog_search(
    q: str = "", in_stock: bool = False, strength: str = "",
    sort: str = "relevance", offset: int = 0, limit: int = 40
):
    query = (q or "").strip()
    if len(query) < 2:
        return {
            "items": [],
            "total": 0,
            "ai": {
                "clean_query": query,
                "search_query": query,
                "tags": [],
                "strength_pref": "любой",
            },
        }

    limit = max(1, min(80, int(limit or 40)))
    offset = max(0, int(offset or 0))
    ai, ranked = await _run_ai_catalog_search(query, in_stock=in_stock, strength=strength)
    ranked = _sort_ai_ranked_items(ranked, sort)

    total = len(ranked)
    page = ranked[offset: offset + limit]
    for it in page:
        it.pop("_ai_score", None)

    return {
        "items": page,
        "total": total,
        "ai": {
            "clean_query": ai.get("clean_query") or query,
            "search_query": ai.get("search_query") or query,
            "tags": ai.get("tags") or [],
            "strength_pref": ai.get("strength_pref") or "любой",
        },
    }

@app.get("/api/community-mixes")
async def get_community_mixes(limit: int = 20):
    async with pool.acquire() as conn:
        mixes = await conn.fetch("""
            SELECT id, title, tobaccos_summary, source_channel, origin_date
            FROM mixes
            WHERE title IS NOT NULL AND title != ''
            ORDER BY id
            LIMIT $1
        """, limit)
        result = []
        for m in mixes:
            items = await conn.fetch("""
                SELECT tobacco_name, brand, pack_grams, percentage
                FROM mix_items WHERE mix_id=$1 ORDER BY sort_order, id
            """, m["id"])
            if not items:
                continue
            result.append({
                "id": m["id"],
                "title": m["title"],
                "summary": m["tobaccos_summary"],
                "source": m["source_channel"],
                "created_at": m["origin_date"].isoformat() if m["origin_date"] else None,
                "items": [dict(i) for i in items],
            })
        return result

@app.get("/api/catalog/top-mixes")
async def get_top_mixes(period: str = "week", limit: int = 10):
    items_subq = """
        COALESCE(
            (SELECT JSON_AGG(mi ORDER BY mi.sort_order)
             FROM hl_mix_items mi WHERE mi.mix_id=m.id),
            '[]'::json
        ) AS items
    """
    async with pool.acquire() as conn:
        if period == "all":
            rows = await conn.fetch(f"""
                SELECT m.id, m.name, m.pack_method, m.bowl_type, m.strength,
                       u.username, u.avatar,
                       COUNT(DISTINCT l.user_id) AS likes,
                       COALESCE(ROUND(AVG(r.rating),1),0) AS avg_rating,
                       {items_subq}
                FROM hl_mixes m
                JOIN hl_users u ON u.id=m.user_id
                LEFT JOIN hl_likes l ON l.mix_id=m.id
                LEFT JOIN hl_mix_ratings r ON r.mix_id=m.id
                WHERE m.is_public=TRUE
                GROUP BY m.id, u.username, u.avatar
                ORDER BY likes DESC, avg_rating DESC
                LIMIT $1
            """, limit)
        else:
            days = 7 if period == "week" else 30
            rows = await conn.fetch(f"""
                SELECT m.id, m.name, m.pack_method, m.bowl_type, m.strength,
                       u.username, u.avatar,
                       COUNT(DISTINCT l.user_id) AS likes,
                       COALESCE(ROUND(AVG(r.rating),1),0) AS avg_rating,
                       {items_subq}
                FROM hl_mixes m
                JOIN hl_users u ON u.id=m.user_id
                LEFT JOIN hl_likes l ON l.mix_id=m.id
                LEFT JOIN hl_mix_ratings r ON r.mix_id=m.id
                WHERE m.is_public=TRUE
                  AND m.created_at > NOW() - ($1 * INTERVAL '1 day')
                GROUP BY m.id, u.username, u.avatar
                ORDER BY likes DESC, avg_rating DESC
                LIMIT $2
            """, days, limit)
        return [dict(r) for r in rows]

@app.get("/api/catalog/mixes")
async def get_catalog_mixes(
    q: str = "",
    sort: str = "new",
    strength: str = "",
    bowl_type: str = "",
    limit: int = 20,
    offset: int = 0,
):
    limit  = max(1, min(50, limit))
    offset = max(0, offset)
    # Whitelist sort to prevent any SQL injection risk
    sort = sort if sort in ("new", "top") else "new"
    order = "m.created_at DESC" if sort == "new" else "likes DESC, avg_rating DESC"
    async with pool.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT m.id, m.name, m.description, m.bowl_type, m.bowl_grams,
                   m.pack_method, m.coal_tip, m.strength, m.created_at, m.is_llm,
                   u.username, u.avatar,
                   (SELECT COUNT(*) FROM hl_likes    l WHERE l.mix_id = m.id) AS likes,
                   (SELECT COUNT(*) FROM hl_saves    s WHERE s.mix_id = m.id) AS saves,
                   (SELECT COUNT(*) FROM hl_comments c WHERE c.mix_id = m.id) AS comments,
                   COALESCE(ROUND(AVG(r.rating), 1), 0) AS avg_rating,
                   COALESCE(
                       (SELECT JSON_AGG(mi ORDER BY mi.sort_order)
                        FROM hl_mix_items mi WHERE mi.mix_id = m.id),
                       '[]'::json
                   ) AS items
            FROM hl_mixes m
            JOIN hl_users u ON u.id = m.user_id
            LEFT JOIN hl_mix_ratings r ON r.mix_id = m.id
            WHERE m.is_public = TRUE
              AND ($1 = '' OR m.name        ILIKE '%'||$1||'%'
                           OR m.description ILIKE '%'||$1||'%')
              AND ($2 = '' OR m.strength  ILIKE '%'||$2||'%')
              AND ($3 = '' OR m.bowl_type ILIKE '%'||$3||'%')
            GROUP BY m.id, u.username, u.avatar
            ORDER BY {order}
            LIMIT $4 OFFSET $5
        """, q, strength, bowl_type, limit, offset)

        total = await conn.fetchval("""
            SELECT COUNT(*)
            FROM hl_mixes m
            WHERE m.is_public = TRUE
              AND ($1 = '' OR m.name        ILIKE '%'||$1||'%'
                           OR m.description ILIKE '%'||$1||'%')
              AND ($2 = '' OR m.strength  ILIKE '%'||$2||'%')
              AND ($3 = '' OR m.bowl_type ILIKE '%'||$3||'%')
        """, q, strength, bowl_type)

        return {"items": [dict(r) for r in rows], "total": total}

# ── EQUIPMENT ─────────────────────────────────────────────────────────────────

@app.get("/api/equipment")
async def search_equipment(q: str = "", type: str = "", limit: int = 30):
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, source_id, type, name, brand, line, price, image_url, in_stock, extra
            FROM scraper.ali_equipment
            WHERE ($1 = '' OR type = $1)
              AND ($2 = '' OR name ILIKE '%'||$2||'%' OR brand ILIKE '%'||$2||'%')
            ORDER BY
                CASE WHEN $2 != '' AND name ILIKE $2||'%' THEN 0 ELSE 1 END,
                in_stock DESC NULLS LAST,
                name
            LIMIT $3
        """, type, q, limit)
        return [dict(r) for r in rows]

# ── LLM ──────────────────────────────────────────────────────────────────────

def _map_tabak_mix_card(mix: dict, prompt: str) -> dict:
    parts = mix.get("parts") or []
    items = []
    for p in parts:
        title = p.get("title") or ""
        brand, name = _extract_brand_name_from_title(title)
        items.append({
            "ali_id": None,
            "tobacco_name": name or _clean_tobacco_name(title),
            "brand": brand,
            "percentage": int(round(float(p.get("percent") or 0))),
            "strength": _strength_from_score(p.get("strength_score")),
            "description": p.get("flavor_desc") or "",
            "rating": p.get("rating"),
            "shop_url": p.get("url"),
            "review_url": None,
        })

    return {
        "name": (mix.get("title") or "ИИ микс").strip(),
        "description": mix.get("description") or "",
        "strength": _strength_from_score(mix.get("strength_score")),
        "bowl_type": (mix.get("bowl") or "фанел"),
        "pack_method": "секторами",
        "coal_tip": _coal_tip_from_heat(mix.get("heat") or ""),
        "items": items,
        "why": "Собрано из вашей RAG-базы tabak_openai (эмбеддинги + FAISS + mix-логика).",
        "is_llm": True,
        "llm_prompt": prompt,
        "source": "tabak_openai_mix",
        "heat": mix.get("heat") or "",
        "bowl_capacity_hint": mix.get("bowl_capacity_hint") or "",
    }


@app.post("/api/llm/search")
async def llm_search(request: Request):
    d = await request.json()
    prompt = (d.get("prompt") or "").strip()
    mode = (d.get("mode") or "mix").strip().lower()
    mix_parts = int(d.get("mix_parts") or 0)
    max_results = max(1, min(8, int(d.get("max_results") or 4)))
    cabinet_items = d.get("cabinet_items") if isinstance(d.get("cabinet_items"), list) else []
    use_cabinet = bool(d.get("use_cabinet")) and bool(cabinet_items)
    cab_names, cab_pairs = _cabinet_matchers(cabinet_items if use_cabinet else [])

    if not prompt or len(prompt) < 3:
        raise HTTPException(400, "Опиши желаемый вкус")
    if mode not in ("single", "mix"):
        mode = "mix"

    try:
        payload = await _tabak_openai_search(prompt, mode=mode)
        cards = payload.get("items") or []
        if mode == "single":
            out = []
            for c in cards:
                if c.get("card_type") != "single":
                    continue
                title = c.get("title") or ""
                brand, name = _extract_brand_name_from_title(title)
                if use_cabinet and not _is_in_cabinet(name or title, brand, cab_names, cab_pairs):
                    continue
                out.append({
                    "card_type": "single",
                    "id": c.get("id"),
                    "brand": brand,
                    "name": name or _clean_tobacco_name(title),
                    "title": title,
                    "description": c.get("flavor_desc") or "",
                    "strength": c.get("strength_label") or _strength_from_score(c.get("strength_score")),
                    "rating": c.get("rating"),
                    "grams": c.get("grams"),
                    "shop_url": c.get("url"),
                    "bowl_type": c.get("bowl"),
                    "coal_tip": _coal_tip_from_heat(c.get("heat") or ""),
                    "tags": c.get("tags") or "",
                })
                if len(out) >= max_results:
                    break
            return {
                "mode": "single",
                "query": prompt,
                "results": out,
                "source": "tabak_openai_single",
                "cabinet_filtered": use_cabinet,
            }

        mixes = [c for c in cards if c.get("card_type") == "mix" and (c.get("parts") or [])]
        if mix_parts in (2, 3, 4):
            mixes = [m for m in mixes if len(m.get("parts") or []) == mix_parts]
        if use_cabinet:
            filtered = []
            for m in mixes:
                parts = m.get("parts") or []
                ok = True
                for p in parts:
                    ptitle = p.get("title") or ""
                    pbrand, pname = _extract_brand_name_from_title(ptitle)
                    if not _is_in_cabinet(pname or ptitle, pbrand, cab_names, cab_pairs):
                        ok = False
                        break
                if ok:
                    filtered.append(m)
            mixes = filtered
        mixes = mixes[:max_results]
        return {
            "mode": "mix",
            "query": prompt,
            "results": [_map_tabak_mix_card(m, prompt) for m in mixes],
            "source": "tabak_openai_mix",
            "cabinet_filtered": use_cabinet,
        }
    except Exception as e:
        raise HTTPException(502, f"tabak_openai недоступен: {e}")


@app.post("/api/llm/generate")
async def llm_generate(request: Request):
    d = await request.json()
    prompt = (d.get("prompt") or "").strip()
    if not prompt or len(prompt) < 5:
        raise HTTPException(400, "Опиши желаемый вкус")
    try:
        payload = await _tabak_openai_search(prompt, mode="mix")
        mixes = payload.get("items") or []
        mix = next((m for m in mixes if m.get("card_type") == "mix" and (m.get("parts") or [])), None)
        if not mix:
            raise ValueError("No mix card from tabak_openai")
        return _map_tabak_mix_card(mix, prompt)
    except Exception as e:
        raise HTTPException(502, f"tabak_openai недоступен: {e}")

# ── INTERNAL ─────────────────────────────────────────────────────────────────

async def _fetch_mixes(user_id: int):
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT m.*,
                   (SELECT COUNT(*) FROM hl_likes    l WHERE l.mix_id=m.id) AS likes,
                   (SELECT COUNT(*) FROM hl_saves    s WHERE s.mix_id=m.id) AS saves,
                   (SELECT COUNT(*) FROM hl_comments c WHERE c.mix_id=m.id) AS comments,
                   COALESCE(ROUND(AVG(r.rating),1), 0) AS avg_rating,
                   COALESCE(
                       (SELECT JSON_AGG(mi ORDER BY mi.sort_order)
                        FROM hl_mix_items mi WHERE mi.mix_id=m.id),
                       '[]'::json
                   ) AS items
            FROM hl_mixes m
            LEFT JOIN hl_mix_ratings r ON r.mix_id=m.id
            WHERE m.user_id=$1
            GROUP BY m.id
            ORDER BY m.created_at DESC
        """, user_id)
        return [dict(r) for r in rows]
