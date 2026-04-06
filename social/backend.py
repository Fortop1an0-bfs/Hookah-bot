"""
HookahLab Social Network — FastAPI Backend
Port: 8082
"""
from fastapi import FastAPI, Request, Header, HTTPException, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import asyncpg, os, hashlib, secrets, json, re
from typing import Optional
from datetime import datetime

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/static", StaticFiles(directory="static"), name="static")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "hookah_db"),
    "user": os.getenv("DB_USER", "hookah"),
    "password": os.getenv("DB_PASS", "hookah123"),
}

pool = None

@app.on_event("startup")
async def startup():
    global pool
    pool = await asyncpg.create_pool(**DB_CONFIG, min_size=2, max_size=10)
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA_SQL)

@app.on_event("shutdown")
async def shutdown():
    if pool:
        await pool.close()

# ── SCHEMA ──────────────────────────────────────────────────────────────────

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
    coal        TEXT DEFAULT '',
    foil        TEXT DEFAULT ''
);
CREATE TABLE IF NOT EXISTS hl_user_tobaccos (
    user_id     INT REFERENCES hl_users(id) ON DELETE CASCADE,
    tobacco_id  INT REFERENCES tobaccos(id) ON DELETE CASCADE,
    added_at    TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, tobacco_id)
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
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS hl_mix_items (
    id          SERIAL PRIMARY KEY,
    mix_id      INT REFERENCES hl_mixes(id) ON DELETE CASCADE,
    tobacco_id  INT REFERENCES tobaccos(id),
    tobacco_name TEXT NOT NULL,
    brand       TEXT NOT NULL,
    percentage  INT NOT NULL,
    sort_order  INT DEFAULT 0
);
CREATE TABLE IF NOT EXISTS hl_posts (
    id          SERIAL PRIMARY KEY,
    user_id     INT REFERENCES hl_users(id) ON DELETE CASCADE,
    mix_id      INT REFERENCES hl_mixes(id) ON DELETE CASCADE,
    caption     TEXT DEFAULT '',
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS hl_likes (
    user_id     INT REFERENCES hl_users(id) ON DELETE CASCADE,
    post_id     INT REFERENCES hl_posts(id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, post_id)
);
CREATE TABLE IF NOT EXISTS hl_saves (
    user_id     INT REFERENCES hl_users(id) ON DELETE CASCADE,
    mix_id      INT REFERENCES hl_mixes(id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, mix_id)
);
CREATE TABLE IF NOT EXISTS hl_follows (
    follower_id INT REFERENCES hl_users(id) ON DELETE CASCADE,
    following_id INT REFERENCES hl_users(id) ON DELETE CASCADE,
    PRIMARY KEY (follower_id, following_id)
);
"""

# ── HELPERS ──────────────────────────────────────────────────────────────────

def hash_password(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()

def make_token() -> str:
    return secrets.token_hex(32)

async def get_current_user(authorization: Optional[str] = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        return None
    token = authorization[7:]
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT u.* FROM hl_sessions s JOIN hl_users u ON u.id=s.user_id WHERE s.token=$1", token)
        return dict(row) if row else None

async def require_user(authorization: Optional[str] = Header(None)):
    user = await get_current_user(authorization)
    if not user:
        raise HTTPException(401, "Не авторизован")
    return user

# ── SERVE HTML ───────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index():
    with open("templates/social.html", "r", encoding="utf-8") as f:
        return f.read()

# ── AUTH ──────────────────────────────────────────────────────────────────────

@app.post("/api/auth/register")
async def register(request: Request):
    data = await request.json()
    username = (data.get("username") or "").strip()
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""
    if not username or not email or not password:
        raise HTTPException(400, "Заполни все поля")
    if len(username) < 3:
        raise HTTPException(400, "Никнейм минимум 3 символа")
    if len(password) < 6:
        raise HTTPException(400, "Пароль минимум 6 символов")
    async with pool.acquire() as conn:
        existing = await conn.fetchval(
            "SELECT id FROM hl_users WHERE username=$1 OR email=$2", username, email)
        if existing:
            raise HTTPException(409, "Такой пользователь уже существует")
        user_id = await conn.fetchval(
            "INSERT INTO hl_users (username, email, pass_hash) VALUES ($1,$2,$3) RETURNING id",
            username, email, hash_password(password))
        await conn.execute("INSERT INTO hl_user_setup (user_id) VALUES ($1)", user_id)
        token = make_token()
        await conn.execute("INSERT INTO hl_sessions (token, user_id) VALUES ($1,$2)", token, user_id)
    return {"token": token, "username": username, "user_id": user_id}

@app.post("/api/auth/login")
async def login(request: Request):
    data = await request.json()
    login_val = (data.get("login") or "").strip().lower()
    password = data.get("password") or ""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM hl_users WHERE (lower(email)=$1 OR lower(username)=$1) AND pass_hash=$2",
            login_val, hash_password(password))
        if not row:
            raise HTTPException(401, "Неверный логин или пароль")
        token = make_token()
        await conn.execute("INSERT INTO hl_sessions (token, user_id) VALUES ($1,$2)", token, row["id"])
    return {"token": token, "username": row["username"], "user_id": row["id"]}

@app.get("/api/me")
async def get_me(user=Depends(require_user)):
    async with pool.acquire() as conn:
        setup = await conn.fetchrow("SELECT * FROM hl_user_setup WHERE user_id=$1", user["id"])
        tobaccos = await conn.fetch(
            """SELECT t.id, t.brand, t.flavor FROM hl_user_tobaccos ut
               JOIN tobaccos t ON t.id=ut.tobacco_id
               WHERE ut.user_id=$1 ORDER BY ut.added_at DESC""", user["id"])
        mixes_count = await conn.fetchval(
            "SELECT COUNT(*) FROM hl_mixes WHERE user_id=$1", user["id"])
        followers = await conn.fetchval(
            "SELECT COUNT(*) FROM hl_follows WHERE following_id=$1", user["id"])
        following = await conn.fetchval(
            "SELECT COUNT(*) FROM hl_follows WHERE follower_id=$1", user["id"])
    return {
        "id": user["id"],
        "username": user["username"],
        "bio": user["bio"],
        "avatar": user["avatar"],
        "setup": dict(setup) if setup else {},
        "tobaccos": [dict(t) for t in tobaccos],
        "stats": {"mixes": mixes_count, "followers": followers, "following": following},
    }

# ── TOBACCOS ─────────────────────────────────────────────────────────────────

@app.get("/api/tobaccos")
async def search_tobaccos(q: str = "", brand: str = "", in_stock: bool = True, limit: int = 30):
    async with pool.acquire() as conn:
        query = """
            SELECT t.id, t.brand, t.flavor, t.in_stock, t.variants,
                   ht.avg_rating, ht.total_reviews, ht.strength_user, ht.strength_official,
                   ht.flavor_tags, ht.url_path as htr_url
            FROM tobaccos t
            LEFT JOIN scraper.htr_tobaccos ht ON (
                similarity(lower(t.flavor), lower(ht.name)) > 0.4
                AND (
                    lower(t.brand) ILIKE '%' || lower(split_part(ht.url_path, '/', 3)) || '%'
                    OR lower(split_part(ht.url_path, '/', 3)) ILIKE '%' || lower(t.brand) || '%'
                )
            )
            WHERE ($1 = '' OR t.brand ILIKE '%' || $1 || '%' OR t.flavor ILIKE '%' || $1 || '%')
              AND ($2 = '' OR t.brand ILIKE '%' || $2 || '%')
              AND ($3 = FALSE OR t.in_stock = TRUE)
            ORDER BY t.in_stock DESC NULLS LAST, ht.avg_rating DESC NULLS LAST, t.brand, t.flavor
            LIMIT $4
        """
        # Fallback if pg_trgm not available
        try:
            rows = await conn.fetch(query, q, brand, in_stock, limit)
        except Exception:
            rows = await conn.fetch("""
                SELECT id, brand, flavor, in_stock, variants,
                       NULL::numeric as avg_rating, NULL::int as total_reviews,
                       NULL::text as strength_user, NULL::text as strength_official,
                       NULL::text[] as flavor_tags, NULL::text as htr_url
                FROM tobaccos
                WHERE ($1 = '' OR brand ILIKE '%' || $1 || '%' OR flavor ILIKE '%' || $1 || '%')
                  AND ($2 = '' OR brand ILIKE '%' || $2 || '%')
                  AND ($3 = FALSE OR in_stock = TRUE)
                ORDER BY in_stock DESC NULLS LAST, brand, flavor
                LIMIT $4
            """, q, brand, in_stock, limit)

        result = []
        for r in rows:
            d = dict(r)
            if d.get("variants") and isinstance(d["variants"], str):
                try:
                    d["variants"] = json.loads(d["variants"])
                except Exception:
                    d["variants"] = None
            result.append(d)
        return result

@app.get("/api/brands")
async def get_brands():
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT brand, COUNT(*) as total,
                   SUM(CASE WHEN in_stock THEN 1 ELSE 0 END) as in_stock_count
            FROM tobaccos GROUP BY brand ORDER BY in_stock_count DESC, brand
        """)
        return [dict(r) for r in rows]

@app.get("/api/tobaccos/{tobacco_id}/reviews")
async def get_tobacco_reviews(tobacco_id: int, limit: int = 20):
    async with pool.acquire() as conn:
        tob = await conn.fetchrow("SELECT brand, flavor FROM tobaccos WHERE id=$1", tobacco_id)
        if not tob:
            return {"reviews": [], "htr_info": None}
        # Fuzzy match in htr_tobaccos
        htr = await conn.fetchrow("""
            SELECT ht.*, b.name as brand_name
            FROM scraper.htr_tobaccos ht
            LEFT JOIN scraper.htr_brands b ON b.id=ht.brand_id
            WHERE lower(ht.name) ILIKE '%' || lower($1) || '%'
               OR lower($1) ILIKE '%' || lower(ht.name) || '%'
            ORDER BY ht.total_reviews DESC LIMIT 1
        """, tob["flavor"])

        reviews = []
        if htr:
            rows = await conn.fetch("""
                SELECT r.rating, r.review_text, r.reviewed_at, r.likes,
                       rv.username
                FROM scraper.htr_reviews r
                LEFT JOIN scraper.htr_reviewers rv ON rv.id=r.reviewer_id
                WHERE r.tobacco_id=$1
                  AND r.review_text IS NOT NULL AND length(r.review_text) > 20
                ORDER BY r.likes DESC, r.reviewed_at DESC
                LIMIT $2
            """, htr["id"], limit)
            reviews = [dict(r) for r in rows]

        return {
            "htr_info": dict(htr) if htr else None,
            "reviews": reviews,
        }

# ── MIXES ────────────────────────────────────────────────────────────────────

@app.post("/api/mixes")
async def create_mix(request: Request, user=Depends(require_user)):
    data = await request.json()
    name = (data.get("name") or "Без названия").strip()
    items = data.get("items", [])
    if not items:
        raise HTTPException(400, "Добавь хотя бы один табак")
    async with pool.acquire() as conn:
        mix_id = await conn.fetchval("""
            INSERT INTO hl_mixes (user_id, name, description, bowl_type, bowl_grams,
                                  pack_method, coal_tip, strength)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id
        """, user["id"], name,
            data.get("description", ""),
            data.get("bowl_type", "убивашка"),
            data.get("bowl_grams", 20),
            data.get("pack_method", "секторами"),
            data.get("coal_tip", ""),
            data.get("strength", "средний"))
        for i, item in enumerate(items):
            await conn.execute("""
                INSERT INTO hl_mix_items (mix_id, tobacco_id, tobacco_name, brand, percentage, sort_order)
                VALUES ($1,$2,$3,$4,$5,$6)
            """, mix_id,
                item.get("tobacco_id"),
                item.get("tobacco_name", ""),
                item.get("brand", ""),
                item.get("percentage", 0),
                i)
    return {"id": mix_id, "ok": True}

@app.get("/api/mixes")
async def get_my_mixes(user=Depends(require_user)):
    return await _fetch_mixes_for_user(user["id"])

@app.get("/api/mixes/{mix_id}")
async def get_mix(mix_id: int):
    async with pool.acquire() as conn:
        mix = await conn.fetchrow(
            "SELECT m.*, u.username FROM hl_mixes m JOIN hl_users u ON u.id=m.user_id WHERE m.id=$1", mix_id)
        if not mix:
            raise HTTPException(404, "Микс не найден")
        items = await conn.fetch(
            "SELECT * FROM hl_mix_items WHERE mix_id=$1 ORDER BY sort_order", mix_id)
        return {**dict(mix), "items": [dict(i) for i in items]}

async def _fetch_mixes_for_user(user_id: int):
    async with pool.acquire() as conn:
        mixes = await conn.fetch(
            "SELECT * FROM hl_mixes WHERE user_id=$1 ORDER BY created_at DESC", user_id)
        result = []
        for m in mixes:
            items = await conn.fetch(
                "SELECT * FROM hl_mix_items WHERE mix_id=$1 ORDER BY sort_order", m["id"])
            result.append({**dict(m), "items": [dict(i) for i in items]})
        return result

# ── FEED ──────────────────────────────────────────────────────────────────────

@app.get("/api/feed")
async def get_feed(offset: int = 0, limit: int = 20, user=Depends(get_current_user)):
    user_id = user["id"] if user else None
    async with pool.acquire() as conn:
        posts = await conn.fetch("""
            SELECT p.id, p.caption, p.created_at,
                   u.id as user_id, u.username, u.avatar,
                   m.id as mix_id, m.name as mix_name, m.description, m.bowl_type,
                   m.bowl_grams, m.pack_method, m.coal_tip, m.strength,
                   COUNT(DISTINCT l.user_id) as likes_count,
                   COUNT(DISTINCT s.user_id) as saves_count,
                   BOOL_OR(l.user_id = $3) as i_liked,
                   BOOL_OR(s.user_id = $3) as i_saved
            FROM hl_posts p
            JOIN hl_users u ON u.id = p.user_id
            JOIN hl_mixes m ON m.id = p.mix_id
            LEFT JOIN hl_likes l ON l.post_id = p.id
            LEFT JOIN hl_saves s ON s.mix_id = m.id
            GROUP BY p.id, u.id, m.id
            ORDER BY p.created_at DESC
            LIMIT $1 OFFSET $2
        """, limit, offset, user_id)

        result = []
        for post in posts:
            items = await conn.fetch(
                "SELECT * FROM hl_mix_items WHERE mix_id=$1 ORDER BY sort_order", post["mix_id"])
            result.append({
                **dict(post),
                "items": [dict(i) for i in items],
            })
        return result

@app.post("/api/feed")
async def create_post(request: Request, user=Depends(require_user)):
    data = await request.json()
    mix_id = data.get("mix_id")
    caption = data.get("caption", "")
    if not mix_id:
        raise HTTPException(400, "Нужен mix_id")
    async with pool.acquire() as conn:
        # Verify mix belongs to user
        m = await conn.fetchval(
            "SELECT id FROM hl_mixes WHERE id=$1 AND user_id=$2", mix_id, user["id"])
        if not m:
            raise HTTPException(403, "Нет доступа к этому миксу")
        post_id = await conn.fetchval(
            "INSERT INTO hl_posts (user_id, mix_id, caption) VALUES ($1,$2,$3) RETURNING id",
            user["id"], mix_id, caption)
    return {"id": post_id, "ok": True}

@app.post("/api/feed/{post_id}/like")
async def toggle_like(post_id: int, user=Depends(require_user)):
    async with pool.acquire() as conn:
        existing = await conn.fetchval(
            "SELECT 1 FROM hl_likes WHERE user_id=$1 AND post_id=$2", user["id"], post_id)
        if existing:
            await conn.execute("DELETE FROM hl_likes WHERE user_id=$1 AND post_id=$2", user["id"], post_id)
            liked = False
        else:
            await conn.execute("INSERT INTO hl_likes VALUES ($1,$2)", user["id"], post_id)
            liked = True
        count = await conn.fetchval("SELECT COUNT(*) FROM hl_likes WHERE post_id=$1", post_id)
    return {"liked": liked, "count": count}

@app.post("/api/mixes/{mix_id}/save")
async def toggle_save(mix_id: int, user=Depends(require_user)):
    async with pool.acquire() as conn:
        existing = await conn.fetchval(
            "SELECT 1 FROM hl_saves WHERE user_id=$1 AND mix_id=$2", user["id"], mix_id)
        if existing:
            await conn.execute("DELETE FROM hl_saves WHERE user_id=$1 AND mix_id=$2", user["id"], mix_id)
            saved = False
        else:
            await conn.execute("INSERT INTO hl_saves VALUES ($1,$2)", user["id"], mix_id)
            saved = True
        count = await conn.fetchval("SELECT COUNT(*) FROM hl_saves WHERE mix_id=$1", mix_id)
    return {"saved": saved, "count": count}

# ── PROFILE ───────────────────────────────────────────────────────────────────

@app.get("/api/profile/{username}")
async def get_profile(username: str, user=Depends(get_current_user)):
    async with pool.acquire() as conn:
        u = await conn.fetchrow("SELECT * FROM hl_users WHERE lower(username)=lower($1)", username)
        if not u:
            raise HTTPException(404, "Пользователь не найден")
        setup = await conn.fetchrow("SELECT * FROM hl_user_setup WHERE user_id=$1", u["id"])
        tobaccos = await conn.fetch("""
            SELECT t.id, t.brand, t.flavor FROM hl_user_tobaccos ut
            JOIN tobaccos t ON t.id=ut.tobacco_id
            WHERE ut.user_id=$1 ORDER BY ut.added_at DESC
        """, u["id"])
        mixes = await _fetch_mixes_for_user(u["id"])
        followers = await conn.fetchval("SELECT COUNT(*) FROM hl_follows WHERE following_id=$1", u["id"])
        following = await conn.fetchval("SELECT COUNT(*) FROM hl_follows WHERE follower_id=$1", u["id"])
        i_follow = False
        if user:
            i_follow = bool(await conn.fetchval(
                "SELECT 1 FROM hl_follows WHERE follower_id=$1 AND following_id=$2",
                user["id"], u["id"]))
    return {
        "id": u["id"],
        "username": u["username"],
        "bio": u["bio"],
        "avatar": u["avatar"],
        "setup": dict(setup) if setup else {},
        "tobaccos": [dict(t) for t in tobaccos],
        "mixes": mixes,
        "stats": {"mixes": len(mixes), "followers": followers, "following": following},
        "i_follow": i_follow,
    }

@app.put("/api/profile")
async def update_profile(request: Request, user=Depends(require_user)):
    data = await request.json()
    async with pool.acquire() as conn:
        if "bio" in data or "avatar" in data:
            await conn.execute("""
                UPDATE hl_users SET bio=COALESCE($1, bio), avatar=COALESCE($2, avatar) WHERE id=$3
            """, data.get("bio"), data.get("avatar"), user["id"])
        setup = data.get("setup")
        if setup:
            await conn.execute("""
                INSERT INTO hl_user_setup (user_id, hookah, bowl, coal, foil)
                VALUES ($1,$2,$3,$4,$5)
                ON CONFLICT (user_id) DO UPDATE SET
                    hookah=EXCLUDED.hookah, bowl=EXCLUDED.bowl,
                    coal=EXCLUDED.coal, foil=EXCLUDED.foil
            """, user["id"],
                setup.get("hookah", ""), setup.get("bowl", ""),
                setup.get("coal", ""), setup.get("foil", ""))
    return {"ok": True}

@app.post("/api/profile/tobaccos/{tobacco_id}")
async def add_to_cabinet(tobacco_id: int, user=Depends(require_user)):
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO hl_user_tobaccos (user_id, tobacco_id) VALUES ($1,$2)
            ON CONFLICT DO NOTHING
        """, user["id"], tobacco_id)
    return {"ok": True}

@app.delete("/api/profile/tobaccos/{tobacco_id}")
async def remove_from_cabinet(tobacco_id: int, user=Depends(require_user)):
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM hl_user_tobaccos WHERE user_id=$1 AND tobacco_id=$2",
            user["id"], tobacco_id)
    return {"ok": True}

@app.post("/api/follow/{username}")
async def toggle_follow(username: str, user=Depends(require_user)):
    async with pool.acquire() as conn:
        target = await conn.fetchval("SELECT id FROM hl_users WHERE lower(username)=lower($1)", username)
        if not target or target == user["id"]:
            raise HTTPException(400, "Нельзя")
        existing = await conn.fetchval(
            "SELECT 1 FROM hl_follows WHERE follower_id=$1 AND following_id=$2",
            user["id"], target)
        if existing:
            await conn.execute("DELETE FROM hl_follows WHERE follower_id=$1 AND following_id=$2",
                               user["id"], target)
            following = False
        else:
            await conn.execute("INSERT INTO hl_follows VALUES ($1,$2)", user["id"], target)
            following = True
    return {"following": following}
