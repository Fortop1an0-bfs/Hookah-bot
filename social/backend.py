"""
HookahLab — FastAPI Backend
Port: 8082
"""
from fastapi import FastAPI, Request, Header, HTTPException, Depends
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import asyncpg, hashlib, secrets, json, re, httpx, os
from typing import Optional

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

try:
    app.mount("/static", StaticFiles(directory="static"), name="static")
except Exception:
    pass

DB_DSN   = "postgresql://hookah:hookah123@localhost:5432/hookah_db"
GROQ_KEY = os.environ.get("GROQ_KEY", "")
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "llama-3.3-70b-versatile"

pool = None

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
    return hashlib.sha256(pw.encode()).hexdigest()

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
            "SELECT * FROM hl_users WHERE (lower(email)=$1 OR lower(username)=$1) AND pass_hash=$2",
            login_val, hash_pw(password))
        if not row:
            raise HTTPException(401, "Неверный логин или пароль")
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
        if "bio" in d or "avatar" in d:
            await conn.execute(
                "UPDATE hl_users SET bio=COALESCE($1,bio), avatar=COALESCE($2,avatar) WHERE id=$3",
                d.get("bio"), d.get("avatar"), user["id"])
        # map "notes" → "foil" for backward compat
        if "notes" in d: d["foil"] = d.pop("notes")
        setup_fields = {k: d[k] for k in ("hookah","bowl","bowl_type","coal","foil","flask_shape","flask_color") if k in d}
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
        rows = await conn.fetch("""
            SELECT a.id, a.name, a.brand_name as brand, a.line, a.weight,
                   a.price, a.price_before_discount, a.has_discount,
                   a.in_stock, a.stores_count, a.total_amount,
                   a.htreviews_id, a.image_url, a.is_bestseller,
                   ht.avg_rating, ht.strength_user, ht.strength_official,
                   ht.flavor_tags, ht.total_reviews, ht.url_path as htr_url
            FROM scraper.ali_products a
            LEFT JOIN scraper.htr_tobaccos ht ON
                NULLIF(regexp_replace(a.htreviews_id, '[^0-9]', '', 'g'), '')::int = ht.htreviews_id
            WHERE ($1 = '' OR a.name ILIKE '%' || $1 || '%' OR a.brand_name ILIKE '%' || $1 || '%')
              AND ($2 = '' OR a.brand_name ILIKE '%' || $2 || '%')
            ORDER BY
                CASE WHEN a.brand_name ILIKE $1 THEN 0 ELSE 1 END,
                a.in_stock DESC NULLS LAST,
                ht.avg_rating DESC NULLS LAST,
                a.brand_name, a.name
            LIMIT $3
        """, q, brand, limit)
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
                   (SELECT COUNT(*) FROM hl_likes l WHERE l.mix_id=m.id) as likes,
                   (SELECT COUNT(*) FROM hl_saves s WHERE s.mix_id=m.id) as saves,
                   (SELECT COUNT(*) FROM hl_comments c WHERE c.mix_id=m.id) as comments
            FROM hl_mixes m
            JOIN hl_users u ON u.id=m.user_id
            WHERE m.is_public=TRUE
            ORDER BY m.created_at DESC
            OFFSET $1 LIMIT $2
        """, offset, limit)
        result = []
        for row in rows:
            items = await _mix_items(conn, row["id"])
            result.append({**dict(row), "items": items})
        return result

@app.get("/api/saved")
async def get_saved(user=Depends(req_user)):
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT m.id, m.name, m.description, m.bowl_type, m.bowl_grams,
                   m.pack_method, m.coal_tip, m.strength, m.created_at,
                   u.username, u.avatar,
                   (SELECT COUNT(*) FROM hl_likes l WHERE l.mix_id=m.id) as likes
            FROM hl_saves s
            JOIN hl_mixes m ON m.id=s.mix_id
            JOIN hl_users u ON u.id=m.user_id
            WHERE s.user_id=$1
            ORDER BY m.created_at DESC
        """, user["id"])
        result = []
        for row in rows:
            items = await _mix_items(conn, row["id"])
            result.append({**dict(row), "items": items})
        return result

# ── PROFILE ──────────────────────────────────────────────────────────────────

@app.get("/api/profile/{username}")
async def get_profile(username: str):
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
        mixes = await _fetch_mixes(user["id"])
    return {
        "id": user["id"], "username": user["username"],
        "bio": user["bio"], "avatar": user["avatar"],
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

@app.get("/api/catalog")
async def get_catalog(
    q: str = "", brand: str = "", strength: str = "",
    in_stock: bool = False, min_rating: float = 0,
    sort: str = "rating", offset: int = 0, limit: int = 40
):
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT a.id, a.name, a.brand_name as brand, a.line, a.weight,
                   a.price, a.has_discount, a.price_before_discount,
                   a.in_stock, a.stores_count, a.total_amount,
                   a.image_url, a.is_bestseller,
                   ht.avg_rating, ht.strength_user, ht.strength_official,
                   ht.flavor_tags, ht.total_reviews, ht.url_path as htr_url
            FROM scraper.ali_products a
            LEFT JOIN scraper.htr_tobaccos ht ON
                NULLIF(regexp_replace(a.htreviews_id,'[^0-9]','','g'),'')::int = ht.htreviews_id
            WHERE ($1 = '' OR a.name ILIKE '%'||$1||'%' OR a.brand_name ILIKE '%'||$1||'%')
              AND ($2 = '' OR a.brand_name ILIKE '%'||$2||'%')
              AND (NOT $3 OR a.in_stock = TRUE)
              AND ($4 = 0 OR ht.avg_rating >= $4)
              AND ($5 = '' OR ht.strength_user ILIKE '%'||$5||'%' OR ht.strength_official ILIKE '%'||$5||'%')
            ORDER BY
                CASE WHEN $6='rating'    THEN COALESCE(ht.avg_rating,0) END DESC NULLS LAST,
                CASE WHEN $6='price_asc' THEN a.price END ASC NULLS LAST,
                CASE WHEN $6='price_desc' THEN a.price END DESC NULLS LAST,
                CASE WHEN $6='reviews'   THEN COALESCE(ht.total_reviews,0) END DESC NULLS LAST,
                a.is_bestseller DESC, a.brand_name, a.name
            LIMIT $7 OFFSET $8
        """, q, brand, in_stock, min_rating, strength, sort, limit, offset)
        total = await conn.fetchval("""
            SELECT COUNT(*) FROM scraper.ali_products a
            LEFT JOIN scraper.htr_tobaccos ht ON
                NULLIF(regexp_replace(a.htreviews_id,'[^0-9]','','g'),'')::int = ht.htreviews_id
            WHERE ($1='' OR a.name ILIKE '%'||$1||'%' OR a.brand_name ILIKE '%'||$1||'%')
              AND ($2='' OR a.brand_name ILIKE '%'||$2||'%')
              AND (NOT $3 OR a.in_stock=TRUE)
              AND ($4=0 OR ht.avg_rating>=$4)
        """, q, brand, in_stock, min_rating)
        return {"items": [dict(r) for r in rows], "total": total}

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
    async with pool.acquire() as conn:
        if period == "all":
            rows = await conn.fetch("""
                SELECT m.id, m.name, m.pack_method, m.bowl_type, m.strength,
                       u.username, u.avatar,
                       COUNT(DISTINCT l.user_id) as likes,
                       COALESCE(ROUND(AVG(r.rating),1),0) as avg_rating
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
            rows = await conn.fetch("""
                SELECT m.id, m.name, m.pack_method, m.bowl_type, m.strength,
                       u.username, u.avatar,
                       COUNT(DISTINCT l.user_id) as likes,
                       COALESCE(ROUND(AVG(r.rating),1),0) as avg_rating
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
        result = []
        for row in rows:
            async with pool.acquire() as conn2:
                items = await _mix_items(conn2, row["id"])
            result.append({**dict(row), "items": items})
        return result

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

@app.post("/api/llm/generate")
async def llm_generate(request: Request):
    d = await request.json()
    prompt = (d.get("prompt") or "").strip()
    if not prompt or len(prompt) < 5:
        raise HTTPException(400, "Опиши желаемый вкус")

    # Берём топ-80 табаков из наличия для контекста
    async with pool.acquire() as conn:
        cabinet_ids = d.get("cabinet_ids") or []
        if cabinet_ids:
            rows = await conn.fetch("""
                SELECT a.id, a.brand_name, a.name,
                       COALESCE(a.line,'') as line, COALESCE(a.weight,'') as weight,
                       a.price,
                       COALESCE(ht.strength_user, ht.strength_official, '') as strength,
                       COALESCE(array_to_string(ht.flavor_tags,', '),'') as tags
                FROM scraper.ali_products a
                LEFT JOIN scraper.htr_tobaccos ht ON
                    NULLIF(regexp_replace(a.htreviews_id,'[^0-9]','','g'),'')::int = ht.htreviews_id
                WHERE a.id = ANY($1::int[])
            """, cabinet_ids)
        else:
            rows = await conn.fetch("""
                SELECT a.id, a.brand_name, a.name,
                       COALESCE(a.line,'') as line, COALESCE(a.weight,'') as weight,
                       a.price,
                       COALESCE(ht.strength_user, ht.strength_official, '') as strength,
                       COALESCE(array_to_string(ht.flavor_tags,', '),'') as tags
                FROM scraper.ali_products a
                LEFT JOIN scraper.htr_tobaccos ht ON
                    NULLIF(regexp_replace(a.htreviews_id,'[^0-9]','','g'),'')::int = ht.htreviews_id
                WHERE a.in_stock = TRUE
                ORDER BY ht.avg_rating DESC NULLS LAST, a.is_bestseller DESC
                LIMIT 100
            """)
        tobaccos_ctx = "\n".join(
            f"ID:{r['id']} | {r['brand_name']} {r['name']} | {r['strength']} | {r['tags']} | {r['price']}р"
            for r in rows
        )

    system_prompt = """Ты эксперт по кальянным миксам. Пользователь описывает желаемый вкус/сессию.
Твоя задача — создать идеальный микс из КОНКРЕТНЫХ табаков из предоставленного списка.

Правила:
1. Используй ТОЛЬКО табаки из списка (обязательно укажи их ID)
2. 2-4 табака в миксе
3. Сумма процентов = 100
4. Отвечай СТРОГО в JSON формате

JSON формат ответа:
{
  "name": "Название микса",
  "description": "Описание вкусового профиля (2-3 предложения)",
  "strength": "лёгкий|средний|крепкий",
  "bowl_type": "убивашка|фанел|чаша-обратная|стандарт",
  "pack_method": "секторами|слоями|компот|центр+края",
  "coal_tip": "2 угля под колпаком|3 угля|2 угля стандарт",
  "items": [
    {"ali_id": 123, "tobacco_name": "Название вкуса", "brand": "Бренд", "percentage": 60},
    {"ali_id": 456, "tobacco_name": "Название вкуса", "brand": "Бренд", "percentage": 40}
  ],
  "why": "Почему этот микс подойдёт (1-2 предложения)"
}"""

    user_msg = f"Запрос пользователя: {prompt}\n\nДоступные табаки:\n{tobaccos_ctx}"

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(GROQ_URL, json={
                "model": GROQ_MODEL,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_msg}
                ],
                "temperature": 0.7,
                "max_tokens": 1000,
            }, headers={"Authorization": f"Bearer {GROQ_KEY}"})

        content = resp.json()["choices"][0]["message"]["content"]
        # Извлекаем JSON из ответа
        json_match = re.search(r'\{.*\}', content, re.DOTALL)
        if not json_match:
            raise ValueError("No JSON in response")
        mix_data = json.loads(json_match.group())
        mix_data["is_llm"] = True
        mix_data["llm_prompt"] = prompt
        return mix_data

    except Exception as e:
        raise HTTPException(500, f"Ошибка генерации: {str(e)}")

# ── INTERNAL ─────────────────────────────────────────────────────────────────

async def _fetch_mixes(user_id: int):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM hl_mixes WHERE user_id=$1 ORDER BY created_at DESC", user_id)
        result = []
        for row in rows:
            items = await _mix_items(conn, row["id"])
            stats = await _mix_stats(conn, row["id"])
            result.append({**dict(row), "items": items, **stats})
        return result
