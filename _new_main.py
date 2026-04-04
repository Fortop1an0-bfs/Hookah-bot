from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
import asyncpg
import json
import os
import datetime

app = FastAPI()
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

@app.on_event("shutdown")
async def shutdown():
    if pool:
        await pool.close()

@app.get("/", response_class=HTMLResponse)
async def index():
    with open("templates/index.html", "r") as f:
        return f.read()

@app.get("/api/mixes")
async def get_mixes():
    async with pool.acquire() as conn:
        mixes = await conn.fetch("""
            SELECT m.id, m.name, m.category, m.description,
                   m.recommended_bowl, m.bowl_note, m.strength, m.coal_tip, m.pack_method,
                   COALESCE(
                       (SELECT ROUND(AVG(r.rating)::numeric, 1) FROM web_reviews r WHERE r.mix_id = m.id), 0
                   ) as avg_rating,
                   (SELECT COUNT(*) FROM web_reviews r WHERE r.mix_id = m.id) as review_count
            FROM web_mixes m ORDER BY m.category, m.id
        """)
        result = []
        for m in mixes:
            items = await conn.fetch("""
                SELECT tobacco_name, brand, pack_grams, percentage
                FROM web_mix_items WHERE mix_id = $1 ORDER BY sort_order
            """, m["id"])
            result.append({
                "id": m["id"],
                "name": m["name"],
                "category": m["category"],
                "description": m["description"],
                "recommended_bowl": m["recommended_bowl"],
                "bowl_note": m["bowl_note"],
                "strength": m["strength"],
                "coal_tip": m["coal_tip"],
                "pack_method": m["pack_method"],
                "avg_rating": float(m["avg_rating"]),
                "review_count": m["review_count"],
                "items": [dict(i) for i in items]
            })
        return result

@app.get("/api/calendar")
async def get_calendar():
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT r.id, r.smoked_at::text as smoked_at, r.bowl_type, r.rating, r.comment,
                   m.id as mix_id, m.name, m.category, m.strength, m.description
            FROM web_reviews r
            JOIN web_mixes m ON m.id = r.mix_id
            WHERE r.smoked_at IS NOT NULL
            ORDER BY r.smoked_at DESC
        """)
        return [dict(r) for r in rows]

@app.get("/api/reviews/{mix_id}")
async def get_reviews(mix_id: int):
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, bowl_type, rating, comment,
                   TO_CHAR(smoked_at, 'DD.MM.YYYY') as smoked_at,
                   TO_CHAR(created_at, 'DD.MM.YYYY HH24:MI') as date
            FROM web_reviews WHERE mix_id = $1 ORDER BY created_at DESC
        """, mix_id)
        return [dict(r) for r in rows]

@app.post("/api/reviews")
async def add_review(request: Request):
    data = await request.json()
    mix_id = data.get("mix_id")
    bowl_type = data.get("bowl_type")
    rating = data.get("rating")
    comment = data.get("comment", "")
    smoked_at_raw = data.get("smoked_at") or None
    if smoked_at_raw:
        try:
            smoked_at = datetime.date.fromisoformat(smoked_at_raw)
        except (ValueError, TypeError):
            smoked_at = None
    else:
        smoked_at = None
    if not all([mix_id, bowl_type, rating]):
        return JSONResponse({"error": "Заполни все поля"}, status_code=400)
    if rating < 1 or rating > 5:
        return JSONResponse({"error": "Рейтинг от 1 до 5"}, status_code=400)
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO web_reviews (mix_id, bowl_type, rating, comment, smoked_at)
            VALUES ($1, $2, $3, $4, $5::date)
        """, mix_id, bowl_type, rating, comment, smoked_at)
    return {"ok": True}
