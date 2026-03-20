
from __future__ import annotations

import hashlib
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from urllib.parse import quote

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "site.db"
STATIC_DIR = BASE_DIR / "static"
TEMPLATES_DIR = BASE_DIR / "templates"

app = FastAPI(title="Lineage Classic Hub", version="1.0.0")
app.add_middleware(
    SessionMiddleware,
    secret_key=os.environ.get("SECRET_KEY", "lineagehub-secret-change-me"),
    same_site="lax",
    https_only=False,
)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

SERVER_NAMES = ["기란", "오렌", "말하는섬", "켄트"]
GRADES = ["일반", "희귀", "영웅", "전설", "신화"]
CATEGORIES = ["무기", "방어구", "소모품", "장신구", "재료"]
BOARD_TYPES = ["자유", "질문", "거래", "혈맹모집", "팁과노하우", "패치토론"]

def now_str() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat()

def hash_password(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

@contextmanager
def db():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()

def query_all(sql: str, params: tuple = ()) -> list[sqlite3.Row]:
    with db() as conn:
        return conn.execute(sql, params).fetchall()

def query_one(sql: str, params: tuple = ()) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute(sql, params).fetchone()

def exec_sql(sql: str, params: tuple = ()) -> int:
    with db() as conn:
        cur = conn.execute(sql, params)
        return cur.lastrowid

def init_db() -> None:
    with db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                display_name TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'user',
                points INTEGER NOT NULL DEFAULT 10000,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                grade TEXT NOT NULL DEFAULT '일반',
                category TEXT NOT NULL DEFAULT '무기',
                image_url TEXT NOT NULL DEFAULT '/static/images/default.svg',
                description TEXT NOT NULL DEFAULT '',
                drop_info TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS monsters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                level INTEGER NOT NULL DEFAULT 1,
                region TEXT NOT NULL DEFAULT '',
                drop_items TEXT NOT NULL DEFAULT '',
                description TEXT NOT NULL DEFAULT '',
                image_url TEXT NOT NULL DEFAULT '/static/images/monster.svg',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS spells (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                spell_class TEXT NOT NULL DEFAULT '',
                mp_cost INTEGER NOT NULL DEFAULT 0,
                description TEXT NOT NULL DEFAULT '',
                image_url TEXT NOT NULL DEFAULT '/static/images/spell.svg',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS skins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                author TEXT NOT NULL DEFAULT '',
                preview_url TEXT NOT NULL DEFAULT '/static/images/skin.svg',
                color_code TEXT NOT NULL DEFAULT '#6b7280',
                description TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS blood_marks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                clan_name TEXT NOT NULL DEFAULT '',
                preview_url TEXT NOT NULL DEFAULT '/static/images/bloodmark.svg',
                description TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS market_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_id INTEGER NOT NULL,
                server_name TEXT NOT NULL,
                price INTEGER NOT NULL,
                volume INTEGER NOT NULL DEFAULT 0,
                recorded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(item_id) REFERENCES items(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                board_type TEXT NOT NULL DEFAULT '자유',
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                author_id INTEGER NOT NULL,
                views INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'published',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(author_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id INTEGER NOT NULL,
                author_id INTEGER NOT NULL,
                content TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(post_id) REFERENCES posts(id) ON DELETE CASCADE,
                FOREIGN KEY(author_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                reporter_id INTEGER,
                target_type TEXT NOT NULL,
                target_id INTEGER NOT NULL DEFAULT 0,
                reason TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                admin_note TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(reporter_id) REFERENCES users(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS sanctions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                action TEXT NOT NULL,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS ad_slots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                slot_name TEXT UNIQUE NOT NULL,
                location TEXT NOT NULL,
                snippet TEXT NOT NULL DEFAULT '',
                is_active INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS point_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                delta INTEGER NOT NULL,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            """
        )

        if not conn.execute("SELECT 1 FROM users WHERE username='admin'").fetchone():
            conn.execute(
                "INSERT INTO users (username, password_hash, display_name, role, points) VALUES (?, ?, ?, 'admin', 50000)",
                ("admin", hash_password("admin1234"), "관리자",),
            )
        if not conn.execute("SELECT 1 FROM users WHERE username='tester'").fetchone():
            conn.execute(
                "INSERT INTO users (username, password_hash, display_name, role, points) VALUES (?, ?, ?, 'user', 15000)",
                ("tester", hash_password("user1234"), "테스터",),
            )

        if not conn.execute("SELECT 1 FROM items").fetchone():
            items = [
                ("진명황의 집행검", "전설", "무기", "/static/images/sword.svg", "강력한 근접 무기", "드래곤 밸리 보스"),
                ("오림의 장갑", "영웅", "방어구", "/static/images/glove.svg", "명중과 방어에 유리", "오림 던전"),
                ("축복받은 순간이동 주문서", "일반", "소모품", "/static/images/scroll.svg", "순간이동용 소모품", "일반 상점 / 필드 드랍"),
                ("기사단의 방패", "희귀", "방어구", "/static/images/shield.svg", "초중반 효율 방패", "기사단 훈련장"),
                ("마법사의 지팡이", "희귀", "무기", "/static/images/staff.svg", "마법사 성장용 무기", "해골 던전"),
                ("민첩의 목걸이", "영웅", "장신구", "/static/images/ring.svg", "원거리 세팅 핵심 장신구", "사막 보스"),
            ]
            conn.executemany(
                "INSERT INTO items (name, grade, category, image_url, description, drop_info) VALUES (?, ?, ?, ?, ?, ?)",
                items
            )

        if not conn.execute("SELECT 1 FROM monsters").fetchone():
            monsters = [
                ("데스나이트", 80, "본던 7층", "진명황의 집행검, 기사단의 방패", "대표 보스 몬스터", "/static/images/monster.svg"),
                ("해골 궁수", 28, "해골 던전", "마법사의 지팡이, 주문서", "원거리 공격 몬스터", "/static/images/monster.svg"),
                ("오림 수호병", 52, "오림 던전", "오림의 장갑", "오림 계열 드랍 몬스터", "/static/images/monster.svg"),
                ("사막 전갈", 35, "사막", "민첩의 목걸이", "독 공격 주의", "/static/images/monster.svg"),
            ]
            conn.executemany(
                "INSERT INTO monsters (name, level, region, drop_items, description, image_url) VALUES (?, ?, ?, ?, ?, ?)",
                monsters
            )

        if not conn.execute("SELECT 1 FROM spells").fetchone():
            spells = [
                ("캔슬레이션", "마법사", 30, "대상 버프를 제거", "/static/images/spell.svg"),
                ("이뮨 투 함", "마법사", 50, "피해 감소 핵심 스킬", "/static/images/spell.svg"),
                ("트리플 애로우", "요정", 20, "다중 사격", "/static/images/spell.svg"),
                ("쇼크 스턴", "기사", 18, "대상 기절 유발", "/static/images/spell.svg"),
            ]
            conn.executemany(
                "INSERT INTO spells (name, spell_class, mp_cost, description, image_url) VALUES (?, ?, ?, ?, ?)",
                spells
            )

        if not conn.execute("SELECT 1 FROM skins").fetchone():
            skins = [
                ("다크 골드 UI", "운영팀", "/static/images/skin.svg", "#c99a2e", "어두운 톤 + 금색 포인트"),
                ("오렌지 클래식 UI", "운영팀", "/static/images/skin.svg", "#f97316", "클래식 감성 강조"),
                ("실버 미니멀 UI", "운영팀", "/static/images/skin.svg", "#9ca3af", "밝고 깔끔한 배색"),
            ]
            conn.executemany(
                "INSERT INTO skins (name, author, preview_url, color_code, description) VALUES (?, ?, ?, ?, ?)",
                skins
            )

        if not conn.execute("SELECT 1 FROM blood_marks").fetchone():
            marks = [
                ("붉은 용", "드래곤나이츠", "/static/images/bloodmark.svg", "붉은 용 문양"),
                ("황금 사자", "라이온하트", "/static/images/bloodmark.svg", "황금 사자 문양"),
                ("푸른 방패", "블루가드", "/static/images/bloodmark.svg", "수비형 혈맹 문양"),
            ]
            conn.executemany(
                "INSERT INTO blood_marks (name, clan_name, preview_url, description) VALUES (?, ?, ?, ?)",
                marks
            )

        if not conn.execute("SELECT 1 FROM market_prices").fetchone():
            item_rows = conn.execute("SELECT id, name FROM items ORDER BY id").fetchall()
            base = {
                "진명황의 집행검": 3200000,
                "오림의 장갑": 190000,
                "축복받은 순간이동 주문서": 1300,
                "기사단의 방패": 42000,
                "마법사의 지팡이": 98000,
                "민첩의 목걸이": 360000,
            }
            for item in item_rows:
                for server_idx, server in enumerate(SERVER_NAMES):
                    start = base[item["name"]] + server_idx * 5000
                    for day in range(7):
                        price = int(start * (1 + ((day - 3) * 0.015)))
                        volume = 5 + day + server_idx
                        recorded_at = (datetime.utcnow() - timedelta(days=(6-day))).replace(microsecond=0).isoformat()
                        conn.execute(
                            "INSERT INTO market_prices (item_id, server_name, price, volume, recorded_at) VALUES (?, ?, ?, ?, ?)",
                            (item["id"], server, price, volume, recorded_at)
                        )

        if not conn.execute("SELECT 1 FROM posts").fetchone():
            admin_id = conn.execute("SELECT id FROM users WHERE username='admin'").fetchone()[0]
            tester_id = conn.execute("SELECT id FROM users WHERE username='tester'").fetchone()[0]
            posts = [
                ("자유", "오늘 시세 많이 올랐네요", "오림의 장갑 가격이 다시 반등했습니다.", tester_id, 14),
                ("질문", "기사 장비 우선순위가 궁금합니다", "방패와 검 중 무엇을 먼저 맞추면 좋을까요?", tester_id, 32),
                ("팁과노하우", "러쉬는 3회 단위가 체감상 좋았습니다", "포인트 시뮬레이션 결과를 공유합니다.", admin_id, 45),
            ]
            conn.executemany(
                "INSERT INTO posts (board_type, title, content, author_id, views) VALUES (?, ?, ?, ?, ?)",
                posts
            )

        if not conn.execute("SELECT 1 FROM ad_slots").fetchone():
            slots = [
                ("home_top", "홈 상단", "<div class='ad-box'>광고 슬롯: home_top</div>", 1),
                ("content_middle", "본문 중간", "<div class='ad-box'>광고 슬롯: content_middle</div>", 1),
                ("sidebar", "사이드바", "<div class='ad-box'>광고 슬롯: sidebar</div>", 1),
            ]
            conn.executemany(
                "INSERT INTO ad_slots (slot_name, location, snippet, is_active) VALUES (?, ?, ?, ?)",
                slots
            )

def get_current_user(request: Request) -> Optional[sqlite3.Row]:
    user_id = request.session.get("user_id")
    if not user_id:
        return None
    return query_one("SELECT * FROM users WHERE id=?", (user_id,))

def require_login(request: Request) -> sqlite3.Row:
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    if user["status"] != "active":
        raise HTTPException(status_code=403, detail="정지된 계정입니다.")
    return user

def require_admin(request: Request) -> sqlite3.Row:
    user = require_login(request)
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="관리자만 접근 가능합니다.")
    return user

def redirect(url: str) -> RedirectResponse:
    return RedirectResponse(url=url, status_code=303)

def ad_snippet(slot_name: str) -> str:
    row = query_one("SELECT snippet FROM ad_slots WHERE slot_name=? AND is_active=1", (slot_name,))
    return row["snippet"] if row else ""

def sparkline(values: list[int], color: str = "#8b5cf6") -> str:
    if not values:
        return ""
    width, height = 150, 44
    min_v, max_v = min(values), max(values)
    span = max(max_v - min_v, 1)
    points = []
    for idx, v in enumerate(values):
        x = int((idx / max(1, len(values) - 1)) * (width - 8)) + 4
        y = height - int(((v - min_v) / span) * (height - 10)) - 5
        points.append(f"{x},{y}")
    svg = f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}' viewBox='0 0 {width} {height}'><polyline fill='none' stroke='{color}' stroke-width='3' points='{' '.join(points)}'/></svg>"
    return "data:image/svg+xml;utf8," + quote(svg)

def stats_summary() -> dict:
    with db() as conn:
        return {
            "items": conn.execute("SELECT COUNT(*) FROM items").fetchone()[0],
            "monsters": conn.execute("SELECT COUNT(*) FROM monsters").fetchone()[0],
            "spells": conn.execute("SELECT COUNT(*) FROM spells").fetchone()[0],
            "posts": conn.execute("SELECT COUNT(*) FROM posts WHERE status='published'").fetchone()[0],
        }

def add_points(user_id: int, delta: int, reason: str) -> None:
    with db() as conn:
        conn.execute("UPDATE users SET points = points + ? WHERE id=?", (delta, user_id))
        conn.execute("INSERT INTO point_logs (user_id, delta, reason, created_at) VALUES (?, ?, ?, ?)", (user_id, delta, reason, now_str()))

@app.on_event("startup")
def startup() -> None:
    init_db()

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    user = get_current_user(request)
    with db() as conn:
        hot_items = conn.execute(
            """
            SELECT i.*, mp.server_name, mp.price, mp.volume, mp.recorded_at
            FROM items i
            JOIN (
                SELECT item_id, server_name, MAX(recorded_at) latest_at
                FROM market_prices
                GROUP BY item_id, server_name
            ) latest ON latest.item_id = i.id
            JOIN market_prices mp
                ON mp.item_id = latest.item_id
               AND mp.server_name = latest.server_name
               AND mp.recorded_at = latest.latest_at
            ORDER BY mp.price DESC
            LIMIT 6
            """
        ).fetchall()
        market_rows = []
        for row in hot_items:
            hist = conn.execute(
                "SELECT price FROM market_prices WHERE item_id=? AND server_name=? ORDER BY recorded_at ASC",
                (row["id"], row["server_name"])
            ).fetchall()
            values = [x["price"] for x in hist]
            market_rows.append({**dict(row), "sparkline": sparkline(values)})

        boards = conn.execute(
            """
            SELECT p.*, u.display_name
            FROM posts p JOIN users u ON u.id = p.author_id
            WHERE p.status='published'
            ORDER BY p.id DESC LIMIT 8
            """
        ).fetchall()
    return templates.TemplateResponse(
        request, "home.html",
        {
            "user": user,
            "summary": stats_summary(),
            "market_rows": market_rows,
            "boards": boards,
            "home_top_ad": ad_snippet("home_top"),
        }
    )

@app.get("/items", response_class=HTMLResponse)
def items_page(request: Request, q: str = "", category: str = ""):
    user = get_current_user(request)
    sql = "SELECT * FROM items WHERE 1=1"
    params = []
    if q:
        sql += " AND (name LIKE ? OR description LIKE ? OR drop_info LIKE ?)"
        params += [f"%{q}%", f"%{q}%", f"%{q}%"]
    if category:
        sql += " AND category=?"
        params.append(category)
    sql += " ORDER BY grade DESC, id DESC"
    items = query_all(sql, tuple(params))
    return templates.TemplateResponse(request, "items.html", {
        "user": user, "items": items, "q": q, "category": category, "categories": CATEGORIES, "content_ad": ad_snippet("content_middle")
    })

@app.get("/items/{item_id}", response_class=HTMLResponse)
def item_detail(request: Request, item_id: int):
    user = get_current_user(request)
    item = query_one("SELECT * FROM items WHERE id=?", (item_id,))
    if not item:
        raise HTTPException(status_code=404, detail="아이템을 찾을 수 없습니다.")
    with db() as conn:
        prices = conn.execute("SELECT * FROM market_prices WHERE item_id=? ORDER BY recorded_at DESC LIMIT 30", (item_id,)).fetchall()
        grouped = conn.execute(
            """
            SELECT server_name, price, volume, recorded_at
            FROM market_prices
            WHERE item_id=? AND id IN (
                SELECT MAX(id) FROM market_prices WHERE item_id=? GROUP BY server_name
            )
            ORDER BY server_name
            """, (item_id, item_id)
        ).fetchall()
    values = [p["price"] for p in reversed(prices)]
    return templates.TemplateResponse(request, "item_detail.html", {
        "user": user, "item": item, "prices": prices, "grouped": grouped, "sparkline": sparkline(values), "content_ad": ad_snippet("content_middle")
    })

@app.get("/monsters", response_class=HTMLResponse)
def monsters_page(request: Request, q: str = ""):
    user = get_current_user(request)
    sql = "SELECT * FROM monsters"
    params = ()
    if q:
        sql += " WHERE name LIKE ? OR region LIKE ? OR drop_items LIKE ?"
        params = (f"%{q}%", f"%{q}%", f"%{q}%")
    sql += " ORDER BY level DESC, id DESC"
    monsters = query_all(sql, params)
    return templates.TemplateResponse(request, "monsters.html", {"user": user, "monsters": monsters, "q": q})

@app.get("/spells", response_class=HTMLResponse)
def spells_page(request: Request, q: str = "", spell_class: str = ""):
    user = get_current_user(request)
    sql = "SELECT * FROM spells WHERE 1=1"
    params = []
    if q:
        sql += " AND (name LIKE ? OR description LIKE ?)"
        params += [f"%{q}%", f"%{q}%"]
    if spell_class:
        sql += " AND spell_class=?"
        params.append(spell_class)
    sql += " ORDER BY id DESC"
    spells = query_all(sql, tuple(params))
    return templates.TemplateResponse(request, "spells.html", {"user": user, "spells": spells, "q": q, "spell_class": spell_class})

@app.get("/skins", response_class=HTMLResponse)
def skins_page(request: Request):
    user = get_current_user(request)
    skins = query_all("SELECT * FROM skins ORDER BY id DESC")
    return templates.TemplateResponse(request, "skins.html", {"user": user, "skins": skins})

@app.get("/blood-marks", response_class=HTMLResponse)
def blood_marks_page(request: Request):
    user = get_current_user(request)
    marks = query_all("SELECT * FROM blood_marks ORDER BY id DESC")
    return templates.TemplateResponse(request, "blood_marks.html", {"user": user, "marks": marks})

@app.get("/market", response_class=HTMLResponse)
def market_page(request: Request, server_name: str = ""):
    user = get_current_user(request)
    with db() as conn:
        items = conn.execute("SELECT * FROM items ORDER BY name ASC").fetchall()
        rows = []
        for item in items:
            if server_name:
                hist = conn.execute(
                    "SELECT * FROM market_prices WHERE item_id=? AND server_name=? ORDER BY recorded_at ASC",
                    (item["id"], server_name)
                ).fetchall()
            else:
                hist = conn.execute(
                    "SELECT * FROM market_prices WHERE item_id=? AND server_name='기란' ORDER BY recorded_at ASC",
                    (item["id"],)
                ).fetchall()
            if not hist:
                continue
            latest = hist[-1]
            prev = hist[-2] if len(hist) > 1 else hist[-1]
            rows.append({
                "item": item,
                "latest_price": latest["price"],
                "volume": latest["volume"],
                "server_name": latest["server_name"],
                "change": latest["price"] - prev["price"],
                "updated_at": latest["recorded_at"],
                "sparkline": sparkline([h["price"] for h in hist]),
            })
    rows.sort(key=lambda r: r["latest_price"], reverse=True)
    return templates.TemplateResponse(request, "market.html", {
        "user": user, "rows": rows, "server_name": server_name or "기란", "servers": SERVER_NAMES
    })

@app.get("/api/market/latest")
def api_market_latest(server_name: str = "기란"):
    with db() as conn:
        rows = conn.execute(
            """
            SELECT i.name, i.category, mp.server_name, mp.price, mp.volume, mp.recorded_at
            FROM items i
            JOIN market_prices mp ON mp.item_id = i.id
            WHERE mp.id IN (
                SELECT MAX(id) FROM market_prices WHERE server_name=? GROUP BY item_id
            )
            ORDER BY mp.price DESC
            """, (server_name,)
        ).fetchall()
    return JSONResponse([dict(row) for row in rows])

@app.get("/community", response_class=HTMLResponse)
def community_page(request: Request, board_type: str = ""):
    user = get_current_user(request)
    sql = """
        SELECT p.*, u.display_name
        FROM posts p JOIN users u ON u.id = p.author_id
        WHERE p.status='published'
    """
    params = []
    if board_type:
        sql += " AND p.board_type=?"
        params.append(board_type)
    sql += " ORDER BY p.id DESC"
    posts = query_all(sql, tuple(params))
    return templates.TemplateResponse(request, "community.html", {
        "user": user, "posts": posts, "board_type": board_type, "boards": BOARD_TYPES
    })

@app.get("/community/new", response_class=HTMLResponse)
def community_new_page(request: Request):
    user = require_login(request)
    return templates.TemplateResponse(request, "community_new.html", {"user": user, "boards": BOARD_TYPES})

@app.post("/community/new")
def community_new(request: Request, board_type: str = Form(...), title: str = Form(...), content: str = Form(...)):
    user = require_login(request)
    exec_sql("INSERT INTO posts (board_type, title, content, author_id, created_at) VALUES (?, ?, ?, ?, ?)", (board_type, title, content, user["id"], now_str()))
    return redirect("/community")

@app.get("/community/{post_id}", response_class=HTMLResponse)
def community_detail(request: Request, post_id: int):
    user = get_current_user(request)
    with db() as conn:
        post = conn.execute(
            "SELECT p.*, u.display_name FROM posts p JOIN users u ON u.id = p.author_id WHERE p.id=?",
            (post_id,)
        ).fetchone()
        if not post:
            raise HTTPException(status_code=404, detail="게시글을 찾을 수 없습니다.")
        conn.execute("UPDATE posts SET views = views + 1 WHERE id=?", (post_id,))
        comments = conn.execute(
            "SELECT c.*, u.display_name FROM comments c JOIN users u ON u.id=c.author_id WHERE c.post_id=? ORDER BY c.id ASC",
            (post_id,)
        ).fetchall()
    return templates.TemplateResponse(request, "community_detail.html", {"user": user, "post": post, "comments": comments})

@app.post("/community/{post_id}/comment")
def community_comment(request: Request, post_id: int, content: str = Form(...)):
    user = require_login(request)
    exec_sql("INSERT INTO comments (post_id, author_id, content, created_at) VALUES (?, ?, ?, ?)", (post_id, user["id"], content, now_str()))
    return redirect(f"/community/{post_id}")

@app.get("/simulator", response_class=HTMLResponse)
def simulator_page(request: Request, result: str = ""):
    user = get_current_user(request)
    items = query_all("SELECT * FROM items ORDER BY name")
    return templates.TemplateResponse(request, "simulator.html", {"user": user, "items": items, "result": result})

@app.post("/simulator/run")
def simulator_run(
    request: Request,
    item_id: int = Form(...),
    tries: int = Form(...),
    cost: int = Form(...),
    success_rate: int = Form(...),
):
    user = require_login(request)
    item = query_one("SELECT * FROM items WHERE id=?", (item_id,))
    if not item:
        return redirect("/simulator?result=아이템을 찾을 수 없습니다")
    total_cost = tries * cost
    if user["points"] < total_cost:
        return redirect("/simulator?result=포인트가 부족합니다")
    successes = 0
    for i in range(max(1, tries)):
        seed = (user["id"] * 17 + i * 13 + item_id * 5) % 100
        if seed < success_rate:
            successes += 1
    add_points(user["id"], -total_cost, f"러쉬 시뮬레이터 - {item['name']}")
    msg = f"{item['name']} {tries}회 / 성공 {successes}회 / 사용 {total_cost}P"
    return redirect(f"/simulator?result={quote(msg)}")

@app.get("/mypage", response_class=HTMLResponse)
def mypage(request: Request):
    user = require_login(request)
    logs = query_all("SELECT * FROM point_logs WHERE user_id=? ORDER BY id DESC LIMIT 20", (user["id"],))
    my_posts = query_all("SELECT * FROM posts WHERE author_id=? ORDER BY id DESC LIMIT 10", (user["id"],))
    return templates.TemplateResponse(request, "mypage.html", {"user": user, "logs": logs, "my_posts": my_posts})

@app.get("/report", response_class=HTMLResponse)
def report_page(request: Request, success: str = ""):
    user = get_current_user(request)
    return templates.TemplateResponse(request, "report.html", {"user": user, "success": success})

@app.post("/report")
def create_report(request: Request, target_type: str = Form(...), target_id: int = Form(0), reason: str = Form(...)):
    user = require_login(request)
    exec_sql(
        "INSERT INTO reports (reporter_id, target_type, target_id, reason, created_at) VALUES (?, ?, ?, ?, ?)",
        (user["id"], target_type, target_id, reason, now_str())
    )
    return redirect("/report?success=신고가 접수되었습니다")

@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request, error: str = ""):
    return templates.TemplateResponse(request, "login.html", {"user": get_current_user(request), "error": error})

@app.post("/login")
def login(request: Request, username: str = Form(...), password: str = Form(...)):
    user = query_one(
        "SELECT * FROM users WHERE username=? AND password_hash=?",
        (username, hash_password(password))
    )
    if not user:
        return redirect("/login?error=아이디 또는 비밀번호가 올바르지 않습니다")
    if user["status"] != "active":
        return redirect("/login?error=정지된 계정입니다")
    request.session["user_id"] = user["id"]
    return redirect("/")

@app.get("/register", response_class=HTMLResponse)
def register_page(request: Request, error: str = ""):
    return templates.TemplateResponse(request, "register.html", {"user": get_current_user(request), "error": error})

@app.post("/register")
def register(request: Request, username: str = Form(...), display_name: str = Form(...), password: str = Form(...)):
    if query_one("SELECT 1 FROM users WHERE username=?", (username,)):
        return redirect("/register?error=이미 존재하는 아이디입니다")
    exec_sql(
        "INSERT INTO users (username, password_hash, display_name, role, points, status, created_at) VALUES (?, ?, ?, 'user', 10000, 'active', ?)",
        (username, hash_password(password), display_name, now_str())
    )
    return redirect("/login")

@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return redirect("/")

# ---------- Admin ----------

@app.get("/admin", response_class=HTMLResponse)
def admin_dashboard(request: Request):
    user = require_admin(request)
    with db() as conn:
        stats = {
            "users": conn.execute("SELECT COUNT(*) FROM users").fetchone()[0],
            "items": conn.execute("SELECT COUNT(*) FROM items").fetchone()[0],
            "reports_open": conn.execute("SELECT COUNT(*) FROM reports WHERE status='open'").fetchone()[0],
            "posts": conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0],
            "price_rows": conn.execute("SELECT COUNT(*) FROM market_prices").fetchone()[0],
        }
        reports = conn.execute(
            """
            SELECT r.*, u.display_name
            FROM reports r LEFT JOIN users u ON u.id=r.reporter_id
            ORDER BY r.id DESC LIMIT 10
            """
        ).fetchall()
    return templates.TemplateResponse(request, "admin/dashboard.html", {"user": user, "stats": stats, "reports": reports})

@app.get("/admin/items", response_class=HTMLResponse)
def admin_items(request: Request):
    user = require_admin(request)
    items = query_all("SELECT * FROM items ORDER BY id DESC")
    return templates.TemplateResponse(request, "admin/items.html", {"user": user, "items": items, "grades": GRADES, "categories": CATEGORIES})

@app.post("/admin/items")
def admin_items_create(request: Request, name: str = Form(...), grade: str = Form(...), category: str = Form(...), image_url: str = Form(""), description: str = Form(""), drop_info: str = Form("")):
    require_admin(request)
    exec_sql(
        "INSERT INTO items (name, grade, category, image_url, description, drop_info, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (name, grade, category, image_url or "/static/images/default.svg", description, drop_info, now_str())
    )
    return redirect("/admin/items")

@app.get("/admin/monsters", response_class=HTMLResponse)
def admin_monsters(request: Request):
    user = require_admin(request)
    monsters = query_all("SELECT * FROM monsters ORDER BY id DESC")
    return templates.TemplateResponse(request, "admin/monsters.html", {"user": user, "monsters": monsters})

@app.post("/admin/monsters")
def admin_monsters_create(request: Request, name: str = Form(...), level: int = Form(...), region: str = Form(...), drop_items: str = Form(""), description: str = Form(""), image_url: str = Form("")):
    require_admin(request)
    exec_sql("INSERT INTO monsters (name, level, region, drop_items, description, image_url, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)", (name, level, region, drop_items, description, image_url or "/static/images/monster.svg", now_str()))
    return redirect("/admin/monsters")

@app.get("/admin/spells", response_class=HTMLResponse)
def admin_spells(request: Request):
    user = require_admin(request)
    spells = query_all("SELECT * FROM spells ORDER BY id DESC")
    return templates.TemplateResponse(request, "admin/spells.html", {"user": user, "spells": spells})

@app.post("/admin/spells")
def admin_spells_create(request: Request, name: str = Form(...), spell_class: str = Form(...), mp_cost: int = Form(...), description: str = Form(""), image_url: str = Form("")):
    require_admin(request)
    exec_sql("INSERT INTO spells (name, spell_class, mp_cost, description, image_url, created_at) VALUES (?, ?, ?, ?, ?, ?)", (name, spell_class, mp_cost, description, image_url or "/static/images/spell.svg", now_str()))
    return redirect("/admin/spells")

@app.get("/admin/skins", response_class=HTMLResponse)
def admin_skins(request: Request):
    user = require_admin(request)
    skins = query_all("SELECT * FROM skins ORDER BY id DESC")
    return templates.TemplateResponse(request, "admin/skins.html", {"user": user, "skins": skins})

@app.post("/admin/skins")
def admin_skins_create(request: Request, name: str = Form(...), author: str = Form(...), color_code: str = Form(...), description: str = Form(""), preview_url: str = Form("")):
    require_admin(request)
    exec_sql("INSERT INTO skins (name, author, preview_url, color_code, description, created_at) VALUES (?, ?, ?, ?, ?, ?)", (name, author, preview_url or "/static/images/skin.svg", color_code, description, now_str()))
    return redirect("/admin/skins")

@app.get("/admin/blood-marks", response_class=HTMLResponse)
def admin_blood_marks(request: Request):
    user = require_admin(request)
    marks = query_all("SELECT * FROM blood_marks ORDER BY id DESC")
    return templates.TemplateResponse(request, "admin/blood_marks.html", {"user": user, "marks": marks})

@app.post("/admin/blood-marks")
def admin_blood_marks_create(request: Request, name: str = Form(...), clan_name: str = Form(...), description: str = Form(""), preview_url: str = Form("")):
    require_admin(request)
    exec_sql("INSERT INTO blood_marks (name, clan_name, preview_url, description, created_at) VALUES (?, ?, ?, ?, ?)", (name, clan_name, preview_url or "/static/images/bloodmark.svg", description, now_str()))
    return redirect("/admin/blood-marks")

@app.get("/admin/market", response_class=HTMLResponse)
def admin_market(request: Request):
    user = require_admin(request)
    items = query_all("SELECT * FROM items ORDER BY name")
    rows = query_all(
        """
        SELECT i.name, mp.server_name, mp.price, mp.volume, mp.recorded_at
        FROM items i JOIN market_prices mp ON mp.item_id = i.id
        WHERE mp.id IN (
            SELECT MAX(id) FROM market_prices GROUP BY item_id, server_name
        )
        ORDER BY mp.recorded_at DESC
        """
    )
    return templates.TemplateResponse(request, "admin/market.html", {"user": user, "items": items, "rows": rows, "servers": SERVER_NAMES})

@app.post("/admin/market")
def admin_market_create(request: Request, item_id: int = Form(...), server_name: str = Form(...), price: int = Form(...), volume: int = Form(...)):
    require_admin(request)
    exec_sql("INSERT INTO market_prices (item_id, server_name, price, volume, recorded_at) VALUES (?, ?, ?, ?, ?)", (item_id, server_name, price, volume, now_str()))
    return redirect("/admin/market")

@app.get("/admin/users", response_class=HTMLResponse)
def admin_users(request: Request):
    user = require_admin(request)
    users = query_all("SELECT * FROM users ORDER BY id DESC")
    sanctions = query_all(
        """
        SELECT s.*, u.display_name
        FROM sanctions s JOIN users u ON u.id=s.user_id
        ORDER BY s.id DESC LIMIT 30
        """
    )
    return templates.TemplateResponse(request, "admin/users.html", {"user": user, "users": users, "sanctions": sanctions})

@app.post("/admin/users/{user_id}/points")
def admin_user_points(request: Request, user_id: int, points: int = Form(...)):
    require_admin(request)
    with db() as conn:
        user = conn.execute("SELECT points FROM users WHERE id=?", (user_id,)).fetchone()
        if not user:
            raise HTTPException(status_code=404)
        delta = points - user["points"]
        conn.execute("UPDATE users SET points=? WHERE id=?", (points, user_id))
        conn.execute("INSERT INTO point_logs (user_id, delta, reason, created_at) VALUES (?, ?, ?, ?)", (user_id, delta, "관리자 포인트 조정", now_str()))
    return redirect("/admin/users")

@app.post("/admin/users/{user_id}/status")
def admin_user_status(request: Request, user_id: int, status: str = Form(...), reason: str = Form("")):
    require_admin(request)
    with db() as conn:
        conn.execute("UPDATE users SET status=? WHERE id=?", (status, user_id))
        conn.execute("INSERT INTO sanctions (user_id, action, reason, created_at) VALUES (?, ?, ?, ?)", (user_id, status, reason or "관리자 상태 변경", now_str()))
    return redirect("/admin/users")

@app.get("/admin/posts", response_class=HTMLResponse)
def admin_posts(request: Request):
    user = require_admin(request)
    posts = query_all(
        """
        SELECT p.*, u.display_name
        FROM posts p JOIN users u ON u.id=p.author_id
        ORDER BY p.id DESC
        """
    )
    return templates.TemplateResponse(request, "admin/posts.html", {"user": user, "posts": posts})

@app.post("/admin/posts/{post_id}/status")
def admin_post_status(request: Request, post_id: int, status: str = Form(...)):
    require_admin(request)
    exec_sql("UPDATE posts SET status=? WHERE id=?", (status, post_id))
    return redirect("/admin/posts")

@app.get("/admin/reports", response_class=HTMLResponse)
def admin_reports(request: Request):
    user = require_admin(request)
    reports = query_all(
        """
        SELECT r.*, u.display_name
        FROM reports r LEFT JOIN users u ON u.id=r.reporter_id
        ORDER BY r.id DESC
        """
    )
    return templates.TemplateResponse(request, "admin/reports.html", {"user": user, "reports": reports})

@app.post("/admin/reports/{report_id}")
def admin_report_update(request: Request, report_id: int, status: str = Form(...), admin_note: str = Form("")):
    require_admin(request)
    with db() as conn:
        conn.execute("UPDATE reports SET status=?, admin_note=? WHERE id=?", (status, admin_note, report_id))
    return redirect("/admin/reports")

@app.get("/admin/ads", response_class=HTMLResponse)
def admin_ads(request: Request):
    user = require_admin(request)
    ads = query_all("SELECT * FROM ad_slots ORDER BY id")
    return templates.TemplateResponse(request, "admin/ads.html", {"user": user, "ads": ads})

@app.post("/admin/ads/{ad_id}")
def admin_ads_update(request: Request, ad_id: int, snippet: str = Form(...), is_active: int = Form(0)):
    require_admin(request)
    with db() as conn:
        conn.execute("UPDATE ad_slots SET snippet=?, is_active=? WHERE id=?", (snippet, is_active, ad_id))
    return redirect("/admin/ads")

@app.get("/healthz")
def healthz():
    return {"ok": True, "db": str(DB_PATH)}
