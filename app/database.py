import json
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Optional

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / 'data'
DB_PATH = DATA_DIR / 'app.db'


def get_conn():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_all(query: str, params: tuple = ()):
    with closing(get_conn()) as conn:
        return conn.execute(query, params).fetchall()


def fetch_one(query: str, params: tuple = ()) -> Optional[sqlite3.Row]:
    with closing(get_conn()) as conn:
        return conn.execute(query, params).fetchone()


def execute(query: str, params: tuple = ()):
    with closing(get_conn()) as conn:
        cur = conn.execute(query, params)
        conn.commit()
        return cur.lastrowid


def executescript(sql: str):
    with closing(get_conn()) as conn:
        conn.executescript(sql)
        conn.commit()


def init_db():
    executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'user',
            status TEXT NOT NULL DEFAULT 'active',
            points INTEGER NOT NULL DEFAULT 1000,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            grade TEXT,
            category TEXT,
            description TEXT,
            source TEXT,
            drop_monsters TEXT,
            stats_json TEXT,
            tags TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS monsters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            level INTEGER,
            zone TEXT,
            description TEXT,
            drops_json TEXT,
            weak_attr TEXT,
            respawn_note TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS spells (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            class_name TEXT,
            grade TEXT,
            mp_cost INTEGER,
            cooldown_note TEXT,
            source TEXT,
            description TEXT,
            effects_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS skins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT UNIQUE NOT NULL,
            title TEXT NOT NULL,
            category TEXT NOT NULL,
            preview_url TEXT,
            description TEXT,
            colors_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS blood_marks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT UNIQUE NOT NULL,
            title TEXT NOT NULL,
            shape TEXT,
            preview_url TEXT,
            description TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            server_name TEXT NOT NULL,
            market TEXT NOT NULL,
            price INTEGER NOT NULL,
            volume INTEGER NOT NULL DEFAULT 0,
            change_pct REAL NOT NULL DEFAULT 0,
            captured_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            board TEXT NOT NULL,
            title TEXT NOT NULL,
            body TEXT NOT NULL,
            author_id INTEGER,
            status TEXT NOT NULL DEFAULT 'published',
            views INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(author_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id INTEGER NOT NULL,
            author_id INTEGER,
            body TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'published',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(post_id) REFERENCES posts(id),
            FOREIGN KEY(author_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            target_type TEXT NOT NULL,
            target_id INTEGER NOT NULL,
            reason TEXT NOT NULL,
            detail TEXT,
            reporter_id INTEGER,
            status TEXT NOT NULL DEFAULT 'open',
            action_note TEXT,
            reviewed_by INTEGER,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            reviewed_at TEXT,
            FOREIGN KEY(reporter_id) REFERENCES users(id),
            FOREIGN KEY(reviewed_by) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS sanctions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            sanction_type TEXT NOT NULL,
            reason TEXT NOT NULL,
            expires_at TEXT,
            created_by INTEGER,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(created_by) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS simulator_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            item_name TEXT NOT NULL,
            target_level INTEGER NOT NULL,
            scroll_type TEXT NOT NULL,
            attempts INTEGER NOT NULL,
            success_count INTEGER NOT NULL,
            failed_keep INTEGER NOT NULL,
            destroyed_count INTEGER NOT NULL,
            scrolls_used INTEGER NOT NULL,
            estimated_cost INTEGER NOT NULL,
            best_level INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS ad_slots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slot_key TEXT UNIQUE NOT NULL,
            is_enabled INTEGER NOT NULL DEFAULT 1,
            provider TEXT NOT NULL DEFAULT 'adsense',
            code_html TEXT,
            note TEXT
        );
        CREATE TABLE IF NOT EXISTS settings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key TEXT UNIQUE NOT NULL,
            value TEXT NOT NULL
        );
        """
    )
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        migrations = [
            ("ALTER TABLE users ADD COLUMN status TEXT NOT NULL DEFAULT 'active'", None),
            ("ALTER TABLE items ADD COLUMN tags TEXT", None),
            ("ALTER TABLE monsters ADD COLUMN weak_attr TEXT", None),
            ("ALTER TABLE monsters ADD COLUMN respawn_note TEXT", None),
            ("ALTER TABLE posts ADD COLUMN status TEXT NOT NULL DEFAULT 'published'", None),
            ("ALTER TABLE comments ADD COLUMN status TEXT NOT NULL DEFAULT 'published'", None),
        ]
        for sql, _ in migrations:
            try:
                cur.execute(sql)
            except Exception:
                pass
        conn.commit()

    seed_db()


def seed_db():
    with closing(get_conn()) as conn:
        cur = conn.cursor()

        if cur.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 0:
            cur.execute('INSERT INTO users (username,email,password_hash,role,status,points) VALUES (?,?,?,?,?,?)',
                        ('admin', 'admin@example.com', sha256('admin1234'.encode()).hexdigest(), 'admin', 'active', 50000))
            cur.execute('INSERT INTO users (username,email,password_hash,role,status,points) VALUES (?,?,?,?,?,?)',
                        ('tester', 'tester@example.com', sha256('user1234'.encode()).hexdigest(), 'user', 'active', 12000))

        if cur.execute("SELECT COUNT(*) FROM items").fetchone()[0] == 0:
            rows = [
                ('sword-of-knight', '기사의 검', '영웅', '무기', '근거리 성장용 무기 예시', '수기입력 템플릿 예시', '오크 파이터, 해골 전사', json.dumps({'damage': '8/10', 'hit': '+1', 'safeEnchant': 6}, ensure_ascii=False), '근거리,사냥'),
                ('elven-bow', '요정 장궁', '희귀', '무기', '원거리 사냥용 활 예시', '수기입력 템플릿 예시', '다크엘프 궁수', json.dumps({'damage': '3/8', 'dex': '+1', 'safeEnchant': 6}, ensure_ascii=False), '원거리,사냥'),
                ('wizard-robe', '마법사의 로브', '고급', '방어구', 'MP 회복 보조 로브 예시', '수기입력 템플릿 예시', '해골 마법사', json.dumps({'ac': -4, 'mpRegen': '+2', 'safeEnchant': 4}, ensure_ascii=False), '마법사,회복'),
                ('bone-sword', '뼈검', '영웅', '무기', '러쉬 대표 무기 예시', '러쉬 시뮬 설명용', '데스나이트', json.dumps({'damage': '10/12', 'proc': 'bone burst', 'safeEnchant': 0}, ensure_ascii=False), '러쉬,고가'),
                ('blessed-scroll', '축복 무기 주문서', '소모품', '주문서', '실패 시 수치 유지형 예시', '러쉬 시뮬 설명용', '상점/이벤트', json.dumps({'type': 'blessed'}, ensure_ascii=False), '주문서,강화'),
            ]
            cur.executemany('INSERT INTO items (slug,name,grade,category,description,source,drop_monsters,stats_json,tags) VALUES (?,?,?,?,?,?,?,?,?)', rows)

        if cur.execute("SELECT COUNT(*) FROM monsters").fetchone()[0] == 0:
            rows = [
                ('orc-fighter', '오크 파이터', 18, '말하는 섬 필드', '초반 사냥 핵심 몬스터', json.dumps(['기사의 검', '아데나 주머니'], ensure_ascii=False), '화염', '10~15분 주기 예시'),
                ('skeleton-warrior', '해골 전사', 24, '본던 1층', '무기 드랍 파밍 추천', json.dumps(['기사의 검', '마법사의 로브'], ensure_ascii=False), '신성', '고정 리젠 예시'),
                ('dark-elf-archer', '다크엘프 궁수', 32, '용던 입구', '원거리 견제 몬스터', json.dumps(['요정 장궁', '화살 꾸러미'], ensure_ascii=False), '풍', '순찰형 패턴 예시'),
                ('death-knight', '데스나이트', 52, '보스 레이드', '고가 러쉬 아이템 드랍 예시', json.dumps(['뼈검', '축복 무기 주문서'], ensure_ascii=False), '수', '레이드 보스 예시'),
            ]
            cur.executemany('INSERT INTO monsters (slug,name,level,zone,description,drops_json,weak_attr,respawn_note) VALUES (?,?,?,?,?,?,?,?)', rows)

        if cur.execute("SELECT COUNT(*) FROM spells").fetchone()[0] == 0:
            rows = [
                ('greater-heal', '그레이터 힐', '마법사', '희귀', 18, '짧음', '수기입력 템플릿 예시', '대상 회복 마법 예시', json.dumps({'heal': '중', 'target': 'single'}, ensure_ascii=False)),
                ('haste', '헤이스트', '요정', '영웅', 24, '중간', '수기입력 템플릿 예시', '이동 및 공속 증가 예시', json.dumps({'speed': '+', 'duration': '120s'}, ensure_ascii=False)),
                ('shield', '쉴드', '군주', '고급', 10, '짧음', '수기입력 템플릿 예시', '방어 강화 예시', json.dumps({'ac': '-2', 'duration': '180s'}, ensure_ascii=False)),
            ]
            cur.executemany('INSERT INTO spells (slug,name,class_name,grade,mp_cost,cooldown_note,source,description,effects_json) VALUES (?,?,?,?,?,?,?,?,?)', rows)

        if cur.execute("SELECT COUNT(*) FROM skins").fetchone()[0] == 0:
            rows = [
                ('emerald-ui', '에메랄드 UI', 'ui', '', '녹색 계열 UI 스킨 예시', json.dumps({'primary': '#1fbf75', 'accent': '#d4af37', 'bg': '#111827'}, ensure_ascii=False)),
                ('ivory-minimap', '아이보리 미니맵', 'map', '', '미니맵 스킨 예시', json.dumps({'land': '#e5e7eb', 'water': '#93c5fd', 'point': '#ef4444'}, ensure_ascii=False)),
            ]
            cur.executemany('INSERT INTO skins (slug,title,category,preview_url,description,colors_json) VALUES (?,?,?,?,?,?)', rows)

        if cur.execute("SELECT COUNT(*) FROM blood_marks").fetchone()[0] == 0:
            rows = [
                ('lion-gold', '골드 라이온', '원형', '', '혈맹 상징 예시'),
                ('sword-red', '레드 소드', '방패형', '', '전투형 혈마크 예시'),
            ]
            cur.executemany('INSERT INTO blood_marks (slug,title,shape,preview_url,description) VALUES (?,?,?,?,?)', rows)

        if cur.execute("SELECT COUNT(*) FROM prices").fetchone()[0] == 0:
            base = datetime.utcnow()
            servers = [('하이네', 6804, 0.94), ('데포로쥬', 5779, 3.85), ('조우', 5613, 1.41), ('이실로테', 4529, 0.54), ('오웬', 4521, -0.97), ('켄라우헬', 4512, 5.42)]
            for i in range(24):
                ts = (base - timedelta(hours=i)).isoformat()
                for idx, (server, price, change) in enumerate(servers):
                    value = max(1000, round(price * (1 + ((idx - i) % 5) / 100)))
                    volume = 120 + idx * 15 + i
                    cur.execute('INSERT INTO prices (server_name, market, price, volume, change_pct, captured_at) VALUES (?,?,?,?,?,?)', (server, 'ADENA/W', value, volume, change, ts))

        if cur.execute("SELECT COUNT(*) FROM posts").fetchone()[0] == 0:
            admin_id = cur.execute("SELECT id FROM users WHERE username='admin'").fetchone()[0]
            rows = [
                ('notice', '사이트 오픈 안내', 'DB, 시세, 러쉬, 커뮤니티, 신고센터를 한 서버에서 운영하는 통합 포털입니다.', admin_id, 'published'),
                ('tip', '초반 사냥터 추천', '말하는 섬 → 본던 → 용던 입구 순으로 성장 루트를 잡는 예시입니다.', admin_id, 'published'),
                ('market', '하이네 시세 체크', '거래량과 등락률을 함께 보고 진입 타이밍을 판단하세요.', admin_id, 'published'),
                ('clan', '혈맹 모집 예시', '접속 시간대와 공성 참여 여부를 함께 적으면 모집 전환율이 올라갑니다.', admin_id, 'published'),
            ]
            cur.executemany('INSERT INTO posts (board,title,body,author_id,status) VALUES (?,?,?,?,?)', rows)

        if cur.execute("SELECT COUNT(*) FROM ad_slots").fetchone()[0] == 0:
            for key in ['home_top', 'home_mid', 'db_sidebar', 'community_inline', 'simulator_result', 'prices_inline', 'skin_sidebar']:
                cur.execute('INSERT INTO ad_slots (slot_key,is_enabled,provider,code_html,note) VALUES (?,?,?,?,?)', (key, 1, 'adsense', f'<!-- {key} adsense code -->', f'광고 슬롯 {key}'))

        defaults = {
            'site_name': 'Lineage Classic Hub',
            'site_tagline': 'DB · 시세 · 러쉬 · 스킨 · 혈마크 · 커뮤니티를 한 번에',
            'point_cost_simulation': '100',
            'adsense_client': 'ca-pub-xxxxxxxxxxxxxxxx',
            'analytics_id': 'G-XXXXXXXXXX',
            'homepage_notice': '실서비스 전에는 정책 페이지, 개인정보처리방침, 광고 코드, 배너 이미지를 교체하세요.',
        }
        for k, v in defaults.items():
            cur.execute('INSERT OR IGNORE INTO settings (key,value) VALUES (?,?)', (k, v))
        conn.commit()
