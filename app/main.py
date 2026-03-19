import csv
import hashlib
import io
import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, Form, Request, UploadFile, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from .database import execute, fetch_all, fetch_one, get_conn, init_db

BASE_DIR = Path(__file__).resolve().parent.parent
app = FastAPI(title='Lineage Classic Hub')
app.add_middleware(SessionMiddleware, secret_key='lineagehub-secret')
app.mount('/static', StaticFiles(directory=str(BASE_DIR / 'app' / 'static')), name='static')
templates = Jinja2Templates(directory=str(BASE_DIR / 'app' / 'templates'))
init_db()


def settings_map():
    rows = fetch_all('SELECT key, value FROM settings')
    return {r['key']: r['value'] for r in rows}


def current_user(request: Request) -> Optional[dict]:
    return request.session.get('user')


def require_admin(request: Request):
    user = current_user(request)
    return user and user.get('role') == 'admin'


def context(request: Request, **kwargs):
    base = {'request': request, 'user': current_user(request), 'settings': settings_map()}
    base.update(kwargs)
    return base


def latest_prices():
    return fetch_all("""
        SELECT p1.* FROM prices p1
        JOIN (SELECT server_name, MAX(captured_at) max_time FROM prices GROUP BY server_name) p2
        ON p1.server_name=p2.server_name AND p1.captured_at=p2.max_time
        ORDER BY p1.price DESC
    """)


def slugify(value: str):
    return ''.join(ch.lower() if ch.isalnum() else '-' for ch in value).strip('-')


@app.on_event('startup')
def startup():
    init_db()


@app.get('/', response_class=HTMLResponse)
def home(request: Request):
    items = fetch_all('SELECT * FROM items ORDER BY id DESC LIMIT 5')
    monsters = fetch_all('SELECT * FROM monsters ORDER BY level DESC LIMIT 5')
    posts = fetch_all("SELECT p.*, u.username FROM posts p LEFT JOIN users u ON u.id=p.author_id WHERE p.status='published' ORDER BY p.id DESC LIMIT 6")
    spells = fetch_all('SELECT * FROM spells ORDER BY id DESC LIMIT 5')
    slots = {r['slot_key']: r for r in fetch_all('SELECT * FROM ad_slots')}
    return templates.TemplateResponse('home.html', context(request, items=items, monsters=monsters, posts=posts, spells=spells, prices=latest_prices(), slots=slots))


@app.get('/items', response_class=HTMLResponse)
def items(request: Request, q: str = ''):
    rows = fetch_all('SELECT * FROM items WHERE name LIKE ? OR category LIKE ? OR tags LIKE ? ORDER BY id DESC', (f'%{q}%', f'%{q}%', f'%{q}%'))
    slots = {r['slot_key']: r for r in fetch_all('SELECT * FROM ad_slots')}
    return templates.TemplateResponse('items.html', context(request, rows=rows, q=q, slots=slots))


@app.get('/items/{slug}', response_class=HTMLResponse)
def item_detail(request: Request, slug: str):
    row = fetch_one('SELECT * FROM items WHERE slug=?', (slug,))
    if not row:
        return HTMLResponse('Not found', status_code=404)
    related = fetch_all('SELECT * FROM monsters WHERE drops_json LIKE ? LIMIT 8', (f'%{row["name"]}%',))
    return templates.TemplateResponse('item_detail.html', context(request, row=row, stats=json.loads(row['stats_json'] or '{}'), related=related))


@app.get('/monsters', response_class=HTMLResponse)
def monsters(request: Request, q: str = ''):
    rows = fetch_all('SELECT * FROM monsters WHERE name LIKE ? OR zone LIKE ? ORDER BY level DESC', (f'%{q}%', f'%{q}%'))
    return templates.TemplateResponse('monsters.html', context(request, rows=rows, q=q, json=json))


@app.get('/spells', response_class=HTMLResponse)
def spells(request: Request, q: str = ''):
    rows = fetch_all('SELECT * FROM spells WHERE name LIKE ? OR class_name LIKE ? ORDER BY id DESC', (f'%{q}%', f'%{q}%'))
    return templates.TemplateResponse('spells.html', context(request, rows=rows, q=q, json=json))


@app.get('/skins', response_class=HTMLResponse)
def skins(request: Request):
    rows = fetch_all('SELECT * FROM skins ORDER BY id DESC')
    blood_marks = fetch_all('SELECT * FROM blood_marks ORDER BY id DESC')
    slots = {r['slot_key']: r for r in fetch_all('SELECT * FROM ad_slots')}
    return templates.TemplateResponse('skins.html', context(request, rows=rows, blood_marks=blood_marks, json=json, slots=slots))


@app.get('/prices', response_class=HTMLResponse)
def prices(request: Request, server: str = '하이네'):
    rows = latest_prices()
    chart_rows = fetch_all('SELECT server_name, price, captured_at FROM prices WHERE server_name=? ORDER BY captured_at ASC LIMIT 24', (server,))
    return templates.TemplateResponse('prices.html', context(request, rows=rows, chart_rows=chart_rows, server=server))


@app.get('/simulator', response_class=HTMLResponse)
def simulator(request: Request):
    rows = fetch_all("SELECT name FROM items WHERE category IN ('무기','방어구') ORDER BY name ASC")
    runs = fetch_all('SELECT r.*, u.username FROM simulator_runs r LEFT JOIN users u ON u.id=r.user_id ORDER BY r.id DESC LIMIT 20')
    slots = {r['slot_key']: r for r in fetch_all('SELECT * FROM ad_slots')}
    return templates.TemplateResponse('simulator.html', context(request, items=rows, runs=runs, slots=slots))


@app.post('/simulator/run')
def simulator_run(request: Request, item_name: str = Form(...), target_level: int = Form(...), scroll_type: str = Form(...), unit_cost: int = Form(0)):
    user = current_user(request)
    if not user:
        return RedirectResponse('/login', status_code=status.HTTP_303_SEE_OTHER)
    db_user = fetch_one('SELECT * FROM users WHERE id=?', (user['id'],))
    point_cost = int(settings_map().get('point_cost_simulation', '100'))
    if db_user['points'] < point_cost:
        return HTMLResponse('포인트가 부족합니다.', status_code=400)

    current = best = attempts = success_count = failed_keep = destroyed_count = scrolls_used = 0
    rates = [1.0, 1.0, 0.95, 0.8, 0.7, 0.55, 0.4, 0.25, 0.15, 0.08, 0.03]
    while current < target_level and attempts < 200:
        attempts += 1
        scrolls_used += 1
        chance = rates[min(current + 1, len(rates) - 1)]
        if random.random() <= chance:
            current += 1
            success_count += 1
        else:
            if scroll_type == 'blessed':
                failed_keep += 1
            elif scroll_type == 'cursed' and current >= 7:
                destroyed_count += 1
                current = 0
            else:
                current = 0
        best = max(best, current)

    execute('UPDATE users SET points = points - ? WHERE id=?', (point_cost, user['id']))
    execute('INSERT INTO simulator_runs (user_id,item_name,target_level,scroll_type,attempts,success_count,failed_keep,destroyed_count,scrolls_used,estimated_cost,best_level) VALUES (?,?,?,?,?,?,?,?,?,?,?)', (user['id'], item_name, target_level, scroll_type, attempts, success_count, failed_keep, destroyed_count, scrolls_used, scrolls_used * unit_cost, best))
    new_user = fetch_one('SELECT id, username, role, points, status FROM users WHERE id=?', (user['id'],))
    request.session['user'] = dict(new_user)
    return RedirectResponse('/simulator', status_code=status.HTTP_303_SEE_OTHER)


@app.get('/community', response_class=HTMLResponse)
def community(request: Request, board: str = 'notice'):
    posts = fetch_all("SELECT p.*, u.username FROM posts p LEFT JOIN users u ON u.id=p.author_id WHERE board=? AND p.status='published' ORDER BY id DESC", (board,))
    slots = {r['slot_key']: r for r in fetch_all('SELECT * FROM ad_slots')}
    return templates.TemplateResponse('community.html', context(request, board=board, posts=posts, slots=slots))


@app.post('/community/post')
def create_post(request: Request, board: str = Form(...), title: str = Form(...), body: str = Form(...)):
    user = current_user(request)
    if not user:
        return RedirectResponse('/login', status_code=status.HTTP_303_SEE_OTHER)
    execute('INSERT INTO posts (board,title,body,author_id) VALUES (?,?,?,?)', (board, title, body, user['id']))
    return RedirectResponse(f'/community?board={board}', status_code=status.HTTP_303_SEE_OTHER)


@app.get('/community/post/{post_id}', response_class=HTMLResponse)
def post_detail(request: Request, post_id: int):
    execute('UPDATE posts SET views=views+1 WHERE id=?', (post_id,))
    post = fetch_one('SELECT p.*, u.username FROM posts p LEFT JOIN users u ON u.id=p.author_id WHERE p.id=?', (post_id,))
    comments = fetch_all("SELECT c.*, u.username FROM comments c LEFT JOIN users u ON u.id=c.author_id WHERE c.post_id=? AND c.status='published' ORDER BY c.id ASC", (post_id,))
    return templates.TemplateResponse('post_detail.html', context(request, post=post, comments=comments))


@app.post('/community/comment')
def create_comment(request: Request, post_id: int = Form(...), body: str = Form(...)):
    user = current_user(request)
    if not user:
        return RedirectResponse('/login', status_code=status.HTTP_303_SEE_OTHER)
    execute('INSERT INTO comments (post_id,author_id,body) VALUES (?,?,?)', (post_id, user['id'], body))
    return RedirectResponse(f'/community/post/{post_id}', status_code=status.HTTP_303_SEE_OTHER)


@app.post('/report')
def create_report(request: Request, target_type: str = Form(...), target_id: int = Form(...), reason: str = Form(...), detail: str = Form('')):
    user = current_user(request)
    reporter_id = user['id'] if user else None
    execute('INSERT INTO reports (target_type,target_id,reason,detail,reporter_id) VALUES (?,?,?,?,?)', (target_type, target_id, reason, detail, reporter_id))
    back = request.headers.get('referer', '/community')
    return RedirectResponse(back, status_code=status.HTTP_303_SEE_OTHER)


@app.get('/register', response_class=HTMLResponse)
def register_form(request: Request):
    return templates.TemplateResponse('register.html', context(request))


@app.post('/register')
def register(request: Request, username: str = Form(...), email: str = Form(...), password: str = Form(...)):
    pw = hashlib.sha256(password.encode()).hexdigest()
    try:
        execute('INSERT INTO users (username,email,password_hash,role,points,status) VALUES (?,?,?,?,?,?)', (username, email, pw, 'user', 1000, 'active'))
    except Exception:
        return HTMLResponse('이미 존재하는 계정입니다.', status_code=400)
    return RedirectResponse('/login', status_code=status.HTTP_303_SEE_OTHER)


@app.get('/login', response_class=HTMLResponse)
def login_form(request: Request):
    return templates.TemplateResponse('login.html', context(request))


@app.post('/login')
def login(request: Request, username: str = Form(...), password: str = Form(...)):
    user = fetch_one('SELECT * FROM users WHERE username=?', (username,))
    if not user or user['password_hash'] != hashlib.sha256(password.encode()).hexdigest():
        return HTMLResponse('로그인 실패', status_code=400)
    if user['status'] != 'active':
        return HTMLResponse('정지된 계정입니다.', status_code=403)
    request.session['user'] = {'id': user['id'], 'username': user['username'], 'role': user['role'], 'points': user['points'], 'status': user['status']}
    return RedirectResponse('/', status_code=status.HTTP_303_SEE_OTHER)


@app.get('/logout')
def logout(request: Request):
    request.session.clear()
    return RedirectResponse('/', status_code=status.HTTP_303_SEE_OTHER)


@app.get('/mypage', response_class=HTMLResponse)
def mypage(request: Request):
    user = current_user(request)
    if not user:
        return RedirectResponse('/login', status_code=status.HTTP_303_SEE_OTHER)
    runs = fetch_all('SELECT * FROM simulator_runs WHERE user_id=? ORDER BY id DESC LIMIT 30', (user['id'],))
    sanctions = fetch_all('SELECT * FROM sanctions WHERE user_id=? ORDER BY id DESC LIMIT 20', (user['id'],))
    reports = fetch_all('SELECT * FROM reports WHERE reporter_id=? ORDER BY id DESC LIMIT 20', (user['id'],))
    return templates.TemplateResponse('mypage.html', context(request, runs=runs, sanctions=sanctions, reports=reports))


@app.get('/admin', response_class=HTMLResponse)
def admin(request: Request):
    if not require_admin(request):
        return RedirectResponse('/login', status_code=status.HTTP_303_SEE_OTHER)
    stats = {
        'users': fetch_one('SELECT COUNT(*) c FROM users')['c'],
        'posts': fetch_one('SELECT COUNT(*) c FROM posts')['c'],
        'runs': fetch_one('SELECT COUNT(*) c FROM simulator_runs')['c'],
        'items': fetch_one('SELECT COUNT(*) c FROM items')['c'],
        'reports': fetch_one("SELECT COUNT(*) c FROM reports WHERE status='open'")['c'],
    }
    slots = fetch_all('SELECT * FROM ad_slots ORDER BY id ASC')
    users = fetch_all('SELECT * FROM users ORDER BY id ASC')
    reports = fetch_all('SELECT r.*, u.username reporter_name FROM reports r LEFT JOIN users u ON u.id=r.reporter_id ORDER BY CASE WHEN r.status="open" THEN 0 ELSE 1 END, r.id DESC LIMIT 100')
    sanctions = fetch_all('SELECT s.*, u.username FROM sanctions s LEFT JOIN users u ON u.id=s.user_id ORDER BY s.id DESC LIMIT 50')
    return templates.TemplateResponse('admin.html', context(request, stats=stats, slots=slots, users=users, reports=reports, sanctions=sanctions))


@app.post('/admin/ad-slot')
def admin_ad_slot(request: Request, slot_id: int = Form(...), code_html: str = Form(''), note: str = Form(''), is_enabled: Optional[str] = Form(None)):
    if not require_admin(request):
        return RedirectResponse('/login', status_code=status.HTTP_303_SEE_OTHER)
    execute('UPDATE ad_slots SET code_html=?, note=?, is_enabled=? WHERE id=?', (code_html, note, 1 if is_enabled else 0, slot_id))
    return RedirectResponse('/admin', status_code=status.HTTP_303_SEE_OTHER)


@app.post('/admin/user-points')
def admin_user_points(request: Request, user_id: int = Form(...), points: int = Form(...), status_value: str = Form('active')):
    if not require_admin(request):
        return RedirectResponse('/login', status_code=status.HTTP_303_SEE_OTHER)
    execute('UPDATE users SET points=?, status=? WHERE id=?', (points, status_value, user_id))
    return RedirectResponse('/admin', status_code=status.HTTP_303_SEE_OTHER)


@app.post('/admin/item')
def admin_item(request: Request, slug: str = Form(''), name: str = Form(...), grade: str = Form(''), category: str = Form(''), source: str = Form(''), drop_monsters: str = Form(''), description: str = Form(''), stats_json: str = Form('{}'), tags: str = Form('')):
    if not require_admin(request):
        return RedirectResponse('/login', status_code=status.HTTP_303_SEE_OTHER)
    execute('INSERT INTO items (slug,name,grade,category,description,source,drop_monsters,stats_json,tags) VALUES (?,?,?,?,?,?,?,?,?)', (slug or slugify(name), name, grade, category, description, source, drop_monsters, stats_json, tags))
    return RedirectResponse('/admin', status_code=status.HTTP_303_SEE_OTHER)


@app.post('/admin/spell')
def admin_spell(request: Request, name: str = Form(...), class_name: str = Form(''), grade: str = Form(''), mp_cost: int = Form(0), cooldown_note: str = Form(''), source: str = Form(''), description: str = Form(''), effects_json: str = Form('{}')):
    if not require_admin(request):
        return RedirectResponse('/login', status_code=status.HTTP_303_SEE_OTHER)
    execute('INSERT INTO spells (slug,name,class_name,grade,mp_cost,cooldown_note,source,description,effects_json) VALUES (?,?,?,?,?,?,?,?,?)', (slugify(name), name, class_name, grade, mp_cost, cooldown_note, source, description, effects_json))
    return RedirectResponse('/admin', status_code=status.HTTP_303_SEE_OTHER)


@app.post('/admin/skin')
def admin_skin(request: Request, title: str = Form(...), category: str = Form(...), preview_url: str = Form(''), description: str = Form(''), colors_json: str = Form('{}')):
    if not require_admin(request):
        return RedirectResponse('/login', status_code=status.HTTP_303_SEE_OTHER)
    execute('INSERT INTO skins (slug,title,category,preview_url,description,colors_json) VALUES (?,?,?,?,?,?)', (slugify(title), title, category, preview_url, description, colors_json))
    return RedirectResponse('/admin', status_code=status.HTTP_303_SEE_OTHER)


@app.post('/admin/blood-mark')
def admin_blood_mark(request: Request, title: str = Form(...), shape: str = Form(''), preview_url: str = Form(''), description: str = Form('')):
    if not require_admin(request):
        return RedirectResponse('/login', status_code=status.HTTP_303_SEE_OTHER)
    execute('INSERT INTO blood_marks (slug,title,shape,preview_url,description) VALUES (?,?,?,?,?)', (slugify(title), title, shape, preview_url, description))
    return RedirectResponse('/admin', status_code=status.HTTP_303_SEE_OTHER)


@app.post('/admin/report-action')
def admin_report_action(request: Request, report_id: int = Form(...), status_value: str = Form(...), action_note: str = Form('')):
    if not require_admin(request):
        return RedirectResponse('/login', status_code=status.HTTP_303_SEE_OTHER)
    admin_user = current_user(request)
    execute('UPDATE reports SET status=?, action_note=?, reviewed_by=?, reviewed_at=CURRENT_TIMESTAMP WHERE id=?', (status_value, action_note, admin_user['id'], report_id))
    return RedirectResponse('/admin', status_code=status.HTTP_303_SEE_OTHER)


@app.post('/admin/sanction')
def admin_sanction(request: Request, user_id: int = Form(...), sanction_type: str = Form(...), reason: str = Form(...), duration_days: int = Form(0)):
    if not require_admin(request):
        return RedirectResponse('/login', status_code=status.HTTP_303_SEE_OTHER)
    admin_user = current_user(request)
    expires = (datetime.utcnow() + timedelta(days=duration_days)).isoformat() if duration_days > 0 else None
    execute('INSERT INTO sanctions (user_id,sanction_type,reason,expires_at,created_by) VALUES (?,?,?,?,?)', (user_id, sanction_type, reason, expires, admin_user['id']))
    if sanction_type in ('suspend', 'ban'):
        execute('UPDATE users SET status=? WHERE id=?', ('suspended' if sanction_type == 'suspend' else 'banned', user_id))
    return RedirectResponse('/admin', status_code=status.HTTP_303_SEE_OTHER)


@app.post('/admin/import/{dataset}')
async def admin_import(request: Request, dataset: str, upload: UploadFile = File(...)):
    if not require_admin(request):
        return RedirectResponse('/login', status_code=status.HTTP_303_SEE_OTHER)
    raw = await upload.read()
    text = raw.decode('utf-8-sig')
    reader = csv.DictReader(io.StringIO(text))
    count = 0
    with get_conn() as conn:
        cur = conn.cursor()
        for row in reader:
            if dataset == 'items':
                cur.execute('INSERT INTO items (slug,name,grade,category,description,source,drop_monsters,stats_json,tags) VALUES (?,?,?,?,?,?,?,?,?)', (row.get('slug') or slugify(row.get('name','')), row.get('name',''), row.get('grade',''), row.get('category',''), row.get('description',''), row.get('source',''), row.get('drop_monsters',''), row.get('stats_json','{}'), row.get('tags','')))
            elif dataset == 'monsters':
                cur.execute('INSERT INTO monsters (slug,name,level,zone,description,drops_json,weak_attr,respawn_note) VALUES (?,?,?,?,?,?,?,?)', (row.get('slug') or slugify(row.get('name','')), row.get('name',''), int(row.get('level') or 0), row.get('zone',''), row.get('description',''), row.get('drops_json','[]'), row.get('weak_attr',''), row.get('respawn_note','')))
            elif dataset == 'spells':
                cur.execute('INSERT INTO spells (slug,name,class_name,grade,mp_cost,cooldown_note,source,description,effects_json) VALUES (?,?,?,?,?,?,?,?,?)', (row.get('slug') or slugify(row.get('name','')), row.get('name',''), row.get('class_name',''), row.get('grade',''), int(row.get('mp_cost') or 0), row.get('cooldown_note',''), row.get('source',''), row.get('description',''), row.get('effects_json','{}')))
            elif dataset == 'skins':
                cur.execute('INSERT INTO skins (slug,title,category,preview_url,description,colors_json) VALUES (?,?,?,?,?,?)', (row.get('slug') or slugify(row.get('title','')), row.get('title',''), row.get('category','ui'), row.get('preview_url',''), row.get('description',''), row.get('colors_json','{}')))
            elif dataset == 'blood_marks':
                cur.execute('INSERT INTO blood_marks (slug,title,shape,preview_url,description) VALUES (?,?,?,?,?)', (row.get('slug') or slugify(row.get('title','')), row.get('title',''), row.get('shape',''), row.get('preview_url',''), row.get('description','')))
            count += 1
        conn.commit()
    return RedirectResponse(f'/admin?imported={dataset}:{count}', status_code=status.HTTP_303_SEE_OTHER)


@app.get('/api/search')
def api_search(q: str):
    items = fetch_all('SELECT slug, name FROM items WHERE name LIKE ? LIMIT 8', (f'%{q}%',))
    monsters = fetch_all('SELECT slug, name FROM monsters WHERE name LIKE ? LIMIT 8', (f'%{q}%',))
    spells = fetch_all('SELECT slug, name FROM spells WHERE name LIKE ? LIMIT 8', (f'%{q}%',))
    data = [{'type': 'item', 'slug': r['slug'], 'name': r['name']} for r in items]
    data += [{'type': 'monster', 'slug': r['slug'], 'name': r['name']} for r in monsters]
    data += [{'type': 'spell', 'slug': r['slug'], 'name': r['name']} for r in spells]
    return JSONResponse(data)
