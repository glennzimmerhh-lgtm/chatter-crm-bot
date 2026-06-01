"""
Chatter CRM – Subscriber Chat Backend
Telethon Userbot + FastAPI REST API — PostgreSQL edition
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime
from contextlib import asynccontextmanager
from typing import Optional

import uvicorn
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import User

# ── CONFIG ────────────────────────────────────────────────────────────────────
TG_API_ID   = os.environ.get('TG_API_ID', '')
TG_API_HASH = os.environ.get('TG_API_HASH', '')
TG_SESSION  = os.environ.get('TG_SESSION', '')
PORT        = int(os.environ.get('PORT', 8000))
DATABASE_URL = os.environ.get('DATABASE_URL', '')
SUBSCRIBER_BACKUP_WEBHOOK = os.environ.get('SUBSCRIBER_BACKUP_WEBHOOK', '')  # Make.com → Google Sheets

# ── SETUP STATE ───────────────────────────────────────────────────────────────
setup_client: Optional[TelegramClient] = None
setup_phone: str = ''

# ── DATABASE ──────────────────────────────────────────────────────────────────
def db():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    return conn

def init_db():
    with db() as conn:
        with conn.cursor() as c:
            c.execute('''CREATE TABLE IF NOT EXISTS conversations (
                tg_id         TEXT PRIMARY KEY,
                anon_id       TEXT NOT NULL,
                internal_name TEXT DEFAULT '',
                notes         TEXT DEFAULT '',
                last_msg      TEXT DEFAULT '',
                last_time     TEXT DEFAULT '',
                first_time    TEXT DEFAULT '',
                unread        INTEGER DEFAULT 0,
                msg_count     INTEGER DEFAULT 0,
                time_waster   BOOLEAN DEFAULT FALSE,
                tg_username   TEXT DEFAULT '',
                tg_phone      TEXT DEFAULT ''
            )''')
            c.execute('''CREATE TABLE IF NOT EXISTS messages (
                id        SERIAL PRIMARY KEY,
                tg_id     TEXT NOT NULL,
                text      TEXT NOT NULL,
                direction TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                chatter   TEXT DEFAULT ''
            )''')
            c.execute('''CREATE TABLE IF NOT EXISTS sales (
                id        SERIAL PRIMARY KEY,
                tg_id     TEXT NOT NULL,
                anon_id   TEXT NOT NULL,
                amount    REAL NOT NULL,
                product   TEXT DEFAULT '',
                notes     TEXT DEFAULT '',
                chatter   TEXT DEFAULT '',
                timestamp TEXT NOT NULL
            )''')
            c.execute('''CREATE TABLE IF NOT EXISTS lists (
                id    SERIAL PRIMARY KEY,
                name  TEXT NOT NULL,
                color TEXT DEFAULT '#00d4aa'
            )''')
            c.execute('''CREATE TABLE IF NOT EXISTS list_members (
                list_id INTEGER NOT NULL,
                tg_id   TEXT NOT NULL,
                PRIMARY KEY (list_id, tg_id)
            )''')
        # migration: add time_waster if missing
            try:
                c.execute('ALTER TABLE conversations ADD COLUMN IF NOT EXISTS time_waster BOOLEAN DEFAULT FALSE')
                c.execute("ALTER TABLE conversations ADD COLUMN IF NOT EXISTS tg_username TEXT DEFAULT ''")
                c.execute("ALTER TABLE conversations ADD COLUMN IF NOT EXISTS tg_phone TEXT DEFAULT ''")
                conn.commit()
            except Exception:
                conn.rollback()
        conn.commit()

def ensure_conv(tg_id: str, username: str = '', phone: str = '') -> str:
    is_new = False
    anon_id = ''
    with db() as conn:
        with conn.cursor() as c:
            c.execute('SELECT anon_id FROM conversations WHERE tg_id=%s', (tg_id,))
            row = c.fetchone()
            if row:
                # Update username/phone if newly available
                if username or phone:
                    c.execute('UPDATE conversations SET tg_username=COALESCE(NULLIF(tg_username,\'\'),%s), tg_phone=COALESCE(NULLIF(tg_phone,\'\'),%s) WHERE tg_id=%s',
                              (username, phone, tg_id))
                    conn.commit()
                return row['anon_id']
            c.execute('SELECT COUNT(*) as n FROM conversations')
            n = c.fetchone()['n']
            anon_id = f'User #{1001 + n}'
            now = datetime.now().isoformat()
            c.execute(
                'INSERT INTO conversations (tg_id,anon_id,last_msg,last_time,first_time,unread,msg_count,tg_username,tg_phone) VALUES (%s,%s,%s,%s,%s,1,0,%s,%s)',
                (tg_id, anon_id, '', now, now, username, phone)
            )
            is_new = True
        conn.commit()
    # Fire backup webhook for new subscribers
    if is_new and SUBSCRIBER_BACKUP_WEBHOOK:
        try:
            import urllib.request, json as _j
            payload = _j.dumps({
                'tg_id': tg_id,
                'anon_id': anon_id,
                'username': username,
                'phone': phone,
                'first_seen': datetime.now().isoformat()
            }).encode()
            urllib.request.urlopen(
                urllib.request.Request(SUBSCRIBER_BACKUP_WEBHOOK, data=payload,
                                       headers={'Content-Type': 'application/json'}), timeout=5)
        except Exception as e:
            print(f'⚠️  Backup webhook failed: {e}')
    return anon_id

def save_msg(tg_id: str, text: str, direction: str, chatter: str = ''):
    ts = datetime.now().isoformat()
    with db() as conn:
        with conn.cursor() as c:
            c.execute(
                'INSERT INTO messages (tg_id,text,direction,timestamp,chatter) VALUES (%s,%s,%s,%s,%s)',
                (tg_id, text, direction, ts, chatter)
            )
            c.execute(
                '''UPDATE conversations
                   SET last_msg=%s, last_time=%s, unread=unread+%s, msg_count=msg_count+1
                   WHERE tg_id=%s''',
                (text[:100], ts, 1 if direction == 'in' else 0, tg_id)
            )
        conn.commit()

# ── USERBOT ───────────────────────────────────────────────────────────────────
tg_client: Optional[TelegramClient] = None

async def start_userbot():
    global tg_client
    if not (TG_API_ID and TG_API_HASH and TG_SESSION):
        print('⚠️  Userbot nicht gestartet – Env-Variablen fehlen')
        return

    tg_client = TelegramClient(StringSession(TG_SESSION), int(TG_API_ID), TG_API_HASH)

    @tg_client.on(events.NewMessage(incoming=True, func=lambda e: e.is_private))
    async def on_dm(event):
        sender = await event.get_sender()
        if not isinstance(sender, User) or sender.bot:
            return
        tg_id    = str(sender.id)
        username = sender.username or ''
        # phone only visible if in contacts or shared
        phone    = getattr(sender, 'phone', None) or ''
        ensure_conv(tg_id, username=username, phone=phone)
        if event.text:          text = event.text
        elif event.photo:       text = '[📷 Foto]'
        elif event.document:    text = '[📎 Datei]'
        elif event.sticker:     text = '[Sticker]'
        elif event.voice:       text = '[🎤 Sprachnachricht]'
        else:                   text = '[Nachricht]'
        save_msg(tg_id, text, 'in')
        print(f'📨 {tg_id}: {text[:80]}')

    await tg_client.start()
    print('✅ Userbot verbunden!')
    await tg_client.run_until_disconnected()

# ── FASTAPI ───────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    asyncio.create_task(start_userbot())
    yield

app = FastAPI(title='Chatter CRM', lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=['*'], allow_methods=['*'], allow_headers=['*'])

# ── SETUP ─────────────────────────────────────────────────────────────────────
SETUP_HTML = """<!DOCTYPE html><html lang="de"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Chatter CRM – Setup</title><style>*{box-sizing:border-box;margin:0;padding:0}body{font-family:-apple-system,sans-serif;background:#0f0f0f;color:#eee;display:flex;align-items:center;justify-content:center;min-height:100vh;padding:20px}.card{background:#1a1a1a;border:1px solid #333;border-radius:16px;padding:40px;width:100%;max-width:480px}h1{font-size:22px;margin-bottom:6px}.sub{color:#888;font-size:14px;margin-bottom:32px}label{display:block;font-size:13px;color:#aaa;margin-bottom:6px}input{width:100%;background:#111;border:1px solid #333;border-radius:8px;color:#eee;padding:12px 14px;font-size:15px;margin-bottom:18px;outline:none}input:focus{border-color:#6c63ff}button{width:100%;background:#6c63ff;color:#fff;border:none;border-radius:8px;padding:14px;font-size:16px;font-weight:600;cursor:pointer}.result{background:#111;border:1px solid #6c63ff;border-radius:8px;padding:16px;margin-top:24px;word-break:break-all;font-family:monospace;font-size:12px;color:#a0f0a0}.result h3{color:#6c63ff;margin-bottom:10px;font-size:14px}.copy-btn{background:#333;color:#eee;border:none;border-radius:6px;padding:8px 16px;cursor:pointer;font-size:13px;margin-top:12px;width:100%}.error{background:#2a1010;border:1px solid #f55;border-radius:8px;padding:14px;margin-top:16px;color:#f88;font-size:14px}.step{color:#6c63ff;font-size:13px;margin-bottom:20px}.hint{color:#666;font-size:12px;margin-top:-12px;margin-bottom:18px}</style></head><body><div class="card"><h1>🔐 Chatter CRM Setup</h1><p class="sub">Session-String generieren</p>{CONTENT}</div><script>function copySession(){const el=document.getElementById('session-str');navigator.clipboard.writeText(el.innerText).then(()=>{document.getElementById('copy-btn').innerText='✓ Kopiert!';})}</script></body></html>"""

FORM_STEP1 = """<p class="step">Schritt 1 – API-Zugangsdaten</p><form method="POST" action="/setup/send-code"><label>API ID</label><input name="api_id" type="number" required><label>API Hash</label><input name="api_hash" type="text" required><label>Telefonnummer</label><input name="phone" type="text" placeholder="+49151..." required><button type="submit">SMS-Code anfordern →</button></form>"""

def render_setup(content, extra=''):
    return HTMLResponse(SETUP_HTML.replace('{CONTENT}', content + extra))

@app.get('/setup', response_class=HTMLResponse)
async def setup_get():
    return render_setup(FORM_STEP1)

@app.post('/setup/send-code', response_class=HTMLResponse)
async def setup_send_code(api_id: str = Form(...), api_hash: str = Form(...), phone: str = Form(...)):
    global setup_client, setup_phone
    try:
        setup_phone = phone.strip()
        setup_client = TelegramClient(StringSession(), int(api_id.strip()), api_hash.strip())
        await setup_client.connect()
        await setup_client.send_code_request(setup_phone)
        form2 = f"""<p class="step">Schritt 2 – SMS-Code</p><p style="color:#aaa;font-size:14px;margin-bottom:20px;">Code an <strong>{phone}</strong> gesendet.</p><form method="POST" action="/setup/verify"><label>SMS-Code</label><input name="code" type="text" required autofocus><button type="submit">Session generieren ✓</button></form>"""
        return render_setup(form2)
    except Exception as e:
        return render_setup(FORM_STEP1, f'<div class="error">❌ {e}</div>')

@app.post('/setup/verify', response_class=HTMLResponse)
async def setup_verify(code: str = Form(...)):
    global setup_client, setup_phone
    try:
        await setup_client.sign_in(setup_phone, code.strip())
        session_str = setup_client.session.save()
        await setup_client.disconnect()
        result = f"""<div class="result"><h3>✅ TG_SESSION:</h3><div id="session-str">{session_str}</div><button class="copy-btn" id="copy-btn" onclick="copySession()">📋 Kopieren</button></div><p style="color:#888;font-size:13px;margin-top:20px;">Railway → Variables → TG_SESSION einfügen.</p>"""
        return render_setup(result)
    except Exception as e:
        return render_setup(FORM_STEP1, f'<div class="error">❌ {e}</div>')

# ── ENDPOINTS ─────────────────────────────────────────────────────────────────
@app.get('/healthz')
def healthz():
    try:
        with db() as conn:
            with conn.cursor() as c:
                c.execute('SELECT COUNT(*) as n FROM conversations')
                n = c.fetchone()['n']
        ok = tg_client is not None and tg_client.is_connected()
        return {'status': 'ok', 'conversations': n, 'userbot': 'connected' if ok else 'disconnected', 'db': 'postgresql'}
    except Exception as e:
        return {'status': 'error', 'detail': str(e)}

@app.get('/status')
def status():
    ok = tg_client is not None and tg_client.is_connected()
    return {'userbot': 'connected' if ok else 'disconnected'}

@app.get('/conversations')
def get_conversations():
    with db() as conn:
        with conn.cursor() as c:
            c.execute('SELECT tg_id,anon_id,internal_name,notes,last_msg,last_time,first_time,unread,msg_count,time_waster,tg_username,tg_phone FROM conversations ORDER BY last_time DESC')
            rows = c.fetchall()
    return [dict(r) for r in rows]

@app.get('/profile/{tg_id}')
def get_profile(tg_id: str):
    with db() as conn:
        with conn.cursor() as c:
            c.execute('SELECT tg_id,anon_id,internal_name,notes,last_time,first_time,unread,msg_count,time_waster,tg_username,tg_phone FROM conversations WHERE tg_id=%s', (tg_id,))
            row = c.fetchone()
    if not row:
        raise HTTPException(404, 'Not found')
    return dict(row)

class ProfileUpdate(BaseModel):
    internal_name: Optional[str] = None
    notes: Optional[str] = None
    time_waster: Optional[bool] = None

@app.patch('/profile/{tg_id}')
def update_profile(tg_id: str, body: ProfileUpdate):
    with db() as conn:
        with conn.cursor() as c:
            if body.internal_name is not None:
                c.execute('UPDATE conversations SET internal_name=%s WHERE tg_id=%s', (body.internal_name, tg_id))
            if body.notes is not None:
                c.execute('UPDATE conversations SET notes=%s WHERE tg_id=%s', (body.notes, tg_id))
            if body.time_waster is not None:
                c.execute('UPDATE conversations SET time_waster=%s WHERE tg_id=%s', (body.time_waster, tg_id))
        conn.commit()
    return {'ok': True}

@app.get('/messages/{tg_id}')
def get_messages(tg_id: str):
    with db() as conn:
        with conn.cursor() as c:
            c.execute('UPDATE conversations SET unread=0 WHERE tg_id=%s', (tg_id,))
            c.execute('SELECT text,direction,timestamp,chatter FROM messages WHERE tg_id=%s ORDER BY timestamp', (tg_id,))
            rows = c.fetchall()
        conn.commit()
    return [dict(r) for r in rows]

class ReplyIn(BaseModel):
    tg_id: str
    text: str
    chatter: str = 'Chatter'

@app.post('/reply')
async def post_reply(body: ReplyIn):
    if not tg_client or not tg_client.is_connected():
        raise HTTPException(503, 'Userbot nicht verbunden')
    try:
        await tg_client.send_message(int(body.tg_id), body.text)
        save_msg(body.tg_id, body.text, 'out', body.chatter)
        return {'ok': True}
    except Exception as e:
        raise HTTPException(500, str(e))

class SaleIn(BaseModel):
    tg_id: str
    anon_id: str
    amount: float
    product: str = ''
    notes: str = ''
    chatter: str = 'Chatter'

@app.post('/sale')
async def post_sale(body: SaleIn):
    ts = datetime.now().isoformat()
    with db() as conn:
        with conn.cursor() as c:
            c.execute(
                'INSERT INTO sales (tg_id,anon_id,amount,product,notes,chatter,timestamp) VALUES (%s,%s,%s,%s,%s,%s,%s)',
                (body.tg_id, body.anon_id, body.amount, body.product, body.notes, body.chatter, ts)
            )
        conn.commit()
    make_url = os.environ.get('MAKE_SALE_WEBHOOK', '')
    if make_url:
        try:
            import urllib.request, json as _j
            data = _j.dumps({'tg_id': body.tg_id, 'anon_id': body.anon_id, 'amount': body.amount, 'product': body.product, 'notes': body.notes, 'chatter': body.chatter, 'timestamp': ts}).encode()
            urllib.request.urlopen(urllib.request.Request(make_url, data=data, headers={'Content-Type': 'application/json'}), timeout=5)
        except Exception:
            pass
    return {'ok': True, 'timestamp': ts}

@app.get('/sales')
def get_sales(limit: int = 200):
    with db() as conn:
        with conn.cursor() as c:
            c.execute('SELECT id,tg_id,anon_id,amount,product,notes,chatter,timestamp FROM sales ORDER BY timestamp DESC LIMIT %s', (limit,))
            rows = c.fetchall()
    return [dict(r) for r in rows]


# ── LISTS ────────────────────────────────────────────────────────────────────
class ListCreate(BaseModel):
    name: str
    color: str = '#00d4aa'

@app.get('/lists')
def get_lists():
    with db() as conn:
        with conn.cursor() as c:
            c.execute('SELECT id,name,color FROM lists ORDER BY id')
            lists = [dict(r) for r in c.fetchall()]
            for lst in lists:
                c.execute('SELECT tg_id FROM list_members WHERE list_id=%s', (lst['id'],))
                lst['members'] = [r['tg_id'] for r in c.fetchall()]
    return lists

@app.post('/lists')
def create_list(body: ListCreate):
    with db() as conn:
        with conn.cursor() as c:
            c.execute('INSERT INTO lists (name,color) VALUES (%s,%s) RETURNING id', (body.name, body.color))
            lid = c.fetchone()['id']
        conn.commit()
    return {'ok': True, 'id': lid}

@app.delete('/lists/{list_id}')
def delete_list(list_id: int):
    with db() as conn:
        with conn.cursor() as c:
            c.execute('DELETE FROM list_members WHERE list_id=%s', (list_id,))
            c.execute('DELETE FROM lists WHERE id=%s', (list_id,))
        conn.commit()
    return {'ok': True}

@app.post('/lists/{list_id}/members/{tg_id}')
def add_to_list(list_id: int, tg_id: str):
    with db() as conn:
        with conn.cursor() as c:
            c.execute('INSERT INTO list_members (list_id,tg_id) VALUES (%s,%s) ON CONFLICT DO NOTHING', (list_id, tg_id))
        conn.commit()
    return {'ok': True}

@app.delete('/lists/{list_id}/members/{tg_id}')
def remove_from_list(list_id: int, tg_id: str):
    with db() as conn:
        with conn.cursor() as c:
            c.execute('DELETE FROM list_members WHERE list_id=%s AND tg_id=%s', (list_id, tg_id))
        conn.commit()
    return {'ok': True}


# ── SUBSCRIBER BACKUP ────────────────────────────────────────────────────────
from fastapi.responses import StreamingResponse
import csv, io

@app.get('/subscribers')
def get_subscribers():
    """All subscribers with contact info — for backup/export"""
    with db() as conn:
        with conn.cursor() as c:
            c.execute('''SELECT tg_id, anon_id, tg_username, tg_phone,
                                internal_name, first_time, last_time, msg_count,
                                time_waster
                         FROM conversations ORDER BY first_time ASC''')
            rows = [dict(r) for r in c.fetchall()]
    return rows

@app.get('/export/subscribers')
def export_subscribers_csv():
    """Download all subscribers as CSV"""
    with db() as conn:
        with conn.cursor() as c:
            c.execute('''SELECT tg_id, anon_id, tg_username, tg_phone,
                                internal_name, first_time, last_time, msg_count
                         FROM conversations ORDER BY first_time ASC''')
            rows = c.fetchall()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['TG ID','Anon ID','Username','Telefon','Interner Name','Erster Kontakt','Letzter Kontakt','Nachrichten'])
    for r in rows:
        username_fmt = f"@{r['tg_username']}" if r['tg_username'] else '—'
        phone_fmt = f"+{r['tg_phone']}" if r['tg_phone'] else '—'
        writer.writerow([r['tg_id'], r['anon_id'], username_fmt, phone_fmt,
                         r['internal_name'] or '—', r['first_time'] or '—',
                         r['last_time'] or '—', r['msg_count'] or 0])
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type='text/csv',
        headers={'Content-Disposition': 'attachment; filename="subscriber_backup.csv"'}
    )

# ── START ─────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=PORT)
