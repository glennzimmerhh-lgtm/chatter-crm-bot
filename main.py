"""
Chatter CRM – Subscriber Chat Backend
Telethon Userbot + FastAPI REST API — PostgreSQL edition (production-ready)
"""
from __future__ import annotations

import asyncio
import os
import csv
import io
from datetime import datetime
from contextlib import asynccontextmanager, contextmanager
from typing import Optional

import uvicorn
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Form
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel

from telethon import TelegramClient, events, functions, types
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession
from telethon.tl.types import (User, InputPeerUser, UpdateReadHistoryOutbox, PeerUser,
                               UpdateUserStatus, UserStatusOnline, UserStatusOffline,
                               UserStatusRecently, UserStatusLastWeek, UserStatusLastMonth)

# ── CONFIG ────────────────────────────────────────────────────────────────────
TG_API_ID   = os.environ.get('TG_API_ID', '')
TG_API_HASH = os.environ.get('TG_API_HASH', '')
TG_SESSION  = os.environ.get('TG_SESSION', '')
PORT        = int(os.environ.get('PORT', 8000))
DATABASE_URL = os.environ.get('DATABASE_URL', '')
SUBSCRIBER_BACKUP_WEBHOOK = os.environ.get('SUBSCRIBER_BACKUP_WEBHOOK', '')

# ── DEBUG: zeige ob DATABASE_URL gesetzt ist ─────────────────────────────────
_db_preview = DATABASE_URL[:40] + '...' if len(DATABASE_URL) > 40 else DATABASE_URL
print(f'🔍 DATABASE_URL (erste 40 Zeichen): [{_db_preview}]  (leer={not DATABASE_URL})')

# ── SETUP STATE ───────────────────────────────────────────────────────────────
setup_client: Optional[TelegramClient] = None
setup_phone: str = ''

# ── DATABASE ──────────────────────────────────────────────────────────────────
@contextmanager
def db():
    """Open a fresh connection per request — commit on success, rollback+close on error."""
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

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
            # safe migrations + indexes for performance
            for stmt in [
                "ALTER TABLE conversations ADD COLUMN IF NOT EXISTS time_waster BOOLEAN DEFAULT FALSE",
                "ALTER TABLE conversations ADD COLUMN IF NOT EXISTS tg_username TEXT DEFAULT ''",
                "ALTER TABLE conversations ADD COLUMN IF NOT EXISTS tg_phone TEXT DEFAULT ''",
                "ALTER TABLE conversations ADD COLUMN IF NOT EXISTS tg_access_hash TEXT DEFAULT ''",
                "ALTER TABLE conversations ADD COLUMN IF NOT EXISTS followup_at TEXT DEFAULT NULL",
                "ALTER TABLE conversations ADD COLUMN IF NOT EXISTS is_online BOOLEAN DEFAULT FALSE",
                "ALTER TABLE conversations ADD COLUMN IF NOT EXISTS last_seen TEXT DEFAULT NULL",
                "ALTER TABLE conversations ADD COLUMN IF NOT EXISTS funnel_stage TEXT DEFAULT 'kalt'",
                "ALTER TABLE conversations ADD COLUMN IF NOT EXISTS call_followup_at TEXT DEFAULT NULL",
                "ALTER TABLE conversations ADD COLUMN IF NOT EXISTS call_followup_note TEXT DEFAULT ''",
                "ALTER TABLE messages ADD COLUMN IF NOT EXISTS tg_msg_id INTEGER DEFAULT 0",
                "ALTER TABLE messages ADD COLUMN IF NOT EXISTS is_read BOOLEAN DEFAULT FALSE",
                "ALTER TABLE messages ADD COLUMN IF NOT EXISTS read_at TEXT DEFAULT ''",
                "CREATE INDEX IF NOT EXISTS idx_messages_tg_id ON messages(tg_id)",
                "CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_conv_last_time ON conversations(last_time DESC)",
            ]:
                try:
                    c.execute(stmt)
                except Exception as e:
                    print(f'Migration skip: {e}')
                    conn.rollback()
    print('✅ DB initialized')

def _fire_webhook_sync(url: str, payload: dict):
    """Non-blocking fire-and-forget webhook via background thread."""
    import threading, urllib.request, json as _j
    def _send():
        try:
            data = _j.dumps(payload).encode()
            req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'})
            urllib.request.urlopen(req, timeout=8)
        except Exception as e:
            print(f'⚠️  Webhook failed: {e}')
    threading.Thread(target=_send, daemon=True).start()

def ensure_conv(tg_id: str, username: str = '', phone: str = '', access_hash: str = '') -> str:
    is_new = False
    anon_id = ''
    with db() as conn:
        with conn.cursor() as c:
            c.execute('SELECT anon_id FROM conversations WHERE tg_id=%s', (tg_id,))
            row = c.fetchone()
            if row:
                c.execute(
                    "UPDATE conversations SET tg_username=COALESCE(NULLIF(tg_username,''),%s), tg_phone=COALESCE(NULLIF(tg_phone,''),%s), tg_access_hash=COALESCE(NULLIF(tg_access_hash,''),%s) WHERE tg_id=%s",
                    (username, phone, access_hash, tg_id)
                )
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
    if is_new and SUBSCRIBER_BACKUP_WEBHOOK:
        _fire_webhook_sync(SUBSCRIBER_BACKUP_WEBHOOK, {
            'tg_id': tg_id, 'anon_id': anon_id,
            'username': username, 'phone': phone,
            'first_seen': datetime.now().isoformat()
        })
    return anon_id

def save_msg(tg_id: str, text: str, direction: str, chatter: str = '', tg_msg_id: int = 0):
    ts = datetime.now().isoformat()
    with db() as conn:
        with conn.cursor() as c:
            c.execute(
                'INSERT INTO messages (tg_id,text,direction,timestamp,chatter,tg_msg_id) VALUES (%s,%s,%s,%s,%s,%s)',
                (tg_id, text, direction, ts, chatter, tg_msg_id)
            )
            if direction == 'in':
                # Incoming message clears follow-up timer
                c.execute(
                    'UPDATE conversations SET last_msg=%s, last_time=%s, unread=unread+1, msg_count=msg_count+1, followup_at=NULL WHERE tg_id=%s',
                    (text[:100], ts, tg_id)
                )
            else:
                c.execute(
                    'UPDATE conversations SET last_msg=%s, last_time=%s, msg_count=msg_count+1 WHERE tg_id=%s',
                    (text[:100], ts, tg_id)
                )

def update_online_status(tg_id: str, is_online: bool):
    """Update subscriber online status — only for known conversations."""
    with db() as conn:
        with conn.cursor() as c:
            c.execute('SELECT 1 FROM conversations WHERE tg_id=%s', (tg_id,))
            if c.fetchone():
                now = datetime.now().isoformat()
                c.execute(
                    'UPDATE conversations SET is_online=%s, last_seen=%s WHERE tg_id=%s',
                    (is_online, now if is_online else None, tg_id)
                )

def mark_read(tg_id: str, max_msg_id: int):
    """Mark outgoing messages as read and start follow-up timer."""
    now = datetime.now().isoformat()
    with db() as conn:
        with conn.cursor() as c:
            c.execute(
                "UPDATE messages SET is_read=TRUE, read_at=%s WHERE tg_id=%s AND direction='out' AND tg_msg_id>0 AND tg_msg_id<=%s AND is_read=FALSE",
                (now, tg_id, max_msg_id)
            )
            updated = c.rowcount
            if updated > 0:
                # Start follow-up timer only if not already set
                c.execute(
                    "UPDATE conversations SET followup_at=%s WHERE tg_id=%s AND followup_at IS NULL",
                    (now, tg_id)
                )
                print(f'✓✓ {tg_id}: gelesen, Follow-up Timer gestartet')

# ── USERBOT WITH AUTO-RECONNECT ───────────────────────────────────────────────
tg_client: Optional[TelegramClient] = None
_userbot_running = False

async def start_userbot():
    global tg_client, _userbot_running
    if not (TG_API_ID and TG_API_HASH and TG_SESSION):
        print('⚠️  Userbot nicht gestartet – Env-Variablen fehlen')
        return

    _userbot_running = True
    retry_delay = 5

    while _userbot_running:
        try:
            tg_client = TelegramClient(
                StringSession(TG_SESSION), int(TG_API_ID), TG_API_HASH,
                connection_retries=5, retry_delay=3, auto_reconnect=True
            )

            @tg_client.on(events.Raw(UpdateUserStatus))
            async def on_user_status(event):
                """Track subscriber online/offline status."""
                tg_id = str(event.user_id)
                is_online = isinstance(event.status, UserStatusOnline)
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, lambda: update_online_status(tg_id, is_online))

            @tg_client.on(events.Raw(UpdateReadHistoryOutbox))
            async def on_outbox_read(event):
                """Fires when subscriber reads our outgoing messages."""
                if isinstance(event.peer, PeerUser):
                    tg_id = str(event.peer.user_id)
                    max_id = event.max_id
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, lambda: mark_read(tg_id, max_id))

            @tg_client.on(events.NewMessage(incoming=True, func=lambda e: e.is_private))
            async def on_dm(event):
                sender = await event.get_sender()
                if not isinstance(sender, User) or sender.bot:
                    return
                tg_id    = str(sender.id)
                username = sender.username or ''
                phone    = getattr(sender, 'phone', None) or ''
                access_hash = str(sender.access_hash) if sender.access_hash else ''
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, lambda: ensure_conv(tg_id, username=username, phone=phone, access_hash=access_hash))
                if event.text:          text = event.text
                elif event.photo:       text = '[📷 Foto]'
                elif event.document:    text = '[📎 Datei]'
                elif event.sticker:     text = '[Sticker]'
                elif event.voice:       text = '[🎤 Sprachnachricht]'
                else:                   text = '[Nachricht]'
                await loop.run_in_executor(None, lambda: save_msg(tg_id, text, 'in'))
                print(f'📨 {tg_id}: {text[:80]}')

            await tg_client.start()
            print('✅ Userbot verbunden!')
            retry_delay = 5  # reset on success
            await tg_client.run_until_disconnected()
            print('⚠️  Userbot getrennt – reconnecting...')

        except Exception as e:
            print(f'❌ Userbot Fehler: {e} — retry in {retry_delay}s')

        if _userbot_running:
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)  # exponential backoff max 60s

# ── FASTAPI ───────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    asyncio.create_task(start_userbot())
    yield
    global _userbot_running
    _userbot_running = False
    if tg_client:
        await tg_client.disconnect()

app = FastAPI(title='Chatter CRM', lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=False,
    allow_methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'],
    allow_headers=['*'],
    expose_headers=['*'],
    max_age=86400,
)

# ── SETUP ─────────────────────────────────────────────────────────────────────
SETUP_HTML = """<!DOCTYPE html><html lang="de"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Chatter CRM – Setup</title><style>*{box-sizing:border-box;margin:0;padding:0}body{font-family:-apple-system,sans-serif;background:#0f0f0f;color:#eee;display:flex;align-items:center;justify-content:center;min-height:100vh;padding:20px}.card{background:#1a1a1a;border:1px solid #333;border-radius:16px;padding:40px;width:100%;max-width:480px}h1{font-size:22px;margin-bottom:6px}.sub{color:#888;font-size:14px;margin-bottom:32px}label{display:block;font-size:13px;color:#aaa;margin-bottom:6px}input{width:100%;background:#111;border:1px solid #333;border-radius:8px;color:#eee;padding:12px 14px;font-size:15px;margin-bottom:18px;outline:none}input:focus{border-color:#6c63ff}button{width:100%;background:#6c63ff;color:#fff;border:none;border-radius:8px;padding:14px;font-size:16px;font-weight:600;cursor:pointer}.result{background:#111;border:1px solid #6c63ff;border-radius:8px;padding:16px;margin-top:24px;word-break:break-all;font-family:monospace;font-size:12px;color:#a0f0a0}.result h3{color:#6c63ff;margin-bottom:10px;font-size:14px}.copy-btn{background:#333;color:#eee;border:none;border-radius:6px;padding:8px 16px;cursor:pointer;font-size:13px;margin-top:12px;width:100%}.error{background:#2a1010;border:1px solid #f55;border-radius:8px;padding:14px;margin-top:16px;color:#f88;font-size:14px}.step{color:#6c63ff;font-size:13px;margin-bottom:20px}</style></head><body><div class="card"><h1>🔐 Chatter CRM Setup</h1><p class="sub">Session-String generieren</p>{CONTENT}</div><script>function copySession(){const el=document.getElementById('session-str');navigator.clipboard.writeText(el.innerText).then(()=>{document.getElementById('copy-btn').innerText='✓ Kopiert!';})}</script></body></html>"""
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

# ── HEALTH ─────────────────────────────────────────────────────────────────────
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

# ── CONVERSATIONS ─────────────────────────────────────────────────────────────
@app.get('/conversations')
def get_conversations():
    with db() as conn:
        with conn.cursor() as c:
            c.execute('SELECT tg_id,anon_id,internal_name,notes,last_msg,last_time,first_time,unread,msg_count,time_waster,tg_username,tg_phone,followup_at,funnel_stage,call_followup_at,call_followup_note,is_online,last_seen FROM conversations ORDER BY is_online DESC, last_time DESC')
            rows = c.fetchall()
    return [dict(r) for r in rows]

@app.get('/online')
def get_online():
    with db() as conn:
        with conn.cursor() as c:
            c.execute('''SELECT tg_id,anon_id,internal_name,last_seen,
                         COALESCE(SUM(s.amount),0) as total_spent
                         FROM conversations c
                         LEFT JOIN sales s USING(tg_id)
                         WHERE c.is_online=TRUE
                         GROUP BY c.tg_id,c.anon_id,c.internal_name,c.last_seen
                         ORDER BY c.last_seen DESC''')
            rows = c.fetchall()
    return [dict(r) for r in rows]

@app.get('/profile/{tg_id}')
def get_profile(tg_id: str):
    with db() as conn:
        with conn.cursor() as c:
            c.execute('SELECT tg_id,anon_id,internal_name,notes,last_time,first_time,unread,msg_count,time_waster,tg_username,tg_phone,funnel_stage,call_followup_at,call_followup_note FROM conversations WHERE tg_id=%s', (tg_id,))
            row = c.fetchone()
    if not row:
        raise HTTPException(404, 'Not found')
    return dict(row)

class ProfileUpdate(BaseModel):
    internal_name: Optional[str] = None
    notes: Optional[str] = None
    time_waster: Optional[bool] = None
    funnel_stage: Optional[str] = None
    call_followup_at: Optional[str] = None
    call_followup_note: Optional[str] = None

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
            if body.funnel_stage is not None:
                c.execute('UPDATE conversations SET funnel_stage=%s WHERE tg_id=%s', (body.funnel_stage, tg_id))
            if body.call_followup_at is not None:
                c.execute('UPDATE conversations SET call_followup_at=%s, call_followup_note=%s WHERE tg_id=%s',
                          (body.call_followup_at or None, body.call_followup_note or '', tg_id))
    return {'ok': True}

# ── ANALYTICS ─────────────────────────────────────────────────────────────────
@app.get('/analytics/chatters')
def get_chatter_analytics():
    with db() as conn:
        with conn.cursor() as c:
            # Sales per chatter
            c.execute('''
                SELECT chatter,
                       COUNT(*) as sales_count,
                       COALESCE(SUM(amount),0) as total_revenue,
                       COALESCE(AVG(amount),0) as avg_sale
                FROM sales
                WHERE chatter != ''
                GROUP BY chatter
                ORDER BY total_revenue DESC
            ''')
            sales_rows = {r['chatter']: dict(r) for r in c.fetchall()}
            # Messages sent per chatter
            c.execute('''
                SELECT chatter, COUNT(*) as msgs_sent
                FROM messages
                WHERE direction='out' AND chatter != ''
                GROUP BY chatter
            ''')
            msg_rows = {r['chatter']: r['msgs_sent'] for r in c.fetchall()}
    chatters = {}
    for name, s in sales_rows.items():
        chatters[name] = {**s, 'msgs_sent': msg_rows.get(name, 0)}
    for name, msgs in msg_rows.items():
        if name not in chatters:
            chatters[name] = {'chatter': name, 'sales_count': 0, 'total_revenue': 0, 'avg_sale': 0, 'msgs_sent': msgs}
    result = list(chatters.values())
    for r in result:
        r['revenue_per_msg'] = round(r['total_revenue'] / r['msgs_sent'], 2) if r['msgs_sent'] > 0 else 0
    return sorted(result, key=lambda x: x['total_revenue'], reverse=True)

@app.get('/messages/{tg_id}')
async def get_messages(tg_id: str):
    with db() as conn:
        with conn.cursor() as c:
            c.execute('UPDATE conversations SET unread=0 WHERE tg_id=%s', (tg_id,))
            c.execute('SELECT text,direction,timestamp,chatter,is_read,read_at,tg_access_hash FROM conversations WHERE tg_id=%s', (tg_id,))
            conv = c.fetchone()
            c.execute('SELECT text,direction,timestamp,chatter,is_read,read_at FROM messages WHERE tg_id=%s ORDER BY timestamp', (tg_id,))
            rows = c.fetchall()
    # Mark as read in Telegram so subscriber sees double-checkmark
    if tg_client and tg_client.is_connected():
        try:
            ah = int(conv['tg_access_hash']) if conv and conv['tg_access_hash'] else 0
            peer = InputPeerUser(int(tg_id), ah) if ah else int(tg_id)
            await tg_client(functions.messages.ReadHistoryRequest(peer=peer, max_id=0))
        except Exception as e:
            print(f'ReadHistory skip: {e}')
    return [dict(r) for r in rows]

@app.post('/typing/{tg_id}')
async def send_typing(tg_id: str):
    """Send typing indicator to subscriber so they see '...' in Telegram."""
    if not tg_client or not tg_client.is_connected():
        return {'ok': False}
    try:
        with db() as conn:
            with conn.cursor() as c:
                c.execute('SELECT tg_access_hash FROM conversations WHERE tg_id=%s', (tg_id,))
                row = c.fetchone()
        ah = int(row['tg_access_hash']) if row and row['tg_access_hash'] else 0
        peer = InputPeerUser(int(tg_id), ah) if ah else int(tg_id)
        await tg_client(functions.messages.SetTypingRequest(
            peer=peer, action=types.SendMessageTypingAction()
        ))
        return {'ok': True}
    except Exception as e:
        return {'ok': False, 'detail': str(e)}

# ── REPLY ─────────────────────────────────────────────────────────────────────
class ReplyIn(BaseModel):
    tg_id: str
    text: str
    chatter: str = 'Chatter'

@app.post('/reply')
async def post_reply(body: ReplyIn):
    if not tg_client or not tg_client.is_connected():
        raise HTTPException(503, 'Userbot nicht verbunden')
    try:
        # Look up access_hash from DB for reliable entity resolution
        access_hash = 0
        with db() as conn:
            with conn.cursor() as c:
                c.execute('SELECT tg_access_hash FROM conversations WHERE tg_id=%s', (body.tg_id,))
                row = c.fetchone()
                if row and row['tg_access_hash']:
                    access_hash = int(row['tg_access_hash'])
        if access_hash:
            peer = InputPeerUser(int(body.tg_id), access_hash)
        else:
            peer = int(body.tg_id)
        sent_msg = await tg_client.send_message(peer, body.text)
        tg_msg_id = sent_msg.id
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: save_msg(body.tg_id, body.text, 'out', body.chatter, tg_msg_id))
        return {'ok': True}
    except FloodWaitError as e:
        raise HTTPException(429, f'Telegram Flood Wait: {e.seconds}s warten')
    except Exception as e:
        raise HTTPException(500, str(e))

# ── BROADCAST ────────────────────────────────────────────────────────────────
class BroadcastIn(BaseModel):
    text: str
    chatter: str = 'Broadcast'
    filter_stage: Optional[str] = None   # 'kalt','warm','hot','angebot','gebucht','done' or None=all
    filter_min_spend: Optional[float] = None
    tg_ids: Optional[list] = None        # explicit list overrides filters

@app.post('/broadcast')
async def post_broadcast(body: BroadcastIn):
    if not tg_client or not tg_client.is_connected():
        raise HTTPException(503, 'Userbot nicht verbunden')
    if not body.text.strip():
        raise HTTPException(400, 'Text darf nicht leer sein')

    # Build recipient list
    with db() as conn:
        with conn.cursor() as c:
            if body.tg_ids:
                fmt = ','.join(['%s'] * len(body.tg_ids))
                c.execute(f'SELECT tg_id,tg_access_hash FROM conversations WHERE tg_id IN ({fmt})', body.tg_ids)
            elif body.filter_stage:
                c.execute('SELECT tg_id,tg_access_hash FROM conversations WHERE funnel_stage=%s', (body.filter_stage,))
            else:
                c.execute('SELECT tg_id,tg_access_hash FROM conversations')
            recipients = c.fetchall()

    sent_ok, sent_fail = 0, 0
    for r in recipients:
        try:
            ah = int(r['tg_access_hash']) if r['tg_access_hash'] else 0
            peer = InputPeerUser(int(r['tg_id']), ah) if ah else int(r['tg_id'])
            sent_msg = await tg_client.send_message(peer, body.text)
            save_msg(r['tg_id'], body.text, 'out', body.chatter, sent_msg.id)
            sent_ok += 1
            await asyncio.sleep(1.2)   # ~50 msg/min — safe for Telegram
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds + 5)
        except Exception as ex:
            print(f'Broadcast skip {r["tg_id"]}: {ex}')
            sent_fail += 1

    return {'ok': True, 'sent': sent_ok, 'failed': sent_fail, 'total': len(recipients)}

# ── SALES ─────────────────────────────────────────────────────────────────────
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
    make_url = os.environ.get('MAKE_SALE_WEBHOOK', '')
    if make_url:
        _fire_webhook_sync(make_url, {
            'tg_id': body.tg_id, 'anon_id': body.anon_id,
            'amount': body.amount, 'product': body.product,
            'notes': body.notes, 'chatter': body.chatter, 'timestamp': ts
        })
    return {'ok': True, 'timestamp': ts}

@app.get('/sales')
def get_sales(limit: int = 200):
    with db() as conn:
        with conn.cursor() as c:
            c.execute('SELECT id,tg_id,anon_id,amount,product,notes,chatter,timestamp FROM sales ORDER BY timestamp DESC LIMIT %s', (limit,))
            rows = c.fetchall()
    return [dict(r) for r in rows]

# ── LISTS ─────────────────────────────────────────────────────────────────────
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
    return {'ok': True, 'id': lid}

@app.delete('/lists/{list_id}')
def delete_list(list_id: int):
    with db() as conn:
        with conn.cursor() as c:
            c.execute('DELETE FROM list_members WHERE list_id=%s', (list_id,))
            c.execute('DELETE FROM lists WHERE id=%s', (list_id,))
    return {'ok': True}

@app.post('/lists/{list_id}/members/{tg_id}')
def add_to_list(list_id: int, tg_id: str):
    with db() as conn:
        with conn.cursor() as c:
            c.execute('INSERT INTO list_members (list_id,tg_id) VALUES (%s,%s) ON CONFLICT DO NOTHING', (list_id, tg_id))
    return {'ok': True}

@app.delete('/lists/{list_id}/members/{tg_id}')
def remove_from_list(list_id: int, tg_id: str):
    with db() as conn:
        with conn.cursor() as c:
            c.execute('DELETE FROM list_members WHERE list_id=%s AND tg_id=%s', (list_id, tg_id))
    return {'ok': True}

# ── SUBSCRIBERS ───────────────────────────────────────────────────────────────
@app.get('/subscribers')
def get_subscribers():
    with db() as conn:
        with conn.cursor() as c:
            c.execute('''SELECT tg_id, anon_id, tg_username, tg_phone,
                                internal_name, first_time, last_time, msg_count, time_waster
                         FROM conversations ORDER BY first_time ASC''')
            rows = c.fetchall()
    return [dict(r) for r in rows]

@app.get('/export/subscribers')
def export_subscribers_csv():
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
        writer.writerow([
            r['tg_id'], r['anon_id'],
            f"@{r['tg_username']}" if r['tg_username'] else '—',
            f"+{r['tg_phone']}" if r['tg_phone'] else '—',
            r['internal_name'] or '—', r['first_time'] or '—',
            r['last_time'] or '—', r['msg_count'] or 0
        ])
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type='text/csv',
        headers={'Content-Disposition': 'attachment; filename="subscriber_backup.csv"'}
    )

# ── START ─────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=PORT)
