"""
Chatter CRM – Subscriber Chat Backend
Telethon Userbot + FastAPI REST API — PostgreSQL edition (production-ready)
"""
from __future__ import annotations

import asyncio
import os
import csv
import io
import shutil
from datetime import datetime, timedelta
from contextlib import asynccontextmanager, contextmanager
from typing import Optional

# Vault storage directory (Railway volume or local)
VAULT_DIR = os.environ.get('VAULT_PATH', '/data/vault')
os.makedirs(VAULT_DIR, exist_ok=True)

import uvicorn
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Form, BackgroundTasks, WebSocket, WebSocketDisconnect, UploadFile, File
from fastapi.responses import JSONResponse, FileResponse
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
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY', '')
SUBSCRIBER_BACKUP_WEBHOOK = os.environ.get('SUBSCRIBER_BACKUP_WEBHOOK', '')

# ── WEBSOCKET MANAGER ────────────────────────────────────────────────────────
class WSManager:
    def __init__(self):
        self._connections: set = set()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self._connections.add(ws)
        print(f'🔌 WS connected — total: {len(self._connections)}')

    def disconnect(self, ws: WebSocket):
        self._connections.discard(ws)
        print(f'🔌 WS disconnected — total: {len(self._connections)}')

    async def broadcast(self, data: dict):
        dead = set()
        for ws in list(self._connections):
            try:
                await ws.send_json(data)
            except Exception:
                dead.add(ws)
        self._connections -= dead

ws_manager = WSManager()

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
            # Users table
            c.execute('''CREATE TABLE IF NOT EXISTS crm_users (
                id            SERIAL PRIMARY KEY,
                username      TEXT NOT NULL UNIQUE,
                email         TEXT DEFAULT '',
                password_hash TEXT NOT NULL,
                role          TEXT DEFAULT 'chatter'
            )''')
            # Add display_name column if missing
            c.execute("ALTER TABLE crm_users ADD COLUMN IF NOT EXISTS display_name TEXT DEFAULT ''")
            # Seed default admin if no users exist
            c.execute('SELECT COUNT(*) as n FROM crm_users')
            if c.fetchone()['n'] == 0:
                import hashlib
                c.execute(
                    "INSERT INTO crm_users (username,email,password_hash,role,display_name) VALUES (%s,%s,%s,%s,%s)",
                    ('Glenn', 'glennzimmerhh@gmail.com', hashlib.sha256('Smartviral1!'.encode()).hexdigest(), 'admin', 'Glenn')
                )

            # Settings table
            c.execute('''CREATE TABLE IF NOT EXISTS crm_settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
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
                "ALTER TABLE conversations ADD COLUMN IF NOT EXISTS last_auto_msg_at TEXT DEFAULT NULL",
                "ALTER TABLE messages ADD COLUMN IF NOT EXISTS tg_msg_id INTEGER DEFAULT 0",
                "ALTER TABLE messages ADD COLUMN IF NOT EXISTS is_read BOOLEAN DEFAULT FALSE",
                "ALTER TABLE messages ADD COLUMN IF NOT EXISTS read_at TEXT DEFAULT ''",
                "ALTER TABLE messages ADD COLUMN IF NOT EXISTS translation TEXT DEFAULT ''",
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
    import threading
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
                # Auto-translate in background thread
                threading.Thread(target=_auto_translate_message, args=(tg_id, text), daemon=True).start()
            else:
                c.execute(
                    'UPDATE conversations SET last_msg=%s, last_time=%s, msg_count=msg_count+1 WHERE tg_id=%s',
                    (text[:100], ts, tg_id)
                )

def _auto_translate_message(tg_id: str, text: str):
    """Translate incoming message to English and store in DB (background thread)."""
    if not OPENAI_API_KEY or not text.strip():
        return
    try:
        import urllib.request as _r, json as _j
        payload = _j.dumps({
            'model': 'gpt-4o-mini',
            'messages': [
                {'role': 'system', 'content': 'Translate the following message to English. Return ONLY the translation.'},
                {'role': 'user', 'content': text}
            ],
            'max_tokens': 200, 'temperature': 0.3
        }).encode()
        req = _r.Request('https://api.openai.com/v1/chat/completions', data=payload,
                         headers={'Content-Type': 'application/json', 'Authorization': f'Bearer {OPENAI_API_KEY}'})
        with _r.urlopen(req, timeout=12) as resp:
            data = _j.loads(resp.read())
        translation = data['choices'][0]['message']['content'].strip()
        with db() as conn:
            with conn.cursor() as c:
                c.execute(
                    "UPDATE messages SET translation=%s WHERE id=(SELECT id FROM messages WHERE tg_id=%s AND text=%s AND direction='in' AND translation='' ORDER BY id DESC LIMIT 1)",
                    (translation, tg_id, text)
                )
    except Exception as e:
        print(f'Auto-translate error: {e}')

def _send_auto_online_msg(tg_id: str):
    """Fire auto-message to subscriber who just came online (run in thread)."""
    import threading
    def _do():
        try:
            enabled = get_setting('auto_online_enabled', '0')
            if enabled != '1':
                return
            text = get_setting('auto_online_text', '').strip()
            if not text:
                return
            cooldown_h = int(get_setting('auto_online_cooldown_h', '24') or 24)
            allowed_stages = get_setting('auto_online_stages', '')  # comma-sep or empty=all

            with db() as conn:
                with conn.cursor() as c:
                    c.execute('SELECT tg_access_hash, funnel_stage, last_auto_msg_at FROM conversations WHERE tg_id=%s', (tg_id,))
                    row = c.fetchone()
            if not row:
                return
            # Stage filter
            if allowed_stages:
                stages = [s.strip() for s in allowed_stages.split(',')]
                if row['funnel_stage'] not in stages:
                    return
            # Cooldown check
            if row['last_auto_msg_at']:
                from datetime import timezone
                last = datetime.fromisoformat(row['last_auto_msg_at'])
                diff_h = (datetime.now() - last).total_seconds() / 3600
                if diff_h < cooldown_h:
                    return
            # Send message via Telethon (must run in event loop)
            import asyncio
            async def _send():
                if not tg_client or not tg_client.is_connected():
                    return
                ah = int(row['tg_access_hash']) if row['tg_access_hash'] else 0
                peer = InputPeerUser(int(tg_id), ah) if ah else int(tg_id)
                sent = await tg_client.send_message(peer, text)
                now = datetime.now().isoformat()
                save_msg(tg_id, text, 'out', 'Auto', tg_msg_id=sent.id)
                with db() as conn:
                    with conn.cursor() as c:
                        c.execute('UPDATE conversations SET last_auto_msg_at=%s WHERE tg_id=%s', (now, tg_id))
            loop = asyncio.get_event_loop()
            asyncio.run_coroutine_threadsafe(_send(), loop).result(timeout=15)
        except Exception as e:
            print(f'Auto-online-msg error: {e}')
    threading.Thread(target=_do, daemon=True).start()

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
                if is_online:
                    _send_auto_online_msg(tg_id)

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
                asyncio.create_task(ws_manager.broadcast({
                    'type': 'online_status', 'tg_id': tg_id, 'is_online': is_online
                }))

            @tg_client.on(events.Raw(UpdateReadHistoryOutbox))
            async def on_outbox_read(event):
                """Fires when subscriber reads our outgoing messages."""
                if isinstance(event.peer, PeerUser):
                    tg_id = str(event.peer.user_id)
                    max_id = event.max_id
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, lambda: mark_read(tg_id, max_id))
                    asyncio.create_task(ws_manager.broadcast({
                        'type': 'read_receipt', 'tg_id': tg_id, 'max_id': max_id
                    }))

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
                # Deduplicate: skip if this exact telegram message was already saved
                msg_tg_id = event.id
                with db() as conn:
                    with conn.cursor() as c:
                        c.execute('SELECT 1 FROM messages WHERE tg_msg_id=%s AND tg_id=%s AND direction=%s', (msg_tg_id, tg_id, 'in'))
                        if c.fetchone():
                            return
                if event.text:          text = event.text
                elif event.photo:       text = '[📷 Foto]'
                elif event.document:    text = '[📎 Datei]'
                elif event.sticker:     text = '[Sticker]'
                elif event.voice:       text = '[🎤 Sprachnachricht]'
                else:                   text = '[Nachricht]'
                await loop.run_in_executor(None, lambda: save_msg(tg_id, text, 'in', tg_msg_id=msg_tg_id))
                print(f'📨 {tg_id}: {text[:80]}')
                # Push to all connected CRM clients
                now_ts = datetime.now().isoformat()
                asyncio.create_task(ws_manager.broadcast({
                    'type': 'new_message',
                    'tg_id': tg_id,
                    'text': text,
                    'direction': 'in',
                    'timestamp': now_ts,
                    'tg_msg_id': msg_tg_id,
                }))
                asyncio.create_task(ws_manager.broadcast({
                    'type': 'notification',
                    'notif_type': 'message',
                    'tg_id': tg_id,
                    'text': text[:80],
                    'timestamp': now_ts,
                }))

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
    return {'userbot': 'connected' if ok else 'disconnected', 'ws_clients': len(ws_manager._connections)}

@app.websocket('/ws')
async def websocket_endpoint(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        while True:
            # Keep alive — client sends pings, we just read them
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception:
        ws_manager.disconnect(websocket)

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
@app.get('/analytics/my-stats')
def get_my_stats(chatter: str, period: str = 'heute'):
    """Personal stats for a single chatter filtered by period."""
    now = datetime.now()
    if period == 'heute':
        since = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    elif period == 'woche':
        since = (now - timedelta(days=7)).isoformat()
    elif period == 'monat':
        since = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()
    else:
        since = '2000-01-01'
    with db() as conn:
        with conn.cursor() as c:
            # Sales
            c.execute('''SELECT COUNT(*) as sales_count, COALESCE(SUM(amount),0) as revenue,
                                COALESCE(AVG(amount),0) as avg_sale
                         FROM sales WHERE chatter=%s AND timestamp >= %s''', (chatter, since))
            sale_row = c.fetchone()
            # Messages sent
            c.execute('''SELECT COUNT(*) as msgs FROM messages
                         WHERE chatter=%s AND direction='out' AND timestamp >= %s''', (chatter, since))
            msg_row = c.fetchone()
            # Avg response time
            c.execute('''SELECT AVG(EXTRACT(EPOCH FROM (m_out.timestamp::timestamp - m_in.timestamp::timestamp))) as avg_resp
                         FROM messages m_in
                         JOIN LATERAL (
                           SELECT timestamp FROM messages m
                           WHERE m.tg_id=m_in.tg_id AND m.direction='out' AND m.chatter=%s
                             AND m.timestamp > m_in.timestamp
                             AND EXTRACT(EPOCH FROM (m.timestamp::timestamp - m_in.timestamp::timestamp)) BETWEEN 5 AND 3600
                           ORDER BY m.timestamp ASC LIMIT 1
                         ) m_out ON true
                         WHERE m_in.direction='in' AND m_in.timestamp >= %s''', (chatter, since))
            resp_row = c.fetchone()
            # Sales by product
            c.execute('''SELECT product, COUNT(*) as cnt, SUM(amount) as rev
                         FROM sales WHERE chatter=%s AND timestamp >= %s AND product != ''
                         GROUP BY product ORDER BY rev DESC''', (chatter, since))
            products = [dict(r) for r in c.fetchall()]
    return {
        'chatter': chatter,
        'period': period,
        'revenue': float(sale_row['revenue'] or 0),
        'sales_count': sale_row['sales_count'] or 0,
        'avg_sale': float(sale_row['avg_sale'] or 0),
        'msgs_sent': msg_row['msgs'] or 0,
        'avg_response_sec': round(float(resp_row['avg_resp'])) if resp_row and resp_row['avg_resp'] else None,
        'products': products,
    }

@app.get('/analytics/recent-chatted')
def get_recent_chatted(hours: int = 5):
    """Subscribers the userbot messaged in the last N hours."""
    since = (datetime.now() - timedelta(hours=hours)).isoformat()
    with db() as conn:
        with conn.cursor() as c:
            c.execute('''SELECT DISTINCT m.tg_id, c.anon_id, c.internal_name
                         FROM messages m
                         JOIN conversations c ON c.tg_id = m.tg_id
                         WHERE m.direction='out' AND m.timestamp >= %s''', (since,))
            rows = c.fetchall()
    return [dict(r) for r in rows]

@app.get('/analytics/chatters')
def get_chatter_analytics():
    with db() as conn:
        with conn.cursor() as c:
            # Sales per chatter
            c.execute('''
                SELECT chatter,
                       COUNT(*) as sales_count,
                       COALESCE(SUM(amount),0) as total_revenue,
                       COALESCE(AVG(amount),0) as avg_sale,
                       COUNT(CASE WHEN LOWER(product) LIKE '%call%' THEN 1 END) as calls_count
                FROM sales WHERE chatter != ''
                GROUP BY chatter ORDER BY total_revenue DESC
            ''')
            sales_rows = {r['chatter']: dict(r) for r in c.fetchall()}

            # Messages sent + active time per chatter
            c.execute('''
                SELECT chatter,
                       COUNT(*) as msgs_sent,
                       MIN(timestamp) as first_msg,
                       MAX(timestamp) as last_msg
                FROM messages
                WHERE direction='out' AND chatter != ''
                GROUP BY chatter
            ''')
            msg_rows = {r['chatter']: dict(r) for r in c.fetchall()}

            # Average response time: time from incoming msg to next outgoing by chatter
            c.execute('''
                SELECT m_out.chatter,
                       AVG(EXTRACT(EPOCH FROM (m_out.timestamp::timestamp - m_in.timestamp::timestamp))) as avg_response_sec,
                       COUNT(*) as response_pairs
                FROM messages m_in
                JOIN LATERAL (
                    SELECT chatter, timestamp FROM messages m
                    WHERE m.tg_id = m_in.tg_id
                      AND m.direction = 'out'
                      AND m.chatter != ''
                      AND m.timestamp > m_in.timestamp
                      AND EXTRACT(EPOCH FROM (m.timestamp::timestamp - m_in.timestamp::timestamp)) BETWEEN 5 AND 3600
                    ORDER BY m.timestamp ASC LIMIT 1
                ) m_out ON true
                WHERE m_in.direction = 'in'
                GROUP BY m_out.chatter
            ''')
            response_rows = {r['chatter']: dict(r) for r in c.fetchall()}

    chatters = {}
    all_names = set(list(sales_rows.keys()) + list(msg_rows.keys()))
    for name in all_names:
        s = sales_rows.get(name, {'sales_count':0,'total_revenue':0,'avg_sale':0,'calls_count':0})
        m = msg_rows.get(name, {'msgs_sent':0,'first_msg':None,'last_msg':None})
        r = response_rows.get(name, {'avg_response_sec':None})

        # Active time in seconds (first to last message)
        active_sec = 0
        if m['first_msg'] and m['last_msg']:
            try:
                from datetime import datetime as _dt
                t1 = _dt.fromisoformat(str(m['first_msg']))
                t2 = _dt.fromisoformat(str(m['last_msg']))
                active_sec = max(0, int((t2 - t1).total_seconds()))
            except Exception:
                pass

        chatters[name] = {
            'chatter': name,
            'sales_count': s['sales_count'],
            'total_revenue': float(s['total_revenue']),
            'avg_sale': float(s['avg_sale']),
            'calls_count': s['calls_count'] or 0,
            'msgs_sent': m['msgs_sent'],
            'active_sec': active_sec,
            'avg_response_sec': round(float(r['avg_response_sec'])) if r.get('avg_response_sec') else None,
            'revenue_per_msg': round(float(s['total_revenue']) / m['msgs_sent'], 2) if m['msgs_sent'] > 0 else 0,
        }
    return sorted(chatters.values(), key=lambda x: x['total_revenue'], reverse=True)

async def _bg_read_history(tg_id: str):
    """Background: mark messages as read in Telegram."""
    if not tg_client or not tg_client.is_connected():
        return
    try:
        with db() as conn:
            with conn.cursor() as c:
                c.execute('SELECT tg_access_hash FROM conversations WHERE tg_id=%s', (tg_id,))
                row = c.fetchone()
        ah = int(row['tg_access_hash']) if row and row['tg_access_hash'] else 0
        peer = InputPeerUser(int(tg_id), ah) if ah else int(tg_id)
        await tg_client(functions.messages.ReadHistoryRequest(peer=peer, max_id=0))
    except Exception as e:
        print(f'ReadHistory skip: {e}')

@app.get('/messages/{tg_id}')
async def get_messages(tg_id: str, bg: BackgroundTasks):
    with db() as conn:
        with conn.cursor() as c:
            c.execute('UPDATE conversations SET unread=0 WHERE tg_id=%s', (tg_id,))
            c.execute('SELECT text,direction,timestamp,chatter,is_read,read_at,translation FROM messages WHERE tg_id=%s ORDER BY timestamp', (tg_id,))
            rows = c.fetchall()
    bg.add_task(_bg_read_history, tg_id)
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
        # Broadcast sent message to all CRM clients
        asyncio.create_task(ws_manager.broadcast({
            'type': 'new_message',
            'tg_id': body.tg_id,
            'text': body.text,
            'direction': 'out',
            'chatter': body.chatter,
            'timestamp': datetime.now().isoformat(),
            'tg_msg_id': tg_msg_id,
        }))
        return {'ok': True}
    except FloodWaitError as e:
        raise HTTPException(429, f'Telegram Flood Wait: {e.seconds}s warten')
    except Exception as e:
        raise HTTPException(500, str(e))

# ── AUTH & USERS ─────────────────────────────────────────────────────────────
import hashlib as _hashlib

def _hash_pw(pw: str) -> str:
    return _hashlib.sha256(pw.encode()).hexdigest()

class LoginIn(BaseModel):
    username: str
    password: str

@app.post('/auth/login')
def auth_login(body: LoginIn):
    with db() as conn:
        with conn.cursor() as c:
            c.execute('SELECT id,username,email,role,display_name FROM crm_users WHERE username=%s AND password_hash=%s',
                      (body.username, _hash_pw(body.password)))
            row = c.fetchone()
    if not row:
        raise HTTPException(401, 'Falscher Benutzername oder Passwort')
    d = dict(row)
    d['display_name'] = d['display_name'] or d['username']
    return d

@app.get('/auth/users')
def list_users():
    with db() as conn:
        with conn.cursor() as c:
            c.execute('SELECT id,username,email,role,display_name FROM crm_users ORDER BY id')
            rows = c.fetchall()
    result = [dict(r) for r in rows]
    for r in result:
        r['display_name'] = r['display_name'] or r['username']
    return result

class UserCreate(BaseModel):
    username: str
    display_name: str = ''
    email: str = ''
    password: str
    role: str = 'chatter'

@app.post('/auth/users')
def create_user(body: UserCreate):
    try:
        dn = body.display_name.strip() or body.username.strip()
        with db() as conn:
            with conn.cursor() as c:
                c.execute(
                    'INSERT INTO crm_users (username,email,password_hash,role,display_name) VALUES (%s,%s,%s,%s,%s) RETURNING id',
                    (body.username.strip(), body.email.strip(), _hash_pw(body.password), body.role, dn)
                )
                new_id = c.fetchone()['id']
        return {'ok': True, 'id': new_id}
    except Exception as e:
        raise HTTPException(400, f'Fehler: {e}')

class UserUpdate(BaseModel):
    display_name: Optional[str] = None
    email: Optional[str] = None
    password: Optional[str] = None
    role: Optional[str] = None

@app.patch('/auth/users/{user_id}')
def update_user(user_id: int, body: UserUpdate):
    with db() as conn:
        with conn.cursor() as c:
            if body.display_name is not None:
                c.execute('UPDATE crm_users SET display_name=%s WHERE id=%s', (body.display_name, user_id))
            if body.email is not None:
                c.execute('UPDATE crm_users SET email=%s WHERE id=%s', (body.email, user_id))
            if body.password:
                c.execute('UPDATE crm_users SET password_hash=%s WHERE id=%s', (_hash_pw(body.password), user_id))
            if body.role is not None:
                c.execute('UPDATE crm_users SET role=%s WHERE id=%s', (body.role, user_id))
    return {'ok': True}

@app.delete('/auth/users/{user_id}')
def delete_user(user_id: int):
    with db() as conn:
        with conn.cursor() as c:
            # Prevent deleting last admin
            c.execute("SELECT COUNT(*) as n FROM crm_users WHERE role='admin'")
            if c.fetchone()['n'] <= 1:
                c.execute("SELECT role FROM crm_users WHERE id=%s", (user_id,))
                row = c.fetchone()
                if row and row['role'] == 'admin':
                    raise HTTPException(400, 'Letzter Admin kann nicht gelöscht werden')
            c.execute('DELETE FROM crm_users WHERE id=%s', (user_id,))
    return {'ok': True}

# ── CRM SETTINGS ─────────────────────────────────────────────────────────────
def get_setting(key: str, default: str = '') -> str:
    with db() as conn:
        with conn.cursor() as c:
            c.execute('SELECT value FROM crm_settings WHERE key=%s', (key,))
            row = c.fetchone()
    return row['value'] if row else default

def set_setting(key: str, value: str):
    with db() as conn:
        with conn.cursor() as c:
            c.execute('INSERT INTO crm_settings (key,value) VALUES (%s,%s) ON CONFLICT (key) DO UPDATE SET value=%s',
                      (key, value, value))

@app.get('/settings/crm')
def get_crm_settings():
    keys = ['auto_online_enabled','auto_online_text','auto_online_cooldown_h','auto_online_stages','shift_goal']
    result = {}
    with db() as conn:
        with conn.cursor() as c:
            for k in keys:
                c.execute('SELECT value FROM crm_settings WHERE key=%s', (k,))
                row = c.fetchone()
                result[k] = row['value'] if row else ''
    return result

class SettingsUpdate(BaseModel):
    auto_online_enabled: Optional[str] = None
    auto_online_text: Optional[str] = None
    auto_online_cooldown_h: Optional[str] = None
    auto_online_stages: Optional[str] = None
    shift_goal: Optional[str] = None

@app.post('/settings/crm')
def save_crm_settings(body: SettingsUpdate):
    for k, v in body.dict().items():
        if v is not None:
            set_setting(k, v)
    return {'ok': True}

# ── AI ENDPOINTS ─────────────────────────────────────────────────────────────
import urllib.request as _urllib_req
import json as _json

def _openai_chat(messages: list, max_tokens: int = 300, temperature: float = 0.8) -> str:
    """Call OpenAI chat completions. Returns content string or raises."""
    if not OPENAI_API_KEY:
        raise HTTPException(503, 'OPENAI_API_KEY nicht gesetzt')
    payload = _json.dumps({
        'model': 'gpt-4o-mini',
        'messages': messages,
        'max_tokens': max_tokens,
        'temperature': temperature,
    }).encode()
    req = _urllib_req.Request(
        'https://api.openai.com/v1/chat/completions',
        data=payload,
        headers={'Content-Type': 'application/json', 'Authorization': f'Bearer {OPENAI_API_KEY}'}
    )
    with _urllib_req.urlopen(req, timeout=15) as resp:
        data = _json.loads(resp.read())
    return data['choices'][0]['message']['content'].strip()

class TranslateIn(BaseModel):
    text: str

@app.post('/ai/translate')
async def ai_translate(body: TranslateIn):
    """Translate incoming subscriber message to English."""
    if not body.text.strip():
        return {'ok': False, 'result': ''}
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, lambda: _openai_chat([
            {'role': 'system', 'content': 'Translate the following message to English. Return ONLY the translation, nothing else.'},
            {'role': 'user', 'content': body.text}
        ], max_tokens=200, temperature=0.3))
        return {'ok': True, 'result': result}
    except Exception as e:
        raise HTTPException(500, str(e))

class StyleIn(BaseModel):
    text: str
    context: Optional[list] = None  # [{role, content}, ...] last few messages

@app.post('/ai/style')
async def ai_style(body: StyleIn):
    """Rewrite outgoing chatter text in Marie's natural German style."""
    if not body.text.strip():
        return {'ok': False, 'result': ''}

    system_prompt = """Du bist Marie, 18 Jahre, aus der Nähe von Köln. Du schreibst mit Fans auf Telegram.

EMOJI-REGEL — SEHR WICHTIG:
Viele Nachrichten haben GAR KEIN Emoji. Benutze stattdessen Text-Smileys: ":)" "^^" "<33" ";)" ":D"
Nur alle 4-5 Nachrichten mal ein echtes Emoji, und dann nur 1. Nie mehrere hintereinander.

STIL:
- Alles kleinschreiben
- 1-2 Sätze — nie länger
- Filler-Wörter: "irgendwie", "voll", "ahh", "ne", "halt", "eigentlich"
- Wiederholungen für Ausdruck: "neeeee", "jaaa", "hmmm"
- Pausen mit "..." oder "-"
- Text-Smileys bevorzugen: :) ^^ <33 ;)
- Gegenfrage am Ende stellen
- Verletzlichkeit zeigen: "bin ja noch relativ neu hier", "macht mich ein bisschen nervös"

ECHTE BEISPIELE (genau so schreiben):
"hey.. danke für dein aboo <33"
"wie hast du mich eigentlich gefunden? bin ja noch relativ neu hier 😊"
"voll vergessen, ist ju eigentlich der name mit dem ich dich ansprechen soll oder hast du einen anderen? ;)"
"irgendwie gefällt mir dein name :) wie alt bist du wenn ich fragen darf?"
"ich bin 18 das ist okay für dich oder?"
"ich komme aus der nähe von köln ^^"
"sag mir ganz ehrlich... gefällt dir was du bisher gesehen hast?"
"ich freue mich das zu hören 🥰 und wo das herkommt gibt es auch noch viel mehr"
"neeeee nicht sofort haha"
"ja könnte dir eins machen"
"ahh oki und ist das hier dein richtiger name?"
"was machst du gerade so? :)"
"schwierige frage... mir fallen ein paar dinge ein"
"na also.. geht doch du kleiner schlingel"

VERBOTEN (nie so schreiben):
❌ "Hey, alles gut bei mir! Und wie geht es dir so? 😊"
❌ "Das klingt wirklich super! Das freut mich sehr! 🥰✨"
❌ "Natürlich! Das wäre eine tolle Idee! 💫"
❌ Jede Nachricht mit Emoji beenden
❌ Mehr als 2 Sätze

Schreibe die Nachricht in Maries echtem Stil um. Nur die Nachricht zurückgeben."""

    # Build conversation context string for the prompt
    context_str = ''
    if body.context:
        lines = []
        for m in body.context[-8:]:
            role = 'Fan' if m.get('role') == 'user' else 'Marie'
            lines.append(f'{role}: {m.get("content", "")}')
        context_str = '\n'.join(lines)

    last_fan_msg = ''
    if body.context:
        for m in reversed(body.context[-4:]):
            if m.get('role') == 'user':
                last_fan_msg = m.get('content', '')
                break

    user_msg = f'''LETZTER FAN: "{last_fan_msg}"

GESPRÄCHSVERLAUF:
{context_str if context_str else "(Erster Kontakt)"}

CHATTER MEINT: "{body.text}"

Marie reagiert jetzt auf den letzten Fan-Satz und sagt was der Chatter meint.
Schreib NUR Maries Antwort — kurz, natürlich, im Kontext. KEIN Kommentar.'''

    messages = [{'role': 'system', 'content': system_prompt}]
    messages.append({'role': 'user', 'content': user_msg})


    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, lambda: _openai_chat(messages, max_tokens=300, temperature=0.85))
        return {'ok': True, 'result': result}
    except Exception as e:
        raise HTTPException(500, str(e))

class SuggestIn(BaseModel):
    tg_id: str
    context: Optional[list] = None  # last messages [{role,content}]

@app.post('/ai/suggest')
async def ai_suggest(body: SuggestIn):
    """Generate 3 reply suggestions for the current conversation."""
    # Get last messages from DB for context
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT text,direction FROM messages WHERE tg_id=%s ORDER BY timestamp DESC LIMIT 10", (body.tg_id,))
            recent = list(reversed(c.fetchall()))
            c.execute("SELECT internal_name,notes,funnel_stage FROM conversations WHERE tg_id=%s", (body.tg_id,))
            conv = c.fetchone()

    context_msgs = [
        {'role': 'user' if m['direction']=='in' else 'assistant', 'content': m['text']}
        for m in recent if not m['text'].startswith('[')
    ]

    stage_hint = ''
    if conv:
        stage_map = {'kalt':'Erster Kontakt','warm':'Subscriber ist warm','hot':'Hot Lead','angebot':'Angebot wurde gemacht','gebucht':'Call gebucht','done':'Deal abgeschlossen'}
        stage_hint = f"\nAktueller Funnel-Status: {stage_map.get(conv['funnel_stage'],'Unbekannt')}"
        if conv['notes']: stage_hint += f"\nNotizen über den Fan: {conv['notes']}"

    system_prompt = f"""Du bist Marie, 18 Jahre aus Köln, Creatorin die Video Calls verkauft (6 Min = 15€).
Du chattest mit einem Fan auf Telegram. Generiere 3 verschiedene kurze Antwortmöglichkeiten.{stage_hint}

Stil: Kleinschreibung, locker, flirty, kurz (1-2 Sätze), echte Chat-Sprache.
Emojis: nur gelegentlich (jede 3-4. Nachricht), nie mehrere hintereinander.

Gib EXAKT dieses JSON-Format zurück (kein anderer Text):
["antwort 1","antwort 2","antwort 3"]

Die 3 Optionen sollen unterschiedliche Stile haben:
1. Warm/neugierig (Frage stellen)
2. Flirty/direkt
3. Kurz und knapp"""

    messages = [{'role': 'system', 'content': system_prompt}]
    messages.extend(context_msgs[-8:])
    messages.append({'role': 'user', 'content': 'Generiere jetzt 3 Antwortoptionen als JSON-Array.'})

    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, lambda: _openai_chat(messages, max_tokens=300, temperature=0.9))
        import json as _json2
        suggestions = _json2.loads(result)
        if isinstance(suggestions, list):
            return {'ok': True, 'suggestions': suggestions[:3]}
        return {'ok': False, 'suggestions': []}
    except Exception as e:
        raise HTTPException(500, str(e))

# ── NOTIFICATIONS FEED ───────────────────────────────────────────────────────
@app.get('/notifications')
def get_notifications(limit: int = 80):
    """Combined activity feed: new subs, messages, sales."""
    events = []
    with db() as conn:
        with conn.cursor() as c:
            # New subscribers
            c.execute('''SELECT tg_id, anon_id, internal_name, tg_username, first_time as ts
                         FROM conversations ORDER BY first_time DESC LIMIT %s''', (limit,))
            for r in c.fetchall():
                events.append({'type': 'new_sub', 'ts': str(r['ts']),
                               'tg_id': r['tg_id'], 'anon_id': r['anon_id'],
                               'name': r['internal_name'] or r['anon_id'],
                               'username': r['tg_username'] or ''})
            # Incoming messages (last N)
            c.execute('''SELECT m.tg_id, m.text, m.timestamp as ts,
                                c.anon_id, c.internal_name
                         FROM messages m
                         JOIN conversations c ON c.tg_id = m.tg_id
                         WHERE m.direction='in'
                         ORDER BY m.timestamp DESC LIMIT %s''', (limit,))
            for r in c.fetchall():
                events.append({'type': 'message', 'ts': str(r['ts']),
                               'tg_id': r['tg_id'], 'anon_id': r['anon_id'],
                               'name': r['internal_name'] or r['anon_id'],
                               'text': r['text'][:80]})
            # Sales
            c.execute('''SELECT s.tg_id, s.amount, s.product, s.chatter, s.timestamp as ts,
                                c.anon_id, c.internal_name
                         FROM sales s
                         JOIN conversations c ON c.tg_id = s.tg_id
                         ORDER BY s.timestamp DESC LIMIT %s''', (limit,))
            for r in c.fetchall():
                events.append({'type': 'sale', 'ts': str(r['ts']),
                               'tg_id': r['tg_id'], 'anon_id': r['anon_id'],
                               'name': r['internal_name'] or r['anon_id'],
                               'amount': float(r['amount']), 'product': r['product'] or '',
                               'chatter': r['chatter'] or ''})
    # Sort all events by timestamp desc, return top N
    events.sort(key=lambda x: x['ts'], reverse=True)
    return events[:limit]

# ── VAULT ────────────────────────────────────────────────────────────────────
ALLOWED_TYPES = {'image/jpeg','image/png','image/gif','image/webp','video/mp4','video/quicktime','video/x-matroska'}

@app.get('/vault')
def vault_list():
    """List all files in the vault."""
    files = []
    try:
        for fname in sorted(os.listdir(VAULT_DIR)):
            fpath = os.path.join(VAULT_DIR, fname)
            if os.path.isfile(fpath):
                stat = os.stat(fpath)
                ext = fname.rsplit('.', 1)[-1].lower() if '.' in fname else ''
                ftype = 'video' if ext in ('mp4','mov','mkv','avi') else 'image'
                files.append({
                    'name': fname,
                    'size': stat.st_size,
                    'type': ftype,
                    'url': f'/vault/file/{fname}',
                    'modified': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                })
    except Exception as e:
        print(f'Vault list error: {e}')
    return files

@app.post('/vault/upload')
async def vault_upload(file: UploadFile = File(...)):
    """Upload a file to the vault."""
    if file.content_type not in ALLOWED_TYPES:
        raise HTTPException(400, f'File type not allowed: {file.content_type}')
    # Sanitize filename
    safe_name = ''.join(c for c in file.filename if c.isalnum() or c in '._- ')
    safe_name = safe_name or 'file'
    # Avoid overwrite: add timestamp if exists
    fpath = os.path.join(VAULT_DIR, safe_name)
    if os.path.exists(fpath):
        base, ext = os.path.splitext(safe_name)
        safe_name = f'{base}_{int(datetime.now().timestamp())}{ext}'
        fpath = os.path.join(VAULT_DIR, safe_name)
    with open(fpath, 'wb') as f:
        shutil.copyfileobj(file.file, f)
    return {'ok': True, 'name': safe_name, 'url': f'/vault/file/{safe_name}'}

@app.get('/vault/file/{filename}')
def vault_serve(filename: str):
    """Serve a vault file."""
    fpath = os.path.join(VAULT_DIR, filename)
    if not os.path.isfile(fpath) or '..' in filename:
        raise HTTPException(404, 'Not found')
    return FileResponse(fpath)

@app.delete('/vault/file/{filename}')
def vault_delete(filename: str):
    """Delete a vault file."""
    fpath = os.path.join(VAULT_DIR, filename)
    if not os.path.isfile(fpath) or '..' in filename:
        raise HTTPException(404, 'Not found')
    os.remove(fpath)
    return {'ok': True}

class VaultSendIn(BaseModel):
    tg_id: str
    filename: str
    caption: str = ''

@app.post('/vault/send')
async def vault_send(body: VaultSendIn):
    """Send a vault file to a subscriber via Telegram."""
    if not tg_client or not tg_client.is_connected():
        raise HTTPException(503, 'Userbot nicht verbunden')
    fpath = os.path.join(VAULT_DIR, body.filename)
    if not os.path.isfile(fpath) or '..' in body.filename:
        raise HTTPException(404, 'File not found in vault')
    try:
        with db() as conn:
            with conn.cursor() as c:
                c.execute('SELECT tg_access_hash FROM conversations WHERE tg_id=%s', (body.tg_id,))
                row = c.fetchone()
        ah = int(row['tg_access_hash']) if row and row['tg_access_hash'] else 0
        peer = InputPeerUser(int(body.tg_id), ah) if ah else int(body.tg_id)
        await tg_client.send_file(peer, fpath, caption=body.caption or None)
        save_msg(body.tg_id, f'[📎 {body.filename}]', 'out', 'Vault')
        asyncio.create_task(ws_manager.broadcast({
            'type': 'new_message', 'tg_id': body.tg_id,
            'text': f'[📎 {body.filename}]', 'direction': 'out',
            'timestamp': datetime.now().isoformat()
        }))
        return {'ok': True}
    except Exception as e:
        raise HTTPException(500, str(e))

# ── FAKECHECK ────────────────────────────────────────────────────────────────
FAKECHECK_BOT = '@FakecheckAudioBot'

class FakecheckIn(BaseModel):
    tg_id: str           # subscriber to send audio to
    command: str = ''    # what to send to the bot (e.g. subscriber name, or /start)

@app.post('/fakecheck')
async def send_fakecheck(body: FakecheckIn):
    if not tg_client or not tg_client.is_connected():
        raise HTTPException(503, 'Userbot nicht verbunden')
    try:
        # 1. Send command to the fakecheck bot
        trigger = body.command.strip() or 'start'
        await tg_client.send_message(FAKECHECK_BOT, trigger)

        # 2. Wait up to 15s for bot to reply with audio
        import asyncio as _aio
        audio_msg = None
        for _ in range(30):  # poll every 0.5s for 15s
            await _aio.sleep(0.5)
            msgs = await tg_client.get_messages(FAKECHECK_BOT, limit=1)
            if msgs and msgs[0].voice:
                audio_msg = msgs[0]
                break

        if not audio_msg:
            raise HTTPException(408, 'Bot hat keine Sprachnachricht geschickt (Timeout)')

        # 3. Forward audio to subscriber
        with db() as conn:
            with conn.cursor() as c:
                c.execute('SELECT tg_access_hash FROM conversations WHERE tg_id=%s', (body.tg_id,))
                row = c.fetchone()
        ah = int(row['tg_access_hash']) if row and row['tg_access_hash'] else 0
        peer = InputPeerUser(int(body.tg_id), ah) if ah else int(body.tg_id)
        await tg_client.forward_messages(peer, audio_msg)

        # 4. Log as outgoing message
        save_msg(body.tg_id, '[🎙️ Fakecheck Audio]', 'out', 'System')

        # 5. Broadcast to CRM clients
        asyncio.create_task(ws_manager.broadcast({
            'type': 'new_message', 'tg_id': body.tg_id,
            'text': '[🎙️ Fakecheck Audio]', 'direction': 'out',
            'timestamp': datetime.now().isoformat()
        }))
        return {'ok': True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))

# ── BROADCAST ────────────────────────────────────────────────────────────────
class BroadcastIn(BaseModel):
    text: str
    chatter: str = 'Broadcast'
    filter_stage: Optional[str] = None
    filter_min_spend: Optional[float] = None
    tg_ids: Optional[list] = None
    exclude_ids: Optional[list] = None       # exclude specific tg_ids
    exclude_stages: Optional[list] = None    # exclude by funnel stage

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
                c.execute(f'SELECT tg_id,tg_access_hash,funnel_stage FROM conversations WHERE tg_id IN ({fmt})', body.tg_ids)
            elif body.filter_stage:
                c.execute('SELECT tg_id,tg_access_hash,funnel_stage FROM conversations WHERE funnel_stage=%s', (body.filter_stage,))
            else:
                c.execute('SELECT tg_id,tg_access_hash,funnel_stage FROM conversations')
            all_r = c.fetchall()

    # Apply exclusions
    exclude_set = set(body.exclude_ids or [])
    exclude_stages = set(body.exclude_stages or [])
    recipients = [r for r in all_r
                  if r['tg_id'] not in exclude_set
                  and (not exclude_stages or r.get('funnel_stage','') not in exclude_stages)]

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
    # Broadcast sale notification to all connected CRM clients
    asyncio.create_task(ws_manager.broadcast({
        'type': 'notification',
        'notif_type': 'sale',
        'tg_id': body.tg_id,
        'anon_id': body.anon_id,
        'amount': body.amount,
        'product': body.product,
        'chatter': body.chatter,
        'timestamp': ts,
    }))
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
