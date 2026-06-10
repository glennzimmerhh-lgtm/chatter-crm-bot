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

# Sale proof screenshots directory
PROOFS_DIR = os.path.join(VAULT_DIR, '_proofs')
os.makedirs(PROOFS_DIR, exist_ok=True)

# Fake call recordings directory
CALLS_DIR = os.path.join(VAULT_DIR, '_calls')
os.makedirs(CALLS_DIR, exist_ok=True)

# ── pytgcalls (optional — fake call feature) ──────────────────────────────────
_PYTGCALLS_ERR = None
try:
    from pytgcalls import PyTgCalls
    from pytgcalls.types import MediaStream, AudioQuality
    _PYTGCALLS_OK = True
    print('✅ pytgcalls available')
except Exception as _e:
    _PYTGCALLS_OK = False
    _PYTGCALLS_ERR = str(_e)
    print(f'⚠️  pytgcalls not available: {_e}')

calls_client: Optional[object] = None
active_calls: dict = {}   # tg_id → {file, chatter, started_at}

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
            c.execute('''CREATE TABLE IF NOT EXISTS pledges (
                id              SERIAL PRIMARY KEY,
                tg_id           TEXT NOT NULL,
                anon_id         TEXT NOT NULL,
                amount          REAL NOT NULL,
                payment_method  TEXT DEFAULT '',
                notes           TEXT DEFAULT '',
                chatter         TEXT DEFAULT '',
                deadline        TEXT NOT NULL,
                status          TEXT DEFAULT 'open',
                created_at      TEXT NOT NULL,
                paid_at         TEXT DEFAULT ''
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
                # Sale proof system
                "ALTER TABLE sales ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'approved'",
                "ALTER TABLE sales ADD COLUMN IF NOT EXISTS screenshot TEXT DEFAULT ''",
                "ALTER TABLE sales ADD COLUMN IF NOT EXISTS rejection_reason TEXT DEFAULT ''",
                "ALTER TABLE sales ADD COLUMN IF NOT EXISTS reviewed_by TEXT DEFAULT ''",
                "ALTER TABLE sales ADD COLUMN IF NOT EXISTS reviewed_at TEXT DEFAULT ''",
                "ALTER TABLE sales ADD COLUMN IF NOT EXISTS payment_method TEXT DEFAULT ''",
                "ALTER TABLE sales ADD COLUMN IF NOT EXISTS payment_code TEXT DEFAULT ''",
                # YouSafe reference codes
                "ALTER TABLE conversations ADD COLUMN IF NOT EXISTS payment_ref TEXT DEFAULT ''",
                "CREATE INDEX IF NOT EXISTS idx_messages_tg_id ON messages(tg_id)",
                "CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_conv_last_time ON conversations(last_time DESC)",
            ]:
                try:
                    c.execute(stmt)
                except Exception as e:
                    print(f'Migration skip: {e}')
                    conn.rollback()
    # Backfill payment_ref for existing subscribers
    with db() as conn:
        with conn.cursor() as c:
            c.execute("UPDATE conversations SET payment_ref='ZF-'||SUBSTRING(anon_id FROM 7) WHERE (payment_ref IS NULL OR payment_ref='') AND anon_id LIKE 'User #%'")
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
            payment_ref = f'ZF-{1001 + n}'
            now = datetime.now().isoformat()
            c.execute(
                'INSERT INTO conversations (tg_id,anon_id,last_msg,last_time,first_time,unread,msg_count,tg_username,tg_phone,payment_ref) VALUES (%s,%s,%s,%s,%s,0,0,%s,%s,%s)',
                (tg_id, anon_id, '', now, now, username, phone, payment_ref)
            )
            # Backfill payment_ref for any existing subscribers that don't have one
            c.execute("UPDATE conversations SET payment_ref='ZF-'||SUBSTRING(anon_id FROM 7) WHERE payment_ref='' AND anon_id LIKE 'User #%'")
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
                elif event.photo:       text = '[📷 Photo]'
                elif event.document:    text = '[📎 Document]'
                elif event.sticker:     text = '[Sticker]'
                elif event.voice:       text = '[🎤 Voice]'
                else:                   text = '[Message]'
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

            @tg_client.on(events.NewMessage(outgoing=True, func=lambda e: e.is_private))
            async def on_outgoing_dm(event):
                """Capture messages sent directly from Telegram (not via CRM /reply)."""
                tg_id = str(event.chat_id)
                msg_tg_id = event.id
                loop = asyncio.get_event_loop()
                # Only capture for known subscribers
                with db() as conn:
                    with conn.cursor() as c:
                        c.execute('SELECT 1 FROM conversations WHERE tg_id=%s', (tg_id,))
                        if not c.fetchone():
                            return
                # Deduplicate: skip if already saved (e.g. sent via /reply endpoint)
                with db() as conn:
                    with conn.cursor() as c:
                        c.execute('SELECT 1 FROM messages WHERE tg_msg_id=%s AND tg_id=%s AND direction=%s', (msg_tg_id, tg_id, 'out'))
                        if c.fetchone():
                            return
                if event.text:      text = event.text
                elif event.photo:   text = '[📷 Photo]'
                elif event.document:text = '[📎 Document]'
                elif event.voice:   text = '[🎤 Voice]'
                else:               text = '[Message]'
                await loop.run_in_executor(None, lambda: save_msg(tg_id, text, 'out', 'Telegram', tg_msg_id=msg_tg_id))
                print(f'📤 Telegram→CRM {tg_id}: {text[:80]}')
                asyncio.create_task(ws_manager.broadcast({
                    'type': 'new_message',
                    'tg_id': tg_id,
                    'text': text,
                    'direction': 'out',
                    'chatter': 'Telegram',
                    'timestamp': datetime.now().isoformat(),
                    'tg_msg_id': msg_tg_id,
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
    global calls_client
    init_db()
    asyncio.create_task(start_userbot())
    # Give userbot 3s to connect, then init calls_client
    async def _init_calls():
        await asyncio.sleep(5)
        global calls_client
        if _PYTGCALLS_OK and tg_client:
            try:
                calls_client = PyTgCalls(tg_client)
                await calls_client.start()

                @calls_client.on_update()
                async def _on_call_update(update):
                    try:
                        tg_id_str = str(getattr(update, 'chat_id', ''))
                        if not tg_id_str or tg_id_str not in active_calls:
                            return

                        update_type = type(update).__name__

                        # Stream finished → hang up automatically
                        if update_type in ('StreamAudioEnded', 'StreamVideoEnded', 'MutedStream', 'StreamEnded'):
                            print(f'🔔 Stream ended for {tg_id_str} ({update_type}) — hanging up')
                            try:
                                await calls_client.leave_call(int(tg_id_str))
                            except Exception as e:
                                print(f'⚠️  leave_call error: {e}')
                            active_calls.pop(tg_id_str, None)
                            asyncio.create_task(ws_manager.broadcast({'type': 'call_ended', 'tg_id': tg_id_str}))
                            return

                        # Call rejected / hung up by subscriber
                        if update_type in ('CallEnded', 'KickedFromGroupCallParticipant', 'ClosedVoiceChat'):
                            print(f'📵 Call ended by subscriber {tg_id_str} ({update_type})')
                            active_calls.pop(tg_id_str, None)
                            asyncio.create_task(ws_manager.broadcast({'type': 'call_ended', 'tg_id': tg_id_str}))

                    except Exception as e:
                        print(f'⚠️  _on_call_update error: {e}')

                print('✅ PyTgCalls initialized and started')
            except Exception as e:
                print(f'⚠️  PyTgCalls init failed: {e}')
    asyncio.create_task(_init_calls())
    yield
    global _userbot_running
    _userbot_running = False
    if tg_client:
        await tg_client.disconnect()
    if calls_client:
        try:
            await calls_client.stop()
        except Exception:
            pass

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
            c.execute('''SELECT c.tg_id,c.anon_id,c.internal_name,c.last_seen,
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
            c.execute('SELECT tg_id,anon_id,internal_name,notes,last_time,first_time,unread,msg_count,time_waster,tg_username,tg_phone,funnel_stage,call_followup_at,call_followup_note,payment_ref FROM conversations WHERE tg_id=%s', (tg_id,))
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
            c.execute('SELECT id,text,direction,timestamp,chatter,is_read,read_at,translation,tg_msg_id FROM messages WHERE tg_id=%s ORDER BY timestamp', (tg_id,))
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
    """Translate chatter's English text into German in Marie's natural style."""
    if not body.text.strip():
        return {'ok': False, 'result': ''}

    system_prompt = """Du bist ein Übersetzer für Marie, eine 18-jährige Deutsche die mit Fans auf Telegram chattet.

DEINE AUFGABE:
Ein Chatter schreibt auf ENGLISCH was Marie sagen soll.
Übersetze den Text ins Deutsche — so wie Marie es schreiben würde.

ABSOLUT WICHTIG:
✅ Übersetze NUR den Text des Chatters
✅ Gib NUR den deutschen Satz zurück — nichts anderes
❌ Antworte NICHT auf den Fan
❌ Füge KEINE eigenen Gedanken hinzu
❌ Füge KEINE Kommentare hinzu
❌ Stelle KEINE eigenen Fragen die der Chatter nicht gestellt hat

MARIES STIL (beim Übersetzen beachten):
- Alles kleinschreiben
- Locker, natürlich, echte Chat-Sprache
- Text-Smileys bevorzugen: :) ^^ <33 ;) — echte Emojis nur selten
- Filler-Wörter wenn passend: "irgendwie", "voll", "halt", "ne", "eigentlich"
- Nie formell, nie steif

BEISPIELE:
EN: "how are you?" → DE: "wie geht's dir so? :)"
EN: "I missed you" → DE: "ich hab dich vermisst :("
EN: "you look good" → DE: "du siehst gut aus ;)"
EN: "want to do a call?" → DE: "wollen wir einen call machen?"
EN: "I can show you more" → DE: "ich kann dir noch mehr zeigen ;)"
EN: "that's cute" → DE: "das ist süß ^^"
EN: "tell me about yourself" → DE: "erzähl mir von dir :)"
EN: "I'm busy right now" → DE: "bin gerade beschäftigt"

Gib NUR die deutsche Übersetzung zurück — einen Satz, fertig."""

    user_msg = f'Übersetze: "{body.text}"'

    messages = [{'role': 'system', 'content': system_prompt}]
    messages.append({'role': 'user', 'content': user_msg})


    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, lambda: _openai_chat(messages, max_tokens=120, temperature=0.5))
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
        stage_map = {'kalt':'Cold lead (first contact)','warm':'Warm (engaged)','hot':'Hot lead','angebot':'Offer was made','gebucht':'Call booked','done':'Deal closed'}
        stage_hint = f"\nCurrent funnel stage: {stage_map.get(conv['funnel_stage'] or 'kalt','Unknown')}"
        if conv['notes']: stage_hint += f"\nNotes about this fan: {conv['notes']}"

    system_prompt = f"""You are helping a chatter respond as Marie — an 18-year-old German girl selling video calls on Telegram.
The chatter speaks English and needs reply suggestions IN ENGLISH so they can read them.{stage_hint}

Generate 3 short English reply options that Marie would say to this fan.
Keep Marie's personality: casual, flirty, short (1-2 sentences), genuine chat language.
Occasionally ask a question, occasionally be playful or direct.

Return EXACTLY this JSON format (nothing else, no explanation):
["option 1", "option 2", "option 3"]

The 3 options should have different vibes:
1. Warm/curious — ask the fan something
2. Flirty/direct
3. Short and simple"""

    messages = [{'role': 'system', 'content': system_prompt}]
    messages.extend(context_msgs[-8:])
    messages.append({'role': 'user', 'content': 'Generate 3 reply options as a JSON array.'})

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
ALLOWED_TYPES = {
    'image/jpeg','image/jpg','image/png','image/gif','image/webp',
    'image/heic','image/heif',                          # iOS formats
    'image/x-adobe-dng','image/dng','image/x-raw',      # RAW / DNG
    'image/tiff','image/x-tiff',                        # TIFF
    'video/mp4','video/quicktime','video/x-matroska','video/avi','video/x-msvideo',
}

def _vault_safe(name: str) -> str:
    """Sanitize filename: only alphanumeric, dots, dashes, underscores. Spaces → underscore."""
    import re as _re
    name = _re.sub(r'[^a-zA-Z0-9._-]', '_', name)
    name = _re.sub(r'_+', '_', name)   # collapse multiple underscores
    name = name.strip('_')              # remove leading/trailing underscores
    return name or 'file'

def _vault_file_info(fpath: str, relpath: str, folder: str) -> dict:
    stat = os.stat(fpath)
    ext = relpath.rsplit('.', 1)[-1].lower() if '.' in relpath else ''
    ftype = 'video' if ext in ('mp4','mov','mkv','avi') else 'image'
    return {
        'name': relpath,      # e.g. "Folder/file.jpg" or "file.jpg"
        'folder': folder,
        'size': stat.st_size,
        'type': ftype,
        'url': f'/vault/file/{relpath}',
        'modified': datetime.fromtimestamp(stat.st_mtime).isoformat(),
    }

@app.get('/vault/folders')
def vault_folders():
    """List all vault folders (subdirectories)."""
    folders = []
    try:
        for name in sorted(os.listdir(VAULT_DIR)):
            if os.path.isdir(os.path.join(VAULT_DIR, name)) and not name.startswith('.'):
                folders.append(name)
    except Exception as e:
        print(f'Vault folders error: {e}')
    return folders

class VaultFolderIn(BaseModel):
    name: str

@app.post('/vault/folder')
def vault_create_folder(body: VaultFolderIn):
    """Create a new vault folder."""
    safe = _vault_safe(body.name).strip()
    if not safe:
        raise HTTPException(400, 'Invalid folder name')
    fpath = os.path.join(VAULT_DIR, safe)
    os.makedirs(fpath, exist_ok=True)
    return {'ok': True, 'name': safe}

@app.delete('/vault/folder/{folder}')
def vault_delete_folder(folder: str):
    """Delete a vault folder and all its files."""
    if '..' in folder:
        raise HTTPException(400, 'Invalid folder name')
    fpath = os.path.join(VAULT_DIR, folder)
    if not os.path.isdir(fpath):
        raise HTTPException(404, 'Folder not found')
    shutil.rmtree(fpath)
    return {'ok': True}

@app.get('/vault')
def vault_list(folder: Optional[str] = None):
    """List vault files. Optional ?folder=X to filter by folder."""
    files = []
    try:
        if folder:
            # List files in specific folder — use raw name, check both original and safe version
            dir_path = os.path.join(VAULT_DIR, folder)
            if not os.path.isdir(dir_path):
                dir_path = os.path.join(VAULT_DIR, _vault_safe(folder))
            safe_folder = os.path.basename(dir_path)
            if os.path.isdir(dir_path):
                for fname in sorted(os.listdir(dir_path)):
                    fpath = os.path.join(dir_path, fname)
                    if os.path.isfile(fpath):
                        relpath = f'{safe_folder}/{fname}'
                        files.append(_vault_file_info(fpath, relpath, safe_folder))
        else:
            # List ALL files (root + all subfolders)
            for item in sorted(os.listdir(VAULT_DIR)):
                ipath = os.path.join(VAULT_DIR, item)
                if os.path.isfile(ipath):
                    files.append(_vault_file_info(ipath, item, ''))
                elif os.path.isdir(ipath) and not item.startswith('.'):
                    for fname in sorted(os.listdir(ipath)):
                        fpath = os.path.join(ipath, fname)
                        if os.path.isfile(fpath):
                            relpath = f'{item}/{fname}'
                            files.append(_vault_file_info(fpath, relpath, item))
    except Exception as e:
        print(f'Vault list error: {e}')
    return files

@app.post('/vault/upload')
async def vault_upload(file: UploadFile = File(...), folder: str = ''):
    """Upload a file to the vault, optionally into a folder."""
    # Accept common types; also allow empty content-type (some clients omit it)
    ct = (file.content_type or '').split(';')[0].strip().lower()
    if ct and ct not in ALLOWED_TYPES and ct != 'application/octet-stream':
        raise HTTPException(400, f'File type not allowed: {ct}')
    # Sanitize filename only (keep the original folder name to avoid breaking existing folders)
    safe_name = _vault_safe(file.filename or 'file') or 'file'
    if folder:
        # Use folder name as-is if it already exists, otherwise sanitize for new folders
        existing = os.path.join(VAULT_DIR, folder)
        if os.path.isdir(existing):
            target_dir = existing
            safe_folder = folder          # preserve original name (may have spaces)
        else:
            safe_folder = _vault_safe(folder)
            target_dir = os.path.join(VAULT_DIR, safe_folder)
            os.makedirs(target_dir, exist_ok=True)
    else:
        target_dir = VAULT_DIR
        safe_folder = ''
    fpath = os.path.join(target_dir, safe_name)
    if os.path.exists(fpath):
        base, ext = os.path.splitext(safe_name)
        safe_name = f'{base}_{int(datetime.now().timestamp())}{ext}'
        fpath = os.path.join(target_dir, safe_name)
    try:
        with open(fpath, 'wb') as f:
            shutil.copyfileobj(file.file, f)
    except OSError as e:
        print(f'Vault write error: {e}')
        if 'No space left' in str(e):
            raise HTTPException(507, 'Disk full — Railway volume is out of space. Delete old files or expand volume.')
        raise HTTPException(500, f'Write error: {e}')
    relpath = f'{safe_folder}/{safe_name}' if safe_folder else safe_name
    return {'ok': True, 'name': relpath, 'url': f'/vault/file/{relpath}'}

@app.get('/vault/file/{filepath:path}')
def vault_serve(filepath: str):
    """Serve a vault file (supports folder/filename paths)."""
    if '..' in filepath:
        raise HTTPException(400, 'Invalid path')
    fpath = os.path.join(VAULT_DIR, filepath)
    if not os.path.isfile(fpath):
        raise HTTPException(404, 'Not found')
    return FileResponse(fpath)

@app.delete('/vault/file/{filepath:path}')
def vault_delete(filepath: str):
    """Delete a vault file."""
    if '..' in filepath:
        raise HTTPException(400, 'Invalid path')
    fpath = os.path.join(VAULT_DIR, filepath)
    if not os.path.isfile(fpath):
        raise HTTPException(404, 'Not found')
    os.remove(fpath)
    return {'ok': True}

class VaultSendIn(BaseModel):
    tg_id: str
    filename: str   # can be "folder/file.jpg" or "file.jpg"
    caption: str = ''

@app.post('/vault/send')
async def vault_send(body: VaultSendIn):
    """Send a vault file to a subscriber via Telegram."""
    if not tg_client or not tg_client.is_connected():
        raise HTTPException(503, 'Userbot nicht verbunden')
    if '..' in body.filename:
        raise HTTPException(400, 'Invalid path')
    fpath = os.path.join(VAULT_DIR, body.filename)
    if not os.path.isfile(fpath):
        raise HTTPException(404, 'File not found in vault')
    try:
        with db() as conn:
            with conn.cursor() as c:
                c.execute('SELECT tg_access_hash FROM conversations WHERE tg_id=%s', (body.tg_id,))
                row = c.fetchone()
        ah = int(row['tg_access_hash']) if row and row['tg_access_hash'] else 0
        peer = InputPeerUser(int(body.tg_id), ah) if ah else int(body.tg_id)
        await tg_client.send_file(peer, fpath, caption=body.caption or None)
        display_name = body.filename.split('/')[-1]
        save_msg(body.tg_id, f'[📎 {display_name}]', 'out', 'Vault')
        asyncio.create_task(ws_manager.broadcast({
            'type': 'new_message', 'tg_id': body.tg_id,
            'text': f'[📎 {display_name}]', 'direction': 'out',
            'timestamp': datetime.now().isoformat()
        }))
        return {'ok': True}
    except Exception as e:
        raise HTTPException(500, str(e))

# ── PLEDGES ──────────────────────────────────────────────────────────────────

class PledgeIn(BaseModel):
    tg_id: str
    anon_id: str
    amount: float
    payment_method: str = ''
    notes: str = ''
    chatter: str = ''
    deadline: str  # ISO date string e.g. "2026-06-10"

@app.post('/pledges')
def create_pledge(body: PledgeIn):
    ts = datetime.now().isoformat()
    with db() as conn:
        with conn.cursor() as c:
            c.execute(
                'INSERT INTO pledges (tg_id,anon_id,amount,payment_method,notes,chatter,deadline,status,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id',
                (body.tg_id, body.anon_id, body.amount, body.payment_method,
                 body.notes, body.chatter, body.deadline, 'open', ts)
            )
            pid = c.fetchone()['id']
    return {'ok': True, 'id': pid}

@app.get('/pledges')
def get_pledges(status: str = ''):
    with db() as conn:
        with conn.cursor() as c:
            if status:
                c.execute('SELECT * FROM pledges WHERE status=%s ORDER BY deadline ASC', (status,))
            else:
                c.execute("SELECT * FROM pledges WHERE status != 'cancelled' ORDER BY deadline ASC")
            rows = c.fetchall()
    now = datetime.now().isoformat()[:10]
    result = []
    for r in rows:
        d = dict(r)
        d['overdue'] = d['status'] == 'open' and d['deadline'] < now
        result.append(d)
    return result

@app.get('/pledges/summary')
def get_pledges_summary():
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT status, SUM(amount) as total, COUNT(*) as count FROM pledges GROUP BY status")
            rows = c.fetchall()
    now = datetime.now().isoformat()[:10]
    summary = {r['status']: {'total': float(r['total'] or 0), 'count': r['count']} for r in rows}
    # Also get overdue count
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT COUNT(*) as n, SUM(amount) as t FROM pledges WHERE status='open' AND deadline < %s", (now,))
            row = c.fetchone()
    summary['overdue'] = {'count': row['n'], 'total': float(row['t'] or 0)}
    return summary

@app.get('/pledges/{tg_id}')
def get_subscriber_pledges(tg_id: str):
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT * FROM pledges WHERE tg_id=%s AND status != 'cancelled' ORDER BY deadline DESC", (tg_id,))
            rows = c.fetchall()
    now = datetime.now().isoformat()[:10]
    result = []
    for r in rows:
        d = dict(r)
        d['overdue'] = d['status'] == 'open' and d['deadline'] < now
        result.append(d)
    return result

@app.post('/pledges/{pledge_id}/pay')
async def mark_pledge_paid(pledge_id: int):
    """Mark pledge as paid and auto-create an approved sale."""
    ts = datetime.now().isoformat()
    with db() as conn:
        with conn.cursor() as c:
            c.execute('SELECT * FROM pledges WHERE id=%s', (pledge_id,))
            p = c.fetchone()
    if not p:
        raise HTTPException(404, 'Pledge not found')
    # Create approved sale
    with db() as conn:
        with conn.cursor() as c:
            c.execute(
                "INSERT INTO sales (tg_id,anon_id,amount,product,notes,chatter,timestamp,status,payment_method) VALUES (%s,%s,%s,%s,%s,%s,%s,'approved',%s) RETURNING id",
                (p['tg_id'], p['anon_id'], p['amount'],
                 p['payment_method'] or 'Pledge',
                 f'Pledge #{pledge_id}: {p["notes"]}',
                 p['chatter'], ts, p['payment_method'])
            )
            c.execute("UPDATE pledges SET status='paid', paid_at=%s WHERE id=%s", (ts, pledge_id))
    asyncio.create_task(ws_manager.broadcast({
        'type': 'notification', 'notif_type': 'sale',
        'tg_id': p['tg_id'], 'anon_id': p['anon_id'],
        'amount': p['amount'], 'product': 'Pledge paid',
        'chatter': p['chatter'], 'timestamp': ts,
    }))
    return {'ok': True}

@app.post('/pledges/{pledge_id}/cancel')
def cancel_pledge(pledge_id: int):
    with db() as conn:
        with conn.cursor() as c:
            c.execute("UPDATE pledges SET status='cancelled' WHERE id=%s", (pledge_id,))
    return {'ok': True}

# ── PAYMENTS / YOUSAFE CSV IMPORT ────────────────────────────────────────────

@app.post('/payments/import-csv')
async def import_payments_csv(file: UploadFile = File(...)):
    """Parse a bank statement CSV and auto-match transactions to subscribers by payment_ref."""
    import csv as _csv, io as _io
    content = (await file.read()).decode('utf-8-sig', errors='replace')
    reader = _csv.DictReader(_io.StringIO(content))
    rows = list(reader)

    # Normalise header names (YouSafe / generic SEPA formats differ)
    def _find_col(headers, *candidates):
        for h in headers:
            for c in candidates:
                if c.lower() in h.lower():
                    return h
        return None

    headers = reader.fieldnames or []
    col_date   = _find_col(headers, 'date','datum','buchungstag','value date')
    col_amount = _find_col(headers, 'amount','betrag','credit','debit','value')
    col_ref    = _find_col(headers, 'reference','verwendungszweck','purpose','description','remark','memo','details')
    col_sender = _find_col(headers, 'name','sender','auftraggeber','originator','from','debtor')

    # Load all subscribers with their payment_ref
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT tg_id, anon_id, internal_name, payment_ref FROM conversations WHERE payment_ref != '' AND payment_ref IS NOT NULL")
            subs = {r['payment_ref'].upper(): dict(r) for r in c.fetchall()}

    matched = []
    unmatched = []
    ts_now = datetime.now().isoformat()

    for row in rows:
        raw_amount = row.get(col_amount, '0').replace(',', '.').replace('€','').strip() if col_amount else '0'
        try:
            amount = abs(float(raw_amount))
        except ValueError:
            continue
        if amount <= 0:
            continue

        ref_text  = row.get(col_ref, '').upper() if col_ref else ''
        date_text = row.get(col_date, '') if col_date else ''
        sender    = row.get(col_sender, '') if col_sender else ''

        # Try to find ZF-XXXX pattern in reference
        import re as _re
        match = _re.search(r'ZF[-\s]?(\d{4,})', ref_text)
        matched_sub = None
        if match:
            code = f'ZF-{match.group(1)}'
            matched_sub = subs.get(code)

        if matched_sub:
            # Create approved sale immediately
            with db() as conn:
                with conn.cursor() as c:
                    c.execute(
                        "INSERT INTO sales (tg_id,anon_id,amount,product,notes,chatter,timestamp,status,payment_method) VALUES (%s,%s,%s,%s,%s,%s,%s,'approved','YouSafe') RETURNING id",
                        (matched_sub['tg_id'], matched_sub['anon_id'], amount,
                         'YouSafe Transfer', f'CSV import · {date_text} · {sender}',
                         'CSV Import', ts_now)
                    )
                    sale_id = c.fetchone()['id']
            matched.append({
                'date': date_text, 'amount': amount, 'sender': sender,
                'ref': ref_text[:80], 'subscriber': matched_sub['internal_name'] or matched_sub['anon_id'],
                'tg_id': matched_sub['tg_id'], 'sale_id': sale_id
            })
        else:
            unmatched.append({
                'date': date_text, 'amount': amount,
                'sender': sender, 'ref': ref_text[:80]
            })

    return {'ok': True, 'matched': matched, 'unmatched': unmatched,
            'total': len(rows), 'matched_count': len(matched), 'unmatched_count': len(unmatched)}

@app.post('/payments/match-manual')
async def match_manual_payment(tg_id: str, amount: float, date: str = '', sender: str = '', notes: str = ''):
    """Manually match an unmatched transaction to a subscriber."""
    with db() as conn:
        with conn.cursor() as c:
            c.execute('SELECT anon_id, internal_name FROM conversations WHERE tg_id=%s', (tg_id,))
            row = c.fetchone()
    if not row:
        raise HTTPException(404, 'Subscriber not found')
    ts = datetime.now().isoformat()
    with db() as conn:
        with conn.cursor() as c:
            c.execute(
                "INSERT INTO sales (tg_id,anon_id,amount,product,notes,chatter,timestamp,status,payment_method) VALUES (%s,%s,%s,%s,%s,%s,%s,'approved','YouSafe') RETURNING id",
                (tg_id, row['anon_id'], amount,
                 'YouSafe Transfer', f'{notes} · {date} · {sender}'.strip(' ·'),
                 'Manual Match', ts)
            )
    return {'ok': True}

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

class SaleSubmitIn(BaseModel):
    tg_id: str
    anon_id: str
    amount: float
    product: str = ''
    notes: str = ''
    chatter: str = 'Chatter'
    is_admin: bool = False
    payment_method: str = ''
    payment_code: str = ''  # Paysafe 16-digit or Amazon gift card code

@app.post('/sale')
async def post_sale(body: SaleSubmitIn):
    ts = datetime.now().isoformat()
    # Admins self-approve; chatters start as pending
    status = 'approved' if body.is_admin else 'pending'
    sale_id = None
    with db() as conn:
        with conn.cursor() as c:
            c.execute(
                'INSERT INTO sales (tg_id,anon_id,amount,product,notes,chatter,timestamp,status,payment_method,payment_code) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id',
                (body.tg_id, body.anon_id, body.amount, body.product, body.notes, body.chatter, ts, status, body.payment_method, body.payment_code)
            )
            sale_id = c.fetchone()['id']
    if status == 'approved':
        make_url = os.environ.get('MAKE_SALE_WEBHOOK', '')
        if make_url:
            _fire_webhook_sync(make_url, {
                'tg_id': body.tg_id, 'anon_id': body.anon_id,
                'amount': body.amount, 'product': body.product,
                'notes': body.notes, 'chatter': body.chatter, 'timestamp': ts
            })
        asyncio.create_task(ws_manager.broadcast({
            'type': 'notification', 'notif_type': 'sale',
            'tg_id': body.tg_id, 'anon_id': body.anon_id,
            'amount': body.amount, 'product': body.product,
            'chatter': body.chatter, 'timestamp': ts,
        }))
    else:
        # Notify admin of new pending sale
        asyncio.create_task(ws_manager.broadcast({
            'type': 'pending_sale',
            'sale_id': sale_id, 'tg_id': body.tg_id, 'anon_id': body.anon_id,
            'amount': body.amount, 'product': body.product,
            'chatter': body.chatter, 'timestamp': ts,
        }))
    return {'ok': True, 'id': sale_id, 'status': status, 'timestamp': ts}

@app.post('/sale/{sale_id}/screenshot')
async def upload_sale_screenshot(sale_id: int, file: UploadFile = File(...)):
    """Attach a proof screenshot to a sale."""
    ext = (file.filename or 'proof.jpg').rsplit('.', 1)[-1].lower()
    if ext not in ('jpg','jpeg','png','gif','webp','mp4'):
        raise HTTPException(400, 'Invalid file type')
    fname = f'sale_{sale_id}_{int(datetime.now().timestamp())}.{ext}'
    fpath = os.path.join(PROOFS_DIR, fname)
    with open(fpath, 'wb') as f:
        shutil.copyfileobj(file.file, f)
    with db() as conn:
        with conn.cursor() as c:
            c.execute('UPDATE sales SET screenshot=%s WHERE id=%s', (fname, sale_id))
    return {'ok': True, 'screenshot': fname}

@app.get('/sale/{sale_id}/screenshot')
def serve_sale_screenshot(sale_id: int):
    """Serve the proof screenshot for a sale."""
    with db() as conn:
        with conn.cursor() as c:
            c.execute('SELECT screenshot FROM sales WHERE id=%s', (sale_id,))
            row = c.fetchone()
    if not row or not row['screenshot']:
        raise HTTPException(404, 'No screenshot')
    fpath = os.path.join(PROOFS_DIR, row['screenshot'])
    if not os.path.isfile(fpath):
        raise HTTPException(404, 'File not found')
    return FileResponse(fpath)

class ReviewIn(BaseModel):
    reviewed_by: str = 'Admin'
    reason: str = ''

@app.post('/sale/{sale_id}/approve')
async def approve_sale(sale_id: int, body: ReviewIn):
    """Admin approves a pending sale."""
    ts = datetime.now().isoformat()
    with db() as conn:
        with conn.cursor() as c:
            c.execute(
                'UPDATE sales SET status=%s, reviewed_by=%s, reviewed_at=%s WHERE id=%s RETURNING tg_id,anon_id,amount,product,chatter,timestamp',
                ('approved', body.reviewed_by, ts, sale_id)
            )
            row = c.fetchone()
    if not row:
        raise HTTPException(404, 'Sale not found')
    make_url = os.environ.get('MAKE_SALE_WEBHOOK', '')
    if make_url:
        _fire_webhook_sync(make_url, {
            'tg_id': row['tg_id'], 'anon_id': row['anon_id'],
            'amount': row['amount'], 'product': row['product'],
            'notes': '', 'chatter': row['chatter'], 'timestamp': row['timestamp']
        })
    asyncio.create_task(ws_manager.broadcast({
        'type': 'notification', 'notif_type': 'sale',
        'tg_id': row['tg_id'], 'anon_id': row['anon_id'],
        'amount': row['amount'], 'product': row['product'],
        'chatter': row['chatter'], 'timestamp': row['timestamp'],
    }))
    asyncio.create_task(ws_manager.broadcast({
        'type': 'sale_reviewed', 'sale_id': sale_id, 'status': 'approved'
    }))
    return {'ok': True}

@app.post('/sale/{sale_id}/reject')
async def reject_sale(sale_id: int, body: ReviewIn):
    """Admin rejects a pending sale with a reason."""
    ts = datetime.now().isoformat()
    with db() as conn:
        with conn.cursor() as c:
            c.execute(
                'UPDATE sales SET status=%s, reviewed_by=%s, reviewed_at=%s, rejection_reason=%s WHERE id=%s',
                ('rejected', body.reviewed_by, ts, body.reason, sale_id)
            )
    asyncio.create_task(ws_manager.broadcast({
        'type': 'sale_reviewed', 'sale_id': sale_id, 'status': 'rejected', 'reason': body.reason
    }))
    return {'ok': True}

@app.get('/sales/pending')
def get_pending_sales():
    """List all pending sales for admin review."""
    with db() as conn:
        with conn.cursor() as c:
            c.execute(
                "SELECT id,tg_id,anon_id,amount,product,notes,chatter,timestamp,screenshot FROM sales WHERE status='pending' ORDER BY timestamp DESC",
            )
            rows = c.fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d['screenshot_url'] = f'/sale/{r["id"]}/screenshot' if r['screenshot'] else ''
        result.append(d)
    return result

@app.get('/sales/codes')
def get_sale_codes():
    """Return all Paysafe and Amazon codes with their sale status."""
    with db() as conn:
        with conn.cursor() as c:
            c.execute(
                "SELECT id,anon_id,amount,payment_method,payment_code,status,timestamp,chatter FROM sales WHERE payment_code != '' AND payment_code IS NOT NULL ORDER BY timestamp DESC"
            )
            rows = c.fetchall()
    return [dict(r) for r in rows]

@app.get('/sales')
def get_sales(limit: int = 200):
    with db() as conn:
        with conn.cursor() as c:
            c.execute(
                'SELECT id,tg_id,anon_id,amount,product,notes,chatter,timestamp,status,screenshot,rejection_reason,payment_method,payment_code FROM sales ORDER BY timestamp DESC LIMIT %s',
                (limit,)
            )
            rows = c.fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d['screenshot_url'] = f'/sale/{r["id"]}/screenshot' if r['screenshot'] else ''
        result.append(d)
    return result

@app.get('/messages/{tg_id}/{msg_id}/media')
async def download_message_media(tg_id: str, msg_id: int):
    """Download media from a specific Telegram message and return it."""
    if not tg_client or not tg_client.is_connected():
        raise HTTPException(503, 'Userbot not connected')
    try:
        with db() as conn:
            with conn.cursor() as c:
                c.execute('SELECT tg_access_hash FROM conversations WHERE tg_id=%s', (tg_id,))
                row = c.fetchone()
        ah = int(row['tg_access_hash']) if row and row['tg_access_hash'] else 0
        peer = InputPeerUser(int(tg_id), ah) if ah else int(tg_id)
        msgs = await tg_client.get_messages(peer, ids=msg_id)
        if not msgs or not msgs.media:
            raise HTTPException(404, 'No media in this message')
        # Download to temp file in proofs dir
        tmp_path = os.path.join(PROOFS_DIR, f'tmp_{tg_id}_{msg_id}')
        path = await tg_client.download_media(msgs, file=tmp_path)
        if not path or not os.path.isfile(path):
            raise HTTPException(500, 'Download failed')
        return FileResponse(path, filename=os.path.basename(path))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))

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

# ── FAKE CALLS ───────────────────────────────────────────────────────────────

CALL_ALLOWED_EXTS = {'mp3','mp4','wav','ogg','aac','m4a','mov','mkv','flac'}

@app.get('/call/status')
def call_status():
    """Check if pytgcalls is available and how many calls are active."""
    return {
        'pytgcalls_available': _PYTGCALLS_OK,
        'pytgcalls_error': _PYTGCALLS_ERR,
        'calls_client_ready': calls_client is not None,
        'active': len(active_calls),
        'active_calls': active_calls,
    }

@app.get('/call/files')
def get_call_files():
    """List uploaded call recordings."""
    files = []
    try:
        for fname in sorted(os.listdir(CALLS_DIR)):
            fpath = os.path.join(CALLS_DIR, fname)
            if os.path.isfile(fpath) and not fname.startswith('.'):
                ext = fname.rsplit('.',1)[-1].lower() if '.' in fname else ''
                ftype = 'video' if ext in ('mp4','mov','mkv') else 'audio'
                files.append({
                    'name': fname,
                    'size': os.path.getsize(fpath),
                    'type': ftype,
                    'url': f'/call/file/{fname}',
                })
    except Exception as e:
        print(f'call files error: {e}')
    return files

@app.post('/call/upload')
async def upload_call_file(file: UploadFile = File(...)):
    """Upload a call recording (audio or video)."""
    ext = (file.filename or 'call.mp3').rsplit('.',1)[-1].lower()
    if ext not in CALL_ALLOWED_EXTS:
        raise HTTPException(400, f'File type .{ext} not allowed. Use: {", ".join(CALL_ALLOWED_EXTS)}')
    safe_name = _vault_safe(file.filename or f'call.{ext}') or f'call.{ext}'
    fpath = os.path.join(CALLS_DIR, safe_name)
    if os.path.exists(fpath):
        base, e = os.path.splitext(safe_name)
        safe_name = f'{base}_{int(datetime.now().timestamp())}{e}'
        fpath = os.path.join(CALLS_DIR, safe_name)
    try:
        with open(fpath, 'wb') as f:
            shutil.copyfileobj(file.file, f)
    except OSError as e:
        if 'No space left' in str(e):
            raise HTTPException(507, 'Disk full')
        raise HTTPException(500, str(e))
    return {'ok': True, 'name': safe_name}

@app.delete('/call/file/{filename}')
def delete_call_file(filename: str):
    if '..' in filename:
        raise HTTPException(400, 'Invalid')
    fpath = os.path.join(CALLS_DIR, filename)
    if not os.path.isfile(fpath):
        raise HTTPException(404, 'Not found')
    os.remove(fpath)
    return {'ok': True}

@app.get('/call/file/{filename}')
def serve_call_file(filename: str):
    if '..' in filename:
        raise HTTPException(400, 'Invalid')
    fpath = os.path.join(CALLS_DIR, filename)
    if not os.path.isfile(fpath):
        raise HTTPException(404, 'Not found')
    return FileResponse(fpath)

class CallStartIn(BaseModel):
    tg_id: str
    filename: str
    chatter: str = 'Chatter'

@app.post('/call/start')
async def start_fake_call(body: CallStartIn):
    """Initiate a pre-recorded call to a subscriber via Telegram."""
    if not _PYTGCALLS_OK:
        raise HTTPException(503, 'pytgcalls not installed on Railway. Add "py-tgcalls" to requirements.txt and redeploy.')
    if not calls_client:
        raise HTTPException(503, 'Calls client not ready yet (wait a few seconds after startup).')
    if body.tg_id in active_calls:
        raise HTTPException(409, 'A call is already active with this subscriber.')

    fpath = os.path.join(CALLS_DIR, body.filename)
    if '..' in body.filename or not os.path.isfile(fpath):
        raise HTTPException(404, 'Recording file not found.')

    with db() as conn:
        with conn.cursor() as c:
            c.execute('SELECT tg_access_hash FROM conversations WHERE tg_id=%s', (body.tg_id,))
            row = c.fetchone()

    ah = int(row['tg_access_hash']) if row and row['tg_access_hash'] else 0
    peer = InputPeerUser(int(body.tg_id), ah) if ah else int(body.tg_id)

    ext = body.filename.rsplit('.',1)[-1].lower() if '.' in body.filename else ''
    is_video = ext in ('mp4','mov','mkv')

    try:
        if is_video:
            from pytgcalls.types import VideoQuality
            stream = MediaStream(fpath, video_parameters=VideoQuality.HD_720p)
        else:
            stream = MediaStream(fpath, audio_parameters=AudioQuality.HIGH)
        await calls_client.call(peer, stream)
        now_ts = datetime.now().isoformat()
        active_calls[body.tg_id] = {
            'file': body.filename,
            'chatter': body.chatter,
            'started_at': now_ts,
            'type': 'video' if is_video else 'audio',
        }
        save_msg(body.tg_id, f'[📞 Pre-recorded {"Video" if is_video else "Audio"} Call – {body.filename}]', 'out', body.chatter)
        asyncio.create_task(ws_manager.broadcast({
            'type': 'call_started',
            'tg_id': body.tg_id,
            'file': body.filename,
            'call_type': 'video' if is_video else 'audio',
            'chatter': body.chatter,
        }))
        return {'ok': True, 'type': 'video' if is_video else 'audio'}
    except Exception as e:
        raise HTTPException(500, f'Call failed: {e}')

@app.post('/call/stop')
async def stop_fake_call(tg_id: str):
    """Hang up the active call with a subscriber."""
    if not calls_client:
        raise HTTPException(503, 'Calls client not ready.')
    with db() as conn:
        with conn.cursor() as c:
            c.execute('SELECT tg_access_hash FROM conversations WHERE tg_id=%s', (tg_id,))
            row = c.fetchone()
    ah = int(row['tg_access_hash']) if row and row['tg_access_hash'] else 0
    peer = InputPeerUser(int(tg_id), ah) if ah else int(tg_id)
    try:
        await calls_client.leave_call(peer)
    except Exception as e:
        print(f'leave_call error (may already be ended): {e}')
    active_calls.pop(tg_id, None)
    asyncio.create_task(ws_manager.broadcast({'type': 'call_ended', 'tg_id': tg_id}))
    return {'ok': True}

# ── START ─────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=PORT)
