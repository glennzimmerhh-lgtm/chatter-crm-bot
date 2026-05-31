"""
Chatter CRM – Subscriber Chat Backend
Telethon Userbot + FastAPI REST API

Subscriber schreibt das echte Telegram-Konto an.
Userbot fängt ab → speichert in SQLite → CRM-Dashboard zeigt es dem Chatter.
Chatter antwortet im Dashboard → geht raus als echter Account.
Subscriber sieht nie einen Bot.

Umgebungsvariablen (Railway):
  TG_API_ID    – von my.telegram.org
  TG_API_HASH  – von my.telegram.org
  TG_SESSION   – StringSession (über /setup generieren)
  PORT         – automatisch von Railway gesetzt
"""

from __future__ import annotations


import asyncio
import os
import sqlite3
from datetime import datetime
from contextlib import asynccontextmanager
from typing import Optional

import uvicorn
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
DB_PATH     = os.environ.get('DB_PATH', '/data/chatter_crm.db')
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# ── SETUP STATE (in-memory, nur für /setup flow) ──────────────────────────────
setup_client: Optional[TelegramClient] = None
setup_phone: str = ''

# ── DATABASE ──────────────────────────────────────────────────────────────────
def db():
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')   # crash-safe writes
    conn.execute('PRAGMA synchronous=NORMAL') # fast + safe
    conn.execute('PRAGMA foreign_keys=ON')
    return conn

def init_db():
    with db() as c:
        c.execute('''CREATE TABLE IF NOT EXISTS conversations (
            tg_id         TEXT PRIMARY KEY,
            anon_id       TEXT NOT NULL,
            internal_name TEXT DEFAULT '',
            notes         TEXT DEFAULT '',
            last_msg      TEXT DEFAULT '',
            last_time     TEXT DEFAULT '',
            first_time    TEXT DEFAULT '',
            unread        INTEGER DEFAULT 0,
            msg_count     INTEGER DEFAULT 0
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS messages (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id     TEXT NOT NULL,
            text      TEXT NOT NULL,
            direction TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            chatter   TEXT DEFAULT ''
        )''')
        for col, default in [
            ('internal_name', "''"),
            ('notes',         "''"),
            ('first_time',    "''"),
            ('msg_count',     '0'),
        ]:
            try:
                c.execute(f'ALTER TABLE conversations ADD COLUMN {col} TEXT DEFAULT {default}')
                c.commit()
            except Exception:
                pass

def ensure_conv(tg_id: str) -> str:
    with db() as c:
        row = c.execute('SELECT anon_id FROM conversations WHERE tg_id=?', (tg_id,)).fetchone()
        if row:
            return row['anon_id']
        n = c.execute('SELECT COUNT(*) FROM conversations').fetchone()[0]
        anon_id = f'User #{1001 + n}'
        now = datetime.now().isoformat()
        c.execute(
            'INSERT INTO conversations (tg_id,anon_id,last_msg,last_time,first_time,unread,msg_count) VALUES (?,?,?,?,?,1,0)',
            (tg_id, anon_id, '', now, now)
        )
        c.commit()
        return anon_id

def save_msg(tg_id: str, text: str, direction: str, chatter: str = ''):
    ts = datetime.now().isoformat()
    with db() as c:
        c.execute(
            'INSERT INTO messages (tg_id,text,direction,timestamp,chatter) VALUES (?,?,?,?,?)',
            (tg_id, text, direction, ts, chatter)
        )
        c.execute(
            '''UPDATE conversations
               SET last_msg=?, last_time=?, unread=unread+?, msg_count=msg_count+1
               WHERE tg_id=?''',
            (text[:100], ts, 1 if direction == 'in' else 0, tg_id)
        )
        c.commit()

# ── USERBOT ───────────────────────────────────────────────────────────────────
tg_client: Optional[TelegramClient] = None

async def start_userbot():
    global tg_client
    if not (TG_API_ID and TG_API_HASH and TG_SESSION):
        print('⚠️  Userbot nicht gestartet – TG_API_ID, TG_API_HASH, TG_SESSION fehlen')
        return

    tg_client = TelegramClient(StringSession(TG_SESSION), int(TG_API_ID), TG_API_HASH)

    @tg_client.on(events.NewMessage(incoming=True, func=lambda e: e.is_private))
    async def on_dm(event):
        sender = await event.get_sender()
        if not isinstance(sender, User) or sender.bot:
            return
        tg_id = str(sender.id)
        ensure_conv(tg_id)

        if event.text:
            text = event.text
        elif event.photo:
            text = '[📷 Foto]'
        elif event.document:
            text = '[📎 Datei]'
        elif event.sticker:
            text = '[Sticker]'
        elif event.voice:
            text = '[🎤 Sprachnachricht]'
        else:
            text = '[Nachricht]'

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

app = FastAPI(title='Chatter CRM Chat API', lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_methods=['*'],
    allow_headers=['*'],
)

# ── SETUP SEITE ───────────────────────────────────────────────────────────────

SETUP_HTML = """<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Chatter CRM – Setup</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, sans-serif; background: #0f0f0f; color: #eee; display: flex; align-items: center; justify-content: center; min-height: 100vh; padding: 20px; }
  .card { background: #1a1a1a; border: 1px solid #333; border-radius: 16px; padding: 40px; width: 100%; max-width: 480px; }
  h1 { font-size: 22px; margin-bottom: 6px; }
  .sub { color: #888; font-size: 14px; margin-bottom: 32px; }
  label { display: block; font-size: 13px; color: #aaa; margin-bottom: 6px; }
  input { width: 100%; background: #111; border: 1px solid #333; border-radius: 8px; color: #eee; padding: 12px 14px; font-size: 15px; margin-bottom: 18px; outline: none; }
  input:focus { border-color: #6c63ff; }
  button { width: 100%; background: #6c63ff; color: #fff; border: none; border-radius: 8px; padding: 14px; font-size: 16px; font-weight: 600; cursor: pointer; }
  button:hover { background: #5a52e0; }
  .result { background: #111; border: 1px solid #6c63ff; border-radius: 8px; padding: 16px; margin-top: 24px; word-break: break-all; font-family: monospace; font-size: 12px; color: #a0f0a0; }
  .result h3 { color: #6c63ff; margin-bottom: 10px; font-size: 14px; }
  .copy-btn { background: #333; color: #eee; border: none; border-radius: 6px; padding: 8px 16px; cursor: pointer; font-size: 13px; margin-top: 12px; width: 100%; }
  .copy-btn:hover { background: #444; }
  .error { background: #2a1010; border: 1px solid #f55; border-radius: 8px; padding: 14px; margin-top: 16px; color: #f88; font-size: 14px; }
  .step { color: #6c63ff; font-size: 13px; margin-bottom: 20px; }
  .hint { color: #666; font-size: 12px; margin-top: -12px; margin-bottom: 18px; }
</style>
</head>
<body>
<div class="card">
  <h1>🔐 Chatter CRM Setup</h1>
  <p class="sub">Einmalig ausführen um den Telegram Session-String zu generieren</p>
  {CONTENT}
</div>
<script>
function copySession() {
  const el = document.getElementById('session-str');
  navigator.clipboard.writeText(el.innerText).then(() => {
    document.getElementById('copy-btn').innerText = '✓ Kopiert!';
  });
}
</script>
</body>
</html>"""

FORM_STEP1 = """
<p class="step">Schritt 1 von 2 – API-Zugangsdaten</p>
<form method="POST" action="/setup/send-code">
  <label>API ID <small style="color:#666">(von my.telegram.org)</small></label>
  <input name="api_id" type="number" placeholder="12345678" required>
  <label>API Hash <small style="color:#666">(von my.telegram.org)</small></label>
  <input name="api_hash" type="text" placeholder="abcdef1234..." required>
  <label>Telefonnummer <small style="color:#666">(mit Ländervorwahl, z.B. +49...)</small></label>
  <input name="phone" type="text" placeholder="+49151..." required>
  <button type="submit">SMS-Code anfordern →</button>
</form>
"""

def render_setup(content: str, extra: str = '') -> HTMLResponse:
    html = SETUP_HTML.replace('{CONTENT}', content + extra)
    return HTMLResponse(html)

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

        form2 = f"""
        <p class="step">Schritt 2 von 2 – SMS-Code eingeben</p>
        <p style="color:#aaa;font-size:14px;margin-bottom:20px;">Code wurde an <strong>{phone}</strong> gesendet.</p>
        <form method="POST" action="/setup/verify">
          <label>SMS-Code</label>
          <input name="code" type="text" placeholder="12345" required autofocus>
          <p class="hint">Nur die Zahlen eingeben, kein Leerzeichen</p>
          <button type="submit">Session generieren ✓</button>
        </form>
        """
        return render_setup(form2)
    except Exception as e:
        return render_setup(FORM_STEP1, f'<div class="error">❌ Fehler: {e}</div>')

@app.post('/setup/verify', response_class=HTMLResponse)
async def setup_verify(code: str = Form(...)):
    global setup_client, setup_phone
    try:
        await setup_client.sign_in(setup_phone, code.strip())
        session_str = setup_client.session.save()
        await setup_client.disconnect()

        result = f"""
        <div class="result">
          <h3>✅ Dein TG_SESSION String:</h3>
          <div id="session-str">{session_str}</div>
          <button class="copy-btn" id="copy-btn" onclick="copySession()">📋 Kopieren</button>
        </div>
        <p style="color:#888;font-size:13px;margin-top:20px;">
          Jetzt auf Railway unter <strong>Variables → TG_SESSION</strong> einfügen und Service neu starten.
        </p>
        """
        return render_setup(result)
    except Exception as e:
        err = str(e)
        form2 = f"""
        <p class="step">Schritt 2 von 2 – SMS-Code eingeben</p>
        <form method="POST" action="/setup/verify">
          <label>SMS-Code</label>
          <input name="code" type="text" placeholder="12345" required autofocus>
          <button type="submit">Session generieren ✓</button>
        </form>
        <div class="error">❌ Fehler: {err}</div>
        """
        return render_setup(form2)

# ── ENDPOINTS ─────────────────────────────────────────────────────────────────


@app.get('/healthz')
def healthz():
    """Health check – also verifies DB is readable."""
    try:
        with db() as c:
            n = c.execute('SELECT COUNT(*) FROM conversations').fetchone()[0]
        ok = tg_client is not None and tg_client.is_connected()
        return {'status': 'ok', 'conversations': n, 'userbot': 'connected' if ok else 'disconnected', 'db': DB_PATH}
    except Exception as e:
        return {'status': 'error', 'detail': str(e)}

@app.get('/status')
def status():
    ok = tg_client is not None and tg_client.is_connected()
    return {'userbot': 'connected' if ok else 'disconnected'}

@app.get('/conversations')
def get_conversations():
    with db() as c:
        rows = c.execute(
            '''SELECT tg_id,anon_id,internal_name,notes,last_msg,last_time,first_time,unread,msg_count
               FROM conversations ORDER BY last_time DESC'''
        ).fetchall()
    return [dict(r) for r in rows]

@app.get('/profile/{tg_id}')
def get_profile(tg_id: str):
    with db() as c:
        row = c.execute(
            'SELECT tg_id,anon_id,internal_name,notes,last_time,first_time,unread,msg_count FROM conversations WHERE tg_id=?',
            (tg_id,)
        ).fetchone()
    if not row:
        raise HTTPException(404, 'Subscriber nicht gefunden')
    return dict(row)

class ProfileUpdate(BaseModel):
    internal_name: Optional[str] = None
    notes: Optional[str] = None

@app.patch('/profile/{tg_id}')
def update_profile(tg_id: str, body: ProfileUpdate):
    with db() as c:
        if body.internal_name is not None:
            c.execute('UPDATE conversations SET internal_name=? WHERE tg_id=?', (body.internal_name, tg_id))
        if body.notes is not None:
            c.execute('UPDATE conversations SET notes=? WHERE tg_id=?', (body.notes, tg_id))
        c.commit()
    return {'ok': True}

@app.get('/messages/{tg_id}')
def get_messages(tg_id: str):
    with db() as c:
        c.execute('UPDATE conversations SET unread=0 WHERE tg_id=?', (tg_id,))
        c.commit()
        rows = c.execute(
            'SELECT text,direction,timestamp,chatter FROM messages WHERE tg_id=? ORDER BY timestamp',
            (tg_id,)
        ).fetchall()
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

# ── START ─────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=PORT)
