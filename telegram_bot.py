"""
Chatter CRM – Telegram Bot
Sendet Sale- und Shift-Daten an Make.com Webhook → Google Sheets
Screenshot-Zahlungsbeweise werden automatisch archiviert.
Tägliche Zusammenfassung wird um 23:59 Uhr gesendet.
"""

import requests
from datetime import datetime, time
from telegram import Update
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    ContextTypes, filters
)

# ── KONFIGURATION ─────────────────────────────────────────
TELEGRAM_TOKEN   = "8818652149:AAH2MvYD3Ijt7BD8Pk0SYRdfw8t32__rtAg"
MAKE_WEBHOOK_URL = "https://hook.eu1.make.com/lms52ngkdj1o467avqvt65jh7otwnw2p"

# Optional: Telegram-Kanal/-Gruppe für Screenshot-Archiv
# Leer lassen ("") um die Funktion zu deaktivieren
PROOF_CHANNEL_ID = ""   # z.B. "-1001234567890" oder "@dein_kanal"

# Telegram User IDs die die tägliche Summary erhalten sollen
# Deine eigene ID findest du über @userinfobot
SUMMARY_RECEIVER_IDS = []  # z.B. [123456789, 987654321]

# Shift-Zeiten (Stunde: 0-23)
SHIFTS = [
    {"number": 1, "name": "Shift 1", "code": "S1", "start": "00:00", "end": "08:00", "hours": range(0, 8)},
    {"number": 2, "name": "Shift 2", "code": "S2", "start": "08:00", "end": "16:00", "hours": range(8, 16)},
    {"number": 3, "name": "Shift 3", "code": "S3", "start": "16:00", "end": "00:00", "hours": range(16, 24)},
]

# Session: merkt sich aktive Shift-Daten pro Chatter
sessions = {}
# ──────────────────────────────────────────────────────────


def get_current_shift():
    h = datetime.now().hour
    for s in SHIFTS:
        if h in s["hours"]:
            return s
    return SHIFTS[2]


def post_to_make(data: dict) -> bool:
    try:
        r = requests.post(MAKE_WEBHOOK_URL, json=data, timeout=10)
        return r.status_code == 200
    except Exception as e:
        print(f"Make Webhook Fehler: {e}")
        return False


# ── BEFEHLE ───────────────────────────────────────────────

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📋 *Chatter CRM Bot*\n\n"
        "*/startshift [Name]* – Shift starten\n"
        "   Beispiel: `/startshift Alex`\n\n"
        "📸 *Screenshot schicken* – Sale per Zahlungsbeweis\n"
        "   Foto weiterleiten + Betrag als Bildunterschrift\n"
        "   Beispiel: Foto + Caption `150` oder `150 Produkt XL`\n\n"
        "*/sale [Betrag] [Notiz]* – Sale manuell eintragen\n"
        "   Beispiel: `/sale 150 Produkt XL`\n\n"
        "*/endshift [Reply-Time in Min]* – Shift beenden\n"
        "   Beispiel: `/endshift 3.5`\n\n"
        "*/status* – Aktuelle Shift-Info anzeigen",
        parse_mode="Markdown"
    )


async def cmd_startshift(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chatter_name = " ".join(ctx.args) if ctx.args else update.effective_user.first_name

    shift = get_current_shift()
    sessions[user_id] = {
        "chatter": chatter_name,
        "shift": shift,
        "sales": [],
        "started_at": datetime.now().isoformat(),
    }

    now = datetime.now()
    payload = {
        "command": "startshift",
        "chatter": chatter_name,
        "shift": shift["name"],
        "shift_number": shift["number"],
        "shift_start": shift["start"],
        "shift_end": shift["end"],
        "shift_code": shift["code"],
        "date": now.strftime("%Y-%m-%d"),
        "time": now.strftime("%H:%M"),
    }
    post_to_make(payload)

    await update.message.reply_text(
        f"✅ *{shift['name']} gestartet!*\n"
        f"👤 Chatter: {chatter_name}\n"
        f"🕐 Zeit: {shift['start']} – {shift['end']} Uhr\n\n"
        f"Schick einfach den Zahlungsscreenshot mit dem Betrag als Bildunterschrift.",
        parse_mode="Markdown"
    )


async def _record_sale(update: Update, amount: float, note: str, via: str = "Manuell"):
    """Gemeinsame Logik für /sale und Screenshot-Handler."""
    user_id = update.effective_user.id
    session = sessions.get(user_id)

    if session:
        shift   = session["shift"]
        chatter = session["chatter"]
        session["sales"].append(amount)
    else:
        shift   = get_current_shift()
        chatter = update.effective_user.first_name

    now = datetime.now()
    payload = {
        "command": "sale",
        "amount": amount,
        "note": note,
        "chatter": chatter,
        "shift": shift["name"],
        "shift_code": shift["code"],
        "shift_number": shift["number"],
        "date": now.strftime("%Y-%m-%d"),
        "time": now.strftime("%H:%M"),
        "via": via,
    }
    success = post_to_make(payload)

    total = sum(session["sales"]) if session else amount
    return success, shift, chatter, total


async def cmd_sale(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("❌ Betrag fehlt. Beispiel: `/sale 150 Produkt XL`", parse_mode="Markdown")
        return

    try:
        amount = float(ctx.args[0].replace(",", "."))
    except ValueError:
        await update.message.reply_text("❌ Ungültiger Betrag. Beispiel: `/sale 150`", parse_mode="Markdown")
        return

    note = " ".join(ctx.args[1:]) if len(ctx.args) > 1 else ""
    success, shift, chatter, total = await _record_sale(update, amount, note, via="Manuell")

    if success:
        await update.message.reply_text(
            f"💰 *Sale eingetragen!*\n"
            f"Betrag: *{amount:.0f} €*\n"
            f"Notiz: {note or '—'}\n"
            f"Shift: {shift['name']}\n"
            f"Tages-Total: *{total:.0f} €*",
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text("⚠️ Fehler beim Eintragen. Make Webhook prüfen!")


async def handle_proof(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    Screenshot-Handler: Chatter schickt Foto als Zahlungsbeweis.
    Caption = Betrag + optionale Notiz. Beispiel: "150" oder "150 Produkt XL"
    Das Foto wird zusätzlich ins Proof-Archiv weitergeleitet (falls konfiguriert).
    """
    caption = (update.message.caption or "").strip()

    if not caption:
        await update.message.reply_text(
            "❌ *Betrag fehlt!*\n\n"
            "Foto nochmal schicken mit Betrag als Bildunterschrift.\n"
            "Beispiel: Foto + Caption `150` oder `150 Produkt XL`",
            parse_mode="Markdown"
        )
        return

    parts = caption.split(None, 1)
    try:
        amount = float(parts[0].replace(",", ".").replace("€", "").strip())
    except ValueError:
        await update.message.reply_text(
            f"❌ *Konnte keinen Betrag lesen.*\n"
            f"Caption war: `{caption}`\n"
            f"Bitte so schreiben: `150` oder `150 Produkt XL`",
            parse_mode="Markdown"
        )
        return

    note = parts[1].strip() if len(parts) > 1 else "Zahlungsscreenshot"
    success, shift, chatter, total = await _record_sale(update, amount, note, via="Screenshot")

    # ── Screenshot ins Archiv weiterleiten ────────────────
    if PROOF_CHANNEL_ID:
        try:
            now = datetime.now()
            archive_caption = (
                f"💰 Zahlungsbeweis\n"
                f"Datum: {now.strftime('%d.%m.%Y %H:%M')}\n"
                f"Chatter: {chatter}\n"
                f"Betrag: {amount:.0f} €\n"
                f"Notiz: {note}\n"
                f"Shift: {shift['name']}"
            )
            photo = update.message.photo[-1]  # größte Auflösung
            await ctx.bot.send_photo(
                chat_id=PROOF_CHANNEL_ID,
                photo=photo.file_id,
                caption=archive_caption,
            )
        except Exception as e:
            print(f"Archiv-Fehler: {e}")
    # ──────────────────────────────────────────────────────

    if success:
        await update.message.reply_text(
            f"✅ *Zahlung eingetragen!*\n"
            f"💰 Betrag: *{amount:.0f} €*\n"
            f"📝 Notiz: {note}\n"
            f"🕐 Shift: {shift['name']}\n"
            f"📊 Tages-Total: *{total:.0f} €*"
            + (f"\n📁 Archiviert" if PROOF_CHANNEL_ID else ""),
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text("⚠️ Fehler beim Eintragen. Make Webhook prüfen!")


async def cmd_endshift(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    reply_time = None
    if ctx.args:
        try:
            reply_time = float(ctx.args[0].replace(",", "."))
        except ValueError:
            pass

    session = sessions.get(user_id)
    if not session:
        await update.message.reply_text(
            "⚠️ Keine aktive Shift. Starte mit `/startshift`.",
            parse_mode="Markdown"
        )
        return

    shift   = session["shift"]
    chatter = session["chatter"]
    sales   = session["sales"]
    total   = sum(sales)
    count   = len(sales)

    payload = {
        "command": "endshift",
        "chatter": chatter,
        "shift": shift["name"],
        "shift_number": shift["number"],
        "shift_start": shift["start"],
        "shift_end": shift["end"],
        "shift_code": shift["code"],
        "total_revenue": total,
        "sales_count": count,
        "reply_time": reply_time or 0,
        "date": datetime.now().strftime("%Y-%m-%d"),
    }

    success = post_to_make(payload)
    sessions.pop(user_id, None)

    if success:
        reply_str = f"{reply_time:.1f} min" if reply_time else "—"
        await update.message.reply_text(
            f"🏁 *{shift['name']} beendet!*\n\n"
            f"👤 Chatter: {chatter}\n"
            f"💰 Umsatz: *{total:.0f} €*\n"
            f"🛒 Sales: {count}\n"
            f"⏱ Ø Reply Time: {reply_str}\n\n"
            f"Daten wurden ins CRM eingetragen.",
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text("⚠️ Fehler beim Eintragen. Make Webhook prüfen!")


async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    session = sessions.get(user_id)
    shift   = get_current_shift()

    if session:
        total = sum(session["sales"])
        count = len(session["sales"])
        await update.message.reply_text(
            f"📊 *Aktuelle Shift*\n\n"
            f"👤 Chatter: {session['chatter']}\n"
            f"🕐 Shift: {shift['name']} ({shift['start']} – {shift['end']})\n"
            f"💰 Bisher: *{total:.0f} €*\n"
            f"🛒 Sales: {count}",
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text(
            f"ℹ️ Aktuelle Systemzeit → *{shift['name']}*\n"
            f"Keine aktive Session. Starte mit `/startshift`.",
            parse_mode="Markdown"
        )


# ── TÄGLICHE ZUSAMMENFASSUNG ──────────────────────────────

async def send_daily_summary(ctx: ContextTypes.DEFAULT_TYPE):
    """Wird täglich um 23:59 Uhr ausgeführt."""
    if not SUMMARY_RECEIVER_IDS:
        return

    today = datetime.now().strftime("%d.%m.%Y")

    # Alle Sessions des Tages zusammenrechnen
    all_sales   = []
    all_shifts  = []
    for uid, sess in sessions.items():
        all_sales.extend(sess.get("sales", []))
        all_shifts.append(sess)

    total_rev   = sum(all_sales)
    total_sales = len(all_sales)
    shift_count = len(all_shifts)

    msg = (
        f"📊 *Tages-Zusammenfassung – {today}*\n\n"
        f"💰 Gesamtumsatz: *{total_rev:.0f} €*\n"
        f"🛒 Sales gesamt: {total_sales}\n"
        f"⏱ Aktive Shifts: {shift_count}\n\n"
        f"_Detaillierte Daten im CRM-Dashboard._"
    )

    for uid in SUMMARY_RECEIVER_IDS:
        try:
            await ctx.bot.send_message(chat_id=uid, text=msg, parse_mode="Markdown")
        except Exception as e:
            print(f"Summary-Fehler für {uid}: {e}")
# ──────────────────────────────────────────────────────────


# ── START ─────────────────────────────────────────────────

def main():
    print("🚀 Chatter CRM Bot startet...")
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("help",       cmd_help))
    app.add_handler(CommandHandler("start",      cmd_help))
    app.add_handler(CommandHandler("startshift", cmd_startshift))
    app.add_handler(CommandHandler("sale",       cmd_sale))
    app.add_handler(CommandHandler("endshift",   cmd_endshift))
    app.add_handler(CommandHandler("status",     cmd_status))
    app.add_handler(MessageHandler(filters.PHOTO, handle_proof))

    # Tägliche Zusammenfassung um 23:59 Uhr
    app.job_queue.run_daily(
        send_daily_summary,
        time=time(23, 59, 0),
        name="daily_summary"
    )

    print("✅ Bot läuft. Drücke Ctrl+C zum Stoppen.")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
