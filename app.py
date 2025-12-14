#!/usr/bin/env python3
import os
import random
import time
import csv
import math
import logging
from datetime import datetime, timedelta
from threading import Thread, Event
from urllib.parse import quote_plus, unquote_plus
from io import BytesIO

import psycopg2
from psycopg2 import OperationalError, sql
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, InputFile
from flask import Flask, request

# -------------------------
# CONFIG
# -------------------------
TOKEN = os.environ.get("TELEGRAM_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "7011151235"))

LOG_FILE = os.environ.get("LOG_FILE", "logs.txt")
MAX_LINES = int(os.environ.get("MAX_LINES", 200))
SEARCH_PREVIEW_PAGE = int(os.environ.get("SEARCH_PREVIEW_PAGE", 10))  # lines per page in preview

# **COOLDOWN: default 60 seconds (1 minute)**
COOLDOWN_SECONDS = int(os.environ.get("COOLDOWN_SECONDS", 60))

BROADCAST_DELAY = float(os.environ.get("BROADCAST_DELAY", 0.05))
PORT = int(os.environ.get("PORT", 10000))

if not TOKEN or not DATABASE_URL:
    raise RuntimeError("Missing TELEGRAM_TOKEN or DATABASE_URL environment variables")

# -------------------------
# LOGGING
# -------------------------
logging.basicConfig(
    filename="bot.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("logs_bot")

# -------------------------
# KEEP ALIVE (RENDER)
# -------------------------
app = Flask(__name__)
_stop_event = Event()

@app.route("/", methods=["GET"])
def home():
    return "🤖 Logs Search Bot — alive"

# allow admin to upload a log file via HTTP (optional)
@app.route("/uploadlog", methods=["POST"])
def http_upload_log():
    token = request.args.get("token")
    if token != os.environ.get("WEBHOOK_UPLOAD_TOKEN", ""):
        return "Unauthorized", 401
    f = request.files.get("file")
    if not f:
        return "No file", 400
    f.save(LOG_FILE)
    # Invalidate in-memory logs (the bot checks file on next search)
    reload_logs()
    return "OK", 200

def start_keep_alive():
    Thread(target=lambda: app.run(host="0.0.0.0", port=PORT), daemon=True).start()

# -------------------------
# BOT INIT
# -------------------------
bot = telebot.TeleBot(TOKEN, threaded=True)

# -------------------------
# DATABASE (with reconnect helper)
# -------------------------
def new_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")

_conn = None
_curs = None

def ensure_db():
    global _conn, _curs
    try:
        if _conn is None or _conn.closed:
            _conn = new_conn()
            _curs = _conn.cursor()
    except Exception as e:
        logger.exception("DB connect failed")
        raise

def db_execute(query, params=None, fetch=False):
    global _conn, _curs
    for attempt in range(2):
        try:
            ensure_db()
            _curs.execute(query, params or ())
            if fetch:
                return _curs.fetchall()
            return None
        except OperationalError:
            logger.warning("OperationalError - reconnecting to DB (attempt %s)", attempt+1)
            try:
                if _conn:
                    _conn.close()
            except:
                pass
            _conn = new_conn()
            _curs = _conn.cursor()
        except Exception:
            logger.exception("DB execute failed for query: %s", query)
            raise
    return None

def db_fetchone(query, params=None):
    ensure_db()
    _curs.execute(query, params or ())
    return _curs.fetchone()

def db_commit():
    try:
        ensure_db()
        _conn.commit()
    except Exception:
        logger.exception("DB commit failed")
        raise

def init_db():
    ensure_db()
    _curs.execute("""
    CREATE TABLE IF NOT EXISTS keys (
        key TEXT PRIMARY KEY,
        expires TIMESTAMP,
        redeemed_by BIGINT
    )""")

    _curs.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id BIGINT PRIMARY KEY,
        expires TIMESTAMP
    )""")

    _curs.execute("""
    CREATE TABLE IF NOT EXISTS searches (
        user_id BIGINT,
        keyword TEXT,
        line TEXT,
        found_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (user_id, keyword, line)
    )""")
    db_commit()

# -------------------------
# LOGS (in-memory cache, reload function)
# -------------------------
def reload_logs():
    global LOGS
    LOGS = []
    try:
        if not os.path.exists(LOG_FILE):
            open(LOG_FILE, "w").close()
        with open(LOG_FILE, "r", encoding="utf-8", errors="ignore") as fh:
            LOGS = [line.rstrip("\n") for line in fh if line.strip()]
            logger.info("Loaded %d log lines", len(LOGS))
    except Exception:
        logger.exception("Failed to load logs")
        LOGS = []

reload_logs()

# -------------------------
# UTIL HELPERS
# -------------------------
_user_cooldowns = {}  # user_id -> last_search_ts

def check_cooldown(user_id):
    """
    Returns (True, 0) if allowed, or (False, remaining_seconds) if still cooling down.
    """
    now = time.time()
    last = _user_cooldowns.get(user_id, 0)
    remaining = COOLDOWN_SECONDS - (now - last)
    if remaining > 0:
        # round up to nearest second
        return False, int(math.ceil(remaining))
    _user_cooldowns[user_id] = now
    return True, 0

def user_has_access(user_id):
    row = db_fetchone("SELECT expires FROM users WHERE user_id=%s", (user_id,))
    if not row:
        return False
    expires = row[0]
    if datetime.now() <= expires:
        return True
    # expired: remove
    db_execute("DELETE FROM users WHERE user_id=%s", (user_id,))
    db_commit()
    return False

def get_user_expiry(user_id):
    row = db_fetchone("SELECT expires FROM users WHERE user_id=%s", (user_id,))
    return row[0] if row else None

def mk_menu():
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("🔍 Search Logs", callback_data="search"),
        InlineKeyboardButton("📊 My Stats", callback_data="stats"),
        InlineKeyboardButton("⏳ My Access", callback_data="access"),
        InlineKeyboardButton("♻️ Reset Search", callback_data="reset"),
        InlineKeyboardButton("📂 Download Logs", callback_data="download_logs"),
        InlineKeyboardButton("📞 Owner", url="https://t.me/OnlyJosh4")
    )
    return kb

def safe_send(user_id, text, **kwargs):
    try:
        bot.send_message(user_id, text, **kwargs)
        return True, None
    except Exception as e:
        logger.exception("safe_send failed to %s", user_id)
        return False, str(e)

def format_dt(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else "N/A"

# -------------------------
# COMMANDS
# -------------------------
@bot.message_handler(commands=["start"])
def cmd_start(message):
    uid = message.from_user.id
    if not user_has_access(uid):
        bot.send_message(message.chat.id, "❌ Access required.\nUse /redeem <key>")
        return
    bot.send_message(message.chat.id, "✅ Welcome back! Choose an option:", reply_markup=mk_menu())

@bot.message_handler(commands=["about"])
def cmd_about(message):
    about = (
        "🤖 *Logs Search Bot*\n"
        "Purpose: search large log files and return results safely.\n\n"
        f"• Max search lines: *{MAX_LINES}*\n"
        f"• Per-search cooldown: *{COOLDOWN_SECONDS} seconds*\n"
        "• No duplicate lines per user\n"
        "• Admin features: create keys, announcement, upload logs\n\n"
        "Owner: @OnlyJosh4"
    )
    bot.send_message(message.chat.id, about, parse_mode="Markdown")

@bot.message_handler(commands=["createkey"])
def cmd_createkey(message):
    if message.from_user.id != ADMIN_ID:
        bot.reply_to(message, "❌ Admin only")
        return
    try:
        _, days, count = message.text.split()
        days, count = int(days), int(count)
    except Exception:
        bot.reply_to(message, "Usage: /createkey <days> <count>")
        return
    expires = datetime.now() + timedelta(days=days)
    keys = []
    for _ in range(count):
        k = f"KEY-{random.randint(100000, 999999)}"
        db_execute("INSERT INTO keys (key, expires, redeemed_by) VALUES (%s,%s,NULL)", (k, expires))
        keys.append(k)
    db_commit()
    bot.reply_to(message, "✅ Keys created:\n" + "\n".join(keys))

@bot.message_handler(commands=["redeem"])
def cmd_redeem(message):
    try:
        _, key = message.text.split(maxsplit=1)
    except Exception:
        bot.reply_to(message, "Usage: /redeem KEY-XXXXXX")
        return
    uid = message.from_user.id
    row = db_fetchone("SELECT expires FROM keys WHERE key=%s AND redeemed_by IS NULL", (key,))
    if not row:
        bot.reply_to(message, "❌ Invalid or already-used key")
        return
    expires_at = row[0]
    db_execute("""
        INSERT INTO users (user_id, expires) VALUES (%s,%s)
        ON CONFLICT (user_id) DO UPDATE SET expires=EXCLUDED.expires
    """, (uid, expires_at))
    db_execute("UPDATE keys SET redeemed_by=%s WHERE key=%s", (uid, key))
    db_commit()

    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("🔍 Start Searching", callback_data="search"),
        InlineKeyboardButton("📊 View Stats", callback_data="stats")
    )
    txt = (
        "✅ *Key Successfully Redeemed*\n\n"
        f"*Expiration:* `{format_dt(expires_at)}`\n\n"
        "Use the buttons below to start searching or view your stats."
    )
    try:
        bot.send_message(uid, txt, reply_markup=kb, parse_mode="Markdown")
    except Exception:
        bot.send_message(uid, "✅ Key successfully redeemed!\nExpiration: " + format_dt(expires_at), reply_markup=kb)

@bot.message_handler(commands=["announcement"])
def cmd_announcement(message):
    if message.from_user.id != ADMIN_ID:
        bot.reply_to(message, "❌ Not authorized")
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        bot.reply_to(message, "Usage: /announcement <message>")
        return
    ann_text = parts[1].strip()
    ensure_db()
    _curs.execute("SELECT user_id FROM users WHERE expires >= NOW()")
    rows = _curs.fetchall()
    user_ids = [r[0] for r in rows]
    if not user_ids:
        bot.reply_to(message, "⚠️ No active users.")
        return

    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("🔍 Start Searching", callback_data="search"),
        InlineKeyboardButton("📊 View Stats", callback_data="stats")
    )

    sent = 0
    failed = 0
    fails = []
    for uid in user_ids:
        ok, err = safe_send(uid, f"📣 Announcement:\n\n{ann_text}", reply_markup=kb)
        if ok:
            sent += 1
        else:
            failed += 1
            if len(fails) < 5:
                fails.append((uid, err))
        time.sleep(BROADCAST_DELAY)

    summary = f"📣 Sent: {sent}\n❌ Failed: {failed}"
    if fails:
        summary += "\nExamples:\n" + "\n".join(f"{u}: {e[:80]}" for u, e in fails)
    bot.reply_to(message, summary)

@bot.message_handler(commands=["users"])
def cmd_users(message):
    """Admin: get CSV of active users (user_id,expires)"""
    if message.from_user.id != ADMIN_ID:
        bot.reply_to(message, "❌ Admin only")
        return
    ensure_db()
    _curs.execute("SELECT user_id, expires FROM users ORDER BY expires DESC")
    rows = _curs.fetchall()
    if not rows:
        bot.reply_to(message, "No users.")
        return
    output = BytesIO()
    writer = csv.writer(output)
    writer.writerow(["user_id", "expires"])
    for r in rows:
        writer.writerow([r[0], format_dt(r[1])])
    output.seek(0)
    bot.send_document(message.chat.id, InputFile(output, filename="users.csv"))

@bot.message_handler(commands=["uploadlog"])
def cmd_uploadlog(message):
    """Admin: send a document with caption /uploadlog_file to replace LOG_FILE"""
    if message.from_user.id != ADMIN_ID:
        bot.reply_to(message, "❌ Admin only")
        return
    bot.send_message(message.chat.id, "Send the log file as a document with caption /uploadlog_file (admin only).")

@bot.message_handler(content_types=["document"])
def handle_document(message):
    """Handle admin document uploads. To upload logs, admin must use caption /uploadlog_file"""
    if message.caption and message.caption.strip().lower() == "/uploadlog_file" and message.from_user.id == ADMIN_ID:
        doc = message.document
        file_info = bot.get_file(doc.file_id)
        file_bytes = bot.download_file(file_info.file_path)
        try:
            with open(LOG_FILE, "wb") as fh:
                fh.write(file_bytes)
            reload_logs()
            bot.reply_to(message, "✅ Log file replaced and reloaded.")
        except Exception:
            logger.exception("Failed to write uploaded log file")
            bot.reply_to(message, "❌ Failed to save uploaded file.")
    # else: ignore other documents

# -------------------------
# CALLBACKS (menu & pagination)
# -------------------------
@bot.callback_query_handler(func=lambda c: c.data == "search")
def cb_search(call):
    if not user_has_access(call.from_user.id):
        bot.send_message(call.message.chat.id, "❌ You need an active key. Use /redeem <key>")
        return
    msg = bot.send_message(call.message.chat.id, "🔎 Send keyword:")
    bot.register_next_step_handler(msg, process_search)

@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("page:"))
def cb_page(call):
    try:
        _, qkw, page_str = call.data.split(":", 2)
        keyword = unquote_plus(qkw)
        page = int(page_str)
    except Exception:
        bot.answer_callback_query(call.id, "Invalid page data")
        return

    uid = call.from_user.id
    ensure_db()
    _curs.execute("""
        SELECT line FROM searches
        WHERE user_id=%s AND keyword=%s
        ORDER BY found_at ASC
        LIMIT %s OFFSET %s
    """, (uid, keyword, SEARCH_PREVIEW_PAGE, page * SEARCH_PREVIEW_PAGE))
    rows = _curs.fetchall()
    lines = [r[0] for r in rows]
    if not lines:
        bot.answer_callback_query(call.id, "No more lines.")
        return

    kb = InlineKeyboardMarkup()
    prev_disabled = page <= 0
    _curs.execute("""
        SELECT 1 FROM searches WHERE user_id=%s AND keyword=%s
        LIMIT 1 OFFSET %s
    """, (uid, keyword, (page + 1) * SEARCH_PREVIEW_PAGE))
    has_next = bool(_curs.fetchone())

    nav_buttons = []
    if not prev_disabled:
        nav_buttons.append(InlineKeyboardButton("⬅ Prev", callback_data=f"page:{quote_plus(keyword)}:{page-1}"))
    if has_next:
        nav_buttons.append(InlineKeyboardButton("Next ➡", callback_data=f"page:{quote_plus(keyword)}:{page+1}"))
    if nav_buttons:
        kb.row(*nav_buttons)

    kb.add(InlineKeyboardButton("📥 Download All", callback_data=f"download:{quote_plus(keyword)}"))

    preview_text = "🔎 Preview results (page {}):\n\n".format(page+1) + "\n".join(lines[:SEARCH_PREVIEW_PAGE])
    bot.send_message(call.message.chat.id, preview_text, reply_markup=kb)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("download:"))
def cb_download(call):
    try:
        _, qkw = call.data.split(":", 1)
        keyword = unquote_plus(qkw)
    except:
        bot.answer_callback_query(call.id, "Invalid download request")
        return

    uid = call.from_user.id
    ensure_db()
    _curs.execute("""
        SELECT line FROM searches
        WHERE user_id=%s AND keyword=%s
        ORDER BY found_at ASC
        LIMIT %s
    """, (uid, keyword, MAX_LINES))
    rows = _curs.fetchall()
    lines = [r[0] for r in rows]
    if not lines:
        bot.answer_callback_query(call.id, "No results saved for this keyword")
        return

    bio = BytesIO("\n".join(lines).encode("utf-8"))
    bio.seek(0)
    fname = f"results_{keyword[:30]}.txt"
    bot.send_document(call.message.chat.id, InputFile(bio, filename=fname), caption=f"✅ {len(lines)} lines (saved results)")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data == "stats")
def cb_stats(call):
    uid = call.from_user.id
    ensure_db()
    _curs.execute("SELECT COUNT(*) FROM searches WHERE user_id=%s", (uid,))
    total = _curs.fetchone()[0]
    expiry = get_user_expiry(uid)
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🔍 Search Logs", callback_data="search"))
    bot.send_message(call.message.chat.id, f"📊 Total saved unique lines: {total}\n⏳ Access until: {format_dt(expiry)}", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data == "access")
def cb_access(call):
    expiry = get_user_expiry(call.from_user.id)
    bot.send_message(call.message.chat.id, f"⏳ Access until: {format_dt(expiry)}" if expiry else "❌ No active access")

@bot.callback_query_handler(func=lambda c: c.data == "reset")
def cb_reset(call):
    ensure_db()
    _curs.execute("DELETE FROM searches WHERE user_id=%s", (call.from_user.id,))
    db_commit()
    bot.send_message(call.message.chat.id, "♻️ Your search memory has been cleared")

@bot.callback_query_handler(func=lambda c: c.data == "download_logs")
def cb_download_logs(call):
    if call.from_user.id != ADMIN_ID:
        bot.answer_callback_query(call.id, "Only owner can download raw logs.")
        return
    try:
        with open(LOG_FILE, "rb") as fh:
            bot.send_document(call.message.chat.id, fh, caption="Raw logs file")
    except Exception:
        logger.exception("Failed sending raw logs")
        bot.answer_callback_query(call.id, "Failed to send logs")

# -------------------------
# SEARCH CORE
# -------------------------
def process_search(message):
    uid = message.from_user.id

    # --- cooldown check (1 minute default) ---
    allowed, rem = check_cooldown(uid)
    if not allowed:
        # format remaining nicely
        mins = rem // 60
        secs = rem % 60
        if mins > 0:
            rem_text = f"{mins}m {secs}s"
        else:
            rem_text = f"{secs}s"
        bot.send_message(message.chat.id, f"⏳ Please wait {rem_text} before your next search.")
        return

    if not user_has_access(uid):
        bot.send_message(message.chat.id, "❌ You need an active key. Use /redeem <key>")
        return

    keyword = (message.text or "").strip().lower()
    if not keyword:
        bot.send_message(message.chat.id, "❌ Empty keyword")
        return

    found = []
    ensure_db()
    for line in LOGS:
        if len(found) >= MAX_LINES:
            break
        if keyword in line.lower():
            _curs.execute("SELECT 1 FROM searches WHERE user_id=%s AND keyword=%s AND line=%s", (uid, keyword, line))
            if not _curs.fetchone():
                found.append(line)
                _curs.execute("INSERT INTO searches (user_id, keyword, line) VALUES (%s,%s,%s)", (uid, keyword, line))

    if not found:
        bot.send_message(message.chat.id, "❌ No new results found (or duplicates filtered).")
        db_commit()
        return

    db_commit()
    filename = f"results_{quote_plus(keyword)[:50]}.txt"
    bio = BytesIO("\n".join(found).encode("utf-8"))
    bio.seek(0)

    bot.send_document(message.chat.id, InputFile(bio, filename=filename),
                      caption=f"✅ Found {len(found)} new lines (limited to {MAX_LINES})")

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("📥 Download All", callback_data=f"download:{quote_plus(keyword)}"))
    if len(found) > SEARCH_PREVIEW_PAGE:
        kb.row(InlineKeyboardButton("Preview Page 1", callback_data=f"page:{quote_plus(keyword)}:0"))

    first_lines = found[:SEARCH_PREVIEW_PAGE]
    preview_text = "🔎 Preview (first {} lines):\n\n".format(min(len(first_lines), SEARCH_PREVIEW_PAGE)) + "\n".join(first_lines)
    bot.send_message(message.chat.id, preview_text, reply_markup=kb)

# -------------------------
# RUN (with robust polling restart)
# -------------------------
def run_polling():
    while not _stop_event.is_set():
        try:
            logger.info("Starting polling")
            bot.polling(none_stop=True, timeout=60)
        except Exception as e:
            logger.exception("Polling crashed, restarting in 5s")
            time.sleep(5)
        else:
            break

if __name__ == "__main__":
    try:
        init_db()
        start_keep_alive()
        run_polling()
    except KeyboardInterrupt:
        logger.info("Shutting down by KeyboardInterrupt")
    except Exception:
        logger.exception("Fatal error on startup")
    finally:
        _stop_event.set()
        try:
            if _conn:
                _conn.close()
        except:
            pass
