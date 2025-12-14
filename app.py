#!/usr/bin/env python3
"""
Complete Logs Search Bot with Admin Panel (bot + web UI)
- Bot features: keys, redeem, search, cooldown after successful search, pagination, admin notifications.
- Admin bot commands: /delkey, /listkeys, /revoke, /grant, /delsearch, /exportsearches, /statsfull, /announcement, /users
- Web admin panel: /admin?token=ADMIN_WEB_TOKEN (list/manage keys, users, searches, upload logs)
- Requirements: pyTelegramBotAPI, psycopg2-binary, Flask
"""

import os
import random
import time
import csv
import math
import logging
import tempfile
import html
from datetime import datetime, timedelta
from threading import Thread, Event
from urllib.parse import quote_plus, unquote_plus
from io import BytesIO

import psycopg2
from psycopg2 import OperationalError
import telebot
from telebot.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, InputFile
)
from flask import Flask, request, redirect, url_for, Response

# -------------------------
# CONFIG
# -------------------------
TOKEN = os.environ.get("TELEGRAM_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "7011151235"))
ADMIN_WEB_TOKEN = os.environ.get("ADMIN_WEB_TOKEN", "")  # new: protect web admin panel
WEBHOOK_UPLOAD_TOKEN = os.environ.get("WEBHOOK_UPLOAD_TOKEN", "")

LOG_FILE = os.environ.get("LOG_FILE", "logs.txt")
MAX_LINES = int(os.environ.get("MAX_LINES", 200))
SEARCH_PREVIEW_PAGE = int(os.environ.get("SEARCH_PREVIEW_PAGE", 10))
COOLDOWN_SECONDS = int(os.environ.get("COOLDOWN_SECONDS", 60))  # default 60s
BROADCAST_DELAY = float(os.environ.get("BROADCAST_DELAY", 0.05))
PORT = int(os.environ.get("PORT", 10000))

if not TOKEN or not DATABASE_URL or not ADMIN_WEB_TOKEN:
    raise RuntimeError("Missing TELEGRAM_TOKEN, DATABASE_URL, or ADMIN_WEB_TOKEN environment variables")

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
# KEEP ALIVE / WEB UI (RENDER)
# -------------------------
app = Flask(__name__)
_stop_event = Event()

@app.route("/", methods=["GET"])
def home():
    return "🤖 Logs Search Bot — alive"

# Web admin panel (protected by ADMIN_WEB_TOKEN)
def require_admin_token(req):
    token = req.args.get("token", "")
    return token == ADMIN_WEB_TOKEN

@app.route("/admin", methods=["GET", "POST"])
def web_admin():
    if not require_admin_token(request):
        return Response("Unauthorized - provide ?token=ADMIN_WEB_TOKEN", status=401)

    # Handle POST actions: delete key, revoke user, grant access, delete search, upload logs
    action = request.values.get("action", "")
    message = ""
    try:
        ensure_db()
        if action == "delkey":
            key = request.values.get("key", "").strip()
            if key:
                _curs.execute("DELETE FROM keys WHERE key=%s", (key,))
                db_commit()
                message = f"Deleted key: {html.escape(key)}"
        elif action == "revoke":
            uid = int(request.values.get("user_id", "0"))
            if uid:
                _curs.execute("DELETE FROM users WHERE user_id=%s", (uid,))
                db_commit()
                message = f"Revoked user: {uid}"
        elif action == "grant":
            uid = int(request.values.get("user_id", "0"))
            days = int(request.values.get("days", "0"))
            if uid and days > 0:
                expires = datetime.now() + timedelta(days=days)
                _curs.execute("""
                    INSERT INTO users (user_id, expires) VALUES (%s,%s)
                    ON CONFLICT (user_id) DO UPDATE SET expires=EXCLUDED.expires
                """, (uid, expires))
                db_commit()
                message = f"Granted {days} days to {uid} (until {expires.isoformat()})"
        elif action == "delsearch":
            uid = int(request.values.get("user_id", "0"))
            kw = request.values.get("keyword", "").strip().lower()
            if uid and kw:
                _curs.execute("DELETE FROM searches WHERE user_id=%s AND keyword=%s", (uid, kw))
                db_commit()
                message = f"Deleted searches for user {uid} keyword {html.escape(kw)}"
        elif action == "uploadlog":
            token = request.values.get("token", "")
            if token == WEBHOOK_UPLOAD_TOKEN:
                f = request.files.get("file")
                if f:
                    f.save(LOG_FILE)
                    reload_logs()
                    message = "Uploaded logs and reloaded."
            else:
                message = "Invalid upload token"
    except Exception as e:
        logger.exception("Admin action failed")
        message = f"Error: {e}"

    # Build page: list keys, users, and a form for actions
    ensure_db()
    _curs.execute("SELECT key, expires, redeemed_by FROM keys ORDER BY expires DESC LIMIT 200")
    keys = _curs.fetchall()
    _curs.execute("SELECT user_id, expires FROM users ORDER BY expires DESC LIMIT 200")
    users = _curs.fetchall()

    # Simple HTML
    html_out = "<html><head><title>Admin Panel</title></head><body style='font-family:Arial,sans-serif'>"
    html_out += f"<h2>Admin Panel</h2><p style='color:green'>{html.escape(message)}</p>"
    html_out += "<h3>Keys (latest 200)</h3><table border=1 cellpadding=4><tr><th>Key</th><th>Expires</th><th>Redeemed By</th><th>Action</th></tr>"
    for k, ex, rb in keys:
        html_out += "<tr>"
        html_out += f"<td>{html.escape(k)}</td><td>{html.escape(str(ex))}</td><td>{html.escape(str(rb)) if rb else ''}</td>"
        html_out += "<td>"
        html_out += f"<form style='display:inline' method='post'><input type='hidden' name='action' value='delkey'><input type='hidden' name='key' value='{html.escape(k)}'><input type='hidden' name='token' value='{html.escape(request.args.get('token',''))}'><input type='submit' value='Delete'></form>"
        html_out += "</td></tr>"
    html_out += "</table>"

    html_out += "<h3>Users (latest 200)</h3><table border=1 cellpadding=4><tr><th>User ID</th><th>Expires</th><th>Actions</th></tr>"
    for uid, ex in users:
        html_out += "<tr>"
        html_out += f"<td>{html.escape(str(uid))}</td><td>{html.escape(str(ex))}</td>"
        html_out += "<td>"
        html_out += f"<form style='display:inline' method='post'><input type='hidden' name='action' value='revoke'><input type='hidden' name='user_id' value='{uid}'><input type='submit' value='Revoke'></form>"
        html_out += f"&nbsp;<form style='display:inline' method='post'><input type='hidden' name='action' value='grant'><input type='hidden' name='user_id' value='{uid}'><input type='number' name='days' placeholder='days' min='1' style='width:80px'><input type='submit' value='Grant'></form>"
        html_out += "</td></tr>"
    html_out += "</table>"

    # search deletion/export form
    html_out += "<h3>Manage User Searches</h3>"
    html_out += "<form method='post'>User ID: <input name='user_id' required> Keyword: <input name='keyword' required>"
    html_out += "<input type='hidden' name='action' value='delsearch'><input type='submit' value='Delete Searches'></form>"

    # upload log
    html_out += "<h3>Upload Logs (requires WEBHOOK_UPLOAD_TOKEN)</h3>"
    html_out += f"<form method='post' enctype='multipart/form-data'><input type='hidden' name='action' value='uploadlog'><input name='token' placeholder='upload token'> <input type='file' name='file'> <input type='submit' value='Upload'></form>"

    html_out += "</body></html>"
    return html_out

# helper to serve exported searches as downloadable file
@app.route("/export_search", methods=["GET"])
def web_export_search():
    if not require_admin_token(request):
        return Response("Unauthorized - provide ?token=ADMIN_WEB_TOKEN", status=401)
    user_id = int(request.args.get("user_id", "0"))
    keyword = request.args.get("keyword", "").strip().lower()
    if not user_id or not keyword:
        return Response("Bad request", status=400)
    ensure_db()
    _curs.execute("SELECT line FROM searches WHERE user_id=%s AND keyword=%s ORDER BY found_at ASC LIMIT %s", (user_id, keyword, MAX_LINES))
    rows = _curs.fetchall()
    lines = [r[0] for r in rows]
    if not lines:
        return Response("No results", status=404)
    output = "\n".join(lines)
    return Response(output, mimetype="text/plain", headers={"Content-Disposition": f"attachment; filename=results_user{user_id}_{keyword[:30]}.txt"})

def start_keep_alive():
    Thread(target=lambda: app.run(host="0.0.0.0", port=PORT), daemon=True).start()

# -------------------------
# BOT INIT
# -------------------------
bot = telebot.TeleBot(TOKEN, threaded=True)

# -------------------------
# DATABASE (reconnect helper)
# -------------------------
def new_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")

_conn = None
_curs = None

def ensure_db():
    global _conn, _curs
    try:
        if _conn is None or getattr(_conn, "closed", False):
            _conn = new_conn()
            _curs = _conn.cursor()
    except Exception:
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
# LOGS (reload before search)
# -------------------------
LOGS = []
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
# HELPERS & UI
# -------------------------
_user_cooldowns = {}  # user_id -> last_successful_search_ts

def header_text():
    return "✨ *Logs Search Bot* ✨\n\n"

def mk_inline_menu():
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("🔍 Search Logs", callback_data="search"),
        InlineKeyboardButton("📊 My Stats", callback_data="stats"),
        InlineKeyboardButton("⏳ My Access", callback_data="access"),
        InlineKeyboardButton("♻️ Reset Search", callback_data="reset"),
        InlineKeyboardButton("📥 My Searches (Download)", callback_data="my_downloads"),
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

def check_cooldown(user_id):
    now = time.time()
    last = _user_cooldowns.get(user_id, 0)
    remaining = COOLDOWN_SECONDS - (now - last)
    if remaining > 0:
        return False, int(math.ceil(remaining))
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

def send_typing(chat_id, seconds=0.4):
    try:
        bot.send_chat_action(chat_id, "typing")
        if seconds > 0:
            time.sleep(seconds)
    except:
        pass

# -------------------------
# BOT COMMANDS (including admin commands)
# -------------------------
@bot.message_handler(commands=["start"])
def cmd_start(message):
    uid = message.from_user.id
    if not user_has_access(uid):
        bot.send_message(message.chat.id, header_text() + "❌ *Access required*\nUse `/redeem <key>`", parse_mode="Markdown")
        return
    bot.send_message(message.chat.id, header_text() + "Welcome — choose an option:", parse_mode="Markdown", reply_markup=mk_inline_menu())

@bot.message_handler(commands=["about"])
def cmd_about(message):
    about = (
        header_text() +
        "A polished log-search tool.\n\n"
        f"• Max search lines: *{MAX_LINES}*\n"
        f"• Per-search cooldown: *{COOLDOWN_SECONDS}s* (only after successful search)\n"
        "Owner: @OnlyJosh4"
    )
    bot.send_message(message.chat.id, about, parse_mode="Markdown")

# Admin: list keys (unused and all)
@bot.message_handler(commands=["listkeys"])
def cmd_listkeys(message):
    if message.from_user.id != ADMIN_ID:
        bot.reply_to(message, "❌ Not authorized")
        return
    ensure_db()
    _curs.execute("SELECT key, expires, redeemed_by FROM keys ORDER BY expires DESC LIMIT 200")
    rows = _curs.fetchall()
    if not rows:
        bot.reply_to(message, "No keys.")
        return
    lines = []
    for k, ex, rb in rows:
        lines.append(f"{k} | expires: {format_dt(ex)} | redeemed_by: {rb or '-'}")
    msg = "Keys:\n" + "\n".join(lines)
    bot.reply_to(message, msg)

@bot.message_handler(commands=["delkey"])
def cmd_delkey(message):
    if message.from_user.id != ADMIN_ID:
        bot.reply_to(message, "❌ Not authorized")
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "Usage: /delkey KEY-XXXXXX")
        return
    key = parts[1].strip()
    ensure_db()
    _curs.execute("DELETE FROM keys WHERE key=%s RETURNING key", (key,))
    res = _curs.fetchone()
    db_commit()
    if res:
        bot.reply_to(message, f"✅ Deleted key {key}")
    else:
        bot.reply_to(message, "Key not found")

@bot.message_handler(commands=["revoke"])
def cmd_revoke(message):
    if message.from_user.id != ADMIN_ID:
        bot.reply_to(message, "❌ Not authorized")
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "Usage: /revoke <user_id>")
        return
    try:
        uid = int(parts[1].strip())
    except:
        bot.reply_to(message, "Invalid user id")
        return
    ensure_db()
    _curs.execute("DELETE FROM users WHERE user_id=%s RETURNING user_id", (uid,))
    res = _curs.fetchone()
    db_commit()
    if res:
        bot.reply_to(message, f"✅ Revoked access for {uid}")
    else:
        bot.reply_to(message, "User not found or no active access")

@bot.message_handler(commands=["grant"])
def cmd_grant(message):
    if message.from_user.id != ADMIN_ID:
        bot.reply_to(message, "❌ Not authorized")
        return
    parts = message.text.split()
    if len(parts) < 3:
        bot.reply_to(message, "Usage: /grant <user_id> <days>")
        return
    try:
        uid = int(parts[1]); days = int(parts[2])
    except:
        bot.reply_to(message, "Invalid parameters")
        return
    expires = datetime.now() + timedelta(days=days)
    ensure_db()
    _curs.execute("""
        INSERT INTO users (user_id, expires) VALUES (%s,%s)
        ON CONFLICT (user_id) DO UPDATE SET expires=EXCLUDED.expires
    """, (uid, expires))
    db_commit()
    bot.reply_to(message, f"✅ Granted access to {uid} until {format_dt(expires)}")

@bot.message_handler(commands=["delsearch"])
def cmd_delsearch(message):
    if message.from_user.id != ADMIN_ID:
        bot.reply_to(message, "❌ Not authorized")
        return
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        bot.reply_to(message, "Usage: /delsearch <user_id> <keyword>")
        return
    try:
        uid = int(parts[1]); kw = parts[2].strip().lower()
    except:
        bot.reply_to(message, "Invalid parameters")
        return
    ensure_db()
    _curs.execute("DELETE FROM searches WHERE user_id=%s AND keyword=%s", (uid, kw))
    deleted = _curs.rowcount
    db_commit()
    bot.reply_to(message, f"✅ Deleted {deleted} saved search lines for user {uid} keyword {kw}")

@bot.message_handler(commands=["exportsearches"])
def cmd_exportsearches(message):
    if message.from_user.id != ADMIN_ID:
        bot.reply_to(message, "❌ Not authorized")
        return
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        bot.reply_to(message, "Usage: /exportsearches <user_id> <keyword>")
        return
    try:
        uid = int(parts[1]); kw = parts[2].strip().lower()
    except:
        bot.reply_to(message, "Invalid parameters")
        return
    ensure_db()
    _curs.execute("SELECT line FROM searches WHERE user_id=%s AND keyword=%s ORDER BY found_at ASC LIMIT %s", (uid, kw, MAX_LINES))
    rows = _curs.fetchall()
    lines = [r[0] for r in rows]
    if not lines:
        bot.reply_to(message, "No saved results")
        return
    bio = BytesIO("\n".join(lines).encode("utf-8"))
    bio.seek(0)
    bot.send_document(message.chat.id, InputFile(bio, filename=f"results_user{uid}_{kw[:30]}.txt"), caption=f"Export: {len(lines)} lines")

@bot.message_handler(commands=["statsfull"])
def cmd_statsfull(message):
    if message.from_user.id != ADMIN_ID:
        bot.reply_to(message, "❌ Not authorized")
        return
    ensure_db()
    _curs.execute("SELECT COUNT(*) FROM keys"); keys = _curs.fetchone()[0]
    _curs.execute("SELECT COUNT(*) FROM users"); users = _curs.fetchone()[0]
    _curs.execute("SELECT COUNT(*) FROM searches"); searches = _curs.fetchone()[0]
    bot.reply_to(message, f"DB Stats:\nKeys: {keys}\nUsers: {users}\nSearch lines saved: {searches}")

# Keep previous admin commands: /announcement, /users etc (from earlier)
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
    sent = 0; failed = 0; fails = []
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🔍 Search Logs", callback_data="search"))
    for uid in user_ids:
        ok, err = safe_send(uid, f"📣 Announcement:\n\n{ann_text}", reply_markup=kb)
        if ok: sent += 1
        else:
            failed += 1
            if len(fails) < 5: fails.append((uid, err))
        time.sleep(BROADCAST_DELAY)
    summary = f"📣 Sent: {sent}  Failed: {failed}"
    if fails:
        summary += "\nExamples:\n" + "\n".join(f"{u}: {e[:80]}" for u, e in fails)
    bot.reply_to(message, summary)

@bot.message_handler(commands=["users"])
def cmd_users(message):
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

# -------------------------
# CALLBACKS (search triggers)
# -------------------------
@bot.callback_query_handler(func=lambda c: c.data == "search")
def cb_search(call):
    if not user_has_access(call.from_user.id):
        bot.send_message(call.message.chat.id, "❌ You need an active key. Use `/redeem <key>`", parse_mode="Markdown")
        return
    msg = bot.send_message(call.message.chat.id, "🔎 Send keyword:")
    bot.register_next_step_handler(msg, process_search)

@bot.callback_query_handler(func=lambda c: c.data == "my_downloads")
def cb_my_downloads(call):
    # sends list of saved keywords for user with links to download (bot sends file)
    uid = call.from_user.id
    ensure_db()
    _curs.execute("SELECT DISTINCT keyword, COUNT(*) FROM searches WHERE user_id=%s GROUP BY keyword ORDER BY COUNT DESC", (uid,))
    rows = _curs.fetchall()
    if not rows:
        bot.answer_callback_query(call.id, "No saved searches")
        return
    msg = "Your saved searches:\n"
    for kw, cnt in rows:
        msg += f"- {kw} ({cnt} lines)\n"
    bot.send_message(call.message.chat.id, msg)

# -------------------------
# SEARCH CORE (reliable send + admin notify)
# -------------------------
def get_user_display(user):
    uname = getattr(user, "username", None)
    if uname:
        return f"@{uname}"
    name = (user.first_name or "") + (" " + (user.last_name or "") if user.last_name else "")
    return name.strip() or str(user.id)

def notify_admin_of_search(user, keyword, found_lines, duration_seconds, file_path):
    try:
        examples = "\n".join(found_lines[:5])
        msg = (
            "🔔 *User Search Generated*\n\n"
            f"*User:* {get_user_display(user)} (`{user.id}`)\n"
            f"*Keywords:* `{keyword}`\n"
            f"*Lines Found:* {len(found_lines)}\n\n"
            f"*Example lines:*\n{examples}\n\n"
            f"*Time Searched:* `{format_dt(datetime.now())}`\n"
            f"*Search Duration:* `{duration_seconds:.3f}s`"
        )
        bot.send_message(ADMIN_ID, msg, parse_mode="Markdown")
        if file_path and os.path.exists(file_path):
            with open(file_path, "rb") as fh:
                bot.send_document(ADMIN_ID, fh, caption=f"Full results for {get_user_display(user)} — keyword: {keyword}")
    except Exception:
        logger.exception("notify_admin_of_search failed")

def process_search(message):
    uid = message.from_user.id

    allowed, rem = check_cooldown(uid)
    if not allowed:
        mins = rem // 60; secs = rem % 60
        rem_text = f"{mins}m {secs}s" if mins > 0 else f"{secs}s"
        bot.send_message(message.chat.id, f"⏳ Please wait *{rem_text}* before your next search.", parse_mode="Markdown")
        return

    if not user_has_access(uid):
        bot.send_message(message.chat.id, header_text() + "❌ *You need an active key.*\nUse `/redeem <key>`", parse_mode="Markdown")
        return

    reload_logs()

    keyword_raw = (message.text or "").strip()
    if not keyword_raw:
        bot.send_message(message.chat.id, "❌ Empty keyword")
        return
    keyword = keyword_raw.lower()

    send_typing(message.chat.id, 0.3)
    start_ts = time.time()
    temp_msg = bot.send_message(message.chat.id, f"🔎 Searching for *{keyword_raw}*...", parse_mode="Markdown")

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

    db_commit()
    duration = time.time() - start_ts

    if not found:
        try:
            bot.edit_message_text("❌ No new results found (or duplicates filtered). Try another keyword.", message.chat.id, temp_msg.message_id)
        except Exception:
            bot.send_message(message.chat.id, "❌ No new results found (or duplicates filtered). Try another keyword.")
        return

    _user_cooldowns[uid] = time.time()

    # create temp results file
    try:
        fd, tmp_path = tempfile.mkstemp(prefix="results_", suffix=".txt")
        os.close(fd)
        with open(tmp_path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(found))
    except Exception:
        logger.exception("Failed creating temp results file")
        tmp_path = None

    try:
        if tmp_path and os.path.exists(tmp_path):
            with open(tmp_path, "rb") as fh:
                bot.send_document(message.chat.id, fh, caption=f"✅ Found {len(found)} lines (limited to {MAX_LINES})")
        else:
            bio = BytesIO("\n".join(found).encode("utf-8"))
            bio.seek(0)
            bot.send_document(message.chat.id, bio, caption=f"✅ Found {len(found)} lines (limited to {MAX_LINES})")
    except Exception:
        logger.exception("Failed sending results to user")
        bot.send_message(message.chat.id, "❌ Failed to send results. Try again later.")
        if tmp_path and os.path.exists(tmp_path):
            try: os.remove(tmp_path)
            except: pass
        return

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("📥 Download All", callback_data=f"download:{quote_plus(keyword)}"))
    if len(found) > SEARCH_PREVIEW_PAGE:
        kb.add(InlineKeyboardButton("📄 Preview (page 1)", callback_data=f"page:{quote_plus(keyword)}:0"))

    preview_lines = found[:SEARCH_PREVIEW_PAGE]
    preview_text = header_text() + f"🔎 Preview (first {len(preview_lines)} lines):\n\n" + "\n".join(preview_lines[:SEARCH_PREVIEW_PAGE])
    bot.send_message(message.chat.id, preview_text, parse_mode="Markdown", reply_markup=kb)

    # notify admin
    try:
        notify_admin_of_search(message.from_user, keyword_raw, found, duration, tmp_path if tmp_path else "")
    except Exception:
        logger.exception("Failed to notify admin")

    if tmp_path and os.path.exists(tmp_path):
        try:
            os.remove(tmp_path)
        except:
            pass

# -------------------------
# RUN
# -------------------------
def run_polling():
    while not _stop_event.is_set():
        try:
            logger.info("Starting polling")
            bot.polling(none_stop=True, timeout=60)
        except Exception:
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
