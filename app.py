# app.py
"""
Combined Telegram bot + improved Flask admin panel.

Environment (required):
  - TELEGRAM_TOKEN   : Telegram bot token
  - DATABASE_URL     : Postgres connection string
  - ADMIN_ID         : Telegram owner id (for /createkey in-chat)
  - ADMIN_PASSWORD   : password for admin web UI (recommended)
Optional:
  - PORT             : port for Flask (default 10000)
  - FLASK_SECRET     : flask session secret (default: change-me)
  - MAX_LINES        : max search lines returned in bot (default 200)
Notes:
  - If ADMIN_PASSWORD is not provided but ADMIN_KEY is present, the admin UI will still allow
    login via ?key=ADMIN_KEY once; prefer ADMIN_PASSWORD for production.
  - The script attempts to add users.last_active if missing.
"""
import os
import random
import csv
from io import StringIO
from datetime import datetime, timedelta
from threading import Thread

import psycopg2
import psycopg2.extras
import psycopg2.errors
from flask import (
    Flask, request, redirect, url_for, render_template, flash,
    session, send_file, Response
)
from werkzeug.security import generate_password_hash, check_password_hash

import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# ---------------- CONFIG ----------------
TOKEN = os.environ.get("TELEGRAM_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))
# Admin login password for web UI (recommended). If provided we hash it on startup.
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD")
# Backwards-compat: if ADMIN_KEY exists, allow single-step login via ?key=ADMIN_KEY (not recommended).
ADMIN_KEY = os.environ.get("ADMIN_KEY")

PORT = int(os.environ.get("PORT", 10000))
FLASK_SECRET = os.environ.get("FLASK_SECRET", "change-me")
MAX_LINES = int(os.environ.get("MAX_LINES", "200"))

LOG_FILE = "logs.txt"

if not TOKEN or not DATABASE_URL:
    raise RuntimeError("Missing TELEGRAM_TOKEN or DATABASE_URL")

if ADMIN_PASSWORD:
    ADMIN_PASSWORD_HASH = generate_password_hash(ADMIN_PASSWORD)
else:
    ADMIN_PASSWORD_HASH = None

# ---------------- START TIME ----------------
START_TIME = datetime.utcnow()

# ---------------- APP / DB ----------------
app = Flask(__name__, template_folder="templates")
app.secret_key = FLASK_SECRET

conn = psycopg2.connect(DATABASE_URL, sslmode="require")
conn.autocommit = True

def get_cursor(dict_cursor=False):
    if dict_cursor:
        return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return conn.cursor()

def init_db():
    with get_cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS keys (
            key TEXT PRIMARY KEY,
            expires TIMESTAMP,
            redeemed_by BIGINT
        )""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            expires TIMESTAMP
        )""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS searches (
            user_id BIGINT,
            keyword TEXT,
            line TEXT,
            PRIMARY KEY (user_id, keyword, line)
        )""")
        # safe add last_active
        cur.execute("""
        SELECT column_name FROM information_schema.columns
          WHERE table_name='users' AND column_name='last_active'
        """)
        if cur.fetchone() is None:
            try:
                cur.execute("ALTER TABLE users ADD COLUMN last_active TIMESTAMP")
            except Exception:
                # ignore if cannot add (race condition / pg permissions)
                pass

init_db()

# ---------------- LOGS ----------------
def load_logs():
    if not os.path.exists(LOG_FILE):
        open(LOG_FILE, "w").close()
    with open(LOG_FILE, "r", encoding="utf-8", errors="ignore") as f:
        return [l.rstrip("\n") for l in f if l.strip()]

LOGS = load_logs()

# ---------------- BOT ----------------
bot = telebot.TeleBot(TOKEN, threaded=True)

def touch_user_last_active(user_id: int):
    now = datetime.utcnow()
    with get_cursor() as cur:
        # ensure row exists; keep existing expires value
        cur.execute("""
            INSERT INTO users (user_id, expires, last_active)
            VALUES (%s, NULL, %s)
            ON CONFLICT (user_id) DO UPDATE SET last_active = EXCLUDED.last_active
        """, (user_id, now))

def user_has_access(user_id: int) -> bool:
    with get_cursor() as cur:
        cur.execute("SELECT expires FROM users WHERE user_id=%s", (user_id,))
        row = cur.fetchone()
    if not row:
        return False
    # row[0] may be datetime
    if datetime.utcnow() <= row[0]:
        return True
    # expired -> delete
    with get_cursor() as cur:
        cur.execute("DELETE FROM users WHERE user_id=%s", (user_id,))
    return False

def get_user_expiry(user_id: int):
    with get_cursor() as cur:
        cur.execute("SELECT expires FROM users WHERE user_id=%s", (user_id,))
        row = cur.fetchone()
    return row[0] if row else None

def main_menu():
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("🔍 Search Logs", callback_data="search"),
        InlineKeyboardButton("📊 My Stats", callback_data="stats"),
        InlineKeyboardButton("⏳ My Access", callback_data="access"),
        InlineKeyboardButton("♻️ Reset Search", callback_data="reset"),
        InlineKeyboardButton("📞 Owner", url="https://t.me/OnlyJosh4"),
    )
    return kb

@bot.message_handler(commands=["start"])
def start_cmd(message):
    touch_user_last_active(message.from_user.id)

    if not user_has_access(message.from_user.id):
        bot.send_message(message.chat.id, "❌ Access required\nUse /redeem <key>")
        return

    bot.send_message(message.chat.id, "✅ Welcome! Choose an option:", reply_markup=main_menu())

@bot.message_handler(commands=["createkey"])
def create_key_cmd(message):
    # same admin check as original
    if message.from_user.id != ADMIN_ID:
        bot.reply_to(message, "❌ Admin only")
        return

    try:
        _, days, count = message.text.split()
        days, count = int(days), int(count)
    except Exception:
        bot.reply_to(message, "Usage: /createkey <days> <count>")
        return

    keys = []
    expires = datetime.utcnow() + timedelta(days=days)
    with get_cursor() as cur:
        for _ in range(count):
            # ensure unique
            for attempt in range(10):
                key = f"KEY-{random.randint(100000, 999999)}"
                try:
                    cur.execute("INSERT INTO keys (key, expires, redeemed_by) VALUES (%s,%s,NULL)", (key, expires))
                    keys.append(key)
                    break
                except psycopg2.errors.UniqueViolation:
                    conn.rollback()
                    continue
    bot.reply_to(message, "✅ Keys created:\n" + "\n".join(keys))

@bot.message_handler(commands=["redeem"])
def redeem_cmd(message):
    touch_user_last_active(message.from_user.id)

    try:
        _, key = message.text.split()
    except Exception:
        bot.reply_to(message, "Usage: /redeem KEY-XXXXXX")
        return

    uid = message.from_user.id
    with get_cursor() as cur:
        cur.execute("SELECT expires FROM keys WHERE key=%s AND redeemed_by IS NULL", (key,))
        row = cur.fetchone()
        if not row:
            bot.reply_to(message, "❌ Invalid or used key")
            return
        expires = row[0]
        # upsert user expiry
        cur.execute("""
            INSERT INTO users (user_id, expires)
            VALUES (%s,%s)
            ON CONFLICT (user_id) DO UPDATE SET expires = EXCLUDED.expires
        """, (uid, expires))
        cur.execute("UPDATE keys SET redeemed_by=%s WHERE key=%s", (uid, key))
    bot.reply_to(message, f"✅ Access valid until:\n{expires}")

# Callbacks
@bot.callback_query_handler(func=lambda c: c.data == "search")
def search_prompt(call):
    touch_user_last_active(call.from_user.id)
    msg = bot.send_message(call.message.chat.id, "🔎 Send keyword:")
    bot.register_next_step_handler(msg, process_search)

@bot.callback_query_handler(func=lambda c: c.data == "stats")
def stats_cb(call):
    touch_user_last_active(call.from_user.id)
    with get_cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM searches WHERE user_id=%s", (call.from_user.id,))
        total = cur.fetchone()[0]
    bot.send_message(call.message.chat.id, f"📊 Total unique lines saved: {total}")

@bot.callback_query_handler(func=lambda c: c.data == "access")
def access_cb(call):
    touch_user_last_active(call.from_user.id)
    expiry = get_user_expiry(call.from_user.id)
    if expiry:
        bot.send_message(call.message.chat.id, f"⏳ Access until:\n{expiry}")
    else:
        bot.send_message(call.message.chat.id, "❌ No active access")

@bot.callback_query_handler(func=lambda c: c.data == "reset")
def reset_cb(call):
    touch_user_last_active(call.from_user.id)
    with get_cursor() as cur:
        cur.execute("DELETE FROM searches WHERE user_id=%s", (call.from_user.id,))
    bot.send_message(call.message.chat.id, "♻️ Search memory cleared")

def process_search(message):
    touch_user_last_active(message.from_user.id)
    uid = message.from_user.id
    keyword = message.text.lower().strip()
    if not keyword:
        bot.send_message(message.chat.id, "❌ Empty keyword")
        return

    found = []
    with get_cursor() as cur:
        for line in LOGS:
            if len(found) >= MAX_LINES:
                break
            if keyword in line.lower():
                cur.execute("SELECT 1 FROM searches WHERE user_id=%s AND keyword=%s AND line=%s",
                            (uid, keyword, line))
                if not cur.fetchone():
                    found.append(line)
                    cur.execute("INSERT INTO searches (user_id, keyword, line) VALUES (%s,%s,%s)",
                                (uid, keyword, line))
    if not found:
        bot.send_message(message.chat.id, "❌ No new results found")
        return

    # commit and send file
    conn.commit()
    filename = f"results_{keyword}.txt"
    with open(filename, "w", encoding="utf-8") as f:
        f.write("\n".join(found))
    with open(filename, "rb") as f:
        bot.send_document(message.chat.id, f, caption=f"✅ {len(found)} lines found (limit {MAX_LINES})")
    try:
        os.remove(filename)
    except Exception:
        pass

# ---------------- ADMIN HELPERS ----------------
def compute_keys(filter_q=None, page=1, per_page=50):
    offset = (page - 1) * per_page
    params = []
    base = "SELECT key, expires, redeemed_by FROM keys"
    where = ""
    if filter_q:
        where = " WHERE key ILIKE %s OR CAST(redeemed_by AS TEXT) ILIKE %s"
        params.extend([f"%{filter_q}%", f"%{filter_q}%"])
    order = " ORDER BY expires DESC NULLS LAST"
    limit = " LIMIT %s OFFSET %s"
    params.extend([per_page, offset])
    sql = base + where + order + limit
    with get_cursor(dict_cursor=True) as cur:
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
    now = datetime.utcnow()
    out = []
    for r in rows:
        out.append({
            "key": r["key"],
            "expires": r["expires"],
            "redeemed_by": r["redeemed_by"],
            "active": (r["redeemed_by"] is None) and (r["expires"] and r["expires"] > now)
        })
    # total count
    count_sql = "SELECT COUNT(*) FROM keys" + (" WHERE key ILIKE %s OR CAST(redeemed_by AS TEXT) ILIKE %s" if filter_q else "")
    with get_cursor() as cur:
        if filter_q:
            cur.execute(count_sql, (f"%{filter_q}%", f"%{filter_q}%"))
        else:
            cur.execute(count_sql)
        total = cur.fetchone()[0] or 0
    return out, total

def generate_keys(days: int, count: int):
    expires = datetime.utcnow() + timedelta(days=days)
    created = []
    with get_cursor() as cur:
        for _ in range(count):
            for attempt in range(10):
                key = f"KEY-{random.randint(100000, 999999)}"
                try:
                    cur.execute("INSERT INTO keys (key, expires, redeemed_by) VALUES (%s,%s,NULL)", (key, expires))
                    created.append(key)
                    break
                except psycopg2.errors.UniqueViolation:
                    conn.rollback()
                    continue
    return created

def delete_key(key_str):
    with get_cursor() as cur:
        cur.execute("DELETE FROM keys WHERE key=%s", (key_str,))

def admin_status():
    now = datetime.utcnow()
    uptime = now - START_TIME
    with get_cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM users")
        total_users = cur.fetchone()[0] or 0
        cur.execute("SELECT COUNT(*) FROM users WHERE expires >= %s", (now,))
        active_users = cur.fetchone()[0] or 0
        # check last_active column
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name='users' AND column_name='last_active'
        """)
        online_users = None
        if cur.fetchone():
            threshold = now - timedelta(minutes=5)
            cur.execute("SELECT COUNT(*) FROM users WHERE last_active >= %s", (threshold,))
            online_users = cur.fetchone()[0] or 0
    return {
        "uptime": uptime,
        "total_users": total_users,
        "active_users": active_users,
        "online_users": online_users
    }

# ---------------- AUTH (web) ----------------
def is_authenticated():
    return session.get("admin_authenticated") is True

@app.route("/login", methods=["GET", "POST"])
def login():
    # If ADMIN_PASSWORD set, use login form. If not set but ADMIN_KEY provided, allow quick login.
    if request.method == "POST":
        pw = request.form.get("password", "")
        # check password hash if provided
        if ADMIN_PASSWORD_HASH:
            if check_password_hash(ADMIN_PASSWORD_HASH, pw):
                session["admin_authenticated"] = True
                flash("Logged in.")
                return redirect(url_for("admin_index"))
            else:
                flash("Invalid password", "danger")
                return redirect(url_for("login"))
        else:
            # fallback to ADMIN_KEY in GET param
            if request.args.get("key") and ADMIN_KEY and request.args.get("key") == ADMIN_KEY:
                session["admin_authenticated"] = True
                flash("Logged in via ADMIN_KEY (fallback). Please set ADMIN_PASSWORD for better security.")
                return redirect(url_for("admin_index"))
            flash("No ADMIN_PASSWORD configured and no valid ADMIN_KEY provided", "danger")
            return redirect(url_for("login"))
    # GET
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.pop("admin_authenticated", None)
    flash("Logged out.")
    return redirect(url_for("login"))

# Protect admin routes with decorator
from functools import wraps
def require_admin(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        # allow login via ?key=ADMIN_KEY as a one-off (legacy)
        if not is_authenticated():
            key = request.args.get("key")
            if key and ADMIN_KEY and key == ADMIN_KEY:
                session["admin_authenticated"] = True
                flash("Logged in via ADMIN_KEY (fallback). Consider setting ADMIN_PASSWORD.")
            else:
                return redirect(url_for("login", next=request.path))
        return f(*args, **kwargs)
    return wrapper

# ---------------- ADMIN ROUTES ----------------
@app.route("/admin")
@require_admin
def admin_index():
    q = request.args.get("q", "").strip()
    page = max(int(request.args.get("page", 1)), 1)
    per_page = min(max(int(request.args.get("per_page", 50)), 10), 500)
    keys, total = compute_keys(filter_q=q or None, page=page, per_page=per_page)
    status = admin_status()
    pages = (total + per_page - 1) // per_page
    return render_template("index.html",
                           keys=keys,
                           status=status,
                           q=q,
                           page=page,
                           per_page=per_page,
                           pages=pages,
                           total=total)

@app.route("/admin/generate", methods=["POST"])
@require_admin
def admin_generate():
    try:
        days = int(request.form.get("days", "30"))
        count = int(request.form.get("count", "1"))
    except Exception:
        flash("Invalid input", "danger")
        return redirect(url_for("admin_index"))
    if days < 1 or count < 1 or count > 2000:
        flash("Unreasonable values", "danger")
        return redirect(url_for("admin_index"))
    created = generate_keys(days, count)
    flash(f"Created {len(created)} keys.")
    return redirect(url_for("admin_index"))

@app.route("/admin/delete", methods=["POST"])
@require_admin
def admin_delete():
    key_to_delete = request.form.get("key_to_delete")
    if not key_to_delete:
        flash("No key provided", "danger")
    else:
        delete_key(key_to_delete)
        flash(f"Deleted {key_to_delete}")
    return redirect(url_for("admin_index"))

@app.route("/admin/export.csv")
@require_admin
def admin_export_csv():
    q = request.args.get("q", "").strip()
    keys, _ = compute_keys(filter_q=q or None, page=1, per_page=1000000)
    si = StringIO()
    cw = csv.writer(si)
    cw.writerow(["key", "expires", "redeemed_by", "active"])
    for k in keys:
        cw.writerow([k["key"], k["expires"] or "", k["redeemed_by"] or "", "yes" if k["active"] else "no"])
    output = si.getvalue().encode("utf-8")
    return Response(output, mimetype="text/csv",
                    headers={"Content-Disposition": "attachment;filename=keys_export.csv"})

# ---------------- KEEP-ALIVE / RUN ----------------
def run_flask():
    app.run(host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    print("Starting combined bot + improved admin UI")
    # start flask in background thread
    Thread(target=run_flask, daemon=True).start()
    # start bot polling
    bot.polling(none_stop=True, timeout=60)
