# app.py
"""
Fixed combined admin panel + Telegram bot (single-file).
Deploy notes: set TELEGRAM_TOKEN and DATABASE_URL at minimum.
Optional: ADMIN_ID, ADMIN_KEY, PORT, FLASK_SECRET, LOG_FILE, MAX_LINES.
"""
import os
import random
import csv
import io
from datetime import datetime, timedelta
from threading import Thread
from functools import wraps

import psycopg2
import psycopg2.extras
import psycopg2.errors
from flask import Flask, request, jsonify, render_template, send_file, Response
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# ---------- CONFIG ----------
TOKEN = os.environ.get("TELEGRAM_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))
ADMIN_KEY = os.environ.get("ADMIN_KEY")  # optional
PORT = int(os.environ.get("PORT", 10000))
FLASK_SECRET = os.environ.get("FLASK_SECRET", "change-me")
LOG_FILE = os.environ.get("LOG_FILE", "logs.txt")
MAX_LINES = int(os.environ.get("MAX_LINES", "200"))

if not TOKEN or not DATABASE_URL:
    raise RuntimeError("Missing TELEGRAM_TOKEN or DATABASE_URL")

START_TIME = datetime.utcnow()

# ---------- APP & DB ----------
app = Flask(__name__, template_folder="templates")
app.secret_key = FLASK_SECRET

# Connect to Postgres
conn = psycopg2.connect(DATABASE_URL, sslmode="require")
conn.autocommit = True

def cursor(dict_cursor=False):
    if dict_cursor:
        return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return conn.cursor()

def init_db():
    with cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS keys (
            key TEXT PRIMARY KEY,
            expires TIMESTAMP,
            redeemed_by BIGINT
        )""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            expires TIMESTAMP,
            last_active TIMESTAMP
        )""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS searches (
            user_id BIGINT,
            keyword TEXT,
            line TEXT,
            PRIMARY KEY (user_id, keyword, line)
        )""")
init_db()

# ---------- LOGS ----------
def load_logs():
    if not os.path.exists(LOG_FILE):
        open(LOG_FILE, "w").close()
    try:
        with open(LOG_FILE, "r", encoding="utf-8", errors="ignore") as f:
            return [l.rstrip("\n") for l in f if l.strip()]
    except Exception:
        return []

LOGS = load_logs()

# ---------- UTIL ----------
def now_utc():
    return datetime.utcnow()

# ---------- ADMIN GUARD ----------
def require_admin(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        # If ADMIN_KEY not set, allow (public)
        if not ADMIN_KEY:
            return f(*args, **kwargs)
        provided = request.args.get("admin_key") or request.headers.get("X-ADMIN-KEY")
        if provided == ADMIN_KEY:
            return f(*args, **kwargs)
        if request.method == "POST" and request.form.get("admin_key") == ADMIN_KEY:
            return f(*args, **kwargs)
        return ("Unauthorized - provide admin_key= in query or X-ADMIN-KEY header", 401)
    return wrapped

# ---------- HELPERS (keys/users/stats) ----------
def list_keys(search=None, page=1, per_page=50):
    offset = (page - 1) * per_page
    params = []
    where = ""
    if search:
        where = " WHERE key ILIKE %s OR CAST(redeemed_by AS TEXT) ILIKE %s"
        params.extend([f"%{search}%", f"%{search}%"])
    sql = f"SELECT key, expires, redeemed_by FROM keys {where} ORDER BY expires DESC NULLS LAST LIMIT %s OFFSET %s"
    params.extend([per_page, offset])
    with cursor(dict_cursor=True) as cur:
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
    with cursor() as cur:
        count_sql = "SELECT COUNT(*) FROM keys" + (" WHERE key ILIKE %s OR CAST(redeemed_by AS TEXT) ILIKE %s" if search else "")
        if search:
            cur.execute(count_sql, (f"%{search}%", f"%{search}%"))
        else:
            cur.execute(count_sql)
        total = cur.fetchone()[0] or 0
    now = now_utc()
    out = []
    for r in rows:
        out.append({
            "key": r["key"],
            "expires": r["expires"],
            "redeemed_by": r["redeemed_by"],
            "active": (r["redeemed_by"] is None) and (r["expires"] and r["expires"] > now)
        })
    return out, total

def create_keys(days, count):
    expires = now_utc() + timedelta(days=days)
    created = []
    with cursor() as cur:
        for _ in range(count):
            for attempt in range(10):
                k = f"KEY-{random.randint(100000, 999999)}"
                try:
                    cur.execute("INSERT INTO keys (key, expires, redeemed_by) VALUES (%s,%s,NULL)", (k, expires))
                    created.append(k)
                    break
                except psycopg2.errors.UniqueViolation:
                    conn.rollback()
                    continue
    return created

def delete_key(k):
    with cursor() as cur:
        cur.execute("DELETE FROM keys WHERE key=%s", (k,))

def list_users(search=None, page=1, per_page=50):
    offset = (page - 1) * per_page
    params = []
    where = ""
    if search:
        where = " WHERE CAST(user_id AS TEXT) ILIKE %s"
        params.append(f"%{search}%")
    sql = f"SELECT user_id, expires, last_active FROM users {where} ORDER BY last_active DESC NULLS LAST LIMIT %s OFFSET %s"
    params.extend([per_page, offset])
    with cursor(dict_cursor=True) as cur:
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
    with cursor() as cur:
        count_sql = "SELECT COUNT(*) FROM users" + (" WHERE CAST(user_id AS TEXT) ILIKE %s" if search else "")
        if search:
            cur.execute(count_sql, (f"%{search}%",))
        else:
            cur.execute(count_sql)
        total = cur.fetchone()[0] or 0
    return rows, total

def revoke_user(user_id):
    with cursor() as cur:
        cur.execute("DELETE FROM users WHERE user_id=%s", (user_id,))

def extend_user(user_id, days):
    new_exp = now_utc() + timedelta(days=days)
    with cursor() as cur:
        cur.execute("""
            INSERT INTO users (user_id, expires, last_active)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET expires=EXCLUDED.expires
        """, (user_id, new_exp, now_utc()))

def stats():
    now = now_utc()
    with cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM users")
        total_users = cur.fetchone()[0] or 0
        cur.execute("SELECT COUNT(*) FROM users WHERE expires >= %s", (now,))
        active_users = cur.fetchone()[0] or 0
        # online: last_active within 5 minutes (safe)
        try:
            cur.execute("SELECT COUNT(*) FROM users WHERE last_active >= %s", (now - timedelta(minutes=5),))
            online_users = cur.fetchone()[0] or 0
        except Exception:
            online_users = 0
        cur.execute("SELECT COUNT(*) FROM keys")
        total_keys = cur.fetchone()[0] or 0
    up = now - START_TIME
    return {
        "uptime": str(up).split(".")[0],
        "total_users": total_users,
        "active_users": active_users,
        "online_users": online_users,
        "total_keys": total_keys
    }

# ---------- TELEGRAM BOT ----------
bot = telebot.TeleBot(TOKEN, threaded=True)

def touch_last_active(user_id):
    with cursor() as cur:
        cur.execute("""
            INSERT INTO users (user_id, expires, last_active)
            VALUES (%s, NULL, %s)
            ON CONFLICT (user_id) DO UPDATE SET last_active = EXCLUDED.last_active
        """, (user_id, now_utc()))

@bot.message_handler(commands=["start"])
def cmd_start(m):
    touch_last_active(m.from_user.id)
    with cursor() as cur:
        cur.execute("SELECT expires FROM users WHERE user_id=%s", (m.from_user.id,))
        r = cur.fetchone()
    if not r or r[0] is None or now_utc() > r[0]:
        bot.send_message(m.chat.id, "❌ Access required\nUse /redeem <key>")
        return
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("🔍 Search Logs", callback_data="search"),
        InlineKeyboardButton("📊 My Stats", callback_data="stats"),
        InlineKeyboardButton("⏳ My Access", callback_data="access"),
        InlineKeyboardButton("♻️ Reset Search", callback_data="reset"),
        InlineKeyboardButton("📞 Owner", url="https://t.me/OnlyJosh4"),
    )
    bot.send_message(m.chat.id, "✅ Welcome", reply_markup=kb)

@bot.message_handler(commands=["createkey"])
def cmd_createkey(m):
    if ADMIN_ID and m.from_user.id != ADMIN_ID:
        bot.reply_to(m, "❌ Admin only")
        return
    try:
        _, days, count = m.text.split()
        days, count = int(days), int(count)
    except Exception:
        bot.reply_to(m, "Usage: /createkey <days> <count>")
        return
    created = create_keys(days, count)
    bot.reply_to(m, "✅ Created:\n" + "\n".join(created))

@bot.message_handler(commands=["redeem"])
def cmd_redeem(m):
    touch_last_active(m.from_user.id)
    try:
        _, k = m.text.split()
    except Exception:
        bot.reply_to(m, "Usage: /redeem KEY-XXXXXX")
        return
    with cursor() as cur:
        cur.execute("SELECT expires FROM keys WHERE key=%s AND redeemed_by IS NULL", (k,))
        r = cur.fetchone()
        if not r:
            bot.reply_to(m, "❌ Invalid or used key")
            return
        expires = r[0]
        cur.execute("""
            INSERT INTO users (user_id, expires)
            VALUES (%s,%s)
            ON CONFLICT (user_id) DO UPDATE SET expires = EXCLUDED.expires
        """, (m.from_user.id, expires))
        cur.execute("UPDATE keys SET redeemed_by=%s WHERE key=%s", (m.from_user.id, k))
    bot.reply_to(m, f"✅ Access until {expires}")

# Optionally add your original search handler implementation here
# e.g. process_search function and callback registration.

# ---------- FLASK ROUTES ----------
@app.route("/")
@require_admin
def dashboard():
    return render_template("index.html")

@app.route("/api/stats")
@require_admin
def api_stats():
    return jsonify(stats())

@app.route("/api/keys", methods=["GET"])
@require_admin
def api_keys():
    q = request.args.get("q", "").strip() or None
    page = max(int(request.args.get("page", "1")), 1)
    per = min(max(int(request.args.get("per_page", "50")), 5), 1000)
    keys, total = list_keys(search=q, page=page, per_page=per)
    return jsonify({"keys": keys, "total": total, "page": page, "per_page": per})

@app.route("/api/keys/generate", methods=["POST"])
@require_admin
def api_gen_keys():
    data = request.form or request.get_json() or {}
    days = int(data.get("days", 30))
    count = int(data.get("count", 1))
    if days < 1 or count < 1 or count > 2000:
        return jsonify({"error":"invalid params"}), 400
    created = create_keys(days, count)
    return jsonify({"created": created, "count": len(created)})

@app.route("/api/keys/delete", methods=["POST"])
@require_admin
def api_delete_key():
    key = request.form.get("key") or (request.get_json() or {}).get("key")
    if not key:
        return jsonify({"error":"missing key"}), 400
    delete_key(key)
    return jsonify({"deleted": key})

@app.route("/api/keys/export.csv")
@require_admin
def api_export_keys():
    q = request.args.get("q", "").strip() or None
    keys, _ = list_keys(search=q, page=1, per_page=1000000)
    si = io.StringIO()
    cw = csv.writer(si)
    cw.writerow(["key","expires","redeemed_by","active"])
    for k in keys:
        cw.writerow([k["key"], k["expires"] or "", k["redeemed_by"] or "", "yes" if k["active"] else "no"])
    output = io.BytesIO(si.getvalue().encode("utf-8"))
    output.seek(0)
    return send_file(output, mimetype="text/csv", as_attachment=True, download_name="keys.csv")

@app.route("/api/users", methods=["GET"])
@require_admin
def api_users():
    q = request.args.get("q", "").strip() or None
    page = max(int(request.args.get("page", "1")), 1)
    per = min(max(int(request.args.get("per_page", "50")), 5), 1000)
    rows, total = list_users(search=q, page=page, per_page=per)
    return jsonify({"users": rows, "total": total, "page": page, "per_page": per})

@app.route("/api/users/revoke", methods=["POST"])
@require_admin
def api_revoke_user():
    uid = request.form.get("user_id") or (request.get_json() or {}).get("user_id")
    if not uid:
        return jsonify({"error":"missing user_id"}), 400
    revoke_user(int(uid))
    return jsonify({"revoked": int(uid)})

@app.route("/api/users/extend", methods=["POST"])
@require_admin
def api_extend_user():
    data = request.form or request.get_json() or {}
    uid = data.get("user_id")
    days = int(data.get("days", 30))
    if not uid:
        return jsonify({"error":"missing user_id"}), 400
    extend_user(int(uid), days)
    return jsonify({"extended": int(uid), "days": days})

@app.route("/api/users/export.csv")
@require_admin
def api_export_users():
    q = request.args.get("q", "").strip() or None
    users, _ = list_users(search=q, page=1, per_page=1000000)
    si = io.StringIO()
    cw = csv.writer(si)
    cw.writerow(["user_id","expires","last_active"])
    for u in users:
        cw.writerow([u["user_id"], u["expires"] or "", u["last_active"] or ""])
    output = io.BytesIO(si.getvalue().encode("utf-8"))
    output.seek(0)
    return send_file(output, mimetype="text/csv", as_attachment=True, download_name="users.csv")

@app.route("/api/logs/tail")
@require_admin
def api_logs_tail():
    n = min(int(request.args.get("n", "200")), 2000)
    try:
        with open(LOG_FILE, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()[-n:]
    except Exception:
        lines = []
    return jsonify({"lines": [l.rstrip("\n") for l in lines]})

@app.route("/api/logs/download")
@require_admin
def api_logs_download():
    try:
        return send_file(LOG_FILE, as_attachment=True)
    except Exception:
        return ("No log file"), 404

@app.route("/health")
def health():
    return jsonify({"status":"ok", "uptime": (now_utc() - START_TIME).total_seconds()})

# ---------- RUN ----------
def run_flask():
    app.run(host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    print("Starting admin panel + bot on port", PORT)
    Thread(target=run_flask, daemon=True).start()
    # polling must be on its own line:
    bot.polling(none_stop=True, timeout=60)
