import os
import random
from datetime import datetime, timedelta
from threading import Thread

import psycopg2
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from flask import Flask

# ================= CONFIG =================
TOKEN = os.environ.get("TELEGRAM_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "7011151235"))

LOG_FILE = "logs.txt"
MAX_LINES = 200

if not TOKEN or not DATABASE_URL:
    raise RuntimeError("Missing required environment variables")

# ================= KEEP ALIVE (RENDER) =================
def keep_alive():
    app = Flask(__name__)

    @app.route("/")
    def home():
        return "Bot running"

    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

# ================= BOT =================
bot = telebot.TeleBot(TOKEN, threaded=True)

# ================= DATABASE =================
conn = psycopg2.connect(DATABASE_URL, sslmode="require")
cursor = conn.cursor()

def init_db():
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS keys (
        key TEXT PRIMARY KEY,
        expires TIMESTAMP,
        redeemed_by BIGINT
    )""")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id BIGINT PRIMARY KEY,
        expires TIMESTAMP
    )""")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS searches (
        user_id BIGINT,
        keyword TEXT,
        line TEXT,
        PRIMARY KEY (user_id, keyword, line)
    )""")

    conn.commit()

init_db()

# ================= LOG LOADER =================
def load_logs():
    if not os.path.exists(LOG_FILE):
        open(LOG_FILE, "w").close()
    with open(LOG_FILE, "r", encoding="utf-8", errors="ignore") as f:
        return [l.strip() for l in f if l.strip()]

LOGS = load_logs()

# ================= ACCESS HELPERS =================
def user_has_access(user_id: int) -> bool:
    cursor.execute("SELECT expires FROM users WHERE user_id=%s", (user_id,))
    row = cursor.fetchone()
    if not row:
        return False

    if datetime.now() <= row[0]:
        return True

    cursor.execute("DELETE FROM users WHERE user_id=%s", (user_id,))
    conn.commit()
    return False

def get_user_expiry(user_id: int):
    cursor.execute("SELECT expires FROM users WHERE user_id=%s", (user_id,))
    row = cursor.fetchone()
    return row[0] if row else None

# ================= UI =================
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

# ================= COMMANDS =================
@bot.message_handler(commands=["start"])
def start_cmd(message):
    if not user_has_access(message.from_user.id):
        bot.send_message(
            message.chat.id,
            "❌ Access required\nUse /redeem <key>"
        )
        return

    bot.send_message(
        message.chat.id,
        "✅ Welcome! Choose an option:",
        reply_markup=main_menu()
    )

@bot.message_handler(commands=["createkey"])
def create_key_cmd(message):
    if message.from_user.id != ADMIN_ID:
        bot.reply_to(message, "❌ Admin only")
        return

    try:
        _, days, count = message.text.split()
        days, count = int(days), int(count)
    except ValueError:
        bot.reply_to(message, "Usage: /createkey <days> <count>")
        return

    keys = []
    expires = datetime.now() + timedelta(days=days)

    for _ in range(count):
        key = f"KEY-{random.randint(100000, 999999)}"
        cursor.execute(
            "INSERT INTO keys VALUES (%s,%s,NULL)",
            (key, expires)
        )
        keys.append(key)

    conn.commit()
    bot.reply_to(message, "✅ Keys created:\n" + "\n".join(keys))

@bot.message_handler(commands=["redeem"])
def redeem_cmd(message):
    try:
        _, key = message.text.split()
    except ValueError:
        bot.reply_to(message, "Usage: /redeem KEY-XXXXXX")
        return

    uid = message.from_user.id
    cursor.execute(
        "SELECT expires FROM keys WHERE key=%s AND redeemed_by IS NULL",
        (key,)
    )
    row = cursor.fetchone()

    if not row:
        bot.reply_to(message, "❌ Invalid or used key")
        return

    cursor.execute("""
        INSERT INTO users (user_id, expires)
        VALUES (%s,%s)
        ON CONFLICT (user_id)
        DO UPDATE SET expires=EXCLUDED.expires
    """, (uid, row[0]))

    cursor.execute(
        "UPDATE keys SET redeemed_by=%s WHERE key=%s",
        (uid, key)
    )

    conn.commit()
    bot.reply_to(message, f"✅ Access valid until:\n{row[0]}")

# ================= CALLBACKS =================
@bot.callback_query_handler(func=lambda c: c.data == "search")
def search_prompt(call):
    msg = bot.send_message(call.message.chat.id, "🔎 Send keyword:")
    bot.register_next_step_handler(msg, process_search)

@bot.callback_query_handler(func=lambda c: c.data == "stats")
def stats_cb(call):
    cursor.execute(
        "SELECT COUNT(*) FROM searches WHERE user_id=%s",
        (call.from_user.id,)
    )
    total = cursor.fetchone()[0]
    bot.send_message(call.message.chat.id, f"📊 Total unique lines saved: {total}")

@bot.callback_query_handler(func=lambda c: c.data == "access")
def access_cb(call):
    expiry = get_user_expiry(call.from_user.id)
    if expiry:
        bot.send_message(call.message.chat.id, f"⏳ Access until:\n{expiry}")
    else:
        bot.send_message(call.message.chat.id, "❌ No active access")

@bot.callback_query_handler(func=lambda c: c.data == "reset")
def reset_cb(call):
    cursor.execute(
        "DELETE FROM searches WHERE user_id=%s",
        (call.from_user.id,)
    )
    conn.commit()
    bot.send_message(call.message.chat.id, "♻️ Search memory cleared")

# ================= SEARCH CORE =================
def process_search(message):
    uid = message.from_user.id
    keyword = message.text.lower().strip()

    if not keyword:
        bot.send_message(message.chat.id, "❌ Empty keyword")
        return

    found = []

    for line in LOGS:
        if len(found) >= MAX_LINES:
            break

        if keyword in line.lower():
            cursor.execute("""
                SELECT 1 FROM searches
                WHERE user_id=%s AND keyword=%s AND line=%s
            """, (uid, keyword, line))

            if not cursor.fetchone():
                found.append(line)
                cursor.execute(
                    "INSERT INTO searches VALUES (%s,%s,%s)",
                    (uid, keyword, line)
                )

    if not found:
        bot.send_message(message.chat.id, "❌ No new results found")
        return

    conn.commit()

    filename = f"results_{keyword}.txt"
    with open(filename, "w", encoding="utf-8") as f:
        f.write("\n".join(found))

    with open(filename, "rb") as f:
        bot.send_document(
            message.chat.id,
            f,
            caption=f"✅ {len(found)} lines found (limit {MAX_LINES})"
        )

    os.remove(filename)

# ================= RUN =================
print("🤖 Bot started (optimized)")
Thread(target=keep_alive).start()
bot.polling(none_stop=True, timeout=60)
