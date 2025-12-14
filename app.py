import os
import random
from datetime import datetime, timedelta
import psycopg2
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# ================= ENV =================
TOKEN = os.environ.get("TELEGRAM_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "7011151235"))

LOG_FILE = "logs.txt"

if not TOKEN or not DATABASE_URL:
    raise Exception("Missing TELEGRAM_TOKEN or DATABASE_URL")

# ================= BOT =================
bot = telebot.TeleBot(TOKEN)

# ================= DATABASE =================
conn = psycopg2.connect(DATABASE_URL, sslmode="require")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS keys (
    key TEXT PRIMARY KEY,
    expires TIMESTAMP,
    redeemed_by BIGINT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    expires TIMESTAMP
)
""")
conn.commit()

# ================= LOAD LOGS =================
def load_logs():
    if not os.path.exists(LOG_FILE):
        open(LOG_FILE, "w").close()
    with open(LOG_FILE, "r", encoding="utf-8", errors="ignore") as f:
        return [line.strip() for line in f if line.strip()]

logs = load_logs()
sent = {}

# ================= KEY CHECK =================
def has_active_key(user_id):
    cursor.execute("SELECT expires FROM users WHERE user_id=%s", (user_id,))
    row = cursor.fetchone()
    if not row:
        return False
    if datetime.now() <= row[0]:
        return True
    cursor.execute("DELETE FROM users WHERE user_id=%s", (user_id,))
    conn.commit()
    return False

# ================= COMMANDS =================
@bot.message_handler(commands=["createkey"])
def create_key(message):
    if message.from_user.id != ADMIN_ID:
        bot.reply_to(message, "❌ Not authorized")
        return
    try:
        _, days, count = message.text.split()
        days, count = int(days), int(count)
        keys = []
        for _ in range(count):
            key = f"KEY-{random.randint(100000,999999)}"
            expires = datetime.now() + timedelta(days=days)
            cursor.execute(
                "INSERT INTO keys VALUES (%s,%s,NULL)",
                (key, expires)
            )
            keys.append(key)
        conn.commit()
        bot.reply_to(message, "✅ Keys generated:\n" + "\n".join(keys))
    except:
        bot.reply_to(message, "Usage: /createkey <days> <count>")

@bot.message_handler(commands=["redeem"])
def redeem(message):
    try:
        _, key = message.text.split()
        uid = message.from_user.id
        cursor.execute(
            "SELECT expires FROM keys WHERE key=%s AND redeemed_by IS NULL",
            (key,)
        )
        row = cursor.fetchone()
        if not row:
            bot.reply_to(message, "❌ Invalid or used key")
            return
        cursor.execute(
            "INSERT INTO users VALUES (%s,%s) "
            "ON CONFLICT (user_id) DO UPDATE SET expires=%s",
            (uid, row[0], row[0])
        )
        cursor.execute(
            "UPDATE keys SET redeemed_by=%s WHERE key=%s",
            (uid, key)
        )
        conn.commit()
        bot.reply_to(message, f"✅ Access until {row[0]}")
    except:
        bot.reply_to(message, "Usage: /redeem KEY-XXXXXX")

@bot.message_handler(commands=["start"])
def start(message):
    if not has_active_key(message.from_user.id):
        bot.send_message(
            message.chat.id,
            "❌ You need a key\nUse /redeem <key>"
        )
        return
    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("🔍 Search Logs", callback_data="search"),
        InlineKeyboardButton("📞 Owner", url="https://t.me/OnlyJosh4")
    )
    bot.send_message(
        message.chat.id,
        "✅ Access granted",
        reply_markup=kb
    )

# ================= SEARCH =================
@bot.callback_query_handler(func=lambda c: c.data == "search")
def ask(call):
    msg = bot.send_message(call.message.chat.id, "🔎 Send keyword:")
    bot.register_next_step_handler(msg, search)

def search(message):
    kw = message.text.lower()
    results = []
    for line in logs:
        if kw in line.lower() and line not in sent.get(kw, set()):
            results.append(line)

    if not results:
        bot.send_message(message.chat.id, "❌ No results found")
        return

    sent.setdefault(kw, set()).update(results)

    file = f"results_{kw}.txt"
    with open(file, "w", encoding="utf-8") as f:
        f.write("\n".join(results))

    with open(file, "rb") as f:
        bot.send_document(
            message.chat.id,
            f,
            caption=f"✅ Found {len(results)} lines"
        )
    os.remove(file)

# ================= RUN =================
print("🤖 Bot running")
bot.polling(none_stop=True)