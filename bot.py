#!/usr/bin/env python3
"""TEAM SPIRIT — Text Collector (Bot API)"""
import html, logging, os, re, sqlite3, sys
from datetime import datetime
from pathlib import Path
from telegram import Update, BotCommand
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("collector")

env_path = Path(__file__).parent / ".env"
if env_path.exists():
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip("\"'"))

BOT_TOKEN = os.getenv("BOT_TOKEN")

# Auto‑detect Render by checking for Render's env variables
IS_RENDER = os.getenv("RENDER") == "true"
RENDER_URL = os.getenv("RENDER_URL") or os.getenv("RENDER_EXTERNAL_URL", "")
RENDER_URL = RENDER_URL.rstrip("/")
# PTB's run_webhook listens on the root path (/) by default
WEBHOOK_URL = RENDER_URL if RENDER_URL else None
PORT = int(os.getenv("PORT", 8080))

if not BOT_TOKEN: log.error("Задай BOT_TOKEN"); sys.exit(1)
DATA_DIR = Path(os.getenv("DATA_DIR", Path(__file__).parent / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "collector.db"
TEXT_OUTPUT = DATA_DIR / "collected_texts.txt"
PRIVATE_FILE = DATA_DIR / "private_links.txt"
MIN_WORDS = 50

conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("""CREATE TABLE IF NOT EXISTS texts (
    id INTEGER PRIMARY KEY AUTOINCREMENT, content TEXT NOT NULL,
    source_chat TEXT, source_chat_title TEXT, source_message_id INTEGER,
    source_link TEXT, collected_at TEXT DEFAULT (datetime('now')),
    word_count INTEGER DEFAULT 0)""")
conn.execute("""CREATE TABLE IF NOT EXISTS tracked_channels (
    username TEXT PRIMARY KEY, title TEXT, added_at TEXT DEFAULT (datetime('now')))""")
conn.commit()

def wc(text): return len(text.split())
def esc(text): return html.escape(str(text or ""))

def save_text(content, chat_title="?", chat_id="?", msg_id=None, link=None):
    content = content.strip()
    if not content or wc(content) < MIN_WORDS: return False
    words = wc(content)
    conn.execute("INSERT INTO texts (content,source_chat,source_chat_title,source_message_id,source_link,word_count) VALUES (?,?,?,?,?,?)",
                 (content, chat_id, chat_title, msg_id, link, words))
    conn.commit()
    with open(TEXT_OUTPUT, "a", encoding="utf-8") as f:
        f.write(f"=== {chat_title} | msg#{msg_id or '?'} | {words} слов | {datetime.now():%Y-%m-%d %H:%M} ===\n{content}\n\n")
    log.info("💾 %d слов из %s", words, chat_title)
    return True

async def handle_msg(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    # Работает и для лички (update.message), и для каналов (update.channel_post)
    msg = update.effective_message or update.channel_post
    if not msg: return
    chat = msg.chat
    title = chat.title or chat.username or chat.first_name or "?"
    is_channel = chat.type in ("channel", "supergroup")
    if is_channel:
        log.info("📢 Канал %s: сообщение #%d (%d слов)", title, msg.message_id, wc(msg.text or ""))

    if msg.text and wc(msg.text) >= MIN_WORDS:
        link = f"https://t.me/{chat.username}/{msg.message_id}" if chat.username else None
        save_text(msg.text, title, str(chat.id), msg.message_id, link)

    if msg.text:
        for link in re.findall(r'(?:https?://)?(?:t\.me|telegram\.me)/([a-zA-Z0-9_+\-]{3,})', msg.text):
            if link.lower() in ("bot","botfather","telegram","gif","sticker","premium"): continue
            if conn.execute("SELECT 1 FROM tracked_channels WHERE username=?", (link,)).fetchone(): continue
            t = link
            if not link.startswith("+"):
                try:
                    ci = await ctx.bot.get_chat(f"@{link}")
                    t = ci.title or ci.username or link
                except: pass
            conn.execute("INSERT OR IGNORE INTO tracked_channels (username,title) VALUES (?,?)", (link, t))
            conn.commit()
            if link.startswith("+"):
                with open(PRIVATE_FILE, "a") as f: f.write(f"t.me/{link} | из {title} | {datetime.now()}\n")
                # Не отвечаем в канал — только логируем
                if not is_channel:
                    await msg.reply_text(f"🔒 Приватная: <code>t.me/{link}</code>\nДобавь бота вручную.", parse_mode="HTML")
            else:
                if not is_channel:
                    await msg.reply_text(f"📡 <b>{esc(t)}</b> (@{link}) запомнил.", parse_mode="HTML")
    if msg.document:
        try:
            fn = msg.document.file_name or "unknown.txt"
            if fn.endswith(".txt") or "text/plain" in (msg.document.mime_type or ""):
                file = await msg.document.get_file()
                data = await file.download_as_bytearray()
                for b in re.split(r'\n\s*\n', data.decode("utf-8", errors="replace")):
                    if wc(b) >= MIN_WORDS:
                        save_text(b.strip(), title, str(chat.id), msg.message_id, f"file:{fn}")
                if not is_channel:
                    await msg.reply_text(f"✅ {fn} обработан.", parse_mode="HTML")
        except Exception as e: log.error("Файл: %s", e)

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 <b>TEAM SPIRIT — Text Collector</b>\n\n"
        "Собираю все тексты от <b>50 слов</b> из чатов где я есть.\n\n"
        "Команды:\n"
        "/stats — статистика\n"
        "/channels — список каналов\n"
        "/search <слова> — поиск\n"
        "/export — скачать всё\n"
        "/help — справка\n\n"
        "<b>Как работает:</b>\n"
        "• Добавь меня в канал/группу админом — читаю всё\n"
        "• Кидай ссылки t.me/канал — запоминаю\n"
        "• Кидай .txt файлы — разбираю",
        parse_mode="HTML")

async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    total = conn.execute("SELECT COUNT(*) FROM texts").fetchone()[0]
    words = conn.execute("SELECT COALESCE(SUM(word_count),0) FROM texts").fetchone()[0]
    today = conn.execute("SELECT COUNT(*) FROM texts WHERE collected_at>=datetime('now','-1 day')").fetchone()[0]
    ch = conn.execute("SELECT COUNT(*) FROM tracked_channels").fetchone()[0]
    top = conn.execute("SELECT source_chat_title,COUNT(*) FROM texts GROUP BY source_chat_title ORDER BY COUNT(*) DESC LIMIT 5").fetchall()
    lines = [f"📊 <b>Статистика</b>\n", f"Текстов: <b>{total}</b>", f"Слов: <b>{words:,}</b>",
             f"За 24ч: <b>{today}</b>", f"Каналов: <b>{ch}</b>"]
    if top: lines.extend(["\n🏆 <b>Топ:</b>"] + [f"• {esc(t or '?')}: {c}" for t,c in top])
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")

async def cmd_channels(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    rows = conn.execute("SELECT username,title FROM tracked_channels ORDER BY added_at DESC LIMIT 30").fetchall()
    if not rows: await update.message.reply_text("📭 Пусто."); return
    lines = ["📋 <b>Каналы:</b>\n"] + [f"📡 @{u} — {esc(t or u)[:60]}" for u,t in rows]
    text = "\n".join(lines)
    if len(text) > 3500: text = text[:3500] + "\n\n..."
    await update.message.reply_text(text, parse_mode="HTML")

async def cmd_search(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args: return
    q = " ".join(ctx.args)
    rows = conn.execute("SELECT id,substr(content,1,200),source_chat_title,word_count FROM texts WHERE content LIKE ? ORDER BY id DESC LIMIT 15", (f"%{q}%",)).fetchall()
    if not rows: await update.message.reply_text(f"🔍 Ничего: <b>{esc(q)}</b>", parse_mode="HTML"); return
    lines = [f"🔍 <b>{esc(q)}</b> ({len(rows)})\n"]
    for pid,prev,src,wc in rows:
        lines.append(f"#{pid} | {esc(src or '?')} | {wc} слов\n<i>{esc(prev[:150])}</i>\n")
    text = "\n".join(lines)
    if len(text) > 3500: text = text[:3500] + "\n\n..."
    await update.message.reply_text(text, parse_mode="HTML")

async def cmd_export(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not TEXT_OUTPUT.exists(): await update.message.reply_text("❌ Пусто."); return
    await update.message.reply_text("📦 Отправляю...")
    with open(TEXT_OUTPUT, "rb") as f:
        await update.message.reply_document(f, filename=f"texts_{datetime.now():%Y%m%d_%H%M}.txt")

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 <b>Справка</b>\n\n"
        "1. Добавь бота в канал/группу <b>администратором</b>\n"
        "2. Бот видит новые сообщения, сохраняет тексты ≥50 слов\n"
        "3. Кидай ссылки t.me/... — запоминаю\n"
        "4. .txt файлы — разбираю\n"
        "5. Приватные (t.me/+...) — добавь бота вручную\n\n"
        "Команды:\n"
        "/stats — статистика\n"
        "/channels — список каналов\n"
        "/search <слова> — поиск\n"
        "/export — скачать всё\n"
        "/help — справка\n\n"
        "Файл: data/collected_texts.txt — все тексты подряд",
        parse_mode="HTML")

async def post_init(app: Application):
    me = await app.bot.get_me()
    log.info("🤖 Бот @%s запущен", me.username or "?")
    log.info("📁 База: %s", DB_PATH)
    if not IS_RENDER:
        log.info("🔄 Режим: polling")
    await app.bot.set_my_commands([
        BotCommand("start","Привет"), BotCommand("stats","Статистика"),
        BotCommand("channels","Каналы"), BotCommand("search","Поиск"),
        BotCommand("export","Скачать"), BotCommand("help","Помощь"),
    ])

app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
app.add_handler(CommandHandler("start", cmd_start))
app.add_handler(CommandHandler("stats", cmd_stats))
app.add_handler(CommandHandler("channels", cmd_channels))
app.add_handler(CommandHandler("search", cmd_search))
app.add_handler(CommandHandler("export", cmd_export))
app.add_handler(CommandHandler("help", cmd_help))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_msg))
app.add_handler(MessageHandler(filters.Document.ALL, handle_msg))

if __name__ == "__main__":
    if IS_RENDER:
        log.info("🌐 Вебхук: %s", WEBHOOK_URL)
        app.run_webhook(listen="0.0.0.0", port=PORT, webhook_url=WEBHOOK_URL)
    else:
        log.info("Polling...")
        app.run_polling(allowed_updates=Update.ALL_TYPES)
