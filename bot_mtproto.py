#!/usr/bin/env python3
"""TEAM SPIRIT — Telegram Collector (MTProto/Telethon)
   Читает публичные каналы без вступления. Для Fly.io/Render."""
import asyncio, logging, os, re, sqlite3, sys
from datetime import datetime
from pathlib import Path
from telethon import TelegramClient, errors, events

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("collector-mt")

env_path = Path(__file__).parent / ".env"
if env_path.exists():
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip("\"'"))

API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
if not all([API_ID, API_HASH, BOT_TOKEN]):
    log.error("Задай API_ID, API_HASH, BOT_TOKEN в .env"); sys.exit(1)

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
conn.execute("""CREATE TABLE IF NOT EXISTS scrape_queue (
    username TEXT PRIMARY KEY, title TEXT,
    added_at TEXT DEFAULT (datetime('now')),
    status TEXT DEFAULT 'pending', last_msg_id INTEGER DEFAULT 0,
    total_saved INTEGER DEFAULT 0, error TEXT)""")
conn.commit()

def wc(text): return len(text.split())
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

client = TelegramClient("mt_session", API_ID, API_HASH).start(bot_token=BOT_TOKEN)
RE_LINK = re.compile(r'(?:https?://)?(?:t\.me|telegram\.me)/([a-zA-Z0-9_+\-]{3,})')
_scanning = True

@client.on(events.NewMessage)
async def on_msg(event):
    global _scanning
    msg = event.message; chat = await event.get_chat()
    title = getattr(chat, "title", None) or getattr(chat, "username", None) or "?"
    if msg.text and wc(msg.text) >= MIN_WORDS:
        link = f"https://t.me/{chat.username}/{msg.id}" if getattr(chat,"username",None) else None
        save_text(msg.text, title, str(chat.id), msg.id, link)
    if msg.text:
        for link in RE_LINK.findall(msg.text):
            if link.lower() in ("bot","botfather","telegram","gif","sticker","premium"): continue
            if conn.execute("SELECT 1 FROM scrape_queue WHERE username=?", (link,)).fetchone(): continue
            if link.startswith("+"):
                with open(PRIVATE_FILE, "a") as f: f.write(f"t.me/{link} | из {title} | {datetime.now()}\n")
                await safe_send(chat.id, f"🔒 Приватная: <code>t.me/{link}</code>\nДобавь бота вручную.")
                continue
            try:
                entity = await client.get_entity(link)
                if not hasattr(entity, "title"): continue
                t = getattr(entity, "title", None) or link
                conn.execute("INSERT OR IGNORE INTO scrape_queue (username,title) VALUES (?,?)", (link, t))
                conn.commit()
                log.info("➕ @%s (%s) в очередь", link, t)
                await safe_send(chat.id, f"📡 <b>{t}</b> (@{link}) в очереди на сканирование.")
            except: pass
    if msg.document:
        try:
            fn = msg.file.name or "unknown.txt"
            if fn.endswith(".txt") or "text/plain" in (msg.file.mime_type or ""):
                fb = await msg.download_media(bytes=True) or await client.download_file(msg.document, bytes=True)
                for b in re.split(r'\n\s*\n', fb.decode("utf-8", errors="replace")):
                    if wc(b) >= MIN_WORDS: save_text(b.strip(), title, str(chat.id), msg.id, f"file:{fn}")
                await safe_send(chat.id, f"✅ {fn} обработан.")
        except: pass

@client.on(events.NewMessage(pattern=r"^/start$"))
async def cmd_start(event):
    await safe_send(event.chat_id,
        "👋 <b>TEAM SPIRIT MTProto</b>\n\n"
        "Читаю публичные каналы без вступления. Собираю тексты ≥50 слов.\n\n"
        "Команды:\n/stats — статистика\n/queue — очередь каналов\n"
        "/search <слова> — поиск\n/export — скачать всё\n"
        "/pause — пауза\n/resume — продолжить\n\n"
        "Кидай ссылки t.me/канал — сам зайду и выкачаю всё.")

@client.on(events.NewMessage(pattern=r"^/stats$"))
async def cmd_stats(event):
    total = conn.execute("SELECT COUNT(*) FROM texts").fetchone()[0]
    words = conn.execute("SELECT COALESCE(SUM(word_count),0) FROM texts").fetchone()[0]
    today = conn.execute("SELECT COUNT(*) FROM texts WHERE collected_at>=datetime('now','-1 day')").fetchone()[0]
    qd = conn.execute("SELECT COUNT(*) FROM scrape_queue WHERE status='done'").fetchone()[0]
    qp = conn.execute("SELECT COUNT(*) FROM scrape_queue WHERE status='pending'").fetchone()[0]
    await safe_send(event.chat_id,
        f"📊 <b>Статистика</b>\n\nТекстов: <b>{total}</b>\nСлов: <b>{words:,}</b>\n"
        f"За 24ч: <b>{today}</b>\n\nКаналы: ✅ {qd} готово, ⏳ {qp} в очереди")

@client.on(events.NewMessage(pattern=r"^/queue$"))
async def cmd_queue(event):
    rows = conn.execute("SELECT username,title,status,total_saved FROM scrape_queue ORDER BY added_at DESC LIMIT 20").fetchall()
    if not rows: await safe_send(event.chat_id, "📭 Пусто."); return
    lines = ["📋 <b>Очередь:</b>\n"]
    emoji = {"done":"✅","pending":"⏳","active":"🔄","error":"❌"}
    for u,t,s,sv in rows:
        lines.append(f"{emoji.get(s,'❓')} @{u} — {t or u}" + (f" ({sv})" if sv else ""))
    await safe_send(event.chat_id, "\n".join(lines))

@client.on(events.NewMessage(pattern=r"^/search (.+)"))
async def cmd_search(event):
    q = event.pattern_match.group(1)
    rows = conn.execute("SELECT id,substr(content,1,200),source_chat_title,word_count FROM texts WHERE content LIKE ? ORDER BY id DESC LIMIT 15", (f"%{q}%",)).fetchall()
    if not rows: await safe_send(event.chat_id, f"🔍 Ничего: <b>{q}</b>"); return
    lines = [f"🔍 <b>{q}</b> ({len(rows)})\n"]
    for pid,prev,src,wc in rows:
        lines.append(f"#{pid} | {src or '?'} | {wc} слов\n<i>{prev[:150]}</i>\n")
    await safe_send(event.chat_id, "\n".join(lines))

@client.on(events.NewMessage(pattern=r"^/export$"))
async def cmd_export(event):
    if not TEXT_OUTPUT.exists(): await safe_send(event.chat_id, "❌ Пусто."); return
    await safe_send(event.chat_id, "📦 Отправляю...")
    await client.send_file(event.chat_id, str(TEXT_OUTPUT))

@client.on(events.NewMessage(pattern=r"^/pause$"))
async def cmd_pause(event):
    global _scanning; _scanning = False
    await safe_send(event.chat_id, "⏸ Пауза.")

@client.on(events.NewMessage(pattern=r"^/resume$"))
async def cmd_resume(event):
    global _scanning; _scanning = True
    await safe_send(event.chat_id, "▶️ Продолжаем.")

@client.on(events.NewMessage(pattern=r"^/help$"))
async def cmd_help(event):
    await safe_send(event.chat_id,
        "📖 <b>Справка</b>\n\n"
        "1. Добавь бота в канал/группу админом — читает всё\n"
        "2. Кидай ссылки t.me/канал — бот сканирует всю историю\n"
        "3. Публичные каналы читает <b>без вступления</b>\n"
        "4. Приватные (t.me/+...) — добавь бота вручную\n"
        "5. .txt файлы — разбирает\n"
        "6. Сохраняет прогресс — перезапуск не страшен\n\n"
        "Команды:\n"
        "/stats — статистика\n/queue — очередь\n/search слова — поиск\n"
        "/export — скачать всё\n/pause — пауза\n/resume — продолжить\n"
        "/help — справка")

async def safe_send(chat_id, text, parse_mode="html"):
    try: await client.send_message(chat_id, text, parse_mode=parse_mode)
    except Exception as e: log.warning("Не отправилось: %s", e)

async def scrape_channel(username, resume_from=0):
    try: entity = await client.get_entity(username)
    except errors.UsernameNotOccupiedError: return -1
    except Exception as e: log.warning("@%s: %s", username, e); return -1
    title = getattr(entity, "title", None) or username
    saved = processed = 0; last_id = resume_from
    log.info("📡 @%s с msg#%d...", username, resume_from)
    conn.execute("INSERT OR REPLACE INTO scrape_queue (username,title,status,last_msg_id) VALUES (?,?,'active',?)",
                 (username, title, resume_from)); conn.commit()
    try:
        async for msg in client.iter_messages(entity, min_id=resume_from, reverse=True, wait_time=2):
            processed += 1; last_id = msg.id
            if msg.text and wc(msg.text) >= MIN_WORDS:
                link = f"https://t.me/{entity.username}/{msg.id}" if getattr(entity,"username",None) else None
                save_text(msg.text, title, str(entity.id), msg.id, link); saved += 1
            if processed % 500 == 0:
                conn.execute("UPDATE scrape_queue SET last_msg_id=?,total_saved=total_saved+? WHERE username=?",
                             (last_id, saved, username)); conn.commit()
                log.info("  ⏳ @%s: %d обработано, %d сохранено", username, processed, saved)
                await asyncio.sleep(1)
    except errors.FloodWaitError as e:
        conn.execute("UPDATE scrape_queue SET status='pending',last_msg_id=?,total_saved=total_saved+? WHERE username=?",
                     (last_id, saved, username)); conn.commit()
        log.warning("FloodWait @%s: %dс", username, e.seconds)
        await asyncio.sleep(e.seconds)
        return await scrape_channel(username, last_id)
    except Exception as e:
        conn.execute("UPDATE scrape_queue SET status='error',error=?,last_msg_id=?,total_saved=total_saved+? WHERE username=?",
                     (str(e)[:500], last_id, saved, username)); conn.commit()
        log.error("❌ @%s: %s", username, e); return saved
    conn.execute("UPDATE scrape_queue SET status='done',last_msg_id=?,total_saved=total_saved+? WHERE username=?",
                 (last_id, saved, username)); conn.commit()
    log.info("✅ @%s: %d сообщений, %d сохранено", username, processed, saved)
    return saved

async def queue_worker():
    while True:
        if not _scanning: await asyncio.sleep(5); continue
        try:
            row = conn.execute("SELECT username,last_msg_id FROM scrape_queue WHERE status='pending' ORDER BY added_at LIMIT 1").fetchone()
            if row:
                await scrape_channel(row[0], row[1])
                await asyncio.sleep(30)
            else: await asyncio.sleep(10)
        except Exception as e: log.error("Очередь: %s", e); await asyncio.sleep(30)

async def main():
    me = await client.get_me()
    log.info("=" * 50)
    log.info("🤖 @%s (MTProto) запущен", me.username or "?")
    log.info("📁 База: %s", DB_PATH)
    log.info("🌐 Режим: чтение каналов без входа + активные чаты")
    log.info("=" * 50)
    asyncio.create_task(queue_worker())
    await client.run_until_disconnected()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt: log.info("Остановлен.")
    finally: conn.close()
