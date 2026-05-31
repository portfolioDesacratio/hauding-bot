#!/usr/bin/env python3
"""Thesaurus Collector — Telegram Collector (MTProto/Telethon)
   Читает публичные каналы без вступления.
   Собирает тексты ≥50 слов и отправляет в назначенный канал."""
import asyncio, logging, os, re, sqlite3, sys
from datetime import datetime
from pathlib import Path
from telethon import TelegramClient, errors, events

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("thesaurus")

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

OWNER_ID = 8587090554  # @desacratio

IS_RENDER = os.getenv("RENDER") == "true"
RENDER_URL = os.getenv("RENDER_URL") or os.getenv("RENDER_EXTERNAL_URL", "")
RENDER_URL = RENDER_URL.rstrip("/")
PORT = int(os.getenv("PORT", 8080))

DATA_DIR = Path(os.getenv("DATA_DIR", Path(__file__).parent / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "collector.db"
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
conn.execute("""CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY, value TEXT NOT NULL)""")
conn.commit()

def wc(text): return len(text.split())

def get_output_chat():
    row = conn.execute("SELECT value FROM config WHERE key='output_chat'").fetchone()
    return int(row[0]) if row else None

def set_output_chat(chat_id):
    conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", ("output_chat", str(chat_id)))
    conn.commit()

def save_text(content, chat_title="?", chat_id="?", msg_id=None, link=None):
    content = content.strip()
    if not content or wc(content) < MIN_WORDS: return False
    words = wc(content)
    conn.execute("INSERT INTO texts (content,source_chat,source_chat_title,source_message_id,source_link,word_count) VALUES (?,?,?,?,?,?)",
                 (content, chat_id, chat_title, msg_id, link, words))
    conn.commit()
    log.info("💾 %d слов из %s | канал #%s", words, chat_title, chat_id)
    return True

session_path = str(DATA_DIR / "mt_session")
client = TelegramClient(session_path, API_ID, API_HASH)
RE_LINK = re.compile(r'(?:https?://)?(?:t\.me|telegram\.me)/([a-zA-Z0-9_+\-]{3,})')
_scanning = True

# ─── Health‑check HTTP (Render) ──────────────────────────────────────
async def health_server():
    async def handler(reader, writer):
        while b"\r\n\r\n" not in await reader.read(1024): pass
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 2\r\n\r\nok")
        await writer.drain()
        writer.close()
    server = await asyncio.start_server(handler, "0.0.0.0", PORT)
    log.info("🏥 Health check на 0.0.0.0:%d", PORT)
    async with server: await server.serve_forever()

# ─── Отправка в выходной канал ───────────────────────────────────────
async def send_to_output(text, source_title, source_link=None):
    out = get_output_chat()
    if not out: return
    header = f"📡 <b>{source_title}</b>"
    if source_link: header += f"\n🔗 {source_link}"
    text_clean = text.replace("<", "&lt;").replace(">", "&gt;")
    message = f"{header}\n\n{text_clean}"
    if len(message) > 3950:
        message = message[:3950] + "\n\n✂️ ..."
    try:
        await client.send_message(out, message, parse_mode="html")
        log.info("📤 Отправлено в канал вывода")
    except Exception as e:
        log.warning("Не отправилось в канал: %s", e)

# ─── Обработчики сообщений ──────────────────────────────────────────
@client.on(events.NewMessage)
async def on_msg(event):
    try:
        if event.out: return
        global _scanning
        msg = event.message
        chat = await event.get_chat()
        title = getattr(chat, "title", None) or getattr(chat, "username", None) or "?"
        log.info("📩 Сообщение от %s: «%s»", title, (msg.text or "")[:80])

        # Сохраняем и отправляем текст ≥50 слов
        if msg.text and wc(msg.text) >= MIN_WORDS:
            link = f"https://t.me/{chat.username}/{msg.id}" if getattr(chat, "username", None) else None
            if save_text(msg.text, title, str(chat.id), msg.id, link):
                await send_to_output(msg.text, title, link)

        # Ищем ссылки на каналы
        if msg.text:
            for link in RE_LINK.findall(msg.text):
                link_lower = link.lower()
                if link_lower in ("bot", "botfather", "telegram", "gif", "sticker", "premium"): continue
                if conn.execute("SELECT 1 FROM scrape_queue WHERE username=?", (link,)).fetchone(): continue

                if link.startswith("+"):
                    log.info("🔑 Пытаюсь зайти в %s из %s", link, title)
                    try:
                        await client.join_chat(link)
                        log.info("✅ Зашёл в %s!", link)
                        conn.execute("INSERT OR IGNORE INTO scrape_queue (username,title) VALUES (?,?)",
                                     (link, f"private:{link}"))
                        conn.commit()
                    except Exception as e:
                        log.warning("❌ Не зашёл в %s: %s", link, e)
                        with open(DATA_DIR / "private_links.txt", "a") as f:
                            f.write(f"t.me/{link} | из {title} | {datetime.now()}\n")
                    continue

                try:
                    entity = await client.get_entity(link)
                    if not hasattr(entity, "title"): continue
                    t = getattr(entity, "title", None) or link
                    conn.execute("INSERT OR IGNORE INTO scrape_queue (username,title) VALUES (?,?)", (link, t))
                    conn.commit()
                    log.info("➕ @%s (%s) в очередь", link, t)
                except errors.UsernameNotOccupiedError: pass
                except errors.FloodWaitError as e:
                    log.warning("FloodWait при get_entity: %dс", e.seconds)
                    await asyncio.sleep(e.seconds)
                except Exception as e:
                    log.debug("Не удалось получить @%s: %s", link, e)

        # Файлы .txt
        if msg.document:
            try:
                fn = msg.file.name or "unknown.txt"
                if fn.endswith(".txt") or "text/plain" in (msg.file.mime_type or ""):
                    fb = await msg.download_media(bytes=True) or await client.download_file(msg.document, bytes=True)
                    for b in re.split(r'\n\s*\n', fb.decode("utf-8", errors="replace")):
                        if wc(b) >= MIN_WORDS:
                            if save_text(b.strip(), title, str(chat.id), msg.id, f"file:{fn}"):
                                await send_to_output(b.strip(), title, f"file:{fn}")
            except Exception as e:
                log.error("Файл: %s", e)
    except Exception as e:
        log.exception("❌ on_msg: %s", e)

# ─── Добавление в чат ────────────────────────────────────────────────
@client.on(events.ChatAction)
async def on_chat_action(event):
    try:
        if event.user_added and event.user_id and event.user_id == (await client.get_me()).id:
            chat = await event.get_chat()
            title = getattr(chat, "title", None) or "?"
            log.info("➕ Добавлен в чат: %s (%d)", title, event.chat_id)
            if not getattr(chat, "username", None):
                conn.execute("INSERT OR IGNORE INTO config (key,value) VALUES (?,?)",
                             ("chat_" + str(event.chat_id), title))
                conn.commit()
    except Exception as e:
        log.exception("on_chat_action: %s", e)

# ─── Проверка владельца ──────────────────────────────────────────────
def is_owner(event):
    return event.sender_id == OWNER_ID

async def safe_send(chat_id, text, parse_mode="html"):
    try:
        await client.send_message(chat_id, text, parse_mode=parse_mode)
    except Exception as e:
        log.warning("Не отправилось: %s", e)

# ─── Команды (только для владельца) ──────────────────────────────────
@client.on(events.NewMessage(pattern=r"^/start$"))
async def cmd_start(event):
    try:
        await safe_send(event.chat_id,
            "👋 <b>Thesaurus Collector</b>\n\n"
            "Собираю тексты <b>≥50 слов</b> из публичных каналов.\n"
            "Нахожу новые каналы через ссылки в сообщениях.\n"
            "Пытаюсь зайти в закрытые чаты.\n\n"
            "Команды:\n"
            "/stats — статистика\n"
            "/add <username> — добавить канал\n"
            "/queue — очередь каналов\n"
            "/search <слова> — поиск\n"
            "/export — скачать всё\n"
            "/set_output — назначить этот чат для вывода\n"
            "/pause — пауза\n"
            "/resume — продолжить")
    except Exception as e:
        log.exception("cmd_start: %s", e)

@client.on(events.NewMessage(pattern=r"^/stats$"))
async def cmd_stats(event):
    try:
        if not is_owner(event): return
        total = conn.execute("SELECT COUNT(*) FROM texts").fetchone()[0]
        words = conn.execute("SELECT COALESCE(SUM(word_count),0) FROM texts").fetchone()[0]
        today = conn.execute("SELECT COUNT(*) FROM texts WHERE collected_at>=datetime('now','-1 day')").fetchone()[0]
        qd = conn.execute("SELECT COUNT(*) FROM scrape_queue WHERE status='done'").fetchone()[0]
        qp = conn.execute("SELECT COUNT(*) FROM scrape_queue WHERE status='pending'").fetchone()[0]
        qa = conn.execute("SELECT COUNT(*) FROM scrape_queue WHERE status='active'").fetchone()[0]
        out = get_output_chat()
        await safe_send(event.chat_id,
            f"📊 <b>Статистика</b>\n\n"
            f"Текстов: <b>{total}</b>\nСлов: <b>{words:,}</b>\n"
            f"За 24ч: <b>{today}</b>\n\n"
            f"Каналы: ✅ {qd} готово, 🔄 {qa} active, ⏳ {qp} в очереди\n"
            f"📤 Канал вывода: {'✅ ID ' + str(out) if out else '❌ не назначен'}")
    except Exception as e:
        log.exception("cmd_stats: %s", e)

@client.on(events.NewMessage(pattern=r"^/add (.+)"))
async def cmd_add(event):
    try:
        if not is_owner(event): return
        raw = event.pattern_match.group(1).strip().lower()
        username = re.sub(r'^(?:https?://)?(?:t\.me/|@)', '', raw).split('/')[0].split('?')[0]
        if not username or username.startswith("+"):
            await safe_send(event.chat_id, "❌ Приватная ссылка. Добавь бота в канал админом.")
            return
        if conn.execute("SELECT 1 FROM scrape_queue WHERE username=?", (username,)).fetchone():
            await safe_send(event.chat_id, f"ℹ️ @{username} уже в очереди.")
            return
        try:
            entity = await client.get_entity(username)
            if not hasattr(entity, "title"):
                await safe_send(event.chat_id, f"❌ @{username} — не канал.")
                return
            title = getattr(entity, "title", None) or username
            conn.execute("INSERT OR IGNORE INTO scrape_queue (username,title) VALUES (?,?)", (username, title))
            conn.commit()
            await safe_send(event.chat_id, f"📡 <b>{title}</b> (@{username}) добавлен в очередь.")
        except errors.UsernameNotOccupiedError:
            await safe_send(event.chat_id, f"❌ @{username} не существует.")
        except errors.FloodWaitError as e:
            await safe_send(event.chat_id, f"⏳ FloodWait {e.seconds}с, попробуй позже.")
        except Exception as e:
            await safe_send(event.chat_id, f"❌ Ошибка: {str(e)[:200]}")
    except Exception as e:
        log.exception("cmd_add: %s", e)

@client.on(events.NewMessage(pattern=r"^/queue$"))
async def cmd_queue(event):
    try:
        if not is_owner(event): return
        rows = conn.execute("SELECT username,title,status,total_saved FROM scrape_queue ORDER BY added_at DESC LIMIT 30").fetchall()
        if not rows:
            await safe_send(event.chat_id, "📭 Пусто."); return
        lines = ["📋 <b>Очередь (последние 30):</b>\n"]
        emoji = {"done": "✅", "pending": "⏳", "active": "🔄", "error": "❌"}
        for u, t, s, sv in rows:
            lines.append(f"{emoji.get(s, '❓')} @{u} — {t or u}" + (f" ({sv})" if sv else ""))
        await safe_send(event.chat_id, "\n".join(lines))
    except Exception as e:
        log.exception("cmd_queue: %s", e)

@client.on(events.NewMessage(pattern=r"^/search (.+)"))
async def cmd_search(event):
    try:
        if not is_owner(event): return
        q = event.pattern_match.group(1)
        rows = conn.execute("SELECT id,substr(content,1,200),source_chat_title,word_count FROM texts WHERE content LIKE ? ORDER BY id DESC LIMIT 15", (f"%{q}%",)).fetchall()
        if not rows:
            await safe_send(event.chat_id, f"🔍 Ничего: <b>{q}</b>"); return
        lines = [f"🔍 <b>{q}</b> ({len(rows)})\n"]
        for pid, prev, src, wc in rows:
            lines.append(f"#{pid} | {src or '?'} | {wc} слов\n<i>{prev[:150]}</i>\n")
        await safe_send(event.chat_id, "\n".join(lines))
    except Exception as e:
        log.exception("cmd_search: %s", e)

@client.on(events.NewMessage(pattern=r"^/export$"))
async def cmd_export(event):
    try:
        if not is_owner(event): return
        rows = conn.execute("SELECT content,source_chat_title,collected_at FROM texts ORDER BY id").fetchall()
        if not rows:
            await safe_send(event.chat_id, "❌ Пусто."); return
        tmp = DATA_DIR / "export.txt"
        with open(tmp, "w", encoding="utf-8") as f:
            for content, src, dt in rows:
                f.write(f"=== {src or '?'} | {dt} ===\n{content}\n\n")
        await safe_send(event.chat_id, "📦 Отправляю...")
        await client.send_file(event.chat_id, str(tmp))
        tmp.unlink()
    except Exception as e:
        log.exception("cmd_export: %s", e)

@client.on(events.NewMessage(pattern=r"^/set_output$"))
async def cmd_set_output(event):
    try:
        if not is_owner(event): return
        chat = await event.get_chat()
        chat_id = event.chat_id
        title = getattr(chat, "title", None) or getattr(chat, "username", None) or "этот чат"
        set_output_chat(chat_id)
        await safe_send(event.chat_id,
            f"✅ <b>{title}</b> назначен каналом вывода.\nВсе тексты будут отправляться сюда.")
    except Exception as e:
        log.exception("cmd_set_output: %s", e)

@client.on(events.NewMessage(pattern=r"^/pause$"))
async def cmd_pause(event):
    try:
        if not is_owner(event): return
        global _scanning; _scanning = False
        await safe_send(event.chat_id, "⏸ Пауза.")
    except Exception as e:
        log.exception("cmd_pause: %s", e)

@client.on(events.NewMessage(pattern=r"^/resume$"))
async def cmd_resume(event):
    try:
        if not is_owner(event): return
        global _scanning; _scanning = True
        await safe_send(event.chat_id, "▶️ Продолжаем.")
    except Exception as e:
        log.exception("cmd_resume: %s", e)

# ─── Сканирование истории канала ─────────────────────────────────────
async def scrape_channel(username, resume_from=0):
    try:
        entity = await client.get_entity(username)
    except errors.UsernameNotOccupiedError:
        return -1
    except Exception as e:
        log.warning("@%s: %s", username, e); return -1
    title = getattr(entity, "title", None) or username
    saved = processed = 0; last_id = resume_from
    log.info("📡 @%s с msg#%d...", username, resume_from)
    conn.execute("INSERT OR REPLACE INTO scrape_queue (username,title,status,last_msg_id) VALUES (?,?,'active',?)",
                 (username, title, resume_from)); conn.commit()
    try:
        async for msg in client.iter_messages(entity, min_id=resume_from, reverse=True, wait_time=2):
            processed += 1; last_id = msg.id
            if msg.text and wc(msg.text) >= MIN_WORDS:
                link = f"https://t.me/{entity.username}/{msg.id}" if getattr(entity, "username", None) else None
                if save_text(msg.text, title, str(entity.id), msg.id, link):
                    await send_to_output(msg.text, title, link)
                saved += 1
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
        if not _scanning:
            await asyncio.sleep(5); continue
        try:
            row = conn.execute("SELECT username,last_msg_id FROM scrape_queue WHERE status='pending' ORDER BY added_at LIMIT 1").fetchone()
            if row:
                await scrape_channel(row[0], row[1])
                await asyncio.sleep(30)
            else:
                await asyncio.sleep(10)
        except Exception as e:
            log.error("Очередь: %s", e); await asyncio.sleep(30)

# ─── Seed-каналы для первого запуска ─────────────────────────────────
async def add_seed_channels():
    log.info("📡 Добавляю начальные каналы...")
    seed_channels = [
        "rian_ru", "tass_agency", "rt_russian", "rbc_news",
        "meduzalive", "lentadnya", "varlamov",
    ]
    added = 0
    for ch in seed_channels:
        try:
            entity = await client.get_entity(ch)
            if hasattr(entity, "title"):
                title = getattr(entity, "title", None) or ch
                conn.execute("INSERT OR IGNORE INTO scrape_queue (username,title) VALUES (?,?)", (ch, title))
                conn.commit()
                added += 1
                log.info("  ➕ @%s — %s", ch, title)
            await asyncio.sleep(2)
        except Exception as e:
            log.debug("  ❌ @%s: %s", ch, e)
    log.info("📡 Добавлено %d начальных каналов", added)

# ─── Главный запуск ──────────────────────────────────────────────────
async def main():
    # Запускаем health‑сервер ДО client.start(), чтобы Render не убил контейнер
    if IS_RENDER:
        asyncio.create_task(health_server())

    await client.start(bot_token=BOT_TOKEN)
    me = await client.get_me()
    log.info("=" * 50)
    log.info("🤖 @%s запущен", me.username or "?")
    log.info("📁 База: %s", DB_PATH)
    if IS_RENDER:
        log.info("🌐 Render: %s | Health check на порту %d", RENDER_URL or "?", PORT)
    else:
        log.info("🖥 Локальный режим")
    log.info("=" * 50)

    asyncio.create_task(queue_worker())

    total_q = conn.execute("SELECT COUNT(*) FROM scrape_queue").fetchone()[0]
    if total_q == 0:
        asyncio.create_task(add_seed_channels())

    await client.run_until_disconnected()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Остановлен.")
    finally:
        conn.close()
