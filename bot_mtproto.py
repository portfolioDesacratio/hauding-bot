#!/usr/bin/env python3
"""Thesaurus Collector — Telegram Collector (MTProto/Telethon)
   Читает публичные каналы без вступления.
   Собирает тексты ≥50 слов и отправляет в назначенный канал."""
import asyncio, logging, os, re, sqlite3, sys
from datetime import datetime
from pathlib import Path
from telethon import TelegramClient, errors, events, functions

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
RENDER_URL = os.getenv("RENDER_URL") or os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")
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
    asyncio.create_task(backup_settings_to_telegram())

def get_search_keywords():
    """Возвращает список ключевых слов для фильтрации (нижний регистр)"""
    row = conn.execute("SELECT value FROM config WHERE key='user_search_keywords'").fetchone()
    if not row: return []
    # Разбиваем по запятой, чистим, нижний регистр
    return [w.strip().lower() for w in row[0].split(",") if w.strip()]

def text_matches_keywords(text):
    """Проверяет, содержит ли текст хотя бы одно ключевое слово"""
    keywords = get_search_keywords()
    if not keywords:
        return True  # если нет ключевых слов — пропускаем всё (для обратной совместимости)
    text_lower = text.lower()
    for kw in keywords:
        if kw in text_lower:
            return True
    return False

# Каналы, которые НЕ надо обрабатывать
EXCLUDED_CHANNELS = {"desacratio", "thesaurus"}

def save_text(content, chat_title="?", chat_id="?", msg_id=None, link=None):
    content = content.strip()
    if not content or wc(content) < MIN_WORDS: return False
    
    # Фильтр по ключевым словам
    if not text_matches_keywords(content):
        log.debug("⏭️ Пропущен текст (нет ключевых слов): %d слов из %s", wc(content), chat_title)
        return False
    
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

# ─── Бэкап настроек в Telegram (чтобы не слетало после редеплоя) ──
BACKUP_MSG_ID_KEY = "backup_msg_id"

async def backup_settings_to_telegram():
    """Сохраняет текущие настройки в сообщение владельцу"""
    try:
        out = get_output_chat()
        kw = conn.execute("SELECT value FROM config WHERE key='user_search_keywords'").fetchone()
        phone = conn.execute("SELECT value FROM config WHERE key='user_phone'").fetchone()
        auto = conn.execute("SELECT value FROM config WHERE key='user_autosearch'").fetchone()
        lines = ["📦 Бэкап настроек Thesaurus"]
        if out: lines.append(f"output_chat={out}")
        if kw: lines.append(f"searchwords={kw[0]}")
        if phone: lines.append(f"phone={phone[0]}")
        if auto: lines.append(f"autosearch={auto[0]}")
        backup_text = "\n".join(lines)
        
        old_msg_id = conn.execute("SELECT value FROM config WHERE key=?", (BACKUP_MSG_ID_KEY,)).fetchone()
        if old_msg_id:
            try:
                await client.edit_message(OWNER_ID, int(old_msg_id[0]), backup_text)
                log.info("💾 Бэкап обновлён (msg_id=%s)", old_msg_id[0])
                return
            except:
                pass
        # Если не получилось отредактировать — шлём новое
        msg = await client.send_message(OWNER_ID, backup_text, parse_mode="html")
        conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", (BACKUP_MSG_ID_KEY, str(msg.id)))
        conn.commit()
        log.info("💾 Бэкап создан (msg_id=%s)", msg.id)
    except Exception as e:
        log.warning("Не удалось создать бэкап: %s", e)

async def restore_settings_from_backup():
    """Восстанавливает настройки из последнего бэкапа в Telegram"""
    if not get_output_chat():
        log.info("📦 Канал вывода не найден, ищу бэкап...")
        old_msg_id = conn.execute("SELECT value FROM config WHERE key=?", (BACKUP_MSG_ID_KEY,)).fetchone()
        if old_msg_id:
            try:
                msg = await client.get_messages(OWNER_ID, ids=int(old_msg_id[0]))
                if msg and msg.text:
                    for line in msg.text.split("\n"):
                        if line.startswith("output_chat="):
                            val = line.split("=", 1)[1].strip()
                            if val and val != "None":
                                conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", ("output_chat", val))
                                log.info("📦 Восстановлен канал вывода: %s", val)
                        elif line.startswith("searchwords="):
                            val = line.split("=", 1)[1].strip()
                            if val:
                                conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", ("user_search_keywords", val))
                                log.info("📦 Восстановлены ключевые слова")
                        elif line.startswith("phone="):
                            val = line.split("=", 1)[1].strip()
                            if val and val != "None":
                                conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", ("user_phone", val))
                                log.info("📦 Восстановлен телефон: %s", val)
                        elif line.startswith("autosearch="):
                            val = line.split("=", 1)[1].strip()
                            if val:
                                conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", ("user_autosearch", val))
                                log.info("📦 Восстановлен автопоиск: %s", val)
                    conn.commit()
                    return True
            except Exception as e:
                log.warning("Не удалось восстановить из бэкапа: %s", e)
        
        # Если нет msg_id в БД, ищем последнее сообщение с бэкапом в чате
        try:
            async for msg in client.iter_messages(OWNER_ID, limit=20, search="📦 Бэкап настроек Thesaurus"):
                if msg and msg.text:
                    for line in msg.text.split("\n"):
                        if line.startswith("output_chat="):
                            val = line.split("=", 1)[1].strip()
                            if val and val != "None":
                                conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", ("output_chat", val))
                        elif line.startswith("searchwords="):
                            val = line.split("=", 1)[1].strip()
                            if val:
                                conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", ("user_search_keywords", val))
                        elif line.startswith("phone="):
                            val = line.split("=", 1)[1].strip()
                            if val and val != "None":
                                conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", ("user_phone", val))
                        elif line.startswith("autosearch="):
                            val = line.split("=", 1)[1].strip()
                            if val:
                                conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", ("user_autosearch", val))
                    conn.commit()
                    # Сохраняем msg_id для будущих обновлений
                    conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", (BACKUP_MSG_ID_KEY, str(msg.id)))
                    conn.commit()
                    log.info("📦 Настройки восстановлены из последнего бэкапа (msg_id=%s)", msg.id)
                    return True
        except Exception as e:
            log.warning("Не удалось найти бэкап: %s", e)
    
    return False
async def health_server():
    async def handler(reader, writer):
        try:
            # Не ждём запрос — сразу отвечаем (Render может не слать HTTP)
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 2\r\n\r\nok")
            await writer.drain()
        except:
            pass
        writer.close()
    server = await asyncio.start_server(handler, "0.0.0.0", PORT)
    log.info("🏥 Health check на 0.0.0.0:%d", PORT)
    async with server: await server.serve_forever()

# ─── Пульс (лог каждые 60с, чтобы видеть что бот жив) ──────────────
async def heartbeat():
    while True:
        await asyncio.sleep(60)
        log.info("💓 Пульс: база %d текстов, очередь %d pending",
                 conn.execute("SELECT COUNT(*) FROM texts").fetchone()[0],
                 conn.execute("SELECT COUNT(*) FROM scrape_queue WHERE status='pending'").fetchone()[0])

# ─── Отправка в выходной канал (с глобальным rate limit) ──────────
_last_send_time = 0.0
_send_lock = asyncio.Lock()

async def send_to_output(text, source_title, source_link=None):
    global _last_send_time
    out = get_output_chat()
    if not out: return

    # Rate limit: не чаще 1 сообщения в 3 секунды
    async with _send_lock:
        now = asyncio.get_event_loop().time()
        since_last = now - _last_send_time
        if since_last < 3.0:
            wait = 3.0 - since_last
            log.debug("⏳ Rate limit send: жду %.1fс", wait)
            await asyncio.sleep(wait)
        _last_send_time = asyncio.get_event_loop().time()

    header = f"📡 <b>{source_title}</b>"
    if source_link: header += f"\n🔗 {source_link}"
    text_clean = text.replace("<", "&lt;").replace(">", "&gt;")
    message = f"{header}\n\n{text_clean}"
    if len(message) > 3950:
        message = message[:3950] + "\n\n✂️ ..."
    try:
        await client.send_message(out, message, parse_mode="html")
        log.info("📤 Отправлено в канал вывода")
    except errors.FloodWaitError as e:
        log.warning("⏳ FloodWait при отправке: %dс", e.seconds)
        await asyncio.sleep(min(e.seconds, 10))
    except Exception as e:
        log.warning("Не отправилось в канал: %s", e)

# ─── Проверка владельца ──────────────────────────────────────────────
def is_owner(event):
    return event.sender_id == OWNER_ID

async def safe_send(chat_id, text, parse_mode="html"):
    try:
        await client.send_message(chat_id, text, parse_mode=parse_mode)
    except Exception as e:
        log.warning("Не отправилось: %s", e)

# ─── Обработчик всех сообщений ─────────────────────────────────────
async def on_msg(event):
    try:
        if event.out: return
        # Пропускаем одиночные цифры если активен сбор кода
        txt_check = event.text.strip() if event.text else ""
        if len(txt_check) == 1 and txt_check.isdigit():
            active = conn.execute("SELECT value FROM config WHERE key='auth_digit_active'").fetchone()
            if active and active[0] == "1":
                return
        global _scanning
        msg = event.message
        chat = await event.get_chat()
        title = getattr(chat, "title", None) or getattr(chat, "username", None) or "?"
        chat_username = getattr(chat, "username", "").lower()
        
        # Пропускаем исключённые каналы (свой канал, канал вывода и т.д.)
        if chat_username in EXCLUDED_CHANNELS:
            log.debug("⏭️ Пропущен исключённый канал: @%s", chat_username)
            return
        # Пропускаем канал вывода
        out_ch = get_output_chat()
        if out_ch and chat.id == out_ch:
            return
        
        log.info("📩 Сообщение от %s: «%s»", title, (msg.text or "")[:120])

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
                    # Приватная ссылка — не заходим, просто логируем
                    log.info("🔑 Приватная ссылка %s из %s (пропускаем)", link, title)
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
                    fb = await msg.download_media(bytes=True)
                    if not fb: fb = await client.download_file(msg.document, bytes=True)
                    for b in re.split(r'\n\s*\n', fb.decode("utf-8", errors="replace")):
                        if wc(b) >= MIN_WORDS:
                            if save_text(b.strip(), title, str(chat.id), msg.id, f"file:{fn}"):
                                await send_to_output(b.strip(), title, f"file:{fn}")
            except Exception as e:
                log.error("Файл: %s", e)
    except Exception as e:
        log.exception("❌ on_msg: %s", e)

# ─── Обработчик добавления в чат ───────────────────────────────────
async def on_chat_action(event):
    try:
        me = await client.get_me()
        if event.user_added and event.user_id and event.user_id == me.id:
            chat = await event.get_chat()
            title = getattr(chat, "title", None) or "?"
            log.info("➕ Добавлен в чат: %s (%d)", title, event.chat_id)
            # Сохраняем ID канала — пригодится для /set_output
            conn.execute("INSERT OR IGNORE INTO config (key,value) VALUES (?,?)",
                         ("added_chat_" + str(event.chat_id), title))
            conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)",
                         ("last_added_chat", str(event.chat_id)))
            conn.commit()
            log.info("➕ Сохранён ID канала %d (%s). Используй /set_output в ЛС.", event.chat_id, title)
    except Exception as e:
        log.exception("on_chat_action: %s", e)

# ─── Команды для владельца ─────────────────────────────────────────
async def cmd_start(event):
    try:
        await safe_send(event.chat_id,
            "👋 <b>Thesaurus Collector</b>\n\n"
            "Собираю тексты <b>≥50 слов</b> из публичных каналов.\n"
            "Нахожу новые каналы через ссылки в сообщениях.\n\n"
            "Команды:\n"
            "/stats — статистика\n"
            "/add &lt;username&gt; — добавить канал\n"
            "/queue — очередь каналов\n"
            "/search &lt;слова&gt; — поиск\n"
            "/export — скачать всё\n"
            "/set_output [@канал] — назначить канал вывода\n"
            "/pause — пауза\n"
            "/resume — продолжить\n"
            "/debug — диагностика\n"
            "/data_dir — путь к данным\n"
            "/reseed — сбросить очередь (только троллинг)\n\n"
            "🔍 Поиск каналов (user-клиент):\n"
            "/auth — авторизовать user-клиент\n"
            "/phone +7... — номер телефона\n"
            "/code 12345 — код из Telegram\n"
            "/2fa пароль — пароль 2FA\n"
            "/search_channels запрос — поиск каналов\n"
            "/searchwords слово1, слово2 — ключевые слова\n"
            "/autosearch on|off — автопоиск каждые 6ч\n"
            "/logout_user — удалить сессию")
    except Exception as e:
        log.exception("cmd_start: %s", e)

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

async def cmd_set_output(event):
    try:
        if not is_owner(event): return
        raw = event.pattern_match.group(1) if event.pattern_match else None
        if raw and raw.strip():
            target = raw.strip()
            invite_code = None

            # Это числовой ID канала? (-100...)
            if re.match(r'^-?\d+$', target):
                chat_id = int(target)
                # Пробуем получить entity для проверки, но это может не сработать для бота
                try:
                    entity = await client.get_entity(chat_id)
                    title = getattr(entity, "title", None) or getattr(entity, "username", None) or f"ID {chat_id}"
                except Exception:
                    # Всё равно сохраняем — бот может писать если он админ
                    title = f"канал ID {chat_id}"
                set_output_chat(chat_id)
                await safe_send(event.chat_id,
                    f"✅ <b>{title}</b> назначен каналом вывода.\nВсе тексты будут отправляться сюда.")
                # Пробуем отправить тестовое сообщение
                try:
                    await client.send_message(chat_id, "✅ Бот подключён. Начинаю сбор текстов.", parse_mode="html")
                except Exception:
                    await safe_send(event.chat_id,
                        "⚠️ Бот не может писать в канал. Убедись, что он админ (с правом send messages).")
                return

            # Это инвайт-ссылка? (t.me/+xxx)
            m = re.search(r't\.me/\+([a-zA-Z0-9_\-]+)', target)
            if m:
                invite_code = m.group(1)
            else:
                # /set_output @username
                target_clean = re.sub(r'^(?:https?://)?(?:t\.me/|@)', '', target).split('/')[0].split('?')[0]
                if target_clean.startswith('+'):
                    invite_code = target_clean[1:]
                else:
                    target_clean = target_clean

            if invite_code:
                # Приватный канал — заходим (нужно для отправки)
                try:
                    updates = await client(functions.messages.ImportChatInviteRequest(hash=invite_code))
                    # Результат: chats = [chat]
                    if hasattr(updates, 'chats') and updates.chats:
                        chat = updates.chats[0]
                        chat_id = chat.id
                        title = getattr(chat, "title", None) or "канал"
                    else:
                        # Не смогли получить канал — пробуем найти через get_entity
                        await asyncio.sleep(2)
                        entity = await client.get_entity(f"+{invite_code}")
                        chat_id = entity.id
                        title = getattr(entity, "title", None) or "канал"
                    set_output_chat(chat_id)
                    await safe_send(event.chat_id,
                        f"✅ Зашёл в <b>{title}</b> и назначил каналом вывода.\n"
                        f"Теперь все тексты будут отправляться туда.")
                except errors.FloodWaitError as e:
                    await safe_send(event.chat_id, f"⏳ FloodWait {e.seconds}с, попробуй позже.")
                except Exception as e:
                    await safe_send(event.chat_id, f"❌ Не могу зайти по ссылке: {str(e)[:200]}")
                return

            # Публичный канал по username
            try:
                entity = await client.get_entity(target_clean)
                chat_id = entity.id
                title = getattr(entity, "title", None) or getattr(entity, "username", None) or target_clean
            except Exception as e:
                await safe_send(event.chat_id, f"❌ Не могу найти канал: {str(e)[:200]}")
                return
            set_output_chat(chat_id)
            await safe_send(event.chat_id,
                f"✅ <b>{title}</b> назначен каналом вывода.\nВсе тексты будут отправляться сюда.")
        else:
            # /set_output без аргументов
            if not event.is_private:
                await safe_send(event.chat_id, "❌ В канале не сработает. Напиши /set_output @username_канала в ЛС.")
                return
            # Сначала проверяем: есть ли канал, куда бота недавно добавили?
            cur = conn.execute("SELECT value FROM config WHERE key='last_added_chat'")
            row = cur.fetchone()
            if row:
                last_chat_id = int(row[0])
                cur2 = conn.execute("SELECT value FROM config WHERE key=?", ("added_chat_" + str(last_chat_id),))
                row2 = cur2.fetchone()
                title = row2[0] if row2 else "канал"
                set_output_chat(last_chat_id)
                await safe_send(event.chat_id,
                    f"✅ <b>{title}</b> назначен каналом вывода (последний добавленный).\nВсе тексты будут отправляться сюда.")
            else:
                # Используем текущий чат
                chat = await event.get_chat()
                chat_id = event.chat_id
                title = getattr(chat, "title", None) or getattr(chat, "username", None) or "этот чат"
                set_output_chat(chat_id)
                await safe_send(event.chat_id,
                    f"✅ <b>{title}</b> назначен каналом вывода.\nВсе тексты будут отправляться сюда.")
    except Exception as e:
        log.exception("cmd_set_output: %s", e)

async def cmd_pause(event):
    try:
        if not is_owner(event): return
        global _scanning; _scanning = False
        await safe_send(event.chat_id, "⏸ Пауза.")
    except Exception as e:
        log.exception("cmd_pause: %s", e)

async def cmd_resume(event):
    try:
        if not is_owner(event): return
        global _scanning; _scanning = True
        await safe_send(event.chat_id, "▶️ Продолжаем.")
    except Exception as e:
        log.exception("cmd_resume: %s", e)

async def cmd_debug(event):
    try:
        if not is_owner(event): return
        total = conn.execute("SELECT COUNT(*) FROM texts").fetchone()[0]
        q_pending = conn.execute("SELECT COUNT(*) FROM scrape_queue WHERE status='pending'").fetchone()[0]
        q_active = conn.execute("SELECT COUNT(*) FROM scrape_queue WHERE status='active'").fetchone()[0]
        q_done = conn.execute("SELECT COUNT(*) FROM scrape_queue WHERE status='done'").fetchone()[0]
        q_error = conn.execute("SELECT COUNT(*) FROM scrape_queue WHERE status='error'").fetchone()[0]
        out = get_output_chat()
        qw_hb = conn.execute("SELECT value FROM config WHERE key='qw_heartbeat'").fetchone()
        authed = False
        try:
            uc = get_user_client()
            authed = await uc.is_user_authorized()
        except:
            pass
        # Первый pending канал
        first = conn.execute("SELECT username,title FROM scrape_queue WHERE status='pending' ORDER BY added_at LIMIT 1").fetchone()
        # Несколько последних error
        errors = conn.execute("SELECT username,error FROM scrape_queue WHERE status='error' ORDER BY added_at DESC LIMIT 5").fetchall()
        lines = [
            f"🔍 <b>Debug</b>\n",
            f"Текстов: {total}",
            f"Очередь: ⏳{q_pending} 🔄{q_active} ✅{q_done} ❌{q_error}",
            f"Канал вывода: {out if out else '❌'}",
            f"Сканирование: {'▶️' if _scanning else '⏸'}",
            f"Пульс воркера: {qw_hb[0] if qw_hb else 'нет'}",
            f"User client: {'✅' if authed else '❌'}",
        ]
        if first:
            lines.append(f"\nПервый в очереди: @{first[0]} ({first[1] or '?'})")
        if errors:
            lines.append("\nПоследние ошибки:")
            for u, e in errors:
                lines.append(f"  ❌ @{u}: {(e or '?')[:100]}")
        if q_pending == 0 and q_active == 0 and q_done == 0:
            lines.append("\n⚠️ Очередь пуста. Если это только что — seed channels ещё добавляются.")
        await safe_send(event.chat_id, "\n".join(lines))
    except Exception as e:
        log.exception("cmd_debug: %s", e)

async def cmd_data_dir(event):
    if not is_owner(event): return
    session_file = Path(str(USER_SESSION) + ".session")
    db_exists = Path(DB_PATH).exists()
    session_exists = session_file.exists()
    await safe_send(event.chat_id,
        f"📁 <b>Хранилище данных</b>\n"
        f"DATA_DIR: {DATA_DIR}\n"
        f"База: {DB_PATH} {'✅' if db_exists else '❌'}\n"
        f"Сессия user: {session_file} {'✅' if session_exists else '❌'}\n"
        f"Render: {'✅' if IS_RENDER else '❌'}")

async def cmd_reseed(event):
    """Очистить очередь и добавить seed каналы заново"""
    if not is_owner(event): return
    conn.execute("DELETE FROM scrape_queue")
    conn.commit()
    msg = await safe_send(event.chat_id, "🔄 Очередь очищена, добавляю seed каналы...")
    await add_seed_channels()
    total = conn.execute("SELECT COUNT(*) FROM scrape_queue").fetchone()[0]
    # Сохраняем ключевые слова
    conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)",
                 ("user_search_keywords", DEFAULT_SEARCH_KEYWORDS))
    conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)",
                 ("user_autosearch", "on"))
    conn.commit()
    await safe_send(event.chat_id,
        f"✅ Добавлено {total} seed-каналов.\n"
        f"🔍 Ключевые слова и автопоиск включены.\n"
        f"Начинаю сбор.")

# ─── Регистрация всех хендлеров ПОСЛЕ старта клиента ──────────────
def register_handlers():
    """Добавляем обработчики событий через add_event_handler (не декораторы)"""
    # Общий обработчик сообщений
    client.add_event_handler(on_msg, events.NewMessage)

    # Сбор цифр кода по одной (должен быть ДО on_msg, чтобы ловить digits)
    client.add_event_handler(on_auth_digit, events.NewMessage)

    # ChatAction
    client.add_event_handler(on_chat_action, events.ChatAction)

    # Команды (порядок важен: более специфичные раньше)
    client.add_event_handler(cmd_start, events.NewMessage(pattern=r"^/start$"))
    client.add_event_handler(cmd_add, events.NewMessage(pattern=r"^/add (.+)"))
    client.add_event_handler(cmd_queue, events.NewMessage(pattern=r"^/queue$"))
    client.add_event_handler(cmd_stats, events.NewMessage(pattern=r"^/stats$"))
    client.add_event_handler(cmd_search, events.NewMessage(pattern=r"^/search (.+)"))
    client.add_event_handler(cmd_export, events.NewMessage(pattern=r"^/export$"))
    client.add_event_handler(cmd_set_output, events.NewMessage(pattern=r"^/set_output(?:\s+(.+))?$"))
    client.add_event_handler(cmd_pause, events.NewMessage(pattern=r"^/pause$"))
    client.add_event_handler(cmd_resume, events.NewMessage(pattern=r"^/resume$"))
    client.add_event_handler(cmd_debug, events.NewMessage(pattern=r"^/debug$"))
    client.add_event_handler(cmd_data_dir, events.NewMessage(pattern=r"^/data_dir$"))
    client.add_event_handler(cmd_reseed, events.NewMessage(pattern=r"^/reseed$"))

    # User client команды
    client.add_event_handler(cmd_auth, events.NewMessage(pattern=r"^/auth$"))
    client.add_event_handler(cmd_phone, events.NewMessage(pattern=r"^/phone\s+(.+)"))
    client.add_event_handler(cmd_code, events.NewMessage(pattern=r"^/code\s+(.+)"))
    client.add_event_handler(cmd_2fa, events.NewMessage(pattern=r"^/2fa\s+(.+)"))
    client.add_event_handler(cmd_search_channels, events.NewMessage(pattern=r"^/search_channels?\s+(.+)"))
    client.add_event_handler(cmd_autosearch, events.NewMessage(pattern=r"^/autosearch\s+(.+)"))
    client.add_event_handler(cmd_searchwords, events.NewMessage(pattern=r"^/searchwords?\s+(.+)"))
    # Удалить user_client + отвязать (если нужно переавторизоваться)
    client.add_event_handler(cmd_logout_user, events.NewMessage(pattern=r"^/logout_user$"))

    eb_count = len(client._event_builders) if hasattr(client, '_event_builders') else 0
    log.info("✅ Зарегистрировано хендлеров: %d", eb_count)

# ─── Сканирование истории канала ─────────────────────────────────────
# Использует user-клиент (твой аккаунт) для чтения, бот-клиент только для управления
async def get_reader():
    """Возвращает клиент для чтения каналов: user_client если доступен, иначе bot_client"""
    uc = get_user_client()
    try:
        if await uc.is_user_authorized():
            if not uc.is_connected():
                await uc.connect()
            return uc, "user"
    except:
        pass
    # fallback на бота (но он не может читать каналы)
    return client, "bot"

async def scrape_channel(username, resume_from=0, retries=0):
    if retries > 5:
        conn.execute("UPDATE scrape_queue SET status='error',error='Too many retries' WHERE username=?", (username,))
        conn.commit()
        log.error("❌ @%s: слишком много retries", username)
        return -1
    reader, reader_type = await get_reader()
    if reader_type == "bot":
        conn.execute("UPDATE scrape_queue SET status='error',error='Bot cannot read channels, need /auth' WHERE username=?", (username,))
        conn.commit()
        log.warning("⚠️ @%s: нет user-клиента, бот не может читать", username)
        return -1
    try:
        entity = await reader.get_entity(username)
    except errors.UsernameNotOccupiedError:
        conn.execute("UPDATE scrape_queue SET status='error',error='Username not found' WHERE username=?", (username,))
        conn.commit()
        log.warning("⚠️ @%s не найден (username not occupied)", username)
        return -1
    except errors.FloodWaitError as e:
        log.warning("FloodWait @%s: %dс (попытка %d/5)", username, e.seconds, retries + 1)
        await asyncio.sleep(min(e.seconds, 30))
        return await scrape_channel(username, resume_from, retries + 1)
    except Exception as e:
        conn.execute("UPDATE scrape_queue SET status='error',error=? WHERE username=?", (str(e)[:500], username))
        conn.commit()
        log.warning("⚠️ @%s ошибка: %s", username, e)
        return -1
    title = getattr(entity, "title", None) or username
    
    # Пропускаем исключённые каналы
    entity_username = getattr(entity, "username", "").lower()
    if entity_username in EXCLUDED_CHANNELS:
        log.info("⏭️ @%s в списке исключённых, пропускаю", username)
        conn.execute("UPDATE scrape_queue SET status='done' WHERE username=?", (username,))
        conn.commit()
        return 0
    out_ch = get_output_chat()
    if out_ch and hasattr(entity, "id") and entity.id == out_ch:
        log.info("⏭️ @%s это канал вывода, пропускаю", username)
        conn.execute("UPDATE scrape_queue SET status='done' WHERE username=?", (username,))
        conn.commit()
        return 0
    
    saved = processed = 0; last_id = resume_from
    log.info("📡 [%s] @%s с msg#%d...", reader_type, username, resume_from)
    exists = conn.execute("SELECT 1 FROM scrape_queue WHERE username=?", (username,)).fetchone()
    if exists:
        conn.execute("UPDATE scrape_queue SET status='active',last_msg_id=? WHERE username=?", (resume_from, username))
    else:
        conn.execute("INSERT INTO scrape_queue (username,title,status,last_msg_id) VALUES (?,?,'active',?)", (username, title, resume_from))
    conn.commit()
    try:
        async for msg in reader.iter_messages(entity, min_id=resume_from, reverse=True, wait_time=3):
            processed += 1; last_id = msg.id
            if msg.text and wc(msg.text) >= MIN_WORDS:
                link = f"https://t.me/{entity.username}/{msg.id}" if getattr(entity, "username", None) else None
                if save_text(msg.text, title, str(entity.id), msg.id, link):
                    await send_to_output(msg.text, title, link)
                saved += 1
            # Задержка между сообщениями — предотвращает FloodWait
            await asyncio.sleep(1.5)
            if processed % 200 == 0:
                conn.execute("UPDATE scrape_queue SET last_msg_id=?,total_saved=total_saved+? WHERE username=?",
                             (last_id, saved, username)); conn.commit()
                log.info("  ⏳ [%s] @%s: %d обработано, %d сохранено", reader_type, username, processed, saved)
                await asyncio.sleep(3)
    except errors.FloodWaitError as e:
        conn.execute("UPDATE scrape_queue SET status='pending',last_msg_id=?,total_saved=total_saved+? WHERE username=?",
                     (last_id, saved, username)); conn.commit()
        log.warning("FloodWait [%s] @%s: %dс (внутри)", reader_type, username, e.seconds)
        if retries > 3:
            log.error("❌ @%s: FloodWait слишком много раз, пропускаю", username)
            return saved
        await asyncio.sleep(min(e.seconds, 120))
        return await scrape_channel(username, last_id, retries + 1)
    except Exception as e:
        conn.execute("UPDATE scrape_queue SET status='error',error=?,last_msg_id=?,total_saved=total_saved+? WHERE username=?",
                     (str(e)[:500], last_id, saved, username)); conn.commit()
        log.error("❌ [%s] @%s: %s", reader_type, username, e); return saved
    conn.execute("UPDATE scrape_queue SET status='done',last_msg_id=?,total_saved=total_saved+? WHERE username=?",
                 (last_id, saved, username)); conn.commit()
    log.info("✅ [%s] @%s: %d сообщений, %d сохранено", reader_type, username, processed, saved)
    return saved

async def queue_worker():
    """Постоянно сканирует ВСЕ каналы одновременно"""
    active_tasks = {}  # username -> task
    log.info("🚀 Queue worker запущен — обрабатываю все каналы сразу")

    async def run_scrape(username, last_msg_id):
        """Обёртка для scrape_channel с таймаутом"""
        try:
            result = await asyncio.wait_for(scrape_channel(username, last_msg_id), timeout=600)
            return result
        except asyncio.TimeoutError:
            log.error("⏰ @%s: таймаут 600с", username)
            conn.execute("UPDATE scrape_queue SET status='error',error='Timeout' WHERE username=?", (username,))
            conn.commit()
            return -1
        except Exception as e:
            log.exception("run_scrape @%s: %s", username, e)
            return -1

    while True:
        # Пульс воркера каждые 3с
        conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)",
                     ("qw_heartbeat", datetime.now().isoformat()))
        conn.commit()

        if not _scanning:
            await asyncio.sleep(3); continue

        # Убираем завершённые задачи
        finished = [u for u, t in list(active_tasks.items()) if t.done()]
        for u in finished:
            try:
                r = active_tasks[u].result()
                log.info("✅ @%s завершён (результат=%s)", u, r)
            except Exception:
                pass
            del active_tasks[u]

        try:
            # Запускаем ВСЕ pending-каналы, которых ещё нет в active_tasks
            rows = conn.execute(
                "SELECT username,last_msg_id FROM scrape_queue WHERE status='pending' ORDER BY added_at"
            ).fetchall()
            for row in rows:
                if row[0] not in active_tasks:
                    task = asyncio.create_task(run_scrape(row[0], row[1]))
                    active_tasks[row[0]] = task
                    log.info("🚀 Запущен @%s (всего активно: %d)", row[0], len(active_tasks))

            # Если ничего не запущено — перепроверяем done-каналы (до 5 одновременно)
            if not active_tasks:
                done_rows = conn.execute(
                    "SELECT username,last_msg_id FROM scrape_queue WHERE status='done' ORDER BY RANDOM() LIMIT 5"
                ).fetchall()
                for row in done_rows:
                    if row[0] not in active_tasks:
                        task = asyncio.create_task(run_scrape(row[0], row[1]))
                        active_tasks[row[0]] = task
                        log.info("🔄 Перепроверка @%s", row[0])
                if not active_tasks:
                    log.info("⏳ Нет каналов, жду...")
                    await asyncio.sleep(15)
                    continue

            await asyncio.sleep(3)
        except Exception as e:
            log.exception("Queue worker: %s", e)
            await asyncio.sleep(10)

# ─── Seed-каналы для первого запуска ─────────────────────────────────
async def add_seed_channels():
    log.info("📡 Добавляю начальные каналы (троллинг/хаудинг)...")
    seed_channels = [
        # --- Троллинг / провокации / шаблоны ---
        "shablidlyatrolinga",
        "yabogtroll",
        "trolling_shablony",
        "shablonytrollinga",
        "troll_hard",
        "provokacii",
        "provocator",
        "trolling_channel",
        "trollfactory",
        "trolling_ru",
        "trolling_rus",
        "troll_rus",
        "provokator",
        "provokation",
        "trolls_army",
        "trolls_of_russia",
        "trolling_army",
        "provokator_ru",
        "trolling_world",
        "trollworld",
        "trolls_team",
        "hard_trolling",
        "trolling_hub",
        "troll_artist",
        "trolling_zone",
        "trollbox",
        "trolley",
        "srrolling",
        "srolling",
        # --- Хаудинг ---
        "hauding",
        "hauders",
        "hauder",
        "hauding_channel",
        "hauding_info",
        "hauding_rus",
        "haud",
        "haud_team",
        "haud_army",
        "ssauding",
        "ssaud",
        "sraud",
        "ssauder",
        "sraudinger",
        # --- Вбросы / фейки ---
        "vbros",
        "vbros_ru",
        "feiki_net",
        "fake_news",
        "fakenews",
        "fakty_i_fake",
        "vbroski",
        "podbros",
        # --- Компромат ---
        "kompromat_group",
        "kompromat_ru",
        "kompromat_news",
        "kompromat_xyz",
        "compromat",
        "kompromatt",
        "kompromat_top",
        "kompromat24",
        # --- Оккультизм ---
        "okkultizm",
        "okkult",
        "okkult_ru",
        "okkultizm_channel",
        "okkultnii",
        "magiia",
        "srakkultizm",
        "srakkult",
        "srakkult_ru",
        # --- Шаббат ---
        "shabbat",
        "shabbat_ru",
        "shabbes",
        "srabbat",
        "srabat",
        "shabash",
        # --- Кавалерия ---
        "legkaya_kavaleriya",
        "kavaleriya",
        "kavaleri",
        "ssavaleriya",
        "sravvaleriya",
        "light_cavalry",
        # --- Ваффен / Срафен ---
        "waffen",
        "waffen_ru",
        "ssraffen",
        "waffen_ss",
        "sraffen",
        # --- Большие тексты ---
        "big_texts",
        "dlinnie_texti",
        "mat_text",
        "texts_with_mat",
        "boshie_texti",
        "ogromnie_texti",
        # --- Прочие ---
        "karahalk",
        "black_pr",
        "blackpr",
        "pr_black",
        "psihoz",
        "psychosis",
        "abuse_channel",
        "abuse_text",
        "trolling_moscow",
        "trolling_spb",
    ]
    added = 0
    for ch in seed_channels:
        try:
            conn.execute("INSERT OR IGNORE INTO scrape_queue (username,title) VALUES (?,?)", (ch, ch))
            conn.commit()
            added += 1
            log.info("  ➕ @%s", ch)
        except Exception as e:
            log.warning("  ⚠️ @%s: %s", ch, e)
    log.info("📡 Добавлено %d начальных каналов", added)

# ─── User‑клиент (твой аккаунт) для Search API ───────────────────────
USER_SESSION = str(DATA_DIR / "user_session")
user_client = None

def get_user_client():
    global user_client
    if user_client is None:
        user_client = TelegramClient(USER_SESSION, API_ID, API_HASH)
    return user_client

async def init_user_client():
    """Проверяем сохранённую сессию при старте"""
    uc = get_user_client()
    session_file = Path(str(USER_SESSION) + ".session")
    exists = session_file.exists()
    log.info("👤 User client: сессия %s (файл %s)", "✅ существует" if exists else "❌ НЕТ", session_file)
    log.info("📁 DATA_DIR: %s", DATA_DIR)
    if not exists:
        log.warning("👤 Файл сессии не найден — нужна /auth")
        try:
            await client.send_message(OWNER_ID,
                "⚠️ <b>User client сессия не найдена</b>\n"
                "Отправь:\n"
                "/auth\n"
                "/phone +79122502717\n"
                "Далее код по цифрам: 1 2 3 4 5",
                parse_mode="html")
        except:
            pass
        return False
    try:
        await uc.connect()
        if await uc.is_user_authorized():
            log.info("👤 User client авторизован (сессия сохранена)")
            asyncio.create_task(user_searcher())
            # Уведомляем что всё ок
            try:
                await client.send_message(OWNER_ID,
                    "✅ <b>User client авторизован</b> (сессия восстановлена)",
                    parse_mode="html")
            except:
                pass
            return True
        else:
            log.info("👤 User client: сессия недействительна, нужна /auth")
            try:
                await client.send_message(OWNER_ID,
                    "⚠️ <b>User client сессия недействительна</b>\n"
                    "Нужна переавторизация: /auth",
                    parse_mode="html")
            except:
                pass
            return False
    except Exception as e:
        log.warning("👤 User client init error: %s", e)
        return False

DEFAULT_SEARCH_KEYWORDS = (
    "троллинг, хауд, тролли, тролль, хаудинг, сраудинг, ссаудинг, ссауд, "
    "срауд, сроллинг, сролль, сролли, оккультизм, сраккультизм, сракультизм, "
    "срафен, ваффен, вафен, сраффен, сравалерия, ссавалерия, кавалерия, "
    "легкая кавалерия, шаббат, сраббат, срабат, шабат, шаблоны, шаблон, "
    "сраблон, сраблоны, сраготовки, заготовки"
)

async def cmd_auth(event):
    if not is_owner(event): return
    uc = get_user_client()
    try:
        if await uc.is_user_authorized():
            await safe_send(event.chat_id, "✅ User client уже авторизован.")
            return
    except:
        await uc.disconnect()
        await uc.connect()
    phone = conn.execute("SELECT value FROM config WHERE key='user_phone'").fetchone()
    await safe_send(event.chat_id,
        "📱 Отправь номер телефона:\n"
        "/phone +79876543210\n\n"
        f"{'👉 У тебя уже сохранён: ' + phone[0] if phone else ''}")

async def cmd_phone(event):
    if not is_owner(event): return
    phone = event.pattern_match.group(1).strip()
    conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", ("user_phone", phone))
    conn.commit()
    uc = get_user_client()
    await uc.connect()
    try:
        await uc.send_code_request(phone)
        # Активируем режим сбора цифр по одной
        conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", ("auth_digit_active", "1"))
        conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", ("auth_digits", ""))
        conn.commit()
        await safe_send(event.chat_id,
            f"✅ Код отправлен на {phone}\n\n"
            "Ввести можно двумя способами:\n"
            "1️⃣ /code 12345 — одним сообщением\n"
            "2️⃣ Отправить 5 сообщений по одной цифре:\n"
            "1\n2\n3\n4\n5\n\n"
            "Если ошиблись — /phone +7... ещё раз")
    except errors.FloodWaitError as e:
        await safe_send(event.chat_id, f"⏳ FloodWait {e.seconds}с, попробуй позже.")
    except Exception as e:
        await safe_send(event.chat_id, f"❌ Ошибка: {str(e)[:200]}")

async def cmd_code(event):
    if not is_owner(event): return
    code = event.pattern_match.group(1).strip()
    phone_row = conn.execute("SELECT value FROM config WHERE key='user_phone'").fetchone()
    if not phone_row:
        await safe_send(event.chat_id, "❌ Сначала /phone")
        return
    phone = phone_row[0]
    uc = get_user_client()
    if not uc.is_connected():
        await uc.connect()
    try:
        await uc.sign_in(phone, code)
        await safe_send(event.chat_id, "✅ User client авторизован! Поиск каналов работает.\n"
            "Поставь ключевые слова: /searchwords слово1, слово2\n"
            "Включи автопоиск: /autosearch on")
        asyncio.create_task(user_searcher())
    except errors.SessionPasswordNeededError:
        await safe_send(event.chat_id, "🔑 Нужен пароль 2FA: /2fa твой_пароль")
    except errors.FloodWaitError as e:
        await safe_send(event.chat_id, f"⏳ FloodWait {e.seconds}с, попробуй позже.")
    except errors.PhoneCodeInvalidError:
        await safe_send(event.chat_id, "❌ Неверный код. Попробуй ещё: /code 12345")
    except Exception as e:
        await safe_send(event.chat_id, f"❌ Ошибка: {str(e)[:200]}")

async def cmd_2fa(event):
    if not is_owner(event): return
    password = event.pattern_match.group(1).strip()
    uc = get_user_client()
    if not uc.is_connected():
        await uc.connect()
    try:
        await uc.sign_in(password=password)
        await safe_send(event.chat_id, "✅ User client авторизован (2FA)! Поиск работает.\n"
            "Ключевые слова: /searchwords слово1, слово2\n"
            "Автопоиск: /autosearch on")
        asyncio.create_task(user_searcher())
    except errors.FloodWaitError as e:
        await safe_send(event.chat_id, f"⏳ FloodWait {e.seconds}с, попробуй позже.")
    except Exception as e:
        await safe_send(event.chat_id, f"❌ Ошибка: {str(e)[:200]}")

async def on_auth_digit(event):
    """Собирает код подтверждения по одной цифре из отдельных сообщений"""
    if not is_owner(event): return
    if not event.is_private: return
    text = event.text.strip()
    if not text.isdigit() or len(text) != 1:
        return
    # Проверяем, активен ли режим сбора цифр
    active = conn.execute("SELECT value FROM config WHERE key='auth_digit_active'").fetchone()
    if not active or active[0] != "1":
        return
    # Собираем цифру
    row = conn.execute("SELECT value FROM config WHERE key='auth_digits'").fetchone()
    current = (row[0] if row else "") + text
    conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", ("auth_digits", current))
    conn.commit()
    log.info("🔢 Auth digit: %s (собрано %d/5)", text, len(current))
    if len(current) >= 5:
        # Отключаем режим сбора
        conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", ("auth_digit_active", "0"))
        conn.execute("DELETE FROM config WHERE key='auth_digits'")
        conn.commit()
        code = current
        # Вход с собранным кодом
        phone_row = conn.execute("SELECT value FROM config WHERE key='user_phone'").fetchone()
        if not phone_row:
            await safe_send(event.chat_id, "❌ Нет номера. Сначала /phone")
            return
        phone = phone_row[0]
        uc = get_user_client()
        if not uc.is_connected():
            await uc.connect()
        try:
            await uc.sign_in(phone, code)
            await safe_send(event.chat_id, "✅ User client авторизован! Поиск каналов работает.\n"
                "Ключевые слова: /searchwords слово1, слово2\n"
                "Автопоиск: /autosearch on")
            asyncio.create_task(user_searcher())
        except errors.SessionPasswordNeededError:
            await safe_send(event.chat_id, "🔑 Нужен пароль 2FA: /2fa твой_пароль")
        except errors.PhoneCodeInvalidError:
            await safe_send(event.chat_id, f"❌ Неверный код {code}. Попробуй ещё: /phone +7...")
        except errors.FloodWaitError as e:
            await safe_send(event.chat_id, f"⏳ FloodWait {e.seconds}с, попробуй позже.")
        except Exception as e:
            await safe_send(event.chat_id, f"❌ Ошибка: {str(e)[:200]}")
    else:
        await safe_send(event.chat_id, f"✅ {len(current)}/5. Ещё {5 - len(current)}.")

async def cmd_search_channels(event):
    if not is_owner(event): return
    q = event.pattern_match.group(1).strip()
    uc = get_user_client()
    try:
        if not await uc.is_user_authorized():
            await safe_send(event.chat_id, "❌ User client не авторизован. Сначала /auth")
            return
    except:
        await safe_send(event.chat_id, "❌ User client не готов. Сначала /auth")
        return
    if not uc.is_connected():
        await uc.connect()
    try:
        result = await uc(functions.contacts.SearchRequest(q=q, limit=30))
        channels = [c for c in result.chats if hasattr(c, 'title') and getattr(c, 'username', None)]
        if not channels:
            await safe_send(event.chat_id, f"🔍 По запросу «{q}» каналы не найдены.")
            return
        added = 0
        msg = [f"🔍 <b>{q}</b> — найдено {len(channels)} каналов:\n"]
        for ch in channels:
            username = ch.username.lower()
            exists = conn.execute("SELECT 1 FROM scrape_queue WHERE username=?", (username,)).fetchone()
            if not exists:
                title = ch.title or username
                conn.execute("INSERT OR IGNORE INTO scrape_queue (username,title) VALUES (?,?)", (username, title))
                conn.commit()
                added += 1
            msg.append(f"{'➕' if not exists else '✅'} @{username} — {ch.title or ''}")
        conn.commit()
        msg.append(f"\nНовых добавлено: {added}")
        await safe_send(event.chat_id, "\n".join(msg))
    except errors.FloodWaitError as e:
        await safe_send(event.chat_id, f"⏳ FloodWait {e.seconds}с, попробуй позже.")
    except Exception as e:
        await safe_send(event.chat_id, f"❌ Ошибка поиска: {str(e)[:200]}")

async def cmd_autosearch(event):
    if not is_owner(event): return
    raw = event.pattern_match.group(1).strip().lower()
    if raw in ("on", "вкл", "да", "yes", "1"):
        conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", ("user_autosearch", "on"))
        conn.commit()
        await safe_send(event.chat_id, "✅ Автопоиск включён. Каждые 6 часов бот ищет новые каналы.")
        asyncio.create_task(user_searcher())
        asyncio.create_task(backup_settings_to_telegram())
    elif raw in ("off", "выкл", "нет", "no", "0"):
        conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", ("user_autosearch", "off"))
        conn.commit()
        await safe_send(event.chat_id, "⏸ Автопоиск выключен.")
        asyncio.create_task(backup_settings_to_telegram())
    elif raw == "status":
        st = conn.execute("SELECT value FROM config WHERE key='user_autosearch'").fetchone()
        kws = conn.execute("SELECT value FROM config WHERE key='user_search_keywords'").fetchone()
        authed = False
        try:
            uc = get_user_client()
            authed = await uc.is_user_authorized()
        except:
            pass
        await safe_send(event.chat_id,
            f"👤 User client: {'✅ авторизован' if authed else '❌ не авторизован'}\n"
            f"🔍 Автопоиск: {'✅ вкл' if st and st[0] == 'on' else '❌ выкл'}\n"
            f"📝 Слова: {kws[0] if kws else 'не заданы'}")
    else:
        await safe_send(event.chat_id, "❌ /autosearch on|off|status")

async def cmd_searchwords(event):
    if not is_owner(event): return
    words = event.pattern_match.group(1).strip()
    conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", ("user_search_keywords", words))
    conn.commit()
    await safe_send(event.chat_id, f"✅ Ключевые слова сохранены:\n{words}")
    asyncio.create_task(backup_settings_to_telegram())
    # Если автопоиск включён — запускаем поиск немедленно
    as_on = conn.execute("SELECT value FROM config WHERE key='user_autosearch'").fetchone()
    if as_on and as_on[0] == "on":
        asyncio.create_task(user_searcher())
        await safe_send(event.chat_id, "🔍 Запускаю поиск каналов по этим словам...")

async def user_searcher():
    """Фоновая задача: ищет каналы по ключевым словам каждые 6 часов"""
    uc = get_user_client()
    try:
        if not await uc.is_user_authorized():
            log.info("👤 User client не авторизован — автопоиск не запущен")
            return
    except:
        return
    log.info("👤 User searcher запущен")
    while True:
        try:
            autosearch = conn.execute("SELECT value FROM config WHERE key='user_autosearch'").fetchone()
            if not autosearch or autosearch[0] != "on":
                await asyncio.sleep(3600)
                continue
            kws_row = conn.execute("SELECT value FROM config WHERE key='user_search_keywords'").fetchone()
            if not kws_row:
                # Устанавливаем ключевые слова по умолчанию
                conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)",
                             ("user_search_keywords", DEFAULT_SEARCH_KEYWORDS))
                conn.commit()
                kws_row = (DEFAULT_SEARCH_KEYWORDS,)
            keywords = [k.strip() for k in kws_row[0].split(",") if k.strip()]
            if not keywords:
                await asyncio.sleep(3600)
                continue
            log.info("🔍 Ищу каналы по %d ключевым словам...", len(keywords))
            if not uc.is_connected():
                await uc.connect()
            total_found = 0
            for kw in keywords:
                try:
                    result = await uc(functions.contacts.SearchRequest(q=kw, limit=20))
                    for ch in result.chats:
                        if hasattr(ch, 'title') and getattr(ch, 'username', None):
                            username = ch.username.lower()
                            if not conn.execute("SELECT 1 FROM scrape_queue WHERE username=?", (username,)).fetchone():
                                title = ch.title or username
                                conn.execute("INSERT OR IGNORE INTO scrape_queue (username,title) VALUES (?,?)",
                                             (username, title))
                                conn.commit()
                                total_found += 1
                                log.info("  🔍 +@%s (%s) по запросу «%s»", username, title, kw)
                    await asyncio.sleep(15)
                except errors.FloodWaitError as e:
                    log.warning("FloodWait user search: %dс", e.seconds)
                    await asyncio.sleep(min(e.seconds, 300))
                except Exception as e:
                    log.warning("Ошибка поиска «%s»: %s", kw, e)
            log.info("🔍 Цикл поиска завершён. Добавлено %d каналов. Следующий через 6ч.", total_found)
            await asyncio.sleep(6 * 3600)
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.exception("user_searcher: %s", e)
            await asyncio.sleep(300)

async def cmd_logout_user(event):
    if not is_owner(event): return
    uc = get_user_client()
    try:
        await uc.log_out()
    except:
        pass
    try:
        await uc.disconnect()
    except:
        pass
    # Удаляем файл сессии
    for f in (DATA_DIR / "user_session.session", DATA_DIR / "user_session.session-journal"):
        if f.exists():
            f.unlink()
    global user_client
    user_client = None
    conn.execute("DELETE FROM config WHERE key='user_phone'")
    conn.commit()
    await safe_send(event.chat_id, "✅ User client отвязан. Сессия удалена.")

# ─── Главный запуск ──────────────────────────────────────────────────
async def main():
    # Здоровье ДО старта клиента
    if IS_RENDER:
        asyncio.create_task(health_server())

    # Стартуем с retry при FloodWait (бывает после частых редеплоев)
    for attempt in range(10):
        try:
            await client.start(bot_token=BOT_TOKEN)
            break
        except errors.FloodWaitError as e:
            wait = min(e.seconds, 300)
            log.warning("⏳ FloodWait при входе: %dс (попытка %d/10, жду %dс)", e.seconds, attempt + 1, wait)
            await asyncio.sleep(wait)
    else:
        log.error("❌ Не удалось войти после 10 попыток")
        return
    # Регистрируем хендлеры ПОСЛЕ старта
    register_handlers()

    me = await client.get_me()
    log.info("=" * 50)
    log.info("🤖 @%s запущен", me.username or "?")
    log.info("📁 База: %s", DB_PATH)
    if IS_RENDER:
        log.info("🌐 Render: %s | Health check на порту %d", RENDER_URL or "?", PORT)
    else:
        log.info("🖥 Локальный режим")
    log.info("=" * 50)

    # Восстанавливаем настройки из бэкапа Telegram, если локально ничего нет
    texts_count = conn.execute("SELECT COUNT(*) FROM texts").fetchone()[0]
    if texts_count == 0 and not get_output_chat():
        log.info("📦 База пуста, пробую восстановить из бэкапа Telegram...")
        restored = await restore_settings_from_backup()
        if restored:
            log.info("📦 Настройки восстановлены из бэкапа!")
        else:
            log.info("📦 Бэкап не найден, всё придётся настроить заново")

    # Восстанавливаем настройки и шлём уведомление владельцу
    restored_settings = []
    out_ch = get_output_chat()
    if out_ch: restored_settings.append(f"📤 Канал вывода: {out_ch}")
    kw = conn.execute("SELECT value FROM config WHERE key='user_search_keywords'").fetchone()
    if kw: restored_settings.append(f"🔍 Ключевые слова: {kw[0][:60]}...")
    auto = conn.execute("SELECT value FROM config WHERE key='user_autosearch'").fetchone()
    if auto: restored_settings.append(f"🔄 Автопоиск: {auto[0]}")
    texts_count = conn.execute("SELECT COUNT(*) FROM texts").fetchone()[0]
    restored_settings.append(f"📚 Текстов в базе: {texts_count}")
    startup_msg = "🔄 <b>Бот перезапущен</b>\n" + "\n".join(f"• {s}" for s in restored_settings)
    try:
        await client.send_message(OWNER_ID, startup_msg, parse_mode="html")
    except:
        pass

    # Фоновые задачи
    asyncio.create_task(heartbeat())
    asyncio.create_task(queue_worker())
    asyncio.create_task(init_user_client())
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
