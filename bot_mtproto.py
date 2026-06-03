#!/usr/bin/env python3
"""Thesaurus Collector — Telegram Collector (MTProto/Telethon)
   Читает публичные каналы без вступления.
   Собирает тексты ≥50 слов и отправляет в назначенный канал."""
import asyncio, logging, os, re, sqlite3, sys, hashlib, time
from datetime import datetime
from pathlib import Path
from telethon import TelegramClient, errors, events, functions

MAX_TEXT_DUPLICATES = 1  # макс повторов одного текста — 1 раз и навсегда

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

# Все credentials только из переменных окружения — ничего не захардкожено!
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]

OWNER_IDS = {8587090554, 895508019}  # я + друг
OWNER_ID = 8587090554  # @desacratio (для уведомлений)

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
conn.execute("""CREATE UNIQUE INDEX IF NOT EXISTS idx_texts_msg 
    ON texts(source_chat, source_message_id)""")
conn.execute("""CREATE TABLE IF NOT EXISTS scrape_queue (
    username TEXT PRIMARY KEY, title TEXT,
    added_at TEXT DEFAULT (datetime('now')),
    status TEXT DEFAULT 'pending', last_msg_id INTEGER DEFAULT 0,
    total_saved INTEGER DEFAULT 0, error TEXT)""")
conn.execute("""CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY, value TEXT NOT NULL)""")
# Таблица для подсчёта дубликатов текстов (ключ — sha256 content)
conn.execute("""CREATE TABLE IF NOT EXISTS text_hashes (
    hash TEXT PRIMARY KEY, count INTEGER DEFAULT 1, banned INTEGER DEFAULT 0)""")
# Таблица для отслеживания прогресса отправки больших файлов (переживает рестарт)
conn.execute("""CREATE TABLE IF NOT EXISTS file_progress (
    chat_id INTEGER NOT NULL,
    msg_id INTEGER NOT NULL,
    filename TEXT,
    total_words INTEGER DEFAULT 0,
    words_sent INTEGER DEFAULT 0,
    total_chunks INTEGER DEFAULT 0,
    chunks_sent INTEGER DEFAULT 0,
    file_size INTEGER DEFAULT 0,
    active INTEGER DEFAULT 1,
    checkpoint_msg_id INTEGER,
    updated_at REAL,
    PRIMARY KEY (chat_id, msg_id))""")
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
            # Проверяем, что это не часть заблокированной фразы
            blocked = False
            for bp in _keyword_blocked:
                if bp in text_lower:
                    blocked = True
                    break
            if not blocked:
                return True
    return False

# Троллинг‑индикаторы (regex с границами слов, чтобы не цеплять "неба" и т.п.)
# Текст должен содержать минимум 2 таких паттерна, чтобы пройти
_TROLLING_COUNT = 2
_TROLLING_PATTERNS = [
    re.compile(r'\bхуй'), re.compile(r'\bхуя'), re.compile(r'\bхуе'), re.compile(r'\bхую'),
    re.compile(r'\bхуем'), re.compile(r'\bхуище'), re.compile(r'\bхуё'), re.compile(r'\bхуйня'),
    re.compile(r'\bпизд'), re.compile(r'\bпиздец'),
    re.compile(r'\bжоп'), re.compile(r'\bочко'),
    re.compile(r'\bчлен'), re.compile(r'\bчленом'), re.compile(r'\bчленовый'),
    re.compile(r'\bшлюх'),
    re.compile(r'\bпедик'), re.compile(r'\bпедераст'), re.compile(r'\bпидор'),
    re.compile(r'\bсосал'), re.compile(r'\bсосешь'), re.compile(r'\bсосёт'), re.compile(r'\bотсос'),
    re.compile(r'\bдроч'), re.compile(r'\bнадрач'),
    re.compile(r'\bзалуп'), re.compile(r'\bзалупа'),
    re.compile(r'\bтерпил'), re.compile(r'\bтерпилойд'),
    re.compile(r'\bочкошник'),
    re.compile(r'\bгей'), re.compile(r'\bгомосек'),
    re.compile(r'\bкал\b'), re.compile(r'\bговно'), re.compile(r'\bдерьмо'),
    re.compile(r'\bминет'),
    re.compile(r'\bчленосос'),
    re.compile(r'\bеба'),        # ебать, ебал, ебали, ебала — НО не "неба", "хлеба" (там нет \b перед е)
    re.compile(r'\bебу'),        # ебу, ебут
    re.compile(r'\bебё'),        # ебёшь, ебёт, ебём, ебёте
    re.compile(r'\bёб'),         # ёбаный, заёб
    re.compile(r'\bвыеб'),       # выебал, выебать
    re.compile(r'\bзаеб'),       # заебал, заебать
    re.compile(r'\bнаеб'),       # наебал, наебать
    re.compile(r'\bподъеб'),     # подъебал
]

def has_trolling_content(text):
    """Проверяет, содержит ли текст достаточно троллинг-лексики (минимум _TROLLING_COUNT совпадений)"""
    text_lower = text.lower()
    count = 0
    for pat in _TROLLING_PATTERNS:
        if pat.search(text_lower):
            count += 1
            if count >= _TROLLING_COUNT:
                return True
    return False

# Фразы, которые содержат ключевые слова, но НЕ должны триггерить совпадение
_keyword_blocked = [
    "мумий тролль",      # группа, магазин, музыка
    "mumiytroll",        # сайт магазина
    "mumiy troll",       # латиницей
    "тролль music bar",  # бар
    "хауди микоски",     # howdie mickoski — эзотерика/саморазвитие
]

# Каналы, которые НЕ надо обрабатывать
EXCLUDED_CHANNELS = {"desacratio", "thesaurus", "exstrorezov", "vldvstk3000", "mtbarmoscow", "howdie_mickoski", "a_toolsx"}

# ─── Многослойная дедупликация ──────────────────────────────────────
# Слой 1: in-memory счётчик на текущую сессию (не依赖 от БД)
_sess_dup = {}  # content_hash -> сколько раз ОТПРАВИЛИ за эту сессию
_SENT_HASHES = set()  # SHA256 хэши текстов, уже отправленных в канал вывода

def save_text(content, chat_title="?", chat_id="?", msg_id=None, link=None):
    content = content.strip()
    if not content or wc(content) < MIN_WORDS:
        log.debug("⏭️ save_text: пусто/<50слов от %s", chat_title)
        return False
    
    # Фильтр по ключевым словам
    if not text_matches_keywords(content):
        log.debug("⏭️ save_text: нет ключевых слов от %s", chat_title)
        return False
    
    # Фильтр троллинг-лексики
    if not has_trolling_content(content):
        log.debug("⏭️ save_text: нет троллинг-лексики от %s", chat_title)
        return False
    
    # ────────────────────────────────────────────────────────────────
    # СЛОЙ 1: проверка что это сообщение (chat_id+msg_id) уже сохранено
    # ────────────────────────────────────────────────────────────────
    if chat_id != "?" and msg_id is not None:
        existing = conn.execute(
            "SELECT 1 FROM texts WHERE source_chat=? AND source_message_id=?",
            (chat_id, msg_id)
        ).fetchone()
        if existing:
            log.debug("⏭️ save_text: msg#%s из %s уже в БД (слой1)", msg_id, chat_title)
            return False
    
    # ────────────────────────────────────────────────────────────────
    # СЛОЙ 2: in-memory счётчик сессии — текст УЖЕ отправляли в этой сессии?
    # ────────────────────────────────────────────────────────────────
    ch = hashlib.sha256(content.encode("utf-8")).hexdigest()
    sess_sent = _sess_dup.get(ch, 0)
    if sess_sent >= MAX_TEXT_DUPLICATES:
        log.warning("🚫 СЛОЙ2 (сессия) — уже отправляли %d раз: %d слов из %s",
                    sess_sent, wc(content), chat_title)
        return False
    # пока не увеличиваем — увеличим только если реально сохранится в БД
    
    # ────────────────────────────────────────────────────────────────
    # СЛОЙ 3: COUNT(*) в БД — сколько раз этот текст УЖЕ сохранён
    # ────────────────────────────────────────────────────────────────
    db_count = conn.execute(
        "SELECT COUNT(*) FROM texts WHERE content=?",
        (content,)
    ).fetchone()[0]
    if db_count >= MAX_TEXT_DUPLICATES:
        log.warning("🚫 СЛОЙ3 (БД) — уже %d копий в БД: %d слов из %s",
                    db_count, wc(content), chat_title)
        return False
    
    # ────────────────────────────────────────────────────────────────
    # Сохраняем
    # ────────────────────────────────────────────────────────────────
    words = wc(content)
    conn.execute("INSERT OR IGNORE INTO texts (content,source_chat,source_chat_title,source_message_id,source_link,word_count) VALUES (?,?,?,?,?,?)",
                 (content, chat_id, chat_title, msg_id, link, words))
    conn.commit()
    
    # Проверяем что реально вставилось
    rowid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    actually_saved = conn.execute(
        "SELECT 1 FROM texts WHERE source_chat=? AND source_message_id=?",
        (chat_id, msg_id)
    ).fetchone()
    
    if not actually_saved:
        log.warning("⏭️ save_text: INSERT OR IGNORE проигнорирован (msg#%s из %s уже есть)", msg_id, chat_title)
        return False
    
    # Увеличиваем счётчик сессии ТОЛЬКО после успешного сохранения
    _sess_dup[ch] = sess_sent + 1
    
    log.info("💾 %d слов из %s | канал #%s | msg#%s (сессия: %d, БД: %d/%d)",
             words, chat_title, chat_id, msg_id,
             _sess_dup[ch], db_count + 1, MAX_TEXT_DUPLICATES)
    return True

# ─── Persistent pause state (переживает рестарты и редеплои через SQLite) ──
def _load_pause_state():
    """Читает состояние паузы из БД. Возвращает True если было сохранено, False если нет."""
    global _scanning
    row = conn.execute("SELECT value FROM config WHERE key='paused'").fetchone()
    if row:
        _scanning = row[0] != '1'
        log.info("⏸ Восстановлено состояние паузы из БД: scanning=%s", _scanning)
        return True
    return False

def _save_pause_state(paused):
    """Сохраняет состояние паузы в БД"""
    conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)",
                 ('paused', '1' if paused else '0'))
    conn.commit()

# ─── File progress tracking (для возобновления отправки файлов после перезапуска) ──
def _get_active_file_progress():
    """Возвращает активный прогресс отправки файла или None"""
    row = conn.execute(
        "SELECT chat_id, msg_id, filename, total_words, words_sent, "
        "total_chunks, chunks_sent, file_size, checkpoint_msg_id "
        "FROM file_progress WHERE active=1 ORDER BY updated_at DESC LIMIT 1"
    ).fetchone()
    if not row:
        return None
    return {
        'chat_id': row[0], 'msg_id': row[1], 'filename': row[2],
        'total_words': row[3], 'words_sent': row[4],
        'total_chunks': row[5], 'chunks_sent': row[6],
        'file_size': row[7], 'checkpoint_msg_id': row[8]
    }

def _save_file_progress(chat_id, msg_id, filename, total_words, words_sent,
                         total_chunks, chunks_sent, file_size,
                         checkpoint_msg_id=None):
    conn.execute("""INSERT OR REPLACE INTO file_progress 
        (chat_id, msg_id, filename, total_words, words_sent,
         total_chunks, chunks_sent, file_size, active, checkpoint_msg_id, updated_at)
        VALUES (?,?,?,?,?,?,?,?,1,?,?)""",
        (chat_id, msg_id, filename, total_words, words_sent,
         total_chunks, chunks_sent, file_size,
         checkpoint_msg_id, time.time()))
    conn.commit()

def _mark_file_progress_done(chat_id, msg_id):
    conn.execute("UPDATE file_progress SET active=0, words_sent=total_words "
                 "WHERE chat_id=? AND msg_id=?", (chat_id, msg_id))
    conn.commit()

# ─── Telegram checkpoint для файлового прогресса (переживает редеплой) ──
# ВНИМАНИЕ: без __префикса — Telegram/markdown интерпретирует __как__ italic!
_FP_PREFIX = "FPROGRESS\n"

def _build_checkpoint_text(chat_id, msg_id, filename, file_size, total_words, words_sent, active=True):
    return (
        f"{_FP_PREFIX}"
        f"chat_id: {chat_id}\n"
        f"msg_id: {msg_id}\n"
        f"filename: {filename}\n"
        f"file_size: {file_size}\n"
        f"total_words: {total_words}\n"
        f"words_sent: {words_sent}\n"
        f"active: {'1' if active else '0'}\n"
    )

def _parse_checkpoint_text(text):
    """Парсит чекпойнт из Telegram сообщения, возвращает dict или None"""
    # Принимаем как с __FPROGRESS__ (старый формат, сломанный markdown'ом)
    # так и с FPROGRESS (новый формат, без подчёркиваний)
    if not text:
        return None
    if text.startswith("__FPROGRESS__\n"):
        text = text[len("__FPROGRESS__\n"):]
    elif text.startswith(_FP_PREFIX):
        text = text[len(_FP_PREFIX):]
    else:
        return None
    data = {}
    for line in text.split("\n"):
        if line.startswith("chat_id:"):
            data['chat_id'] = int(line.split(":", 1)[1].strip())
        elif line.startswith("msg_id:"):
            data['msg_id'] = int(line.split(":", 1)[1].strip())
        elif line.startswith("filename:"):
            data['filename'] = line.split(":", 1)[1].strip()
        elif line.startswith("file_size:"):
            data['file_size'] = int(line.split(":", 1)[1].strip())
        elif line.startswith("total_words:"):
            data['total_words'] = int(line.split(":", 1)[1].strip())
        elif line.startswith("words_sent:"):
            data['words_sent'] = int(line.split(":", 1)[1].strip())
        elif line.startswith("active:"):
            data['active'] = line.split(":", 1)[1].strip() == '1'
    if 'active' in data and data['active'] and 'chat_id' in data and 'msg_id' in data:
        return data
    return None

async def _send_checkpoint(data):
    """Отправляет или обновляет чекпойнт в ЛС владельцу.
       Отправляем с кастомным parse_mode (без markdown/html), чтобы FPROGRESS не сломался."""
    def _no_parse(text):
        return text, []
    try:
        text = _build_checkpoint_text(
            data['chat_id'], data['msg_id'], data['filename'],
            data['file_size'], data['total_words'], data['words_sent'],
            active=True
        )
        old_id = data.get('checkpoint_msg_id')
        if old_id:
            try:
                await client.edit_message(data['chat_id'], old_id, text, parse_mode=_no_parse)
                return old_id
            except Exception:
                pass  # не смогли отредактировать — шлём новое
        msg = await client.send_message(data['chat_id'], text, parse_mode=_no_parse)
        return msg.id
    except Exception as e:
        log.warning("Не удалось отправить чекпойнт: %s", e)
        return None

async def _find_latest_checkpoint(owner_id):
    """Сканирует ЛС владельца (переписку с ботом) в поисках последнего чекпойнта FPROGRESS.
       Использует user-клиент (uc), который должен читать чат с БОТОМ,
       а не с owner_id (иначе user-клиент читает Saved Messages)."""
    uc = get_user_client()
    if not uc or not uc.is_connected():
        log.warning("📂 User-клиент недоступен — не могу сканировать ЛС")
        return None

    # Получаем юзернейм бота (чтобы user-клиент читал именно ЛС с ботом)
    try:
        me_bot = await client.get_me()
        bot_username = me_bot.username or me_bot.id
    except Exception as e:
        log.warning("📂 Не удалось получить юзернейм бота: %s", e)
        return None

    log.info("📂 Сканирую ЛС с @%s через user-клиент...", bot_username)

    # Шаг 1: пробуем search API (может не работать даже для user-клиента)
    try:
        found_search = 0
        async for msg in uc.iter_messages(bot_username, search="FPROGRESS", limit=20):
            if not msg or not msg.raw_text:
                continue
            found_search += 1
            raw = msg.raw_text
            log.info("📂 Search: найдено msg #%d с 'FPROGRESS': %s", msg.id, raw[:200])
            data = _parse_checkpoint_text(raw)
            if data:
                log.info("📂 Чекпойнт найден через search! words_sent=%s", data.get('words_sent','?'))
                data['checkpoint_msg_id'] = msg.id
                return data
        if found_search > 0:
            log.info("📂 Search: найдено %d сообщений, но ни одно не распарсилось", found_search)
        else:
            log.info("📂 Search: 0 результатов")
    except Exception as e:
        log.warning("📂 Search API не сработал: %s", str(e)[:200])

    # Шаг 2: Fallback — прямой перебор 500 последних сообщений (ЧЕРЕЗ user-клиент)
    log.info("📂 Fallback: перебор 500 сообщений через user-клиент...")
    try:
        found_fb = 0
        async for msg in uc.iter_messages(bot_username, limit=500):
            if not msg or not msg.raw_text:
                continue
            raw = msg.raw_text
            if "FPROGRESS" in raw:
                found_fb += 1
                log.info("📂 Fallback: msg #%d содержит FPROGRESS, raw_text[:200]=%s", msg.id, raw[:200])
                data = _parse_checkpoint_text(raw)
                if data:
                    log.info("📂 Fallback: Чекпойнт найден! words_sent=%s", data.get('words_sent','?'))
                    data['checkpoint_msg_id'] = msg.id
                    return data
        log.info("📂 Fallback: проверено 500 сообщений, найдено FPROGRESS: %d", found_fb)
    except Exception as e:
        log.warning("📂 Fallback тоже не сработал: %s", str(e)[:300])

    return None

async def _delete_checkpoint(chat_id, msg_id):
    """Удаляет сообщение-чекпойнт"""
    if msg_id:
        try:
            await client.delete_messages(chat_id, msg_id)
        except Exception:
            pass

session_path = str(DATA_DIR / "mt_session")
client = TelegramClient(session_path, API_ID, API_HASH)
RE_LINK = re.compile(r'(?:https?://)?(?:t\.me|telegram\.me)/([a-zA-Z0-9_+\-]{3,})')
_scanning = False  # после любого рестарта/редеплоя — пауза (force=True для файлов владельца)

# ─── Бэкап настроек в Telegram (чтобы не слетало после редеплоя) ──
BACKUP_MSG_ID_KEY = "backup_msg_id"
FALLBACK_OUTPUT_CHAT = -1003956215779  # канал вывода (постоянный, бот там админ)

def _build_backup_text():
    """Собирает текст бэкапа из текущих настроек в БД"""
    out = get_output_chat()
    kw = conn.execute("SELECT value FROM config WHERE key='user_search_keywords'").fetchone()
    phone = conn.execute("SELECT value FROM config WHERE key='user_phone'").fetchone()
    auto = conn.execute("SELECT value FROM config WHERE key='user_autosearch'").fetchone()
    ss = conn.execute("SELECT value FROM config WHERE key='user_session_string'").fetchone()
    lines = ["📦 Бэкап настроек Thesaurus"]
    if out: lines.append(f"output_chat={out}")
    if kw: lines.append(f"searchwords={kw[0]}")
    if phone: lines.append(f"phone={phone[0]}")
    if ss and ss[0].strip(): lines.append(f"session_string={ss[0]}")
    if auto: lines.append(f"autosearch={auto[0]}")
    pers = get_persistent_channels()
    if pers: lines.append(f"persistent={','.join(pers)}")
    # Сохраняем прогресс по каждому каналу (last_msg_id) — после рестарта не начинаем с нуля
    ch_rows = conn.execute(
        "SELECT username, last_msg_id FROM scrape_queue WHERE last_msg_id > 0 ORDER BY username"
    ).fetchall()
    if ch_rows:
        progress_parts = [f"{r[0]}:{r[1]}" for r in ch_rows[:50]]  # макс 50 каналов
        lines.append(f"channel_progress={','.join(progress_parts)}")
    # Сохраняем прогресс активного файла (чтобы возобновить после редеплоя)
    fp_row = conn.execute(
        "SELECT chat_id, msg_id, filename, file_size, words_sent FROM file_progress WHERE active=1 ORDER BY updated_at DESC LIMIT 1"
    ).fetchone()
    if fp_row:
        fp_chat, fp_msg, fp_fn, fp_size, fp_words = fp_row
        # | как разделитель между полями (не встречается в именах файлов из Telegram)
        lines.append(f"file_progress={fp_chat}|{fp_msg}|{fp_fn}|{fp_size}|{fp_words}")
    return "\n".join(lines)

def _parse_backup_lines(text):
    """Парсит строки бэкапа и восстанавливает настройки в БД"""
    if not text or not text.startswith("📦 Бэкап настроек Thesaurus"):
        return False
    for line in text.split("\n"):
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
        elif line.startswith("session_string="):
            val = line.split("=", 1)[1].strip()
            if val:
                conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", ("user_session_string", val))
                log.info("📦 Восстановлена session_string из бэкапа")
        elif line.startswith("persistent="):
            val = line.split("=", 1)[1].strip()
            if val:
                conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", ("persistent_channels", val))
                log.info("📦 Восстановлены persistent-каналы: %s", val)
        elif line.startswith("channel_progress="):
            val = line.split("=", 1)[1].strip()
            if val:
                restored_count = 0
                for chunk in val.split(","):
                    chunk = chunk.strip()
                    if ":" not in chunk: continue
                    ch_name, ch_msg_id = chunk.split(":", 1)
                    if ch_name and ch_msg_id.isdigit():
                        conn.execute(
                            "UPDATE scrape_queue SET last_msg_id=max(last_msg_id, ?) WHERE username=?",
                            (int(ch_msg_id), ch_name)
                        )
                        restored_count += 1
                log.info("📦 Восстановлен прогресс %d каналов", restored_count)
        elif line.startswith("file_progress="):
            val = line.split("=", 1)[1].strip()
            if val:
                parts = val.split("|")
                if len(parts) == 5:
                    fp_chat, fp_msg, fp_fn, fp_size, fp_words = parts
                    # Валидация: chat_id может быть отрицательным, остальные — цифры
                    if (fp_chat.lstrip("-").isdigit() and fp_msg.isdigit() 
                            and fp_size.isdigit() and fp_words.isdigit()):
                        _save_file_progress(
                            int(fp_chat), int(fp_msg), fp_fn,
                            0, int(fp_words), 0, 0, int(fp_size),
                            checkpoint_msg_id=None
                        )
                        log.info("📦 Восстановлен прогресс файла: %s (слов: %s)", fp_fn, fp_words)
    conn.commit()
    return True

async def backup_settings_to_telegram():
    """Сохраняет настройки владельцу в ЛС (только я, без канала вывода)"""
    try:
        backup_text = _build_backup_text()
        
        # Бэкап только владельцу (8587090554) — в ЛС с ботом
        try:
            key = f"backup_msg_id_{OWNER_ID}"
            old_msg_id = conn.execute("SELECT value FROM config WHERE key=?", (key,)).fetchone()
            if old_msg_id:
                try:
                    await client.edit_message(OWNER_ID, int(old_msg_id[0]), backup_text)
                except:
                    msg = await client.send_message(OWNER_ID, backup_text, parse_mode="html")
                    conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", (key, str(msg.id)))
            else:
                msg = await client.send_message(OWNER_ID, backup_text, parse_mode="html")
                conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", (key, str(msg.id)))
            conn.commit()
        except Exception as e:
            log.warning("Не удалось отправить бэкап владельцу: %s", e)
            
    except Exception as e:
        log.warning("backup_settings_to_telegram: %s", e)

async def _find_backup_in_chat(chat_id, uc):
    """Ищет бэкап среди последних 50 сообщений в чате (через user-клиент)"""
    try:
        msg_id_key = f"backup_msg_id_{chat_id}"
        old_id = conn.execute("SELECT value FROM config WHERE key=?", (msg_id_key,)).fetchone()
        
        # Сначала пробуем по сохранённому msg_id (если БД не сброшена)
        if old_id:
            try:
                msg = await uc.get_messages(chat_id, ids=int(old_id[0]))
                if msg and msg.text and msg.text.startswith("📦 Бэкап настроек Thesaurus"):
                    return msg.text
            except Exception:
                pass
        
        # Иначе листаем последние сообщения (user-клиент может читать историю)
        async for msg in uc.iter_messages(chat_id, limit=50):
            if msg and msg.text and msg.text.startswith("📦 Бэкап настроек Thesaurus"):
                # Обновляем msg_id в БД для будущих обновлений
                conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", (msg_id_key, str(msg.id)))
                conn.commit()
                return msg.text
    except Exception as e:
        log.warning("Не удалось прочитать бэкап из чата %s: %s", chat_id, e)
    return None

async def restore_settings_from_backup():
    """Восстанавливает настройки из бэкапа в канале вывода (через user-клиент)"""
    log.info("📦 Ищу бэкап для восстановления настроек...")
    
    uc = get_user_client()
    if not uc or not uc.is_connected():
        log.warning("📦 User-клиент не подключён — восстановление невозможно")
        return False
    
    # Пробуем найти бэкап в канале вывода (или в FALLBACK_OUTPUT_CHAT)
    target = get_output_chat() or FALLBACK_OUTPUT_CHAT
    if target:
        backup_text = await _find_backup_in_chat(target, uc)
        if backup_text and _parse_backup_lines(backup_text):
            log.info("📦 Настройки восстановлены из бэкапа в канале %s", target)
            return True
    
    # Если в канале не нашли, пробуем в личке владельца (через юзернейм бота)
    log.info("📦 Бэкап в канале не найден, ищу в личке владельца...")
    try:
        # Используем юзернейм бота, чтобы user client прочитал свою переписку с ботом
        bot_entity = await client.get_me()
        bot_username = bot_entity.username or bot_entity.id
        async for msg in uc.iter_messages(bot_username, limit=50):
            if msg and msg.text and msg.text.startswith("📦 Бэкап настроек Thesaurus"):
                conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", (BACKUP_MSG_ID_KEY, str(msg.id)))
                conn.commit()
                if _parse_backup_lines(msg.text):
                    log.info("📦 Настройки восстановлены из бэкапа в личке (msg_id=%s)", msg.id)
                    return True
        # Если не нашли по юзернейму бота, пробуем как fallback диалог с самим собой (Saved Messages)
        async for msg in uc.iter_messages("me", limit=50):
            if msg and msg.text and msg.text.startswith("📦 Бэкап настроек Thesaurus"):
                if _parse_backup_lines(msg.text):
                    log.info("📦 Настройки восстановлены из Saved Messages (msg_id=%s)", msg.id)
                    return True
    except Exception as e:
        log.warning("Не удалось прочитать бэкап из лички: %s", e)
    
    log.info("📦 Бэкап не найден")
    return False

# ─── Persistent‑каналы (выживают при редеплое) ─────────────────────
def get_persistent_channels():
    """Возвращает список username каналов, которые должны быть в очереди всегда"""
    row = conn.execute("SELECT value FROM config WHERE key='persistent_channels'").fetchone()
    if not row or not row[0].strip(): return []
    return [ch.strip().lower() for ch in row[0].split(",") if ch.strip()]

def add_persistent_channel(username):
    username = username.lower().strip().lstrip("@")
    cur = get_persistent_channels()
    if username in cur: return False
    cur.append(username)
    conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", ("persistent_channels", ",".join(cur)))
    conn.commit()
    asyncio.create_task(backup_settings_to_telegram())
    return True

def remove_persistent_channel(username):
    username = username.lower().strip().lstrip("@")
    cur = get_persistent_channels()
    if username not in cur: return False
    cur.remove(username)
    conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)",
                 ("persistent_channels", ",".join(cur) if cur else ""))
    conn.commit()
    asyncio.create_task(backup_settings_to_telegram())
    return True

async def add_persistent_to_queue():
    """Добавляет все persistent-каналы в очередь (если их там нет)"""
    added = 0
    for ch in get_persistent_channels():
        try:
            conn.execute("INSERT OR IGNORE INTO scrape_queue (username,title) VALUES (?,?)", (ch, ch))
            added += 1
        except Exception as e:
            log.warning("⚠️ persistent @%s: %s", ch, e)
    if added:
        conn.commit()
        log.info("📌 Добавлено %d persistent-каналов в очередь", added)

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
        try:
            active_scrapes = conn.execute("SELECT COUNT(*) FROM scrape_queue WHERE status='active'").fetchone()[0]
            pending_scrapes = conn.execute("SELECT COUNT(*) FROM scrape_queue WHERE status='pending'").fetchone()[0]
            log.info("💓 Пульс: база %d текстов, active=%d pending=%d",
                     conn.execute("SELECT COUNT(*) FROM texts").fetchone()[0],
                     active_scrapes, pending_scrapes)
            # Сохраняем прогресс каналов в бэкап каждую минуту (на случай краша)
            try:
                await asyncio.wait_for(backup_settings_to_telegram(), timeout=20)
            except Exception as e:
                log.debug("heartbeat backup: %s", e)
        except Exception as e:
            log.warning("heartbeat error: %s", e)

# ─── Watchdog: проверяет что все критические задачи живы ──────────────
async def watchdog():
    """Каждые 30 секунд проверяет что background-задачи не умерли.
       Если какая-то умерла — перезапускает её. Процесс НЕ убивает."""
    if not IS_RENDER:
        return
    tasks_to_monitor = {
        "heartbeat": heartbeat,
        "self_pinger": self_pinger,
        "queue_worker": queue_worker,
    }
    # Даём время задачам запуститься
    await asyncio.sleep(10)
    while True:
        await asyncio.sleep(30)
        try:
            all_tasks = asyncio.all_tasks()
            task_strs = [str(t) for t in all_tasks]
            for name, factory in list(tasks_to_monitor.items()):
                found = any(name in s for s in task_strs)
                if not found:
                    log.warning("🔄 Watchdog: задача '%s' умерла, перезапускаю...", name)
                    asyncio.create_task(factory())
            
            # Дополнительно: проверяем что queue_worker обновляет heartbeat
            qw_ts = conn.execute("SELECT value FROM config WHERE key='qw_heartbeat'").fetchone()
            if qw_ts:
                try:
                    last_qw = datetime.fromisoformat(qw_ts[0])
                    if (datetime.now() - last_qw).total_seconds() > 60:
                        log.warning("🔄 Watchdog: queue_worker не отвечал >60с — перезапускаю задачу")
                        # Жёстко отменяем старый queue_worker (если он ещё висит)
                        for t in asyncio.all_tasks():
                            if "queue_worker" in str(t) and not t.done():
                                t.cancel()
                                break
                        # Создаём новый
                        asyncio.create_task(queue_worker())
                except:
                    pass
        except Exception as e:
            log.warning("watchdog error: %s", e)

# auto_restart отключён — каждые 5 минут убивал процесс, Render переставал рестартить
async def auto_restart():
    """Отключено. Авторестарт вызывал дубликаты и остановку бота."""
    pass

async def hard_deadline():
    """Отключено. Хард-дедлайн убивал процесс с os._exit(0) — Render не рестартил."""
    pass

async def self_pinger():
    """Пингует Render URL каждые 5 минут, чтобы контейнер не уснул"""
    if not IS_RENDER:
        return
    ping_url = RENDER_URL or f"http://localhost:{PORT}"
    while True:
        await asyncio.sleep(300)  # 5 минут
        try:
            # Асинхронный HTTP запрос — не блокирует event loop
            import aiohttp
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(ping_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        log.debug("🏓 Self-ping → %s", resp.status)
            except ImportError:
                # fallback на синхронный urllib
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, lambda: __import__('urllib.request').urlopen(ping_url, timeout=10))
                log.debug("🏓 Self-ping OK (urllib)")
        except Exception as e:
            log.debug("🏓 Self-ping: %s", e)

# ─── Telegram-based dedup — сканируем канал вывода при старте ──────
async def _load_sent_hashes():
    """Сканирует канал вывода, строит множество SHA256 хэшей уже отправленных текстов.
       Эта проверка НЕ зависит от файловой системы — работает через сам Telegram.
       Переживает любые рестарты и пересоздания инстанса Render."""
    global _SENT_HASHES
    _SENT_HASHES.clear()
    out = get_output_chat()
    if not out:
        log.warning("⚠️ Telegram dedup: канал вывода не назначен")
        return
    try:
        count = 0
        async for msg in client.iter_messages(out, limit=10000):
            if msg.text and len(msg.text) >= 20:  # короткие системные сообщения пропускаем
                _SENT_HASHES.add(hashlib.sha256(msg.text.encode("utf-8")).hexdigest())
                count += 1
        log.info("📋 Telegram dedup: загружено %d хэшей из %d сообщений в канале вывода",
                 len(_SENT_HASHES), count)
    except Exception as e:
        log.warning("⚠️ Telegram dedup: не удалось просканировать канал: %s", e)

# ─── Отправка в выходной канал (с глобальным rate limit + Telegram dedup) ─
_last_send_time = 0.0
_send_lock = asyncio.Lock()

async def send_to_output(text, source_title=None, source_link=None, force=False):
    global _last_send_time, _SENT_HASHES
    out = get_output_chat()
    if not out: return
    if not _scanning and not force:
        log.debug("⏸ Пауза: send_to_output пропущен (force=%s)", force)
        return

    text_clean = text.replace("<", "&lt;").replace(">", "&gt;")
    message = text_clean
    if len(message) > 3950:
        message = message[:3950] + "\n\n✂️ ..."
    
    # ─── Telegram dedup: проверяем что такой текст ещё не отправляли ──
    # Хэшируем текст в том виде, как его увидит пользователь в канале:
    # Telegram хранит HTML-сущности (&lt; → <) уже декодированными.
    # Поэтому для обеих сторон (проверка перед отправкой и msg.text при сканировании)
    # используем html.unescape() чтобы получить единую каноническую форму.
    import html as _html
    canonical = _html.unescape(message)
    msg_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    if msg_hash in _SENT_HASHES:
        log.warning("🚫 Telegram dedup: текст с хэшем %s уже есть в канале вывода, пропускаю", msg_hash[:8])
        # Всё равно сохраняем в базу (чтобы счётчик текстов был верным)
        return  # ← не отправляем дубль

    # Rate limit + отправка под одним lock — предотвращает FloodWait
    async with _send_lock:
        now = asyncio.get_event_loop().time()
        since_last = now - _last_send_time
        if since_last < 3.0:
            wait = 3.0 - since_last
            log.debug("⏳ Rate limit send: жду %.1fс", wait)
            await asyncio.sleep(wait)
        try:
            await safe_send_message(client, out, message, parse_mode="html")
            _last_send_time = asyncio.get_event_loop().time()
            # Добавляем хэш в множество — этот текст больше не отправится
            _SENT_HASHES.add(msg_hash)
            log.info("📤 Отправлено в канал вывода (хэш %s)", msg_hash[:8])
        except errors.FloodWaitError as e:
            log.warning("⏳ FloodWait при отправке: %dс (ждём)", e.seconds)
            await asyncio.sleep(min(e.seconds, 30))
            # после FloodWait обновляем время — следующий send подождёт 3с
            _last_send_time = asyncio.get_event_loop().time()
        except Exception as e:
            log.warning("Не отправилось в канал: %s", e)

# ─── Broadcast .txt файла частями в канал ──────────────────────────
# Слова-паразиты для удаления из текстов
_BROADCAST_BAD_WORDS = re.compile(
    r'\b(?:тфотт|сравалерия|ссавалерия|ваффен|шаббат|сраббат|среон|ксеон|оккультизм|сраккультизм)\b',
    re.IGNORECASE,
)

def _clean_broadcast_text(text: str) -> str:
    """Удаляет плохие слова из текста для broadcast"""
    text = _BROADCAST_BAD_WORDS.sub('', text)
    text = re.sub(r' +', ' ', text).strip()
    return text

async def broadcast_file(text, filename, event, force=True):
    """Разбивает текст на части по ~600 слов и отправляет в канал с интервалом ~4с"""
    global _last_send_time
    
    # Удаляем плохие слова из файла
    text = _clean_broadcast_text(text)
    
    words = text.split()
    total = len(words)
    chunk_size = 600
    chunks = []
    for i in range(0, total, chunk_size):
        chunks.append(" ".join(words[i:i + chunk_size]))
    
    total_chunks = len(chunks)
    date_str = datetime.now().strftime("%d.%m.%Y %H:%M")
    
    out = get_output_chat()
    if not out:
        await safe_send(event.chat_id, "❌ Канал вывода не назначен.")
        return
    
    if total_chunks == 1:
        # Одна часть — через обычный send (с rate limit, force=True — файлы всегда)
        await send_to_output(text, filename, force=force)
        await safe_send(event.chat_id, f"✅ {filename} ({total} слов) отправлен.")
        return
    
    await safe_send(event.chat_id, f"📤 Начинаю broadcast {filename} — {total_chunks} частей по ~{chunk_size} слов, каждые 4с.")
    
    for i, chunk in enumerate(chunks, 1):
        header = f"[{date_str}] [{filename}] {i}/{total_chunks}"
        text_clean = chunk.replace("<", "&lt;").replace(">", "&gt;")
        message = f"{header}\n\n{text_clean}"
        if len(message) > 3950:
            message = message[:3950] + "\n\n✂️ ..."
        
        # Используем тот же lock, что и send_to_output — не конфликтует
        async with _send_lock:
            now = asyncio.get_event_loop().time()
            since_last = now - _last_send_time
            if since_last < 3.0:
                await asyncio.sleep(3.0 - since_last)
            try:
                await safe_send_message(client, out, message, parse_mode="html")
                _last_send_time = asyncio.get_event_loop().time()
                log.info("📤 Broadcast %s %d/%d", filename, i, total_chunks)
            except errors.FloodWaitError as e:
                log.warning("⏳ FloodWait broadcast: %dс (ждём)", e.seconds)
                await asyncio.sleep(min(e.seconds, 30))
                _last_send_time = asyncio.get_event_loop().time()
            except Exception as e:
                log.warning("Broadcast ошибка: %s", e)
        
        if i < total_chunks:
            await asyncio.sleep(1)  # +3s rate limit = ~4s между частями
    
    await safe_send(event.chat_id, f"✅ Broadcast {filename} завершён ({total_chunks} частей).")

# ─── Проверка владельца ──────────────────────────────────────────────
def is_owner(event):
    return event.sender_id in OWNER_IDS

async def notify_owners(text, parse_mode="html"):
    """Отправляет сообщение всем владельцам бота"""
    for uid in OWNER_IDS:
        try:
            await client.send_message(uid, text, parse_mode=parse_mode)
        except Exception as e:
            log.warning("Не отправилось владельцу %s: %s", uid, e)

async def safe_send(chat_id, text, parse_mode="html"):
    try:
        await client.send_message(chat_id, text, parse_mode=parse_mode)
    except Exception as e:
        log.warning("Не отправилось: %s", e)

# ─── Стриминг большого .txt файла с возможностью докачки ─────────────────
async def _stream_file_from_msg(msg, chat_id, msg_id, file_size, filename, title, fn, is_owner_dm, words_to_skip=0):
    """Стримит большой .txt файл через iter_download, с поддержкой resume.
       words_to_skip — сколько слов пропустить (для возобновления после рестарта).
       Возвращает (total_chunks_sent, total_words_sent)."""
    doc = msg.document if msg.document else (msg.media.document if msg.media and hasattr(msg.media, 'document') else None)
    if not doc:
        return 0, 0

    CHUNK_WORDS = 600
    word_buffer = []
    total_chunks = 0
    total_words = 0
    skipped = 0
    chunk_buf = b''
    report_time = time.time()
    bytes_downloaded = 0
    checkpoint_interval = 20  # обновлять чекпойнт в Telegram каждые N чанков
    last_checkpoint_chunks = 0

    def _decode_chunk(buf):
        try:
            return buf.decode('utf-8'), b''
        except UnicodeDecodeError:
            for cut in range(len(buf), max(0, len(buf) - 8), -1):
                try:
                    return buf[:cut].decode('utf-8'), buf[cut:]
                except UnicodeDecodeError:
                    continue
            return buf.decode('utf-8', errors='replace'), b''

    async def _flush_chunk(force=False):
        nonlocal word_buffer, total_chunks, total_words, last_checkpoint_chunks
        text = ' '.join(word_buffer)
        text = _clean_broadcast_text(text)
        if not text.strip():
            word_buffer = []
            return
        if is_owner_dm:
            # Владельцу — без фильтра, сразу отправляем
            if force or len(word_buffer) >= CHUNK_WORDS:
                await send_to_output(text, fn, f"file:{fn}", force=True)
                total_chunks += 1
                total_words += len(word_buffer)
                word_buffer = []
        else:
            # Не владельцу — с фильтром 50 слов
            if wc(text) >= MIN_WORDS:
                if save_text(text.strip(), title, str(chat_id), msg_id, f"file:{fn}"):
                    await send_to_output(text.strip(), title, f"file:{fn}", force=True)
                    total_chunks += 1
                    total_words += len(word_buffer)
            word_buffer = []

        # Сохраняем прогресс в SQLite (каждые 1 чанк — чтобы при любом рестарте минимум потерь)
        cumulative_words = words_to_skip + total_words
        if total_chunks > 0 and total_chunks % 1 == 0:
            _save_file_progress(chat_id, msg_id, fn, 0, cumulative_words, 0, total_chunks, file_size)

        # Чекпойнт в Telegram каждые checkpoint_interval чанков
        if is_owner_dm and total_chunks > 0 and (total_chunks - last_checkpoint_chunks) >= checkpoint_interval:
            last_checkpoint_chunks = total_chunks
            cp_data = _get_active_file_progress()
            if cp_data:
                cp_data['words_sent'] = cumulative_words
                cp_msg_id = await _send_checkpoint(cp_data)
                if cp_msg_id:
                    conn.execute(
                        "UPDATE file_progress SET checkpoint_msg_id=? WHERE chat_id=? AND msg_id=?",
                        (cp_msg_id, chat_id, msg_id)
                    )
                    conn.commit()

    # Создаём/обновляем запись прогресса в SQLite (сохраняем words_to_skip на случай ошибки до первого флаша)
    _save_file_progress(chat_id, msg_id, fn, 0, words_to_skip, 0, 0, file_size)

    # Отправляем чекпойнт в Telegram сразу же — чтобы при редеплое был fallback
    if is_owner_dm:
        cp_data = _get_active_file_progress()
        if cp_data:
            cp_data['words_sent'] = words_to_skip
            cp_msg_id = await _send_checkpoint(cp_data)
            if cp_msg_id:
                conn.execute(
                    "UPDATE file_progress SET checkpoint_msg_id=? WHERE chat_id=? AND msg_id=?",
                    (cp_msg_id, chat_id, msg_id)
                )
                conn.commit()

    try:
        async for chunk in client.iter_download(doc, request_size=262144, file_size=file_size):
            chunk_buf += chunk
            bytes_downloaded += len(chunk)
            text, chunk_buf = _decode_chunk(chunk_buf)

            words = text.split()
            for w in words:
                # Скипаем слова, если мы в режиме докачки
                if skipped < words_to_skip:
                    skipped += 1
                    continue
                word_buffer.append(w)
                if len(word_buffer) >= CHUNK_WORDS:
                    await _flush_chunk()

            if is_owner_dm and time.time() - report_time > 15:
                report_time = time.time()
                pct = min(100, bytes_downloaded * 100 // max(1, file_size))
                await safe_send(chat_id,
                    f"⏳ {fn}: {bytes_downloaded//1024//1024}MB / {file_size//1024//1024}MB ({pct}%), "
                    f"отправлено {total_chunks} чанков ({total_words} слов)")

    except Exception as stream_err:
        log.exception("Файл %s: ошибка при стриминге: %s", fn, stream_err)
        if is_owner_dm:
            await safe_send(chat_id, f"❌ Ошибка при стриминге {fn}: {str(stream_err)[:200]}")
        # Сохраняем прогресс на случай ошибки (кумулятивный words_sent = words_to_skip + total_words)
        _save_file_progress(chat_id, msg_id, fn, 0, words_to_skip + total_words, 0, total_chunks, file_size)
        return total_chunks, total_words

    # Остаток буфера
    if chunk_buf:
        text, _ = _decode_chunk(chunk_buf)
        for w in text.split():
            if skipped < words_to_skip:
                skipped += 1
                continue
            word_buffer.append(w)

    await _flush_chunk(force=True)

    # Помечаем файл как завершённый
    _mark_file_progress_done(chat_id, msg_id)

    # Обновляем чекпойнт в Telegram: active=0 (чтобы после редеплоя не пытался возобновить)
    cp = conn.execute(
        "SELECT checkpoint_msg_id FROM file_progress WHERE chat_id=? AND msg_id=?",
        (chat_id, msg_id)
    ).fetchone()
    if cp and cp[0]:
        try:
            done_text = _build_checkpoint_text(
                chat_id, msg_id, fn, file_size, total_words, total_words, active=False
            )
            await client.edit_message(chat_id, cp[0], done_text)
        except Exception:
            pass
        await _delete_checkpoint(chat_id, cp[0])

    if is_owner_dm:
        await safe_send(chat_id, f"✅ {fn}: {total_chunks} чанков отправлено ({total_words} слов)")

    return total_chunks, total_words


async def _resume_file_streaming():
    """Проверяет, есть ли незавершённая отправка файла, и возобновляет её.
       Сначала проверяет SQLite, потом Telegram чекпойнты.
       Отправляет диагностику владельцу в ЛС."""
    try:
        # Принудительно резолвим владельца (кэшируем access_hash для нового сеанса после редеплоя)
        try:
            await client.get_entity(OWNER_ID)
            log.info("📂 Владелец зарезолвлен: %d", OWNER_ID)
        except Exception as e:
            log.warning("📂 Не удалось зарезолвить владельца: %s", str(e)[:200])

        fp = _get_active_file_progress()
        source = "SQLite"
        if fp:
            log.info("📂 Найден активный прогресс файла в SQLite: %s (слов: %d/%d)",
                     fp['filename'], fp['words_sent'], fp['total_words'])
        else:
            # Если SQLite пуст (редеплой) — ищем чекпойнт в ЛС владельца
            log.info("📂 SQLite пуст, ищу чекпойнт в ЛС владельца...")
            fp = await _find_latest_checkpoint(OWNER_ID)
            if fp:
                source = "Telegram"
                log.info("📂 Найден чекпойнт в Telegram: %s (слов: %d/%d)",
                         fp.get('filename', '?'), fp.get('words_sent', 0), fp.get('total_words', 0))
                # Восстанавливаем в SQLite
                _save_file_progress(
                    fp['chat_id'], fp['msg_id'],
                    fp.get('filename', 'unknown.txt'),
                    fp.get('total_words', 0), fp.get('words_sent', 0),
                    0, 0, fp.get('file_size', 0),
                    fp.get('checkpoint_msg_id')
                )
            else:
                # Нет прогресса — это нормально (первый запуск, файлов не было)
                log.info("📂 Нет незавершённых файлов (SQLite пуст, чекпойнт не найден)")
                return

        if not fp:
            return

        # Сообщаем владельцу что нашли
        try:
            await safe_send(OWNER_ID,
                f"📂 Найден прогресс файла ({source}): {fp.get('filename','?')}, "
                f"отправлено {fp['words_sent']} слов")
        except:
            pass

        # Получаем сообщение с файлом
        file_msg = None
        try:
            file_msg = await client.get_messages(fp['chat_id'], ids=fp['msg_id'])
            if file_msg is None:
                log.warning("📂 Сообщение с файлом #%d удалено или недоступно", fp['msg_id'])
                await safe_send(OWNER_ID, f"❌ Сообщение с файлом #{fp['msg_id']} удалено или недоступно")
                _mark_file_progress_done(fp['chat_id'], fp['msg_id'])
                return
        except Exception as e:
            log.warning("📂 Не удалось получить сообщение с файлом: %s", e)
            await safe_send(OWNER_ID, f"❌ Ошибка получения файла: {str(e)[:200]}")
            return

        if not file_msg.document:
            log.warning("📂 Сообщение #%d больше не содержит файла", fp['msg_id'])
            await safe_send(OWNER_ID, f"❌ Сообщение #{fp['msg_id']} больше не содержит файла")
            _mark_file_progress_done(fp['chat_id'], fp['msg_id'])
            return

        fn = file_msg.file.name or "unknown.txt"
        file_size = file_msg.file.size or 0
        is_owner_dm = fp['chat_id'] in OWNER_IDS

        log.info("📂 Возобновляю отправку файла %s с позиции %d слов...",
                 fn, fp['words_sent'])

        await safe_send(fp['chat_id'],
            f"🔄 Возобновляю отправку файла {fn} ({file_size//1024//1024}MB) с {fp['words_sent']} слов...")

        await _stream_file_from_msg(
            file_msg, fp['chat_id'], fp['msg_id'], file_size,
            fn, fn, fn, is_owner_dm,
            words_to_skip=fp['words_sent']
        )
    except Exception as e:
        log.exception("📂 _resume_file_streaming: необработанная ошибка: %s", e)
        try:
            await safe_send(OWNER_ID, f"❌ Ошибка при возобновлении файла: {str(e)[:200]}")
        except:
            pass

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
        
        # Пропускаем исключённые каналы (но не ЛС владельца — там файлы и команды)
        if not event.is_private and chat_username in EXCLUDED_CHANNELS:
            log.debug("⏭️ Пропущен исключённый канал: @%s", chat_username)
            return
        # Фильтр спама от @A_ToolsX и подобных
        if msg.text and ("A_ToolsX" in msg.text or "To use this bot, you must join our channel" in msg.text or "a_toolsx" in msg.text.lower()):
            log.debug("⏭️ Пропущен спам: %s", msg.text[:80])
            return
        # Фильтр по отправителю
        sender = await event.get_sender()
        sender_username = getattr(sender, 'username', '').lower() if sender else ''
        if sender_username in ('a_toolsx',):
            log.debug("⏭️ Пропущено от @%s", sender_username)
            return
        # Пропускаем канал вывода
        out_ch = get_output_chat()
        if out_ch and chat.id == out_ch:
            return
        
        log.info("📩 Сообщение от %s: «%s»", title, (msg.text or "")[:120])

        # При паузе НЕ обрабатываем текстовые сообщения (только .txt файлы)
        if not _scanning:
            log.debug("⏸ Пауза: текстовое сообщение из %s пропущено", title)
        else:
            # Сохраняем и отправляем текст ≥50 слов
            if msg.text and wc(msg.text) >= MIN_WORDS:
                link = f"https://t.me/{chat.username}/{msg.id}" if getattr(chat, "username", None) else None
                if save_text(msg.text, title, str(chat.id), msg.id, link):
                    log.info("📤 on_msg → send_to_output: %s msg#%s из %s", chat.id, msg.id, title)
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
                if not (fn.endswith(".txt") or "text/plain" in (msg.file.mime_type or "")):
                    return
                
                file_size = msg.file.size if msg.file and msg.file.size else 0
                is_owner_dm = event.is_private and is_owner(event)
                
                # ── Для больших файлов (>50MB) — стримим через iter_download с поддержкой докачки ──
                if file_size > 50 * 1024 * 1024:
                    if is_owner_dm:
                        await safe_send(event.chat_id, f"📥 Большой файл ({file_size//1024//1024}MB), стримлю и обрабатываю…")
                    
                    await _stream_file_from_msg(
                        msg, event.chat_id, msg.id, file_size,
                        fn, title, fn, is_owner_dm,
                        words_to_skip=0
                    )
                    return
                
                # ── Для маленьких файлов (≤50MB) — скачиваем в память ──
                fb = None
                try:
                    fb = await msg.download_media(file=bytes)
                except Exception as dl_err:
                    log.warning("Файл %s: download_media не сработал (%s), пробую рефреш", fn, str(dl_err)[:80])
                    try:
                        fresh = await client.get_messages(event.chat_id, ids=msg.id)
                        if fresh and len(fresh) > 0:
                            fb = await fresh[0].download_media(file=bytes)
                    except Exception:
                        pass
                
                if not fb:
                    try:
                        fb = await client.download_file(msg.document, file=bytes)
                    except Exception as e2:
                        log.error("Файл %s: download_file не сработал: %s", fn, e2)
                
                if not fb:
                    if is_owner_dm:
                        await safe_send(event.chat_id, f"❌ Файл {fn} не удалось скачать (возможно, удалён или слишком старый).")
                    return
                
                text_content = fb.decode("utf-8", errors="replace")
                text_content = _clean_broadcast_text(text_content)
                
                if is_owner_dm:
                    await broadcast_file(text_content, fn, event)
                else:
                    for b in re.split(r'\n\s*\n', text_content):
                        if wc(b) >= MIN_WORDS:
                            if save_text(b.strip(), title, str(chat.id), msg.id, f"file:{fn}"):
                                await send_to_output(b.strip(), title, f"file:{fn}", force=True)
            except Exception as e:
                log.exception("Файл %s: необработанная ошибка", fn)
                if event.is_private and is_owner(event):
                    await safe_send(event.chat_id, f"❌ Ошибка при обработке файла {fn}: {str(e)[:200]}")
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
            "/add &lt;username&gt; — добавить канал (или /add +invite для приватных)\n"
            "/remove &lt;username&gt; — удалить канал\n"
            "/persist &lt;username&gt; — добавить в persistent (не пропадёт при редеплое)\n"
            "/unpersist &lt;username&gt; — убрать из persistent\n"
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
        
        # Приватная ссылка t.me/+xxx или +xxx
        if username.startswith("+") or raw.startswith("t.me/+") or raw.startswith("https://t.me/+"):
            invite_hash = username.lstrip("+")
            await safe_send(event.chat_id, f"🔑 Пробую открыть приватную ссылку...")
            try:
                uc = get_user_client()
                if not uc.is_connected():
                    await uc.connect()
                if not await uc.is_user_authorized():
                    await safe_send(event.chat_id, "❌ User клиент не авторизован. Сначала /auth")
                    return
                from telethon.tl.functions.messages import CheckChatInviteRequest
                result = await uc(CheckChatInviteRequest(invite_hash))
                if hasattr(result, 'chat'):
                    entity = result.chat
                    ch_username = getattr(entity, 'username', None) or f"priv_{invite_hash}"
                    ch_title = getattr(entity, 'title', None) or ch_username
                    conn.execute("INSERT OR IGNORE INTO scrape_queue (username,title) VALUES (?,?)", (ch_username, ch_title))
                    conn.commit()
                    await safe_send(event.chat_id, f"📡 <b>{ch_title}</b> (приватный) добавлен в очередь.")
                else:
                    await safe_send(event.chat_id, "❌ Ты не участник этого канала/группы. Вступи сначала.")
                    # Если не участник, можно попробовать вступить:
                    # from telethon.tl.functions.messages import ImportChatInviteRequest
                    # await uc(ImportChatInviteRequest(invite_hash))
            except Exception as e:
                await safe_send(event.chat_id, f"❌ Ошибка приватной ссылки: {str(e)[:200]}")
            return
        
        if not username:
            await safe_send(event.chat_id, "❌ Укажи @username канала.")
            return
        if conn.execute("SELECT 1 FROM scrape_queue WHERE username=?", (username,)).fetchone():
            await safe_send(event.chat_id, f"ℹ️ @{username} уже в очереди.")
            return
        try:
            # Сначала пробуем через user client (он видит приватные каналы где ты участник)
            uc = get_user_client()
            try:
                if await uc.is_user_authorized():
                    entity = await uc.get_entity(username)
                else:
                    entity = await client.get_entity(username)
            except:
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

async def cmd_remove(event):
    """Удалить канал из очереди"""
    if not is_owner(event): return
    raw = event.pattern_match.group(1).strip().lower()
    username = re.sub(r'^(?:https?://)?(?:t\.me/|@)', '', raw).split('/')[0].split('?')[0]
    if conn.execute("SELECT 1 FROM scrape_queue WHERE username=?", (username,)).fetchone():
        conn.execute("DELETE FROM scrape_queue WHERE username=?", (username,))
        conn.commit()
        await safe_send(event.chat_id, f"🗑 @{username} удалён из очереди.")
    else:
        await safe_send(event.chat_id, f"❌ @{username} нет в очереди.")

async def cmd_persist(event):
    """Добавить канал в persistent (выживает при редеплое)"""
    if not is_owner(event): return
    raw = event.pattern_match.group(1).strip().lower()
    username = re.sub(r'^(?:https?://)?(?:t\.me/|@)', '', raw).split('/')[0].split('?')[0]
    # Добавляем в очередь если ещё нет
    if not conn.execute("SELECT 1 FROM scrape_queue WHERE username=?", (username,)).fetchone():
        conn.execute("INSERT INTO scrape_queue (username,title) VALUES (?,?)", (username, username))
        conn.commit()
    if add_persistent_channel(username):
        await safe_send(event.chat_id, f"📌 @{username} добавлен в persistent. Не пропадёт при редеплое.")
    else:
        await safe_send(event.chat_id, f"ℹ️ @{username} уже в persistent.")

async def cmd_unpersist(event):
    """Убрать канал из persistent"""
    if not is_owner(event): return
    raw = event.pattern_match.group(1).strip().lower()
    username = re.sub(r'^(?:https?://)?(?:t\.me/|@)', '', raw).split('/')[0].split('?')[0]
    if remove_persistent_channel(username):
        await safe_send(event.chat_id, f"🗑 @{username} убран из persistent.")
    else:
        await safe_send(event.chat_id, f"❌ @{username} не в persistent.")

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
        _save_pause_state(True)
        await safe_send(event.chat_id, "⏸ Пауза.")
    except Exception as e:
        log.exception("cmd_pause: %s", e)

async def cmd_resume(event):
    try:
        if not is_owner(event): return
        global _scanning; _scanning = True
        _save_pause_state(False)
        await safe_send(event.chat_id, "▶️ Продолжаем.")
    except Exception as e:
        log.exception("cmd_resume: %s", e)

async def cmd_resume_file(event):
    """Ручное возобновление отправки файла после рестарта (с диагностикой)"""
    try:
        if not is_owner(event): return
        await safe_send(event.chat_id, "🔍 Диагностика возобновления файла...")
        
        # Шаг 1: пытаемся зарезолвить владельца
        try:
            me = await client.get_entity(OWNER_ID)
            await safe_send(event.chat_id, f"✅ Владелец зарезолвлен: {me.id} ({getattr(me, 'username', '?')})")
        except Exception as e:
            await safe_send(event.chat_id, f"❌ Не удалось зарезолвить владельца: {str(e)[:200]}")
            return
        
        # Шаг 2: проверяем SQLite
        fp = _get_active_file_progress()
        if fp:
            await safe_send(event.chat_id,
                f"📂 SQLite: {fp['filename']}, words_sent={fp['words_sent']}")
        else:
            await safe_send(event.chat_id, "📂 SQLite: пусто")
        
        # Шаг 3: ищем чекпойнт в Telegram
        await safe_send(event.chat_id, "🔍 Сканирую последние 500 сообщений в ЛС...")
        cp = await _find_latest_checkpoint(OWNER_ID)
        if cp:
            await safe_send(event.chat_id,
                f"✅ Чекпойнт найден: {cp.get('filename','?')}, "
                f"words_sent={cp.get('words_sent','?')}, "
                f"active={cp.get('active','?')}")
            # Запускаем стриминг в фоне (чтобы не блокировать хендлер часами)
            await safe_send(event.chat_id, "🔄 Запускаю возобновление отправки в фоне...")
            asyncio.create_task(_resume_file_streaming())
        else:
            await safe_send(event.chat_id, "❌ Чекпойнт НЕ НАЙДЕН в последних 500 сообщениях")
            # Если нет чекпойнта — нечего возобновлять
            return
        
    except Exception as e:
        log.exception("cmd_resume_file: %s", e)
        await safe_send(event.chat_id, f"❌ Ошибка: {str(e)[:300]}")

async def cmd_setfp(event):
    """Ручное восстановление прогресса файла: /setfp chat_id msg_id filename file_size words_sent"""
    try:
        if not is_owner(event): return
        parts = event.message.raw_text.strip().split(maxsplit=5)
        if len(parts) < 6:
            await safe_send(event.chat_id,
                "❌ Формат: /setfp chat_id msg_id filename file_size words_sent\n"
                "Пример: /setfp 8587090554 3387 channels_dump.txt 595323474 36000")
            return
        _, fp_chat, fp_msg, fp_fn, fp_size, fp_words = parts
        if not (fp_chat.lstrip("-").isdigit() and fp_msg.isdigit()
                and fp_size.isdigit() and fp_words.isdigit()):
            await safe_send(event.chat_id, "❌ chat_id, msg_id, file_size, words_sent должны быть числами")
            return
        _save_file_progress(
            int(fp_chat), int(fp_msg), fp_fn,
            0, int(fp_words), 0, 0, int(fp_size),
            checkpoint_msg_id=None
        )
        await safe_send(event.chat_id,
            f"✅ Прогресс сохранён: {fp_fn}, words_sent={fp_words}\n"
            f"Запускаю возобновление...")
        asyncio.create_task(_resume_file_streaming())
    except Exception as e:
        log.exception("cmd_setfp: %s", e)
        await safe_send(event.chat_id, f"❌ Ошибка: {str(e)[:200]}")

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
    client.add_event_handler(cmd_resume_file, events.NewMessage(pattern=r"^/resume_file$"))
    client.add_event_handler(cmd_setfp, events.NewMessage(pattern=r"^/setfp\s+(.+)"))
    client.add_event_handler(cmd_debug, events.NewMessage(pattern=r"^/debug$"))
    client.add_event_handler(cmd_data_dir, events.NewMessage(pattern=r"^/data_dir$"))
    client.add_event_handler(cmd_reseed, events.NewMessage(pattern=r"^/reseed$"))
    client.add_event_handler(cmd_remove, events.NewMessage(pattern=r"^/remove\s+(.+)"))
    client.add_event_handler(cmd_persist, events.NewMessage(pattern=r"^/persist\s+(.+)"))
    client.add_event_handler(cmd_unpersist, events.NewMessage(pattern=r"^/unpersist\s+(.+)"))

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
    client.add_event_handler(cmd_export_session, events.NewMessage(pattern=r"^/export_session$"))
    client.add_event_handler(cmd_restore_session, events.NewMessage(pattern=r"^/restore_session$"))

    eb_count = len(client._event_builders) if hasattr(client, '_event_builders') else 0
    log.info("✅ Зарегистрировано хендлеров: %d", eb_count)

# ─── Универсальная защита от FloodWait ──────────────────────────────
# Глобальные счётчики вызовов API для каждого клиента
_api_call_times = {}  # id(client) -> [timestamp, ...]
_api_lock = asyncio.Lock()
MAX_API_CALLS_PER_SEC = 3  # макс 3 API-вызова в секунду на клиент

async def safe_api_call(client_obj, method_name, *args, min_delay=0.35, **kwargs):
    """
    Безопасный вызов любого метода Telethon с:
    - rate limiting (мин 350мс между вызовами на клиент)
    - автоматическим ожиданием FloodWait + exponential backoff
    - повторными попытками
    """
    client_id = id(client_obj)
    max_retries = 5
    
    for attempt in range(max_retries + 1):
        # Rate limit: макс MAX_API_CALLS_PER_SEC вызовов в секунду на клиент
        # + мин пауза min_delay между вызовами
        async with _api_lock:
            now = time.monotonic()
            times = _api_call_times.get(client_id, [])
            # Оставляем только вызовы за последнюю секунду
            times = [t for t in times if now - t < 1.0]
            if len(times) >= MAX_API_CALLS_PER_SEC:
                # Достигнут лимит — ждём, пока освободится слот
                wait = times[0] + 1.0 - now
                if wait > 0:
                    await asyncio.sleep(wait)
                    now = time.monotonic()
                times = [t for t in times if now - t < 1.0]
            # Минимальная пауза между вызовами
            if times:
                time_since_last = now - times[-1]
                if time_since_last < min_delay:
                    await asyncio.sleep(min_delay - time_since_last)
                    now = time.monotonic()
            times.append(now)
            _api_call_times[client_id] = times
        
        try:
            method = getattr(client_obj, method_name)
            result = await method(*args, **kwargs)
            return result
        except errors.FloodWaitError as e:
            wait_time = min(e.seconds, 600)  # макс 10 минут ожидания
            # Exponential backoff: каждый повтор ждём дольше
            wait_with_backoff = wait_time * (2 ** attempt)
            wait_with_backoff = min(wait_with_backoff, 3600)  # макс 1 час
            log.warning("⏳ FloodWait %ds на %s (попытка %d/%d, жду %ds)",
                       e.seconds, method_name, attempt + 1, max_retries + 1, wait_with_backoff)
            await asyncio.sleep(wait_with_backoff)
            if attempt >= max_retries:
                log.error("❌ Слишком много FloodWait на %s, пропускаю", method_name)
                raise
        except errors.ServerError as e:
            log.warning("⚠️ ServerError на %s: %s (жду 5с, попытка %d/%d)",
                       method_name, e, attempt + 1, max_retries + 1)
            await asyncio.sleep(5)
            if attempt >= max_retries:
                raise
        except errors.RPCError as e:
            if "FLOOD" in str(e).upper():
                log.warning("⏳ Flood (RPC) на %s: %s (жду 30с)", method_name, e)
                await asyncio.sleep(30)
                continue
            raise
    
    raise RuntimeError(f"safe_api_call: все попытки исчерпаны для {method_name}")

async def safe_get_entity(client_obj, identifier):
    """get_entity с защитой от FloodWait"""
    return await safe_api_call(client_obj, "get_entity", identifier, min_delay=0.35)

async def safe_send_message(client_obj, *args, **kwargs):
    """send_message с защитой от FloodWait"""
    return await safe_api_call(client_obj, "send_message", *args, min_delay=0.5, **kwargs)

async def safe_get_messages(client_obj, *args, **kwargs):
    """get_messages с защитой от FloodWait"""
    return await safe_api_call(client_obj, "get_messages", *args, min_delay=0.3, **kwargs)

# Rate limiter для итерации сообщений (используется в scrape_channel)
_msg_iter_lock = asyncio.Lock()
_msg_iter_last_call = 0.0

async def safe_iter_messages(client_obj, entity, **kwargs):
    """iter_messages с защитой от FloodWait и паузами между вызовами"""
    global _msg_iter_last_call
    async with _msg_iter_lock:
        now = time.monotonic()
        since_last = now - _msg_iter_last_call
        if since_last < 0.5:
            await asyncio.sleep(0.5 - since_last)
        result = client_obj.iter_messages(entity, **kwargs)
        _msg_iter_last_call = time.monotonic()
        return result

# Старый rate limiter для get_entity (оставляем для обратной совместимости)
_entity_last_call = 0.0
_entity_lock = asyncio.Lock()

async def _rate_limited_get_entity(reader, username):
    """Вызывает reader.get_entity(username) с паузой min 300мс между вызовами"""
    return await safe_get_entity(reader, username)

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
        entity = await _rate_limited_get_entity(reader, username)
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
                    log.info("📤 scrape_channel → send_to_output: %s msg#%s из %s",
                             entity.id, msg.id, title)
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
    """Постоянно сканирует каналы с ограничением конкурентности"""
    active_tasks = {}  # username -> task
    MAX_CONCURRENT = 3  # макс 3 канала одновременно — предотвращает FloodWait
    log.info("🚀 Queue worker запущен — макс %d канала одновременно", MAX_CONCURRENT)

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
            # Запускаем pending-каналы с учётом лимита конкурентности
            rows = conn.execute(
                "SELECT username,last_msg_id FROM scrape_queue WHERE status='pending' ORDER BY added_at"
            ).fetchall()
            for row in rows:
                slots = MAX_CONCURRENT - len(active_tasks)
                if slots <= 0: break
                if row[0] not in active_tasks:
                    task = asyncio.create_task(run_scrape(row[0], row[1]))
                    active_tasks[row[0]] = task
                    log.info("🚀 Запущен @%s (всего активно: %d)", row[0], len(active_tasks))

            # Если ничего не запущено — перепроверяем done-каналы (макс MAX_CONCURRENT)
            if not active_tasks:
                done_rows = conn.execute(
                    "SELECT username,last_msg_id FROM scrape_queue WHERE status='done' ORDER BY RANDOM() LIMIT ?",
                    (MAX_CONCURRENT,)
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
_telethon_ss_imported = False  # ленивый импорт StringSession

def get_user_client():
    global user_client, _telethon_ss_imported
    if user_client is None:
        from telethon.sessions import StringSession
        _telethon_ss_imported = True
        session_string = os.getenv("USER_SESSION_STRING") or ""
        if session_string.strip():
            log.info("🔑 Использую USER_SESSION_STRING из env")
            session = StringSession(session_string.strip())
        else:
            # Пробуем из БД (сохраняется через бэкап на Telegram)
            db_ss = conn.execute("SELECT value FROM config WHERE key='user_session_string'").fetchone()
            if db_ss and db_ss[0].strip():
                log.info("🔑 Использую session string из БД (восстановлен из бэкапа)")
                session = StringSession(db_ss[0].strip())
            else:
                session = USER_SESSION  # файл (не survives redeploy на Free)
        user_client = TelegramClient(session, API_ID, API_HASH)
    return user_client

async def init_user_client():
    """Проверяем сохранённую сессию при старте (env → БД → файл)"""
    uc = get_user_client()
    session_file = Path(str(USER_SESSION) + ".session")
    exists = session_file.exists()
    has_env_ss = bool(os.getenv("USER_SESSION_STRING", "").strip())
    has_db_ss = bool(conn.execute("SELECT value FROM config WHERE key='user_session_string'").fetchone())
    
    log.info("👤 User client: файл %s, env %s, БД %s",
             "✅" if exists else "❌",
             "✅" if has_env_ss else "❌",
             "✅" if has_db_ss else "❌")
    
    # Если есть хоть какой-то источник сессии — пробуем подключиться
    if not (exists or has_env_ss or has_db_ss):
        log.warning("👤 Нет сессии — нужна /auth")
        try:
            await notify_owners(
                "⚠️ <b>User client сессия не найдена</b>\n"
                "Отправь:\n"
                "/auth\n"
                "/phone +79122502717\n"
                "Далее код по цифрам: 1 2 3 4 5")
        except:
            pass
        return False
    
    try:
        await uc.connect()
        if await uc.is_user_authorized():
            log.info("👤 User client авторизован (сессия сохранена)")
            # Авто-экспорт session string в БД для надёжности
            try:
                from telethon.sessions import StringSession
                ss = StringSession.save(uc.session)
                conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)",
                             ("user_session_string", ss))
                conn.commit()
            except Exception:
                pass
            asyncio.create_task(user_searcher())
            # Уведомляем что всё ок
            try:
                await notify_owners(
                    "✅ <b>User client авторизован</b> (сессия восстановлена)")
            except:
                pass
            return True
        else:
            log.info("👤 User client: сессия недействительна, нужна /auth")
            try:
                await notify_owners(
                    "⚠️ <b>User client сессия недействительна</b>\n"
                    "Нужна переавторизация: /auth")
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

async def _auto_export_session(notify_chat_id=None):
    """Автоматически сохраняет session string в бэкап и уведомляет владельца"""
    try:
        uc = get_user_client()
        if await uc.is_user_authorized() and uc.is_connected():
            from telethon.sessions import StringSession
            ss = StringSession.save(uc.session)
            # Сохраняем в БД (переживёт только до редеплоя, но бэкап в Telegram сохранит)
            conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)",
                         ("user_session_string", ss))
            conn.commit()
            # Сохраняем в бэкап Telegram (в канал вывода)
            asyncio.create_task(backup_settings_to_telegram())
            log.info("🔑 Session string сохранена в бэкап")
            if notify_chat_id:
                from telethon.tl.functions.messages import SetBotPrecheckoutResultsRequest
                pass
    except Exception as e:
        log.warning("auto_export_session: %s", e)

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

_phone_last_call = 0.0  # защита от спама /phone

async def cmd_phone(event):
    if not is_owner(event): return
    phone = event.pattern_match.group(1).strip()
    
    # Защита от повторного /phone (не чаще раза в 90 секунд)
    global _phone_last_call
    now = time.time()
    if now - _phone_last_call < 90:
        remaining = int(90 - (now - _phone_last_call))
        await safe_send(event.chat_id, f"⏳ Подожди {remaining}с перед повторным запросом кода")
        return
    _phone_last_call = now
    
    conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", ("user_phone", phone))
    conn.commit()
    uc = get_user_client()
    await uc.connect()
    try:
        result = await safe_api_call(uc, "send_code_request", phone, min_delay=1.0)
        # result — это SentCode, у которого есть атрибут type
        phone_register = getattr(result, 'phone_registered', True)
        code_type = str(type(result.type).__name__) if hasattr(result, 'type') else "?"
        # Пробуем понять, как отправлен код
        delivery = "через Telegram (служебное сообщение)"
        type_name = getattr(result.type, 'class_name', '') if hasattr(result, 'type') else ''
        if 'sms' in type_name.lower() or 'Sms' in type_name:
            delivery = "по SMS"
        
        # Активируем режим сбора цифр по одной
        conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", ("auth_digit_active", "1"))
        conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", ("auth_digits", ""))
        conn.commit()
        
        await safe_send(event.chat_id,
            f"✅ Код отправлен {delivery} на {phone}\n\n"
            "1️⃣ Введи код: /code XXXXX\n"
            "2️⃣ Или по одной цифре (5 сообщений):\n"
            "1\n2\n3\n4\n5\n\n"
            "⚠️ НЕ отправляй /phone повторно — это убьёт код!")
    except errors.FloodWaitError as e:
        await safe_send(event.chat_id, f"⏳ FloodWait {e.seconds}с, жди.")
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
        # Авто-экспорт сессии в бэкап
        await _auto_export_session(event.chat_id)
        # Восстанавливаем остальные настройки из бэкапа
        restored = await restore_settings_from_backup()
        if restored:
            await safe_send(event.chat_id, "📦 Настройки восстановлены из бэкапа!")
        await safe_send(event.chat_id, "✅ User client авторизован! Поиск каналов работает.\n"
            "Поставь ключевые слова: /searchwords слово1, слово2\n"
            "Включи автопоиск: /autosearch on\n\n"
            "💡 <b>Сессия сохранена в бэкап.</b> После редеплоя восстановится автоматически.\n"
            "Если хочешь сохранить в Render env (надёжнее):\n"
            "1. /export_session\n"
            "2. Скопируй строку\n"
            "3. Добавь USER_SESSION_STRING в Render Dashboard → Environment → Redeploy")
        asyncio.create_task(user_searcher())
        _requeue_bot_errors()
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
        await _auto_export_session(event.chat_id)
        restored = await restore_settings_from_backup()
        if restored:
            await safe_send(event.chat_id, "📦 Настройки восстановлены из бэкапа!")
        await safe_send(event.chat_id, "✅ User client авторизован (2FA)! Поиск работает.\n"
            "Ключевые слова: /searchwords слово1, слово2\n"
            "Автопоиск: /autosearch on\n\n"
            "💡 <b>Сессия сохранена в бэкап.</b> После редеплоя восстановится автоматически.\n"
            "Если хочешь сохранить в Render env (надёжнее):\n"
            "1. /export_session  2. Добавь в Render Dashboard → Environment")
        asyncio.create_task(user_searcher())
        _requeue_bot_errors()
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
            await _auto_export_session(event.chat_id)
            restored = await restore_settings_from_backup()
            if restored:
                await safe_send(event.chat_id, "📦 Настройки восстановлены из бэкапа!")
            await safe_send(event.chat_id, "✅ User client авторизован! Поиск каналов работает.\n"
                "Ключевые слова: /searchwords слово1, слово2\n"
                "Автопоиск: /autosearch on\n\n"
                "💡 <b>Сессия сохранена в бэкап.</b> После редеплоя восстановится автоматически.\n"
                "Если хочешь сохранить в Render env (надёжнее):\n"
                "1. /export_session  2. Добавь в Render Dashboard → Environment")
            asyncio.create_task(user_searcher())
            _requeue_bot_errors()
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

def _requeue_bot_errors():
    """После авторизации user-клиента перезапускаем каналы, 
       которые не смогли прочитаться из-за отсутствия user-клиента"""
    count = conn.execute(
        "UPDATE scrape_queue SET status='pending',error=NULL WHERE status='error' AND error LIKE '%Bot cannot read%'"
    ).rowcount
    if count:
        conn.commit()
        log.info("🔄 Перезапущено %d каналов (были ошибки Bot cannot read)", count)

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

async def cmd_export_session(event):
    """Экспортирует session string для сохранения в Render env"""
    if not is_owner(event): return
    uc = get_user_client()
    try:
        if await uc.is_user_authorized() and uc.is_connected():
            from telethon.sessions import StringSession
            s = StringSession.save(uc.session)
            await safe_send(event.chat_id,
                f"🔑 <b>Session string для Render env:</b>\n\n"
                f"<code>{s}</code>\n\n"
                f"1. Скопируй строку выше\n"
                f"2. Render Dashboard → hauding-bot → Environment → добавить:\n"
                f"   <b>USER_SESSION_STRING</b> = вставленная строка\n"
                f"3. Redeploy\n\n"
                f"После этого сессия будет сохраняться между деплоями.")
        else:
            await safe_send(event.chat_id, "❌ User client не авторизован.\nСначала сделай /auth → /phone → /code")
    except Exception as e:
        await safe_send(event.chat_id, f"❌ Ошибка: {str(e)[:200]}")

async def cmd_restore_session(event):
    """Восстанавливает сессию и настройки из бэкапа в канале вывода"""
    if not is_owner(event): return
    await safe_send(event.chat_id, "🔍 Ищу бэкап для восстановления...")
    try:
        restored = await restore_settings_from_backup()
        if restored:
            # Пробуем переподключить user client с новой session_string
            ss_row = conn.execute("SELECT value FROM config WHERE key='user_session_string'").fetchone()
            if ss_row and ss_row[0].strip():
                global user_client
                if user_client:
                    try: await user_client.disconnect()
                    except: pass
                user_client = None
                uc = get_user_client()  # создаст с session string из БД
                await uc.connect()
                if await uc.is_user_authorized():
                    await safe_send(event.chat_id, "✅ User client подключён! Сессия восстановлена из бэкапа.")
                    asyncio.create_task(user_searcher())
                    return
            await safe_send(event.chat_id, "📦 Настройки восстановлены, но нужна /auth для user client")
        else:
            await safe_send(event.chat_id, "❌ Бэкап не найден. Сделай /auth с нуля, потом /export_session")
    except Exception as e:
        await safe_send(event.chat_id, f"❌ Ошибка: {str(e)[:200]}")

# ─── Главный запуск ──────────────────────────────────────────────────
async def main():
    # Здоровье ДО старта клиента (живёт весь процесс, независимо от цикла)
    if IS_RENDER:
        asyncio.create_task(health_server())

    CYCLE_MINUTES = 5  # полная перезагрузка соединения каждые 5 минут

    while True:
        # ── Подключение к Telegram ─────────────────────────────────
        for attempt in range(10):
            try:
                # Если клиент уже был подключён (повторный цикл), отключаемся чисто
                if client.is_connected():
                    await client.disconnect()
                await asyncio.sleep(1)
                await client.start(bot_token=BOT_TOKEN)
                break
            except errors.FloodWaitError as e:
                wait = min(e.seconds, 300)
                log.warning("⏳ FloodWait при входе: %dс (попытка %d/10, жду %dс)", e.seconds, attempt + 1, wait)
                await asyncio.sleep(wait)
        else:
            log.error("❌ Не удалось войти после 10 попыток, жду 60с и пробую снова...")
            await asyncio.sleep(60)
            continue

        # ── Регистрируем хендлеры (сначала чистим старые) ──────────
        if hasattr(client, '_event_builders'):
            client._event_builders.clear()
        register_handlers()

        me = await client.get_me()
        log.info("=" * 50)
        log.info("🤖 @%s запущен (цикл %d мин)", me.username or "?", CYCLE_MINUTES)
        log.info("📁 База: %s", DB_PATH)
        if IS_RENDER:
            log.info("🌐 Render: %s | Health check на порту %d", RENDER_URL or "?", PORT)
        else:
            log.info("🖥 Локальный режим")
        log.info("=" * 50)

        # ── Фоновые задачи ─────────────────────────────────────────
        bg_tasks = [
            asyncio.create_task(heartbeat()),
            asyncio.create_task(self_pinger()),
            asyncio.create_task(watchdog()),
        ]

        # 1. User-клиент
        user_ok = await init_user_client()
        if user_ok:
            log.info("👤 User-клиент готов")
        else:
            log.warning("👤 User-клиент НЕ готов — каналы не будут читаться до /auth")

        # 2. Восстановление настроек из бэкапа
        texts_count = conn.execute("SELECT COUNT(*) FROM texts").fetchone()[0]
        if texts_count == 0 and not get_output_chat():
            log.info("📦 База пуста, пробую восстановить из бэкапа Telegram...")
            restored = await restore_settings_from_backup()
            if restored:
                log.info("📦 Настройки восстановлены из бэкапа!")
            else:
                log.info("📦 Бэкап не найден, всё придётся настроить заново")

        # 3. Telegram-based dedup
        await _load_sent_hashes()

        # 4. Seed-каналы
        await add_seed_channels()
        reset = conn.execute(
            "UPDATE scrape_queue SET status='pending', error=NULL WHERE status='error'"
        ).rowcount
        if reset:
            conn.commit()
            log.info("🔄 Сброшено %d error-каналов в pending", reset)
        await add_persistent_to_queue()

        # 5. Загружаем состояние паузы из БД. Если не было сохранено — пауза (default).
        if not _load_pause_state():
            _scanning = False  # fresh deploy — пауза по умолчанию
        log.info("📌 Состояние: %s", "⏸ пауза" if not _scanning else "▶️ активен")
        asyncio.create_task(_resume_file_streaming())

        # 6. Queue worker
        qw_task = asyncio.create_task(queue_worker())

        # 7. Уведомление владельцам
        restored_settings = []
        out_ch = get_output_chat()
        if out_ch: restored_settings.append(f"📤 Канал вывода: {out_ch}")
        kw = conn.execute("SELECT value FROM config WHERE key='user_search_keywords'").fetchone()
        if kw: restored_settings.append(f"🔍 Ключевые слова: {kw[0][:60]}...")
        auto = conn.execute("SELECT value FROM config WHERE key='user_autosearch'").fetchone()
        if auto: restored_settings.append(f"🔄 Автопоиск: {auto[0]}")
        pers = get_persistent_channels()
        if pers: restored_settings.append(f"📌 Persistent: {len(pers)} каналов")
        texts_count = conn.execute("SELECT COUNT(*) FROM texts").fetchone()[0]
        restored_settings.append(f"📚 Текстов в базе: {texts_count}")
        restored_settings.append(f"🔐 Telegram dedup: {len(_SENT_HASHES)} хэшей")
        startup_msg = "🔄 <b>Бот перезапущен</b>\n" + "\n".join(f"• {s}" for s in restored_settings)
        try:
            await notify_owners(startup_msg)
        except:
            pass

        # ── Таймер плановой перезагрузки (disconnect через N минут) ──
        async def _auto_disconnect():
            await asyncio.sleep(CYCLE_MINUTES * 60)
            log.info("🔄 Плановая перезагрузка соединения (каждые %d мин)...", CYCLE_MINUTES)
            # Сохраняем текущее состояние
            _save_pause_state(not _scanning)
            try:
                await client.disconnect()
            except Exception:
                pass

        asyncio.create_task(_auto_disconnect())

        # ── Ждём отключения (по таймеру или ошибке) ────────────────
        try:
            await client.run_until_disconnected()
        except Exception as e:
            log.warning("⚠️ run_until_disconnected завершился: %s", str(e)[:100])

        # Даём время фоновым задачам (стриминг файла) сохранить прогресс
        await asyncio.sleep(3)

        # ── Очистка перед следующим циклом ─────────────────────────
        log.info("🔄 Завершаю задачи цикла...")
        qw_task.cancel()
        for t in bg_tasks:
            t.cancel()
        # Сбрасываем сессионные кеши (они будут перестроены при реконнекте)
        # НЕ трогаем file_progress — активный прогресс файла должен выжить между циклами!
        _SENT_HASHES.clear()
        _sess_dup.clear()

        log.info("🔄 Ожидание 5с перед переподключением...")
        await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Остановлен.")
    finally:
        conn.close()
