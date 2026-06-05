import os
import re
import os
import aiofiles
import asyncio
import logging
import requests
import time
import json
import html
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, BufferedInputFile
from aiohttp import web
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import subprocess
import asyncio
import shlex

# ================= SHELL COMMANDS (БЕЗОПАСНОСТЬ) =================
SHELL_ALLOWED_COMMANDS = [
    # Безопасные команды для мониторинга
    "ls", "pwd", "whoami", "date", "uptime", "free", "df", "ps", "top", "cat", "head", "tail",
    # Git и деплой
    "git", "pip", "python", "python3",
    # Сеть (осторожно)
    "curl", "wget", "ping",
    # Файлы (только чтение)
    "grep", "find", "wc", "du"
]

SHELL_BLACKLISTED = [
    "rm -rf", "mkfs", "dd", "chmod 777", "chown", "sudo", "su",
    ":(){:|:&};:", "fork", "eval", "exec", "source", ".", 
    "wget http", "curl http",  # Запрещаем скачивание извне
    ">", ">>", "|", "&", ";", "`", "$(",  # Запрещаем перенаправления и подстановки
]

def is_command_safe(cmd: str) -> tuple[bool, str]:
    """Проверяет команду на безопасность"""
    cmd_lower = cmd.lower().strip()
    
    # Проверка чёрного списка
    for blocked in SHELL_BLACKLISTED:
        if blocked in cmd_lower:
            return False, f"❌ Команда содержит запрещённый элемент: `{blocked}`"
    
    # Проверка белого списка (первое слово команды)
    first_word = cmd_lower.split()[0] if cmd_lower.split() else ""
    if SHELL_ALLOWED_COMMANDS and first_word not in SHELL_ALLOWED_COMMANDS:
        return False, f"❌ Команда `{first_word}` не в списке разрешённых.\nРазрешено: {', '.join(SHELL_ALLOWED_COMMANDS[:10])}..."
    
    return True, ""

async def run_shell_command(cmd: str, timeout: int = 30) -> tuple[str, str, int]:
    """
    Запускает команду и возвращает (stdout, stderr, return_code)
    """
    try:
        # Безопасный запуск: shell=False + shlex.split
        process = await asyncio.create_subprocess_exec(
            *shlex.split(cmd),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd="/opt/render/project/src",  # Ограничиваем рабочую директорию
            env={**os.environ, "PATH": "/usr/local/bin:/usr/bin:/bin"}  # Безопасный PATH
        )
        
        stdout, stderr = await asyncio.wait_for(
            process.communicate(),
            timeout=timeout
        )
        
        return (
            stdout.decode("utf-8", errors="replace")[:4000],  # Ограничиваем вывод
            stderr.decode("utf-8", errors="replace")[:4000],
            process.returncode
        )
    except asyncio.TimeoutError:
        return "", f"⏱️ Таймаут: команда выполнялась дольше {timeout} сек", -1
    except FileNotFoundError:
        return "", f"❌ Команда не найдена: `{cmd.split()[0]}`", 127
    except Exception as e:
        return "", f"❌ Ошибка: {str(e)}", -1


# ================= YANDEX MUSIC INTEGRATION =================
try:
    from yandex_music import Client
    YANDEX_MUSIC_AVAILABLE = True
except ImportError:
    YANDEX_MUSIC_AVAILABLE = False
    print("⚠️ WARNING: yandex-music не установлен — функция отслеживания музыки недоступна")
    print("   Чтобы исправить: добавь 'yandex-music>=2.3.0' в requirements.txt")

# Пытаемся импортировать pymongo
try:
    from pymongo import MongoClient
    MONGO_AVAILABLE = True
except ImportError:
    MONGO_AVAILABLE = False

# ================= НАСТРОЙКИ =================
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = os.getenv("OWNER_ID")
STOP_PASSWORD = os.getenv("STOP_PASSWORD", "stop123")
MIN_DAYS_ENV = os.getenv("MIN_DAYS")
LOG_BOT_TOKEN = os.getenv("LOG_BOT_TOKEN")
LOG_CHAT_ID = os.getenv("LOG_CHAT_ID")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
MONGO_URI = os.getenv("MONGO_URI")
SITE_BASE = "https://lynther.sytes.net"

BASE_URL = SITE_BASE + "/?p=lora"
DEFAULT_MIN_DAYS = int(MIN_DAYS_ENV) if MIN_DAYS_ENV and MIN_DAYS_ENV.isdigit() else 0
DEFAULT_TAGS = []
MAX_PAGES = 50
EXPORT_THRESHOLD = 50
COOLDOWN_SECONDS = 20
FORWARDED_FILE = "forwarded.json"
USERS_FILE = "users.json"
# Yandex Music настройки
YANDEX_MUSIC_TOKEN = os.getenv("YANDEX_MUSIC_TOKEN")
MUSIC_STATUS_CHAT_ID = os.getenv("MUSIC_STATUS_CHAT_ID")
MUSIC_CHECK_INTERVAL = int(os.getenv("MUSIC_CHECK_INTERVAL", "20")) if os.getenv("MUSIC_CHECK_INTERVAL") else 20
music_tracking_enabled = False
music_task = None
current_music_message_id = None
last_track_id = None
music_message_timestamp = None

# === HQRadio парсинг ===
HQ_RADIO_URL = "https://hqradio.ru/stations/phonk_radiorecord-ru"
parse_hq_enabled = False  # Флаг включения парсинга
last_parsed_track = None  # Кэш последнего трека
last_parse_time = 0  # Время последнего парсинга
PARSE_COOLDOWN = 30  # Мин. секунд между парсингами


# ================= ПРЕМИУМ ЭМОДЗИ =================
def premium_emoji(emoji_id: str, fallback: str = "⭐") -> str:
    """
    Возвращает HTML-код премиум-эмодзи
    Пример: premium_emoji("5325819430553263482", "🤩")
    """
    return f'<tg-emoji emoji-id="{emoji_id}">{fallback}</tg-emoji>'

# === Пресеты популярных премиум-эмодзи ===
PREMIUM_EMOJI = {
    "sparkle": premium_emoji("5325819430553263482", "🤩"),  # ✨ Сверкающая звёздочка
    "fire": premium_emoji("5364703953407018920", "🔥"),     # 🔥 Огонь
    "heart": premium_emoji("5364703953407018919", "❤️"),    # ❤️ Сердце
    "star": premium_emoji("5364703953407018921", "⭐"),      # ⭐ Звезда
    "cool": premium_emoji("5364703953407018922", "😎"),      # 😎 Крутой
    "party": premium_emoji("5364703953407018923", "🎉"),     # 🎉 Праздник
}

# === ЭМОДЗИ ===
EMOJI = {
    "brain": "🧠", "id": "🆔", "days": "🕸️", "delete": "🗑️", "search": "🔍",
    "stats": "📊", "settings": "⚙️", "tag": "🏷️", "clock": "⏰", "check": "✅",
    "warning": "⚠️", "error": "❌", "info": "ℹ️", "file": "📄", "stop": "🛑",
    "restart": "🔄", "lock": "🔒", "users": "👥", "log": "📜", "db": "🗄️"
}

def safe_html_text(text: str) -> str:
    """
    Экранирует спецсимволы для безопасной отправки с parse_mode="HTML"
    Сохраняет эмодзи и обычные символы, но защищает от ошибок парсинга
    """
    return html.escape(text)

# === ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ===
bot_running = True
user_settings = {}
awaiting_conversion = set()
forwarded_messages = {}
known_users = {}
log_handler = None
mongo_client = None
db = None
last_search_results = None  # ← Кэш последних найденных лор (<50)
last_search_meta = None     # ← Метаданные последнего поиска
# =============================================

# Настройка логирования
log_level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)

class MoscowFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        ct = datetime.fromtimestamp(record.created, tz=timezone(timedelta(hours=3)))
        return ct.strftime(datefmt or "%Y-%m-%d %H:%M:%S")

logging.basicConfig(level=log_level, format="%(asctime)s МСК | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)

if not BOT_TOKEN or not OWNER_ID:
    raise ValueError("❌ Переменные BOT_TOKEN и OWNER_ID не заданы!")

OWNER_ID_INT = int(OWNER_ID)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ================= TELEGRAM LOG HANDLER =================
class TelegramLogHandler(logging.Handler):
    def __init__(self, bot_token, chat_id, min_level=logging.INFO):
        super().__init__(level=min_level)
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.last_send = 0
        self.cooldown = 3
        self.min_level = min_level
    
    def set_level(self, level):
        self.setLevel(level)
        self.min_level = level
        logger.info(f"📊 Уровень логов изменён на: {logging.getLevelName(level)}")
    
    def emit(self, record):
        try:
            now = time.time()
            if now - self.last_send < self.cooldown:
                return
            moscow_time = datetime.now(timezone(timedelta(hours=3))).strftime('%H:%M:%S')
            level_emoji = {"DEBUG": "🔍", "INFO": "ℹ️", "WARNING": "⚠️", "ERROR": "❌", "CRITICAL": "🚨"}.get(record.levelname, "📋")
            msg = f"{level_emoji} <b>{record.levelname}:</b>\n\n"
            msg += f"🕐 МСК {moscow_time}\n"
            msg += f"📋 <code>{record.getMessage()}</code>"
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            data = {"chat_id": self.chat_id, "text": msg, "parse_mode": "HTML"}
            requests.post(url, json=data, timeout=10)
            self.last_send = now
        except Exception as e:
            print(f"Failed to send log to Telegram: {e}")

# ================= MONGODB ИНИЦИАЛИЗАЦИЯ =================
def init_mongo():
    """Инициализирует MongoDB подключение с Stable API"""
    global mongo_client, db
    
    if not MONGO_AVAILABLE:
        logger.warning("⚠️ pymongo не установлен — работаю в режиме без БД")
        return False
    
    if not MONGO_URI:
        logger.warning("⚠️ MONGO_URI не задан — работаю в режиме без БД")
        return False
    
    try:
        # Импортируем ServerApi для Stable API
        from pymongo.server_api import ServerApi
        
        # Создаём клиент с обязательным указанием версии API
        mongo_client = MongoClient(
            MONGO_URI,
            server_api=ServerApi('1'),  # ← Ключевое исправление!
            serverSelectionTimeoutMS=10000,
            connectTimeoutMS=10000,
            socketTimeoutMS=10000
        )
        
        # Тест подключения с пингом
        mongo_client.admin.command('ping')
        
        # Получаем базу данных
        db = mongo_client.get_database("loonie_bot")
        
        # Создаём индексы (идемпотентно)
        db.forwarded.create_index("message_id", unique=True)
        db.users.create_index("user_id", unique=True)
        
        logger.info("✅ MongoDB подключена (Stable API v1)")
        return True
        
    except Exception as e:
        error_msg = str(e)
        # Логируем только первые 300 символов ошибки
        logger.error("❌ Ошибка подключения к MongoDB: " + error_msg[:300])
        mongo_client = None
        db = None
        return False

# ================= ИНИЦИАЛИЗАЦИЯ ЛОГ-БОТА =================
def init_log_bot():
    global log_handler
    if LOG_BOT_TOKEN and LOG_CHAT_ID:
        try:
            log_handler = TelegramLogHandler(LOG_BOT_TOKEN, LOG_CHAT_ID, min_level=log_level)
            log_handler.setFormatter(MoscowFormatter("%(message)s"))
            logger.addHandler(log_handler)
            logger.info("✅ Лог-бот подключён (уровень: " + LOG_LEVEL + ")")
            moscow_time = datetime.now(timezone(timedelta(hours=3))).strftime('%Y-%m-%d %H:%M:%S')
            url = f"https://api.telegram.org/bot{LOG_BOT_TOKEN}/sendMessage"
            data = {
                "chat_id": LOG_CHAT_ID,
                "text": "🟢 <b>Бот запущен!</b>\n\n" +
                        f"🕐 МСК {moscow_time}\n" +
                        f"🌐 Render: {os.getenv('RENDER_EXTERNAL_URL', 'N/A')}\n" +
                        f"📊 Лог-уровень: {LOG_LEVEL}",
                "parse_mode": "HTML"
            }
            requests.post(url, json=data, timeout=10)
        except Exception as e:
            logger.warning("⚠️ Лог-бот не подключён: " + str(e))
    else:
        logger.warning("⚠️ LOG_BOT_TOKEN или LOG_CHAT_ID не заданы")

# ================= ХРАНИЛИЩЕ (исправлено: db is not None) =================
def load_forwarded():
    global forwarded_messages
    if db is not None:
        try:
            cutoff = datetime.now(timezone(timedelta(hours=3))) - timedelta(hours=168)
            result = db.forwarded.delete_many({"timestamp": {"$lt": cutoff.isoformat()}})
            if result.deleted_count > 0:
                logger.info(f"🧹 Удалено {result.deleted_count} старых записей forwarded")
            
            # Загружаем оставшиеся
            for doc in db.forwarded.find():
                forwarded_messages[doc["message_id"]] = doc["user_id"]
            logger.info(f"📦 Загружено {len(forwarded_messages)} пересланных сообщений из MongoDB")
            return
        except Exception as e:
            logger.warning("⚠️ Ошибка загрузки из MongoDB: " + str(e))
    try:
        if os.path.exists(FORWARDED_FILE):
            with open(FORWARDED_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            forwarded_messages = {int(k): v for k, v in data.items()}
            logger.info(f"📦 Загружено {len(forwarded_messages)} пересланных сообщений из файла")
            # Отладка: покажи первые 3 записи
            for i, (k, v) in enumerate(list(forwarded_messages.items())[:3]):
                logger.debug(f"  - {k} → {v}")
    except Exception as e:
        logger.error("❌ Ошибка загрузки forwarded.json: " + str(e))
        forwarded_messages = {}

def save_forwarded():
    if db is not None:  # ← ИСПРАВЛЕНО
        try:
            db.forwarded.delete_many({})
            for msg_id, user_id in forwarded_messages.items():
                db.forwarded.insert_one({"message_id": msg_id, "user_id": user_id})
            return
        except Exception as e:
            logger.warning("⚠️ Ошибка сохранения в MongoDB: " + str(e))
    try:
        data = {str(k): v for k, v in forwarded_messages.items()}
        with open(FORWARDED_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.error("❌ Ошибка сохранения forwarded.json: " + str(e))

def load_users():
    global known_users
    if db is not None:  # ← ИСПРАВЛЕНО
        try:
            for doc in db.users.find():
                known_users[doc["user_id"]] = doc["data"]
            logger.info(f"👥 Загружено {len(known_users)} пользователей из MongoDB")
            return
        except Exception as e:
            logger.warning("⚠️ Ошибка загрузки users из MongoDB: " + str(e))
    try:
        if os.path.exists(USERS_FILE):
            with open(USERS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            known_users = {int(k): v for k, v in data.items()}
    except Exception as e:
        logger.error("❌ Ошибка загрузки users.json: " + str(e))
        known_users = {}

def save_users():
    if db is not None:  # ← ИСПРАВЛЕНО
        try:
            for user_id, data in known_users.items():
                db.users.update_one(
                    {"user_id": user_id},
                    {"$set": {"data": data, "updated_at": datetime.now(timezone(timedelta(hours=3)))}},
                    upsert=True
                )
            return
        except Exception as e:
            logger.warning("⚠️ Ошибка сохранения users в MongoDB: " + str(e))
    try:
        data = {str(k): v for k, v in known_users.items()}
        with open(USERS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error("❌ Ошибка сохранения users.json: " + str(e))


# ================= НАСТРОЙКИ В БД =================
SETTINGS_COLLECTION = "bot_settings"
OWNER_SETTINGS_ID = "owner"  # Фиксированный ID для настроек владельца

def load_settings():
    """Загружает настройки владельца из БД"""
    global user_settings
    if db is not None:
        try:
            doc = db[SETTINGS_COLLECTION].find_one({"_id": OWNER_SETTINGS_ID})
            if doc:
                # Загружаем настройки для владельца
                owner_id = OWNER_ID_INT
                user_settings[owner_id] = {
                    "min_days": doc.get("min_days", DEFAULT_MIN_DAYS),
                    "tags": doc.get("tags", DEFAULT_TAGS.copy()),
                    "schedule": doc.get("schedule", []),
                    "last_check": doc.get("last_check", 0),
                    "is_checking": doc.get("is_checking", False)
                }
                logger.info(f"⚙️ Загружены настройки владельца из MongoDB")
                return
        except Exception as e:
            logger.warning("⚠️ Ошибка загрузки настроек из MongoDB: " + str(e))
    
    # Fallback: настройки по умолчанию (уже в памяти)
    logger.info("⚙️ Используем настройки по умолчанию")

def save_settings(user_id):
    """Сохраняет настройки пользователя в БД"""
    if db is not None and user_id == OWNER_ID_INT:
        try:
            settings = user_settings.get(user_id, {})
            db[SETTINGS_COLLECTION].update_one(
                {"_id": OWNER_SETTINGS_ID},
                {"$set": {
                    **settings,
                    "updated_at": datetime.now(timezone(timedelta(hours=3)))
                }},
                upsert=True
            )
        except Exception as e:
            logger.warning("⚠️ Ошибка сохранения настроек в MongoDB: " + str(e))


# ================= НАСТРОЙКИ ПОЛЬЗОВАТЕЛЯ =================
def get_settings(user_id):
    if user_id not in user_settings:
        user_settings[user_id] = {
            "min_days": DEFAULT_MIN_DAYS, "tags": DEFAULT_TAGS.copy(),
            "schedule": [], "last_check": 0, "is_checking": False
        }
    return user_settings[user_id]

def update_settings(user_id, **kwargs):
    settings = get_settings(user_id)
    settings.update(kwargs)
    user_settings[user_id] = settings
    if db is not None and user_id == OWNER_ID_INT:
        save_settings(user_id)

def check_cooldown(user_id):
    settings = get_settings(user_id)
    elapsed = time.time() - settings.get("last_check", 0)
    if elapsed >= COOLDOWN_SECONDS:
        return True, 0
    return False, int(COOLDOWN_SECONDS - elapsed)

# ================= ЗАПРОСЫ И ПАРСИНГ =================
def fetch_with_retry(url, max_retries=3):
    headers = {"User-Agent": "Mozilla/5.0"}
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=20)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.warning("Попытка " + str(attempt) + " упала: " + str(e))
            if attempt == max_retries:
                return None
            time.sleep(2 ** attempt)

def parse_loras_from_html(html, min_days):
    if html is None:
        return []
    try:
        soup = BeautifulSoup(html, "html.parser")
        results = []
        for head in soup.find_all("p", class_="lora_head"):
            try:
                text = head.get_text()
                id_match = re.search(r"#️⃣\s*(\d+)", text)
                if not id_match: continue
                lora_id = id_match.group(1)
                days_match = re.search(r"🕸️\s*(\d+)\s*d", text, re.IGNORECASE)
                if not days_match: continue
                lora_days = int(days_match.group(1))
                name_match = re.match(r'^\d+\.\s*(.+?)\s*\|\|', text.strip())
                lora_name = name_match.group(1).strip() if name_match else "Unknown"
                lora_url = SITE_BASE + "/?p=lora_d&lora_id=" + lora_id
                if lora_days >= min_days:
                    results.append({"id": lora_id, "days": lora_days, "name": lora_name, "url": lora_url})
            except Exception as e:
                logger.warning("Ошибка парсинга: " + str(e))
                continue
        return results
    except Exception as e:
        logger.error("Ошибка парсинга: " + str(e))
        return []

def find_loras_by_tag(tag, min_days):
    all_results, pages_scanned = [], 0
    for page in range(1, MAX_PAGES + 1):
        if not bot_running: break
        url = BASE_URL + "&t=" + tag + ("&c="+str(page) if page>1 else "")
        logger.info("=== Тег: " + tag + " | Страница: " + str(page) + " ===")
        html = fetch_with_retry(url)
        if not html: break
        loras = parse_loras_from_html(html, min_days)
        pages_scanned += 1
        if loras:
            all_results.extend(loras)
            logger.info("Стр. " + str(page) + ": найдено " + str(len(loras)) + " лор")
        else:
            logger.info("Стр. " + str(page) + ": лор не найдено")
            if page > 3: break
        if page < MAX_PAGES: time.sleep(1.0)
    logger.info("=== Тег " + tag + " готов === Лор: " + str(len(all_results)) + " | Стр: " + str(pages_scanned))
    return all_results, pages_scanned

def find_all_loras(min_days):
    all_results, pages_scanned = [], 0
    for page in range(1, MAX_PAGES + 1):
        if not bot_running: break
        url = BASE_URL if page == 1 else BASE_URL + "&c=" + str(page)
        logger.info("=== Все лоры | Страница: " + str(page) + " ===")
        html = fetch_with_retry(url)
        if not html: break
        
        # 🔍 Парсим "сырые" лоры (без фильтра) чтобы проверить, есть ли они вообще
        soup = BeautifulSoup(html, "html.parser")
        raw_loras = soup.find_all("p", class_="lora_head")
        
        # Если на странице вообще нет лор — завершаем поиск
        if not raw_loras:
            logger.info("Стр. " + str(page) + ": нет лор на странице → завершаю")
            break
        
        # Применяем фильтр min_days к найденным лорам
        loras = parse_loras_from_html(html, min_days)
        pages_scanned += 1
        if loras:
            all_results.extend(loras)
            logger.info("Стр. " + str(page) + ": найдено " + str(len(loras)) + " лор (после фильтра)")
        else:
            logger.info("Стр. " + str(page) + ": лор есть, но ни один не прошёл фильтр (мин. дней: " + str(min_days) + ")")
        
        if page < MAX_PAGES: time.sleep(1.0)
    
    logger.info("=== ВСЕГО === Стр: " + str(pages_scanned) + " | Лор: " + str(len(all_results)))
    return all_results, pages_scanned

# ================= HQRADIO ПАРСИНГ =================
def parse_hq_radio(html: str) -> dict | None:
    """Парсит информацию о текущем треке с HQRadio"""
    try:
        soup = BeautifulSoup(html, "html.parser")
        
        # 🔍 Ищем элемент с id="track"
        track_elem = soup.find("div", id="track")
        
        if not track_elem:
            logger.warning("⚠️ Элемент #track не найден на странице")
            return None
        
        # Получаем текст в формате "ARTIST - TITLE"
        track_text = track_elem.get_text(strip=True)
        
        if not track_text:
            logger.warning("⚠️ Элемент #track пустой")
            return None
        
        logger.info(f"🎵 Найдено: {track_text}")
        
        # Разделяем по " - " (первое вхождение)
        track_info = {}
        
        if " - " in track_text:
            parts = track_text.split(" - ", 1)
            if len(parts) == 2:
                track_info["artist"] = parts[0].strip()
                track_info["title"] = parts[1].strip()
            else:
                track_info["title"] = track_text
        else:
            track_info["title"] = track_text
        
        # 🖼️ Ищем обложку в <i class="cover" style="background-image: url(...)">
        cover_elem = soup.find("i", class_="cover")
        
        if cover_elem:
            # Получаем style атрибут
            style = cover_elem.get("style", "")
            
            # Ищем URL в background-image: url("...")
            import re
            url_match = re.search(r'background-image:\s*url\(["\']?([^"\')]+)["\']?\)', style)
            
            if url_match:
                cover_url = url_match.group(1)
                # Исправляем HTML-сущности (&quot; → ")
                cover_url = cover_url.replace("&quot;", '"').replace('"', '')
                
                # Исправляем относительные пути
                if cover_url.startswith("//"):
                    cover_url = "https:" + cover_url
                elif cover_url.startswith("/"):
                    cover_url = "https://hqradio.ru" + cover_url
                elif not cover_url.startswith("http"):
                    cover_url = "https://hqradio.ru" + cover_url
                
                track_info["cover_url"] = cover_url
                logger.info(f"🖼️ Обложка найдена: {cover_url}")
            else:
                logger.warning("⚠️ Не удалось извлечь URL из style атрибута")
        else:
            logger.warning("⚠️ Элемент <i class='cover'> не найден")
            
            # Fallback: ищем og:image
            og_image = soup.find("meta", property="og:image")
            if og_image:
                track_info["cover_url"] = og_image.get("content")
                logger.info(f"🖼️ Обложка из og:image: {track_info['cover_url']}")
        
        # ⏱️ Ищем длительность (если есть)
        duration_elem = soup.find("span", class_="duration")
        if not duration_elem:
            duration_elem = soup.find("span", class_="track-duration")
        if not duration_elem:
            duration_elem = soup.find("time")
        
        if duration_elem:
            track_info["duration"] = duration_elem.get_text(strip=True)
        
        # Добавляем метаданные
        track_info["parsed_at"] = datetime.now(timezone(timedelta(hours=3)))
        track_info["source"] = "hqradio.ru"
        track_info["raw_text"] = track_text
        
        logger.info(f"✅ Спаршено: {track_info.get('artist', '?')} - {track_info.get('title', '?')}")
        return track_info
        
    except Exception as e:
        logger.error(f"❌ Ошибка парсинга HQRadio: {e}", exc_info=True)
        return None

# ================= ФОРМАТИРОВАНИЕ И ОТПРАВКА =================
def format_message(lora):
    return "\n".join([
        EMOJI["brain"] + " <a href=\"" + lora["url"] + "\">" + lora["name"] + "</a>",
        EMOJI["id"] + " <code>ID: " + str(lora["id"]) + "</code>",
        EMOJI["days"] + " <b>" + str(lora["days"]) + " дней</b> без использования",
        EMOJI["delete"] + " <code>/dellora " + str(lora["id"]) + "</code>",
        "─" * 30
    ])

def make_export_file(loras, min_days, tags):
    lines = ["# Loonie Bot Export", "# Дата: " + datetime.now(timezone(timedelta(hours=3))).strftime("%Y-%m-%d %H:%M"),
             "# Порог: >= " + str(min_days) + " дней", "# Теги: " + (", ".join(tags) if tags else "все"),
             "# Лор: " + str(len(loras)), ""]
    for l in loras:
        lines.append("/dellora " + l["id"] + "  # " + l["name"] + " (" + str(l["days"]) + " дней)")
    return "\n".join(lines).encode("utf-8")

async def send_loras_to_chat(message, loras, total_pages):
    await message.answer(EMOJI["stats"] + " Найдено: <b>" + str(len(loras)) + "</b> лор", parse_mode="HTML")
    for i, lora in enumerate(loras, 1):
        await message.answer(format_message(lora), parse_mode="HTML")
        await asyncio.sleep(0.3 if i%10 else 0.5)
    if loras:
        avg = sum(l["days"] for l in loras) // len(loras)
        mx, mn = max(loras, key=lambda x:x["days"]), min(loras, key=lambda x:x["days"])
        await message.answer(f"\n{EMOJI['stats']} Страниц: {total_pages} | Лор: {len(loras)} | Среднее: {avg}д | Макс: {mx['days']}д", parse_mode="HTML")

async def send_loras_as_file(message, loras, total_pages, min_days, tags):
    content = make_export_file(loras, min_days, tags)
    file = BufferedInputFile(file=content, filename="loonie_export_" + datetime.now(timezone(timedelta(hours=3))).strftime("%Y%m%d_%H%M") + ".txt")
    caption = EMOJI["file"] + " <b>Экспорт лор</b>\nЛор: " + str(len(loras)) + "\nПорог: >= " + str(min_days) + " дней"
    if tags: caption += "\nТеги: " + ", ".join(tags)
    await message.answer_document(document=file, caption=caption, parse_mode="HTML")
    if loras:
        avg = sum(l["days"] for l in loras) // len(loras)
        mx = max(loras, key=lambda x:x["days"])
        await message.answer(f"{EMOJI['stats']} Страниц: {total_pages} | Лор: {len(loras)} | Среднее: {avg}д | Макс: {mx['days']}д", parse_mode="HTML")

def convert_e621_tags(tag_string):
    tag_string = tag_string.strip().strip('[]')
    tags = tag_string.split()
    converted = [tag.replace('_', ' ').replace('(', '\\(').replace(')', '\\)') for tag in tags]
    return ', '.join(converted)




# ================= ОТСЛЕЖИВАНИЕ ПОЛЬЗОВАТЕЛЕЙ =================
def track_user(user_id, username=None, full_name=None):
    """Отслеживает пользователя (вызывать при каждом сообщении)"""
    if not isinstance(user_id, int):
        if hasattr(user_id, 'from_user'):
            user_id = user_id.from_user.id
        elif isinstance(user_id, str):
            try:
                user_id = int(user_id)
            except (ValueError, TypeError):
                logger.error(f"❌ Неверный user_id: {user_id}")
                return
    
    now = time.time()
    if user_id not in known_users:
        known_users[user_id] = {
            "username": username,
            "full_name": full_name,
            "first_seen": now,
            "last_seen": now,
            "messages_count": 0,
            "forwarded": False,
            "blocked": False,  # ← Добавлено: по умолчанию не заблокирован
            "unsubscribed": False  # ← Для будущей отписки от рассылок
        }
        logger.info(f"🆕 Новый пользователь: {full_name} (@{username}) [{user_id}]")
    else:
        known_users[user_id]["last_seen"] = now
        known_users[user_id]["messages_count"] += 1
        if username:
            known_users[user_id]["username"] = username
        if full_name:
            known_users[user_id]["full_name"] = full_name
    save_users()

def mark_user_forwarded(user_id):
    """Помечает, что пользователь пересылал сообщения"""
    if not isinstance(user_id, int):
        if hasattr(user_id, 'from_user'):
            user_id = user_id.from_user.id
        elif isinstance(user_id, str):
            try:
                user_id = int(user_id)
            except:
                return
    if user_id in known_users:
        known_users[user_id]["forwarded"] = True
        save_users()


@dp.message(Command("testmusic"))
async def cmd_test_music(m: Message):
    if m.from_user.id != OWNER_ID_INT:
        return

    await m.answer("🔍 <b>Запускаю диагностику модуля Яндекс.Музыки...</b>", parse_mode="HTML")
    
    diagnostics = []
    
    try:
        # 1. Проверяем инициализацию клиента
        if not ym_client:
            diagnostics.append("⚠️ Клиент не был запущен. Инициализирую...")
            if not init_yandex_music():
                await m.answer("❌ <b>Ошибка:</b> Не удалось инициализировать клиент Яндекс.Музыки.\nПроверь переменную `YANDEX_MUSIC_TOKEN` и логи сервера.", parse_mode="HTML")
                return
            diagnostics.append("✅ Клиент успешно подключён к API")
        else:
            diagnostics.append("✅ Клиент уже активен")

        # 2. Пытаемся получить текущий трек
        track = get_current_track()
        if not track:
            diagnostics.append("⚠️ Трек не найден (возможно, ничего не играет или очередь ещё не синхронизирована)")
            await m.answer(
                "ℹ️ <b>Результат:</b> Сейчас трек не определяется.\n\n" +
                "\n".join(diagnostics),
                parse_mode="HTML"
            )
            return

        diagnostics.append(f"✅ Трек получен: {track['title']} — {track['artists']}")
        diagnostics.append(f"🆔 ID трека: {track['id']}")
        diagnostics.append(f"🖼️ Обложка: {'Найдена' if track['cover_url'] else 'Отсутствует'}")

        # 3. Отправляем тестовое сообщение владельцу в ЛС
        target_chat = OWNER_ID_INT
        caption = f"{track['text']}\n\n📝 <b>Диагностика:</b>\n" + "\n".join(diagnostics)
        
        if track["cover_url"]:
            await bot.send_photo(
                chat_id=target_chat,
                photo=track["cover_url"],
                caption=caption,
                parse_mode="HTML"
            )
        else:
            await bot.send_message(
                chat_id=target_chat,
                text=caption,
                parse_mode="HTML"
            )
            
        diagnostics.append("✅ Сообщение успешно отправлено в ЛС владельцу")
        
        await m.answer(
            "✅ <b>Тест пройден!</b> Проверь личные сообщения от бота.\n\n" + "\n".join(diagnostics),
            parse_mode="HTML"
        )
    except Exception as error_msg:
        logger.error(f"❌ Ошибка тестирования музыки: {error_msg}")
        await m.answer(
            f"❌ <b>Критическая ошибка при тесте:</b>\n<code>{str(error_msg)[:300]}</code>\n\n"
            f"Проверь логи сервера и убедись, что токен Яндекс.Музыки корректен.",
            parse_mode="HTML"
        )

# ================= YANDEX MUSIC (Ynison API) =================

def get_current_track():
    """Получает текущий трек через Ynison Simple API (yandex-music>=3.0.0)"""
    global YANDEX_MUSIC_TOKEN
    
    if not YANDEX_MUSIC_TOKEN:
        logger.warning("⚠️ YANDEX_MUSIC_TOKEN не задан")
        return None

    try:
        from yandex_music.ynison import simple
        track = simple.get_current_track(YANDEX_MUSIC_TOKEN)
        
        if not track:
            logger.debug("🎵 Ничего не играет (Ynison вернул None)")
            return None

        title = getattr(track, 'title', getattr(track, 'name', 'Unknown Title'))
        
        artists_attr = getattr(track, 'artists', [])
        if artists_attr:
            if isinstance(artists_attr, list):
                artists = ", ".join([getattr(a, 'name', str(a)) for a in artists_attr])
            else:
                artists = str(artists_attr)
        else:
            artists = "Unknown Artist"

        cover_url = None
        cover = getattr(track, 'cover', None)
        if cover:
            if hasattr(cover, 'get_url'):
                cover_url = cover.get_url('200x200')
            elif isinstance(cover, str):
                if cover.startswith('http'):
                    cover_url = cover.replace('%%', '200x200')
                else:
                    cover_url = f"https://{cover.replace('%%', '200x200')}"
            elif hasattr(cover, 'uri'):
                uri = cover.uri
                cover_url = f"https://{uri.replace('%%', '200x200')}" if not uri.startswith('http') else uri.replace('%%', '200x200')

        track_id = getattr(track, 'id', getattr(track, 'track_id', 0))
        if hasattr(track_id, '__iter__') and not isinstance(track_id, str):
            track_id = ":".join(map(str, track_id)) if len(track_id) > 1 else str(track_id[0])

        return {
            "id": str(track_id),
            "title": title,
            "artists": artists,
            "cover_url": cover_url,
            "text": f"🎧 <b>Сейчас играет:</b>\n{title} — {artists}"
        }

    except ImportError:
        logger.error("❌ Модуль yandex_music.ynison не найден. Требуется yandex-music>=3.0.0")
        return None
    except Exception as e:
        logger.error(f"💥 Ошибка получения трека через Ynison: {e}")
        return None

# ⚠️ update_music_status должна быть ОБЪЯВЛЕНА ПЕРЕД start_music_tracking!
async def update_music_status():
    global current_music_message_id, last_track_id, music_message_timestamp, music_tracking_enabled
    try:
        target_chat_id = int(MUSIC_STATUS_CHAT_ID) if MUSIC_STATUS_CHAT_ID else OWNER_ID_INT
    except (ValueError, TypeError):
        logger.error("❌ MUSIC_STATUS_CHAT_ID должен быть числом")
        return
    logger.info(f"🎵 Цикл запущен. Чат: {target_chat_id}")
    
    while music_tracking_enabled:
        try:
            track = get_current_track()
            if not track:
                await asyncio.sleep(MUSIC_CHECK_INTERVAL)
                continue
            if track["id"] != last_track_id or current_music_message_id is None:
                logger.info(f"🔄 Трек: {track['title']} — {track['artists']}")
                now = time.time()
                is_old = music_message_timestamp and (now - music_message_timestamp > 172800)
                try:
                    if current_music_message_id and not is_old:
                        if track["cover_url"]:
                            from aiogram.types import InputMediaPhoto
                            media = InputMediaPhoto(media=track["cover_url"], caption=track["text"], parse_mode="HTML")
                            await bot.edit_message_media(chat_id=target_chat_id, message_id=current_music_message_id, media=media)
                        else:
                            await bot.edit_message_caption(chat_id=target_chat_id, message_id=current_music_message_id, caption=track["text"], parse_mode="HTML")
                    else:
                        if current_music_message_id:
                            try:
                                await bot.delete_message(chat_id=target_chat_id, message_id=current_music_message_id)
                            except:
                                pass
                        if track["cover_url"]:
                            msg = await bot.send_photo(chat_id=target_chat_id, photo=track["cover_url"], caption=track["text"], parse_mode="HTML")
                        else:
                            msg = await bot.send_message(chat_id=target_chat_id, text=track["text"], parse_mode="HTML")
                        current_music_message_id = msg.message_id
                        music_message_timestamp = now
                    last_track_id = track["id"]
                except Exception as err:
                    logger.error(f"❌ Ошибка отправки: {err}")
                    current_music_message_id = None
                    music_message_timestamp = None
            await asyncio.sleep(MUSIC_CHECK_INTERVAL)
        except Exception as e:
            logger.error(f"💥 Ошибка цикла: {e}")
            await asyncio.sleep(MUSIC_CHECK_INTERVAL)
    logger.info("⏹️ Цикл остановлен")

async def start_music_tracking():
    global music_tracking_enabled, music_task, YANDEX_MUSIC_TOKEN
    if music_tracking_enabled:
        logger.warning("⚠️ Отслеживание музыки уже запущено")
        return False
    # ✅ ПРОВЕРКА ТОКЕНА (вместо init_yandex_music)
    if not YANDEX_MUSIC_TOKEN:
        logger.error("❌ YANDEX_MUSIC_TOKEN не задан")
        return False
    music_tracking_enabled = True
    music_task = asyncio.create_task(update_music_status())
    logger.info("🎵 Отслеживание музыки запущено")
    return True

async def stop_music_tracking():
    global music_tracking_enabled, music_task
    if not music_tracking_enabled:
        return
    music_tracking_enabled = False
    if music_task:
        music_task.cancel()
        try:
            await music_task
        except asyncio.CancelledError:
            pass
    logger.info("⏹️ Отслеживание музыки остановлено")

# ================= КОНЕЦ БЛОКА МУЗЫКИ =================



# ================= ОБРАТНАЯ СВЯЗЬ =================
@dp.message(F.from_user.id != OWNER_ID_INT)
async def handle_user_message(message: Message):
    user_id = message.from_user.id  # ← Сначала определяем!
    
    # ❗ Проверяем, не заблокирован ли пользователь
    if user_id in known_users and known_users[user_id].get("blocked", False):
        logger.info(f"🚫 Игнорировано сообщение от заблокированного пользователя {user_id}")
        return
    
    # ❗ Если это команда /start — НЕ пересылаем
    if message.text and message.text.strip() == "/start":
        track_user(user_id, message.from_user.username, message.from_user.full_name)
        return
    
    username = message.from_user.username or None
    full_name = message.from_user.full_name
    track_user(user_id, username, full_name)
    
    try:
        forwarded = await message.forward(chat_id=OWNER_ID_INT)
        forwarded_messages[forwarded.message_id] = user_id
        save_forwarded()
        mark_user_forwarded(user_id)
        moscow_time = datetime.now(timezone(timedelta(hours=3))).strftime('%H:%M')
        user_info = f"<b>Сообщение от:</b>\n• Имя: {full_name}\n• Username: @{username or 'нет'}\n• ID: <code>{user_id}</code>\n• Время: 🕐 МСК {moscow_time}\n\n<i>Ответьте на пересланное сообщение чтобы ответить</i>"
        await bot.send_message(chat_id=OWNER_ID_INT, text=user_info, parse_mode="HTML")
    except Exception as e:
        logger.error("Ошибка пересылки: " + str(e))

@dp.message(F.from_user.id == OWNER_ID_INT, F.reply_to_message)
async def handle_owner_reply(message: Message):
    """Обрабатывает ТОЛЬКО ответы владельца на пересланные сообщения"""
    reply_msg_id = message.reply_to_message.message_id
    logger.info(f"📨 Владелец ответил на message_id={reply_msg_id}")
    
    if reply_msg_id in forwarded_messages:
        user_id = forwarded_messages[reply_msg_id]
        logger.info(f"✅ Найдено соответствие: message_id={reply_msg_id} → user_id={user_id}")
        try:
            # Отправляем текст если есть
            if message.text:
                await bot.send_message(chat_id=user_id, text=f"{PREMIUM_EMOJI['sparkle']} {message.text}", parse_mode="HTML")
            # Отправляем медиа если есть
            if message.photo:
                await bot.send_photo(chat_id=user_id, photo=message.photo[-1].file_id, caption=message.caption or "")
            elif message.video:
                await bot.send_video(chat_id=user_id, video=message.video.file_id, caption=message.caption or "")
            elif message.voice:
                await bot.send_voice(chat_id=user_id, voice=message.voice.file_id)
            elif message.audio:
                await bot.send_audio(chat_id=user_id, audio=message.audio.file_id)
            elif message.document:
                await bot.send_document(chat_id=user_id, document=message.document.file_id)
            elif message.sticker:
                await bot.send_sticker(chat_id=user_id, sticker=message.sticker.file_id)
            
            await message.answer(f"{EMOJI['check']} Ответ отправлен пользователю {user_id}", parse_mode="HTML")
            
            # Удаляем запись и сохраняем
            del forwarded_messages[reply_msg_id]
            save_forwarded()
            return  # Важно: возвращаем, чтобы не сработал silent_ignore
            
        except Exception as e:
            logger.error("Ошибка отправки ответа: " + str(e))
            await message.answer(f"{EMOJI['error']} Не удалось отправить: {str(e)[:100]}", parse_mode="HTML")
            return
    else:
        # Если message_id не найден — просто игнорируем, пусть сработают другие хендлеры
        logger.info(f"⚠️ message_id={reply_msg_id} не найден в forwarded_messages")
    # Если это не ответ — обрабатываем как обычную команду или игнорируем
    # (другие хендлеры команд обработают /start, /check и т.д.)
    # Этот хендлер только для ответов на пересланные сообщения

@dp.message(F.from_user.id == OWNER_ID_INT, F.reply_to_message)
async def handle_owner_reply(message: Message):
    """Дублирующий хендлер для надёжности (может быть удалён если не нужен)"""
    # Этот хендлер дублирует логику выше для дополнительной надёжности
    pass  # Основная логика уже в handle_owner_messages

# ================= КОМАНДЫ =================
@dp.message(Command("users"))
async def cmd_users(m: Message):
    if m.from_user.id != OWNER_ID_INT:
        await cmd_start(m)
        return
    if not known_users:
        await m.answer(f"{EMOJI['info']} Пока никто не писал боту", parse_mode="HTML")
        return
    sorted_users = sorted(known_users.items(), key=lambda x: (not x[1].get("forwarded", False), -x[1].get("messages_count", 0)))
    txt = f"{EMOJI['users']} <b>Пользователи ({len(sorted_users)}):</b>\n\n"
    for user_id, data in sorted_users:
        name = data.get("full_name", "Unknown")
        username = data.get("username")
        first = datetime.fromtimestamp(data["first_seen"], tz=timezone(timedelta(hours=3))).strftime("%d.%m")
        last = datetime.fromtimestamp(data["last_seen"], tz=timezone(timedelta(hours=3))).strftime("%d.%m")
        msgs = data.get("messages_count", 1)
        fwd = "📬" if data.get("forwarded") else ""
        user_line = f"{fwd} <code>{user_id}</code> — {name}" + (f" (@{username})" if username else "") + f" | 💬 {msgs} | 📅 {first}–{last}\n"
        if len(txt) + len(user_line) > 4000:
            txt += "\n<i>...и ещё</i>"
            break
        txt += user_line
    await m.answer(txt, parse_mode="HTML")


@dp.message(Command("write", "sms"))
async def cmd_write(m: Message):
    """Отправляет сообщение пользователю по ID: /write <user_id> <текст>"""
    if m.from_user.id != OWNER_ID_INT:
        return
    
    parts = m.text.split(maxsplit=2)  # split: /write, id, message
    
    if len(parts) < 3:
        await m.answer(
            f"{EMOJI['warning']} <b>Использование:</b>\n"
            f"<code>/write &lt;user_id&gt; &lt;сообщение&gt;</code>\n\n"
            f"<b>Пример:</b>\n"
            f"<code>/write 123456789 Привет! Это тестовое сообщение.</code>\n\n"
            f"<i>Используй /users чтобы узнать ID пользователей</i>",
            parse_mode="HTML"
        )
        return
    
    try:
        target_user_id = int(parts[1])
        message_text = parts[2]
        
        # Проверяем, есть ли пользователь в known_users
        if target_user_id not in known_users:
            await m.answer(
                f"{EMOJI['warning']} Пользователь <code>{target_user_id}</code> не найден в базе.\n"
                f"<i>Он никогда не писал боту или данные были сброшены</i>",
                parse_mode="HTML"
            )
            return
        
        # Получаем инфо о пользователе для лога
        user_info = known_users[target_user_id]
        username = user_info.get("username", "нет")
        name = user_info.get("full_name", "Unknown")
        
        # Отправляем сообщение пользователю (ТОЛЬКО эмодзи + текст, без заголовка)
        await bot.send_message(
            chat_id=target_user_id,
            text=f"{PREMIUM_EMOJI['sparkle']} {message_text}",
            parse_mode="HTML"
        )
        
        # Подтверждение владельцу
        await m.answer(
            f"{EMOJI['check']} <b>Сообщение отправлено!</b>\n\n"
            f"👤 Пользователь: {name} (@{username})\n"
            f"🆔 ID: <code>{target_user_id}</code>\n"
            f"📝 Текст: <i>{message_text[:50]}{'...' if len(message_text) > 50 else ''}</i>",
            parse_mode="HTML"
        )
        
        logger.info(f"📤 Сообщение отправлено пользователю {target_user_id} ({name})")
        
    except ValueError:
        await m.answer(f"{EMOJI['error']} Неверный формат user_id. Используй числа.", parse_mode="HTML")
    except Exception as e:
        logger.error("Ошибка отправки сообщения: " + str(e))
        await m.answer(f"{EMOJI['error']} Ошибка: {str(e)[:100]}", parse_mode="HTML")


@dp.message(Command("broadcast"))
async def cmd_broadcast(m: Message):
    """Рассылка сообщения всем пользователям: /broadcast <текст>"""
    if m.from_user.id != OWNER_ID_INT:
        return
    
    # Получаем текст сообщения (после команды)
    parts = m.text.split(maxsplit=1)
    
    if len(parts) < 2:
        await m.answer(
            f"{EMOJI['warning']} <b>Использование:</b>\n"
            f"<code>/broadcast &lt;текст сообщения&gt;</code>\n\n"
            f"<b>Пример:</b>\n"
            f"<code>/broadcast Всем привет! Обновление бота уже доступно!</code>\n\n"
            f"<i>Сообщение будет отправлено всем {len(known_users)} пользователям</i>",
            parse_mode="HTML"
        )
        return
    
    message_text = parts[1]
    total_users = len(known_users)
    
    if total_users == 0:
        await m.answer(f"{EMOJI['info']} Пока нет пользователей для рассылки.", parse_mode="HTML")
        return
    
    # Отправляем сообщение с подтверждением
    await m.answer(
        f"{EMOJI['info']} <b>Рассылка запущена!</b>\n\n"
        f"👥 Получателей: <b>{total_users}</b>\n"
        f"📝 Текст: <i>{message_text[:100]}{'...' if len(message_text) > 100 else ''}</i>\n\n"
        f"<i>Отчёт будет отправлен после завершения</i>",
        parse_mode="HTML"
    )
    
    # Счётчики для отчёта
    sent_count = 0
    failed_count = 0
    blocked_count = 0
    
    # Отправляем сообщение каждому пользователю
    for user_id, user_data in known_users.items():
        try:
            username = user_data.get("username", "нет")
            safe_text = safe_html_text(message_text)
            await bot.send_message(
                chat_id=user_id,
                text=f"💥 {safe_text}",
                parse_mode="HTML"
            )
            sent_count += 1
            logger.info(f"📤 Рассылка: отправлено пользователю {user_id} (@{username})")
            await asyncio.sleep(0.1)  # Небольшая задержка чтобы не спамить API
        except Exception as e:
            error_str = str(e).lower()
            if "blocked" in error_str or "bot was blocked" in error_str:
                blocked_count += 1
                logger.warning(f"🚫 Рассылка: пользователь {user_id} заблокировал бота")
            else:
                failed_count += 1
                logger.error(f"❌ Рассылка: ошибка отправки пользователю {user_id}: {e}")
    
    # Отправляем отчёт владельцу
    report = (
        f"{EMOJI['check']} <b>Рассылка завершена!</b>\n\n"
        f"📊 <b>Статистика:</b>\n"
        f"✅ Отправлено: <b>{sent_count}</b>\n"
        f"🚫 Заблокировано: <b>{blocked_count}</b>\n"
        f"❌ Ошибок: <b>{failed_count}</b>\n"
        f"👥 Всего: <b>{total_users}</b>\n\n"
        f"📝 Текст: <i>{message_text[:100]}{'...' if len(message_text) > 100 else ''}</i>"
    )
    
    await m.answer(report, parse_mode="HTML")
    logger.info(f"📊 Рассылка завершена: {sent_count}/{total_users} успешно")

# ================= MUSIC COMMANDS =================
@dp.message(Command("music"))
async def cmd_music_status(m: Message):
    """Показать статус отслеживания музыки"""
    if m.from_user.id != OWNER_ID_INT:
        return
    
    status = " <b>Статус отслеживания музыки:</b>\n\n"
    status += f"{'✅' if music_tracking_enabled else '❌'} Отслеживание: <b>{'Активно' if music_tracking_enabled else 'Остановлено'}</b>\n"
    
    if YANDEX_MUSIC_AVAILABLE:
        status += f"✅ Библиотека: <b>yandex-music установлена</b>\n"
    else:
        status += f"❌ Библиотека: <b>yandex-music НЕ установлена</b>\n"
        status += f"<i>Установи: pip install yandex-music</i>\n"
    
    if YANDEX_MUSIC_TOKEN:
        status += f"✅ Токен: <b>задан</b>\n"
    else:
        status += f"❌ Токен: <b>НЕ задан</b>\n"
        status += f"<i>Добавь YANDEX_MUSIC_TOKEN в переменные окружения</i>\n"
    
    if music_tracking_enabled and last_track_id:
        status += f"\n🎶 Последний трек ID: <code>{last_track_id}</code>\n"
    
    status += "\n<b>Управление:</b>\n"
    status += "/startmusic — Запустить отслеживание\n"
    status += "/stopmusic — Остановить отслеживание"
    
    await m.answer(status, parse_mode="HTML")

@dp.message(Command("ymversion"))
async def cmd_ym_version(m: Message):
    if m.from_user.id != OWNER_ID_INT: return
    try:
        import yandex_music
        await m.answer(f"📦 Установлена версия: <code>{getattr(yandex_music, '__version__', 'не определена')}</code>\n📍 Путь: <code>{yandex_music.__file__}</code>", parse_mode="HTML")
    except Exception as e:
        await m.answer(f"❌ Ошибка: {e}", parse_mode="HTML")

@dp.message(Command("startmusic"))
async def cmd_start_music(m: Message):
    """Запустить отслеживание музыки"""
    if m.from_user.id != OWNER_ID_INT:
        return
    
    await m.answer("⏳ Запускаю отслеживание музыки...", parse_mode="HTML")
    
    success = await start_music_tracking()
    
    if success:
        target_chat = int(MUSIC_STATUS_CHAT_ID) if MUSIC_STATUS_CHAT_ID else "твои личные сообщения"
        await m.answer(
            f"{PREMIUM_EMOJI['sparkle']} <b>Отслеживание музыки запущено!</b>\n\n"
            f"📍 Чат: <code>{target_chat}</code>\n"
            f"⏱️ Интервал: {MUSIC_CHECK_INTERVAL} сек\n\n"
            f"<i>Используй /stopmusic чтобы остановить</i>",
            parse_mode="HTML"
        )
    else:
        await m.answer(
            f"{EMOJI['error']} <b>Не удалось запустить отслеживание!</b>\n\n"
            f"Проверь:\n"
            f"• Установлена ли библиотека yandex-music\n"
            f"• Задан ли YANDEX_MUSIC_TOKEN\n"
            f"• Корректен ли токен",
            parse_mode="HTML"
        )

@dp.message(Command("stopmusic"))
async def cmd_stop_music(m: Message):
    """Остановить отслеживание музыки"""
    if m.from_user.id != OWNER_ID_INT:
        return
    
    await stop_music_tracking()
    await m.answer(
        f"{EMOJI['stop']} <b>Отслеживание музыки остановлено</b>",
        parse_mode="HTML"
    )

@dp.message(Command("nowplaying"))
async def cmd_now_playing(m: Message):
    """Показать что играет прямо сейчас"""
    if m.from_user.id != OWNER_ID_INT:
        return
    
    if not ym_client:
        await m.answer(f"{EMOJI['warning']} Клиент Яндекс.Музыки не инициализирован", parse_mode="HTML")
        return
    
    track = get_current_track()
    
    if not track:
        await m.answer(f"{EMOJI['info']} Сейчас ничего не играет", parse_mode="HTML")
        return
    
    if track["cover_url"]:
        await bot.send_photo(
            chat_id=m.chat.id,
            photo=track["cover_url"],
            caption=track["text"],
            parse_mode="HTML"
        )
    else:
        await m.answer(track["text"], parse_mode="HTML")


@dp.message(Command("block"))
async def cmd_block(m: Message):
    """Заблокировать пользователя: /block <user_id>"""
    if m.from_user.id != OWNER_ID_INT:
        return
    
    parts = m.text.split()
    
    if len(parts) < 2:
        await m.answer(
            f"{EMOJI['warning']} <b>Использование:</b>\n"
            f"<code>/block &lt;user_id&gt;</code>\n\n"
            f"<b>Пример:</b>\n"
            f"<code>/block 123456789</code>\n\n"
            f"<i>Используй /users чтобы узнать ID</i>",
            parse_mode="HTML"
        )
        return
    
    try:
        target_user_id = int(parts[1])
        
        if target_user_id not in known_users:
            await m.answer(
                f"{EMOJI['warning']} Пользователь <code>{target_user_id}</code> не найден в базе.",
                parse_mode="HTML"
            )
            return
        
        # Блокируем пользователя
        known_users[target_user_id]["blocked"] = True
        save_users()
        
        user_info = known_users[target_user_id]
        username = user_info.get("username", "нет")
        name = user_info.get("full_name", "Unknown")
        
        await m.answer(
            f"{EMOJI['check']} <b>Пользователь заблокирован!</b>\n\n"
            f"👤 {name} (@{username})\n"
            f"🆔 ID: <code>{target_user_id}</code>\n\n"
            f"<i>Теперь его сообщения будут игнорироваться</i>",
            parse_mode="HTML"
        )
        logger.info(f"🚫 Заблокирован пользователь {target_user_id} ({name})")
        
    except ValueError:
        await m.answer(f"{EMOJI['error']} Неверный формат user_id. Используй числа.", parse_mode="HTML")
    except Exception as e:
        logger.error("Ошибка блокировки: " + str(e))
        await m.answer(f"{EMOJI['error']} Ошибка: {str(e)[:100]}", parse_mode="HTML")

@dp.message(Command("unblock"))
async def cmd_unblock(m: Message):
    """Разблокировать пользователя: /unblock <user_id>"""
    if m.from_user.id != OWNER_ID_INT:
        return
    
    parts = m.text.split()
    
    if len(parts) < 2:
        await m.answer(
            f"{EMOJI['warning']} <b>Использование:</b>\n"
            f"<code>/unblock &lt;user_id&gt;</code>\n\n"
            f"<b>Пример:</b>\n"
            f"<code>/unblock 123456789</code>",
            parse_mode="HTML"
        )
        return
    
    try:
        target_user_id = int(parts[1])
        
        if target_user_id not in known_users:
            await m.answer(
                f"{EMOJI['warning']} Пользователь <code>{target_user_id}</code> не найден в базе.",
                parse_mode="HTML"
            )
            return
        
        # Разблокируем пользователя
        known_users[target_user_id]["blocked"] = False
        save_users()
        
        user_info = known_users[target_user_id]
        username = user_info.get("username", "нет")
        name = user_info.get("full_name", "Unknown")
        
        await m.answer(
            f"{EMOJI['check']} <b>Пользователь разблокирован!</b>\n\n"
            f"👤 {name} (@{username})\n"
            f"🆔 ID: <code>{target_user_id}</code>\n\n"
            f"<i>Теперь его сообщения будут обрабатываться</i>",
            parse_mode="HTML"
        )
        logger.info(f"✅ Разблокирован пользователь {target_user_id} ({name})")
        
    except ValueError:
        await m.answer(f"{EMOJI['error']} Неверный формат user_id. Используй числа.", parse_mode="HTML")
    except Exception as e:
        logger.error("Ошибка разблокировки: " + str(e))
        await m.answer(f"{EMOJI['error']} Ошибка: {str(e)[:100]}", parse_mode="HTML")

@dp.message(Command("blocked"))
async def cmd_blocked(m: Message):
    """Показать список заблокированных пользователей"""
    if m.from_user.id != OWNER_ID_INT:
        return
    
    blocked_users = [
        (uid, data) for uid, data in known_users.items() 
        if data.get("blocked", False)
    ]
    
    if not blocked_users:
        await m.answer(f"{EMOJI['info']} Нет заблокированных пользователей.", parse_mode="HTML")
        return
    
    txt = f"{EMOJI['lock']} <b>Заблокированные ({len(blocked_users)}):</b>\n\n"
    
    for user_id, data in blocked_users:
        name = data.get("full_name", "Unknown")
        username = data.get("username", "нет")
        first = datetime.fromtimestamp(data["first_seen"], tz=timezone(timedelta(hours=3))).strftime("%d.%m")
        txt += f"🆔 <code>{user_id}</code> — {name} (@{username}) | 📅 {first}\n"
    
    txt += f"\n<i>Используй /unblock &lt;id&gt; чтобы разблокировать</i>"
    await m.answer(txt, parse_mode="HTML")

@dp.message(Command("shell"))
async def cmd_shell(m: Message):
    """Выполняет shell-команду: /shell <command>"""
    if m.from_user.id != OWNER_ID_INT:
        return
    
    parts = m.text.split(maxsplit=1)
    
    if len(parts) < 2:
        await m.answer(
            f"{EMOJI['info']} <b>Shell-доступ (только для владельца):</b>\n\n"
            f"<code>/shell &lt;команда&gt;</code>\n\n"
            f"<b>Примеры:</b>\n"
            f"<code>/shell ls -la</code>\n"
            f"<code>/shell pwd</code>\n"
            f"<code>/shell git status</code>\n\n"
            f"<i>⚠️ Команды проверяются на безопасность</i>",
            parse_mode="HTML"
        )
        return
    
    command = parts[1].strip()
    
    # Проверка безопасности
    is_safe, error_msg = is_command_safe(command)
    if not is_safe:
        await m.answer(error_msg, parse_mode="HTML")
        logger.warning(f"🚫 Заблокирована опасная команда от владельца: {command}")
        return
    
    # Отправляем "задумался"
    status_msg = await m.answer(f"⏳ Выполняю: <code>{safe_html_text(command)}</code>...", parse_mode="HTML")
    
    start_time = time.time()
    stdout, stderr, returncode = await run_shell_command(command)
    exec_time = time.time() - start_time
    
    # Формируем ответ
    result = f"{EMOJI['info']} <b>Результат:</b>\n\n"
    result += f"🔹 Команда: <code>{safe_html_text(command)}</code>\n"
    result += f"🔹 Время: <b>{exec_time:.2f} сек</b>\n"
    result += f"🔹 Код возврата: <code>{returncode}</code>\n\n"
    
    if stdout:
        result += f"<b>📤 STDOUT:</b>\n<code>{safe_html_text(stdout)}</code>\n"
    if stderr:
        result += f"<b>📥 STDERR:</b>\n<code>{safe_html_text(stderr)}</code>\n"
    if not stdout and not stderr:
        result += "<i>(нет вывода)</i>\n"
    
    # Если вывод очень длинный — отправляем файлом
    if len(result) > 4000:
        await status_msg.delete()
        file = BufferedInputFile(
            file=result.encode("utf-8"),
            filename=f"shell_output_{datetime.now(timezone(timedelta(hours=3))).strftime('%Y%m%d_%H%M')}.txt"
        )
        await m.answer_document(document=file, caption=f"📄 Вывод команды `{command}` (обрезан до 4000 символов)")
    else:
        await status_msg.edit_text(result, parse_mode="HTML")
    
    logger.info(f"🐚 Shell: {command} → код {returncode} за {exec_time:.2f} сек")

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

def create_ls_keyboard(path: str, items: list) -> InlineKeyboardMarkup:
    """Создаёт инлайн-клавиатуру для навигации по файлам"""
    keyboard = []
    
    # Кнопка "Наверх" (если не в корне)
    if path != "/opt/render/project/src" and path != ".":
        parent = os.path.dirname(path) if path != "." else "/opt/render/project/src"
        keyboard.append([
            InlineKeyboardButton(text="🔙 Наверх", callback_data=f"ls:{parent}")
        ])
    
    # Кнопки для директорий (первые 10)
    dirs = [item for item in items if item['type'] == 'dir'][:10]
    if dirs:
        row = []
        for d in dirs:
            # Обрезаем длинные имена
            name = d['name'][:20] + "…" if len(d['name']) > 20 else d['name']
            row.append(InlineKeyboardButton(text=f"📁 {name}", callback_data=f"ls:{d['path']}"))
        keyboard.append(row)
    
    # Кнопки для файлов (первые 10) - только действия
    files = [item for item in items if item['type'] == 'file'][:10]
    if files:
        row = []
        for f in files:
            name = f['name'][:15] + "…" if len(f['name']) > 15 else f['name']
            # Кнопка для просмотра/скачивания файла
            row.append(InlineKeyboardButton(text=f"📄 {name}", callback_data=f"file:{f['path']}"))
        keyboard.append(row)
    
    # Кнопки действий
    action_row = [
        InlineKeyboardButton(text="🔄 Обновить", callback_data=f"ls:{path}"),
        InlineKeyboardButton(text="📤 Загрузить всё", callback_data=f"upload_dir:{path}"),
    ]
    keyboard.append(action_row)
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)



# ================= FILE MANAGEMENT =================
ALLOWED_DIRECTORIES = [
    "/opt/render/project/src",
    "/opt/render/project/src/logs",
]

MAX_FILE_SIZE = 50 * 1024 * 1024  # 50 MB лимит Telegram

def is_path_safe(path: str) -> tuple[bool, str]:
    """Проверяет безопасность пути"""
    # Нормализуем путь
    abs_path = os.path.abspath(path)
    
    # Проверка на выход за пределы разрешённых директорий
    is_allowed = any(abs_path.startswith(allowed) for allowed in ALLOWED_DIRECTORIES)
    if not is_allowed:
        return False, f"❌ Доступ за пределы разрешённых директорий запрещён.\nРазрешено: {', '.join(ALLOWED_DIRECTORIES)}"
    
    # Проверка на опасные символы
    dangerous_chars = ["|", "&", ";", ">", "<", "`", "$", "(", ")"]
    for char in dangerous_chars:
        if char in path:
            return False, f"❌ Опасный символ в пути: `{char}`"
    
    return True, ""

@dp.message(Command("ls"))
async def cmd_ls(m: Message):
    """Список файлов в директории: /ls [путь]"""
    if m.from_user.id != OWNER_ID_INT:
        return
    
    parts = m.text.split()
    path = parts[1] if len(parts) > 1 else "."
    
    # Проверка безопасности
    is_safe, error_msg = is_path_safe(path)
    if not is_safe:
        await m.answer(error_msg, parse_mode="HTML")
        return
    
    try:
        # Нормализуем путь
        abs_path = os.path.abspath(path)
        
        if not os.path.exists(abs_path):
            await m.answer(f"{EMOJI['error']} Путь не существует: <code>{safe_html_text(abs_path)}</code>", parse_mode="HTML")
            return
        
        if not os.path.isdir(abs_path):
            await m.answer(f"{EMOJI['error']} Это не директория: <code>{safe_html_text(abs_path)}</code>", parse_mode="HTML")
            return
        
        # Получаем список файлов
        items = os.listdir(abs_path)
        items.sort()
        
        txt = f"{EMOJI['file']} <b>Список файлов:</b>\n"
        txt += f"📁 Путь: <code>{safe_html_text(abs_path)}</code>\n\n"
        
        dirs = []
        files = []
        
        for item in items:
            item_path = os.path.join(abs_path, item)
            if os.path.isdir(item_path):
                dirs.append(f"📁 <code>{safe_html_text(item)}</code>/")
            else:
                size = os.path.getsize(item_path)
                size_str = f"{size / 1024:.1f} KB" if size < 1024 * 1024 else f"{size / 1024 / 1024:.1f} MB"
                files.append(f"📄 <code>{safe_html_text(item)}</code> ({size_str})")
        
        txt += "\n".join(dirs[:20]) + "\n" if dirs else ""
        txt += "\n".join(files[:20]) + "\n" if files else ""
        
        if len(dirs) > 20 or len(files) > 20:
            txt += f"\n<i>...и ещё {len(dirs) + len(files) - 40} файлов (показано первые 40)</i>"
        
        await m.answer(txt, parse_mode="HTML")
        
    except Exception as e:
        await m.answer(f"{EMOJI['error']} Ошибка: {safe_html_text(str(e))}", parse_mode="HTML")
        logger.error(f"❌ Ошибка /ls: {e}")

@dp.message(Command("upload"))
async def cmd_upload(m: Message):
    """Загрузить файл с сервера в Telegram: /upload <путь>"""
    if m.from_user.id != OWNER_ID_INT:
        return
    
    parts = m.text.split(maxsplit=1)
    
    if len(parts) < 2:
        await m.answer(
            f"{EMOJI['info']} <b>Загрузка файла с сервера:</b>\n\n"
            f"<code>/upload &lt;путь к файлу&gt;</code>\n\n"
            f"<b>Примеры:</b>\n"
            f"<code>/upload bot.py</code>\n"
            f"<code>/upload /opt/render/project/src/bot.py</code>\n"
            f"<code>/upload logs/error.log</code>\n\n"
            f"<i>Макс. размер: 50 MB</i>",
            parse_mode="HTML"
        )
        return
    
    file_path = parts[1].strip()
    
    # Проверка безопасности
    is_safe, error_msg = is_path_safe(file_path)
    if not is_safe:
        await m.answer(error_msg, parse_mode="HTML")
        return
    
    try:
        abs_path = os.path.abspath(file_path)
        
        if not os.path.exists(abs_path):
            await m.answer(f"{EMOJI['error']} Файл не найден: <code>{safe_html_text(abs_path)}</code>", parse_mode="HTML")
            return
        
        if os.path.isdir(abs_path):
            await m.answer(f"{EMOJI['error']} Это директория, укажите файл: <code>{safe_html_text(abs_path)}</code>", parse_mode="HTML")
            return
        
        file_size = os.path.getsize(abs_path)
        
        if file_size > MAX_FILE_SIZE:
            await m.answer(
                f"{EMOJI['warning']} <b>Файл слишком большой!</b>\n\n"
                f"📦 Размер: <b>{file_size / 1024 / 1024:.1f} MB</b>\n"
                f"⚠️ Максимум: <b>{MAX_FILE_SIZE / 1024 / 1024:.0f} MB</b>",
                parse_mode="HTML"
            )
            return
        
        # Отправляем "задумался"
        status_msg = await m.answer(f"⏳ Загружаю файл... <code>{safe_html_text(os.path.basename(abs_path))}</code>", parse_mode="HTML")
        
        # Отправляем файл
        async with aiofiles.open(abs_path, 'rb') as f:
            file_data = await f.read()
        
        file = BufferedInputFile(file=file_data, filename=os.path.basename(abs_path))
        
        # Определяем тип файла
        if abs_path.endswith('.py'):
            await status_msg.delete()
            await m.answer_document(document=file, caption=f"📄 <code>{safe_html_text(abs_path)}</code>\n📦 Размер: {file_size / 1024:.1f} KB")
        elif abs_path.endswith(('.txt', '.log', '.json', '.md', '.csv')):
            await status_msg.delete()
            await m.answer_document(document=file, caption=f"📄 <code>{safe_html_text(abs_path)}</code>\n📦 Размер: {file_size / 1024:.1f} KB")
        else:
            await status_msg.delete()
            await m.answer_document(document=file, caption=f"📄 <code>{safe_html_text(abs_path)}</code>\n📦 Размер: {file_size / 1024:.1f} KB")
        
        logger.info(f"📤 Upload: {abs_path} ({file_size} байт)")
        
    except Exception as e:
        await m.answer(f"{EMOJI['error']} Ошибка: {safe_html_text(str(e))}", parse_mode="HTML")
        logger.error(f"❌ Ошибка /upload: {e}")

@dp.message(Command("download"))
async def cmd_download(m: Message):
    """Скачать файл из Telegram на сервер: /download <путь> (ответом на файл)"""
    if m.from_user.id != OWNER_ID_INT:
        return
    
    if not m.reply_to_message or not m.reply_to_message.document:
        await m.answer(
            f"{EMOJI['info']} <b>Скачивание файла на сервер:</b>\n\n"
            f"1️⃣ Отправь файл боту\n"
            f"2️⃣ Ответь на него командой:\n"
            f"<code>/download &lt;путь сохранения&gt;</code>\n\n"
            f"<b>Пример:</b>\n"
            f"<code>/download /opt/render/project/src/config.json</code>\n\n"
            f"<i>Макс. размер: 50 MB</i>",
            parse_mode="HTML"
        )
        return
    
    parts = m.text.split(maxsplit=1)
    
    if len(parts) < 2:
        await m.answer(f"{EMOJI['warning']} Укажи путь сохранения: <code>/download &lt;путь&gt;</code>", parse_mode="HTML")
        return
    
    save_path = parts[1].strip()
    
    # Проверка безопасности
    is_safe, error_msg = is_path_safe(save_path)
    if not is_safe:
        await m.answer(error_msg, parse_mode="HTML")
        return
    
    try:
        abs_path = os.path.abspath(save_path)
        
        # Создаём директории если нужно
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
        
        file = m.reply_to_message.document
        file_size = file.file_size
        
        if file_size > MAX_FILE_SIZE:
            await m.answer(
                f"{EMOJI['warning']} <b>Файл слишком большой!</b>\n\n"
                f"📦 Размер: <b>{file_size / 1024 / 1024:.1f} MB</b>\n"
                f"⚠️ Максимум: <b>{MAX_FILE_SIZE / 1024 / 1024:.0f} MB</b>",
                parse_mode="HTML"
            )
            return
        
        # Отправляем "задумался"
        status_msg = await m.answer(f"⏳ Скачиваю файл... <code>{safe_html_text(file.file_name)}</code>", parse_mode="HTML")
        
        # Скачиваем файл
        file_data = await bot.get_file(file.file_id)
        file_content = await bot.download_file(file_data.file_path)
        
        # Сохраняем на сервер
        async with aiofiles.open(abs_path, 'wb') as f:
            await f.write(file_content.read())
        
        await status_msg.edit_text(
            f"{EMOJI['check']} <b>Файл сохранён!</b>\n\n"
            f"📄 Имя: <code>{safe_html_text(file.file_name)}</code>\n"
            f"📁 Путь: <code>{safe_html_text(abs_path)}</code>\n"
            f"📦 Размер: <b>{file_size / 1024:.1f} KB</b>",
            parse_mode="HTML"
        )
        
        logger.info(f"📥 Download: {file.file_name} → {abs_path} ({file_size} байт)")
        
    except Exception as e:
        await m.answer(f"{EMOJI['error']} Ошибка: {safe_html_text(str(e))}", parse_mode="HTML")
        logger.error(f"❌ Ошибка /download: {e}")

@dp.message(Command("cat"))
async def cmd_cat(m: Message):
    """Показать содержимое файла: /cat <путь>"""
    if m.from_user.id != OWNER_ID_INT:
        return
    
    parts = m.text.split(maxsplit=1)
    
    if len(parts) < 2:
        await m.answer(f"{EMOJI['warning']} Используй: <code>/cat &lt;путь к файлу&gt;</code>", parse_mode="HTML")
        return
    
    file_path = parts[1].strip()
    
    # Проверка безопасности
    is_safe, error_msg = is_path_safe(file_path)
    if not is_safe:
        await m.answer(error_msg, parse_mode="HTML")
        return
    
    try:
        abs_path = os.path.abspath(file_path)
        
        if not os.path.exists(abs_path):
            await m.answer(f"{EMOJI['error']} Файл не найден: <code>{safe_html_text(abs_path)}</code>", parse_mode="HTML")
            return
        
        if os.path.isdir(abs_path):
            await m.answer(f"{EMOJI['error']} Это директория, укажите файл: <code>{safe_html_text(abs_path)}</code>", parse_mode="HTML")
            return
        
        file_size = os.path.getsize(abs_path)
        
        if file_size > 10 * 1024 * 1024:  # 10 MB лимит для текста
            await m.answer(
                f"{EMOJI['warning']} <b>Файл слишком большой для просмотра!</b>\n\n"
                f"📦 Размер: <b>{file_size / 1024 / 1024:.1f} MB</b>\n"
                f"⚠️ Максимум: <b>10 MB</b>\n\n"
                f"<i>Используй /upload чтобы скачать файл</i>",
                parse_mode="HTML"
            )
            return
        
        # Читаем файл
        async with aiofiles.open(abs_path, 'r', encoding='utf-8', errors='replace') as f:
            content = await f.read()
        
        # Форматируем вывод
        txt = f"{EMOJI['file']} <b>Содержимое файла:</b>\n"
        txt += f"📁 Путь: <code>{safe_html_text(abs_path)}</code>\n"
        txt += f"📦 Размер: <b>{file_size / 1024:.1f} KB</b>\n\n"
        txt += f"<code>{safe_html_text(content[:4000])}</code>"  # Ограничиваем вывод
        
        if len(content) > 4000:
            txt += f"\n\n<i>...обрезано до 4000 символов. Используй /upload для полного файла</i>"
        
        await m.answer(txt, parse_mode="HTML")
        
    except Exception as e:
        await m.answer(f"{EMOJI['error']} Ошибка: {safe_html_text(str(e))}", parse_mode="HTML")
        logger.error(f"❌ Ошибка /cat: {e}")

@dp.message(Command("rm"))
async def cmd_rm(m: Message):
    """Удалить файл: /rm <путь>"""
    if m.from_user.id != OWNER_ID_INT:
        return
    
    parts = m.text.split(maxsplit=1)
    
    if len(parts) < 2:
        await m.answer(f"{EMOJI['warning']} Используй: <code>/rm &lt;путь к файлу&gt;</code>", parse_mode="HTML")
        return
    
    file_path = parts[1].strip()
    
    # Проверка безопасности
    is_safe, error_msg = is_path_safe(file_path)
    if not is_safe:
        await m.answer(error_msg, parse_mode="HTML")
        return
    
    # Дополнительная проверка на опасные пути
    if any(x in file_path.lower() for x in ["requirements.txt", "bot.py", ".env", "config"]):
        await m.answer(
            f"{EMOJI['warning']} <b>⚠️ Внимание!</b>\n\n"
            f"Вы пытаетесь удалить важный файл системы!\n"
            f"Это может сломать бота.\n\n"
            f"Для подтверждения отправьте: <code>/rmforce {safe_html_text(file_path)}</code>",
            parse_mode="HTML"
        )
        return
    
    try:
        abs_path = os.path.abspath(file_path)
        
        if not os.path.exists(abs_path):
            await m.answer(f"{EMOJI['error']} Файл не найден: <code>{safe_html_text(abs_path)}</code>", parse_mode="HTML")
            return
        
        if os.path.isdir(abs_path):
            await m.answer(f"{EMOJI['error']} Это директория. Используйте /shell rm -rf для удаления папок.", parse_mode="HTML")
            return
        
        os.remove(abs_path)
        
        await m.answer(
            f"{EMOJI['check']} <b>Файл удалён!</b>\n\n"
            f"📁 Путь: <code>{safe_html_text(abs_path)}</code>",
            parse_mode="HTML"
        )
        
        logger.info(f"🗑️ RM: {abs_path}")
        
    except Exception as e:
        await m.answer(f"{EMOJI['error']} Ошибка: {safe_html_text(str(e))}", parse_mode="HTML")
        logger.error(f"❌ Ошибка /rm: {e}")

@dp.message(Command("rmforce"))
async def cmd_rmforce(m: Message):
    """Принудительное удаление важных файлов: /rmforce <путь>"""
    if m.from_user.id != OWNER_ID_INT:
        return
    
    parts = m.text.split(maxsplit=1)
    
    if len(parts) < 2:
        await m.answer(f"{EMOJI['warning']} Используй: <code>/rmforce &lt;путь&gt;</code>", parse_mode="HTML")
        return
    
    file_path = parts[1].strip()
    
    is_safe, error_msg = is_path_safe(file_path)
    if not is_safe:
        await m.answer(error_msg, parse_mode="HTML")
        return
    
    try:
        abs_path = os.path.abspath(file_path)
        os.remove(abs_path)
        
        await m.answer(f"{EMOJI['check']} Файл удалён: <code>{safe_html_text(abs_path)}</code>", parse_mode="HTML")
        logger.warning(f"🗑️ RMFORCE: {abs_path}")
        
    except Exception as e:
        await m.answer(f"{EMOJI['error']} Ошибка: {safe_html_text(str(e))}", parse_mode="HTML")
        logger.error(f"❌ Ошибка /rmforce: {e}")


@dp.message(Command("loglevel"))
async def cmd_loglevel(m: Message):
    if m.from_user.id != OWNER_ID_INT: return
    parts = m.text.split()
    if len(parts) != 2:
        await m.answer(f"{EMOJI['log']} <b>Уровень логов:</b>\n\n<code>/loglevel debug</code> — все логи\n<code>/loglevel info</code> — INFO и выше (рекомендуется)\n<code>/loglevel warning</code> — предупреждения и ошибки\n<code>/loglevel error</code> — только ошибки", parse_mode="HTML")
        return
    level_name = parts[1].upper()
    level_map = {"DEBUG": logging.DEBUG, "ALL": logging.DEBUG, "INFO": logging.INFO, "WARNING": logging.WARNING, "ERROR": logging.ERROR, "CRITICAL": logging.CRITICAL}
    if level_name not in level_map:
        await m.answer(f"{EMOJI['warning']} Неверный уровень. Доступно: debug, info, warning, error", parse_mode="HTML")
        return
    logging.getLogger().setLevel(level_map[level_name])
    if log_handler: log_handler.set_level(level_map[level_name])
    await m.answer(f"{EMOJI['check']} Уровень логов изменён на: <b>{level_name}</b>", parse_mode="HTML")
    logger.info(f"📊 Уровень логов изменён пользователем на: {level_name}")

@dp.message(Command("dbstats"))
async def cmd_dbstats(m: Message):
    """Показывает статистику использования MongoDB"""
    if m.from_user.id != OWNER_ID_INT or db is None:
        return
    
    try:
        stats = db.command("dbstats")
        users_count = db.users.count_documents({})
        forwarded_count = db.forwarded.count_documents({})
        settings_count = db[SETTINGS_COLLECTION].count_documents({})
        
        # Конвертируем байты в читаемый формат
        def format_size(bytes_val):
            for unit in ['B', 'KB', 'MB', 'GB']:
                if bytes_val < 1024:
                    return f"{bytes_val:.1f} {unit}"
                bytes_val /= 1024
            return f"{bytes_val:.1f} TB"
        
        txt = f"{EMOJI['db']} <b>Статистика MongoDB:</b>\n\n"
        txt += f"💾 Всего занято: <b>{format_size(stats['storageSize'])}</b>\n"
        txt += f"📊 Всего документов: <b>{stats['objects']}</b>\n"
        txt += f"👥 Пользователей: <b>{users_count}</b>\n"
        txt += f"📬 Forwarded: <b>{forwarded_count}</b>\n"
        txt += f"⚙️ Настроек: <b>{settings_count}</b>\n\n"
        txt += f"<i>Лимит тарифа: 512 MB</i>"
        
        await m.answer(txt, parse_mode="HTML")
    except Exception as e:
        await m.answer(f"{EMOJI['error']} Ошибка: {str(e)[:100]}", parse_mode="HTML")


@dp.message(Command("convert"))
async def cmd_convert_start(m: Message):
    if m.from_user.id != OWNER_ID_INT: await cmd_start(m); return
    awaiting_conversion.add(m.from_user.id)
    await m.answer("🔄 <b>Введите теги для конвертации:</b>\n\nПример: <code>anthro female red_eyes</code>\n\n<i>Отправь теги следующим сообщением</i>", parse_mode="HTML")

@dp.message(lambda m: m.from_user.id in awaiting_conversion)
async def handle_conversion_input(m: Message):
    user_id = m.from_user.id
    if user_id != OWNER_ID_INT: awaiting_conversion.discard(user_id); return
    try:
        result = convert_e621_tags(m.text.strip())
        await m.answer(f"<code>{result}</code>", parse_mode="HTML")
    except Exception as e:
        logger.error("Ошибка конвертации: " + str(e))
        await m.answer(f"{EMOJI['error']} Ошибка", parse_mode="HTML")
    finally: awaiting_conversion.discard(user_id)

@dp.message(Command("help"))
async def cmd_help(message: Message):
    if message.from_user.id != OWNER_ID_INT: return
    txt = f"{EMOJI['info']} <b>Справка:</b>\n\n"
    txt += f"<b>{EMOJI['search']} Основные:</b>\n/check — Найти лоры\n/status — Настройки\n/help — Справка\n\n"
    txt += f"<b>{EMOJI['settings']} Настройки:</b>\n/setdays N — Порог дней\n/addtag &lt;тег&gt; — Добавить тег\n/rmtag &lt;тег&gt; — Удалить тег\n/tags — Теги\n\n"
    txt += f"<b>{EMOJI['log']} Логи:</b>\n/loglevel &lt;уровень&gt; — info/warning/error/debug\n\n"
    txt += f"<b>{EMOJI['users']} Пользователи:</b>\n/users — Показать всех, кто писал боту\n\n"
    txt += f"<b>{EMOJI['stop']} Управление:</b>\n/stop &lt;пароль&gt; — Остановить\n/start — Запустить"
    await message.answer(txt, parse_mode="HTML")

@dp.message(Command("check"))
async def cmd_check(message: Message):
    if message.from_user.id != OWNER_ID_INT: return
    if not bot_running: await message.answer(EMOJI["error"] + " Бот остановлен.", parse_mode="HTML"); return
    user_id = message.from_user.id
    settings = get_settings(user_id)
    if settings.get("is_checking"): await message.answer(EMOJI["lock"] + " Проверка уже запущена!", parse_mode="HTML"); return
    can_use, remaining = check_cooldown(user_id)
    if not can_use: await message.answer(EMOJI["clock"] + f" Кулдаун! Повтори через <b>{remaining}</b> сек.", parse_mode="HTML"); return
    try:
        update_settings(user_id, is_checking=True)
        await message.answer(EMOJI["search"] + " Поиск запущен...", parse_mode="HTML")
        min_days, tags = settings["min_days"], settings["tags"]
        if tags:
            all_loras, total_pages = [], 0
            for tag in tags:
                loras, pages = find_loras_by_tag(tag, min_days)
                all_loras.extend(loras); total_pages += pages
        else:
            all_loras, total_pages = find_all_loras(min_days)
        if not all_loras: 
            await message.answer(EMOJI["check"] + " Лоры не найдены.")
            update_settings(user_id, is_checking=False, last_check=time.time())
            return
        all_loras.sort(key=lambda x: x["days"], reverse=True)
        
        # 📤 Отправляем результаты
        if len(all_loras) > EXPORT_THRESHOLD:
            await message.answer(EMOJI["file"] + f" Лор много (<b>{len(all_loras)}</b>), отправляю файлом...", parse_mode="HTML")
            await send_loras_as_file(message, all_loras, total_pages, min_days, tags)
        else:
            await send_loras_to_chat(message, all_loras, total_pages)
        
        # 🗄️ Сохраняем в кэш если лор <50 (для /export) — ВЫНОСИМ ЗА ПРЕДЕЛЫ if/else
        if len(all_loras) < 50:
            global last_search_results, last_search_meta
            last_search_results = all_loras.copy()
            last_search_meta = {
                "min_days": min_days,
                "tags": tags.copy(),
                "pages": total_pages,
                "timestamp": datetime.now(timezone(timedelta(hours=3)))
            }
            logger.info(f"💾 Сохранено {len(all_loras)} лор в кэш для /export")
        
        # ⏱️ Обновляем кулдаун (ОДИН РАЗ)
        update_settings(user_id, last_check=time.time())
        logger.info("✅ Поиск завершён: " + str(len(all_loras)) + " лор")
    except Exception as e:
        logger.error("❌ Ошибка в /check: " + str(e), exc_info=True)
        await message.answer(EMOJI["error"] + " Ошибка: " + str(e)[:100], parse_mode="HTML")
    finally: 
        update_settings(user_id, is_checking=False)

# ================= HQRADIO ПАРСИНГ =================

async def fetch_hq_radio() -> dict | None:
    """Парсит трек из HLS-плейлиста HQRadio (.m3u8)"""
    import re
    
    try:
        # URL плейлиста (обновляй если изменится)
        playlist_url = "https://hls-01-radiorecord.hostingradio.ru/record-phonk/112/playlist.m3u8"
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "*/*",
            "Referer": "https://hqradio.ru/",
        }
        
        response = requests.get(playlist_url, headers=headers, timeout=10)
        response.raise_for_status()
        
        playlist_text = response.text
        
        # 🔍 Ищем все #EXT-X-DATERANGE с метаданными
        # Формат: #EXT-X-DATERANGE:...X-ARTIST="...",X-TITLE="...",X-COVER-IMAGE-URL="..."...
        pattern = r'#EXT-X-DATERANGE:[^#]*X-ARTIST="([^"]*)"[^#]*X-TITLE="([^"]*)"'
        matches = re.findall(pattern, playlist_text)
        
        if not matches:
            # Пробуем альтернативный порядок атрибутов
            pattern = r'#EXT-X-DATERANGE:[^#]*X-TITLE="([^"]*)"[^#]*X-ARTIST="([^"]*)"'
            matches = re.findall(pattern, playlist_text)
            if matches:
                # Меняем местами: (title, artist) → (artist, title)
                matches = [(m[1], m[0]) for m in matches]
        
        if not matches:
            logger.warning("⚠️ Не найдено метаданных в плейлисте")
            return None
        
        # Берём ПОСЛЕДНИЙ трек (самый свежий)
        artist, title = matches[-1]
        
        # 🖼️ Ищем обложку в том же теге
        cover_pattern = r'#EXT-X-DATERANGE:[^#]*X-COVER-IMAGE-URL="([^"]*)"'
        cover_matches = re.findall(cover_pattern, playlist_text)
        cover_url = cover_matches[-1] if cover_matches else None
        
        # ⏱️ Ищем длительность (опционально)
        duration_pattern = r'#EXTINF:([\d.]+),'
        duration_matches = re.findall(duration_pattern, playlist_text)
        duration = None
        if duration_matches:
            # Конвертируем секунды в ММ:СС
            total_sec = float(duration_matches[-1])
            mins = int(total_sec // 60)
            secs = int(total_sec % 60)
            duration = f"{mins}:{secs:02d}"
        
        track_info = {
            "artist": artist.strip() if artist else "Unknown Artist",
            "title": title.strip() if title else "Unknown Title",
            "parsed_at": datetime.now(timezone(timedelta(hours=3))),
            "source": "hqradio.ru (HLS)"
        }
        
        if cover_url:
            # Исправляем относительные пути
            if cover_url.startswith("//"):
                cover_url = "https:" + cover_url
            elif cover_url.startswith("/"):
                cover_url = "https://hqradio.ru" + cover_url
            elif not cover_url.startswith("http"):
                cover_url = "https://hqradio.ru" + cover_url
            track_info["cover_url"] = cover_url
        
        if duration:
            track_info["duration"] = duration
        
        logger.info(f"✅ HLS: {track_info['artist']} - {track_info['title']}")
        return track_info
        
    except requests.RequestException as e:
        logger.warning(f"⚠️ Ошибка загрузки плейлиста: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Ошибка парсинга HLS: {e}", exc_info=True)
        return None

async def get_latest_playlist_url() -> str:
    """Получает актуальную ссылку на плейлист с главной страницы"""
    try:
        response = requests.get("https://www.radiorecord.ru/station/phonk", timeout=10)
        # Ищем ссылку на .m3u8 в HTML
        match = re.search(r'(https://[^\s"\'<>]+\.m3u8[^"\']*)', response.text)
        if match:
            return match.group(1).split('?')[0]  # Убираем параметры если нужно
    except:
        pass
    # Fallback на старую ссылку
    return "https://hls-01-radiorecord.hostingradio.ru/record-phonk/112/playlist.m3u8"

def parse_hq_radio(html: str) -> dict | None:
    """Парсит информацию о текущем треке с HQRadio"""
    try:
        soup = BeautifulSoup(html, "html.parser")
        
        # 🔍 Ищем элемент с id="track"
        track_elem = soup.find("div", id="track")
        
        if not track_elem:
            logger.warning("⚠️ Элемент #track не найден на странице")
            return None
        
        # Получаем текст в формате "ARTIST - TITLE"
        track_text = track_elem.get_text(strip=True)
        
        if not track_text:
            logger.warning("⚠️ Элемент #track пустой")
            return None
        
        logger.info(f"🎵 Найдено: {track_text}")
        
        # Разделяем по " - " (первое вхождение)
        track_info = {}
        
        if " - " in track_text:
            parts = track_text.split(" - ", 1)
            if len(parts) == 2:
                track_info["artist"] = parts[0].strip()
                track_info["title"] = parts[1].strip()
            else:
                track_info["title"] = track_text
        else:
            track_info["title"] = track_text
        
        # 🖼️ Ищем обложку в <i class="cover" style="background-image: url(...)">
        cover_elem = soup.find("i", class_="cover")
        
        if cover_elem:
            # Получаем style атрибут
            style = cover_elem.get("style", "")
            
            # Ищем URL в background-image: url("...")
            import re
            url_match = re.search(r'background-image:\s*url\(["\']?([^"\')]+)["\']?\)', style)
            
            if url_match:
                cover_url = url_match.group(1)
                # Исправляем HTML-сущности (&quot; → ")
                cover_url = cover_url.replace("&quot;", '"').replace('"', '')
                
                # Исправляем относительные пути
                if cover_url.startswith("//"):
                    cover_url = "https:" + cover_url
                elif cover_url.startswith("/"):
                    cover_url = "https://hqradio.ru" + cover_url
                elif not cover_url.startswith("http"):
                    cover_url = "https://hqradio.ru" + cover_url
                
                track_info["cover_url"] = cover_url
                logger.info(f"🖼️ Обложка найдена: {cover_url}")
            else:
                logger.warning("⚠️ Не удалось извлечь URL из style атрибута")
        else:
            logger.warning("⚠️ Элемент <i class='cover'> не найден")
            
            # Fallback: ищем og:image
            og_image = soup.find("meta", property="og:image")
            if og_image:
                track_info["cover_url"] = og_image.get("content")
                logger.info(f"🖼️ Обложка из og:image: {track_info['cover_url']}")
        
        # ⏱️ Ищем длительность (если есть)
        duration_elem = soup.find("span", class_="duration")
        if not duration_elem:
            duration_elem = soup.find("span", class_="track-duration")
        if not duration_elem:
            duration_elem = soup.find("time")
        
        if duration_elem:
            track_info["duration"] = duration_elem.get_text(strip=True)
        
        # Добавляем метаданные
        track_info["parsed_at"] = datetime.now(timezone(timedelta(hours=3)))
        track_info["source"] = "hqradio.ru"
        track_info["raw_text"] = track_text
        
        logger.info(f"✅ Спаршено: {track_info.get('artist', '?')} - {track_info.get('title', '?')}")
        return track_info
        
    except Exception as e:
        logger.error(f"❌ Ошибка парсинга HQRadio: {e}", exc_info=True)
        return None  # ← Обязательно вернуть None при ошибке!
        

@dp.message(Command("parse"))
async def cmd_parse(m: Message):
    """Включить/выключить парсинг HQRadio: /parse on/off/status"""
    global parse_hq_enabled, last_parsed_track, last_parse_time  # ← ВСЕ global в начале!
    
    if m.from_user.id != OWNER_ID_INT:
        return
    
    parts = m.text.split()
    action = parts[1].lower() if len(parts) > 1 else "status"
    
    if action in ["on", "true", "1", "вкл", "включить"]:
        parse_hq_enabled = True
        track = await fetch_hq_radio()
        if track:
            last_parsed_track = track  # ← Теперь можно присваивать
            last_parse_time = time.time()
            txt = f"{PREMIUM_EMOJI['sparkle']} <b>Парсинг включён!</b>\n\n"
            txt += f"🎵 <b>{safe_html_text(track.get('title', 'Unknown'))}</b>\n"
            if track.get("artist"):
                txt += f"🎤 {safe_html_text(track['artist'])}\n"
            if track.get("duration"):
                txt += f"⏱️ {track['duration']}\n"
            if track.get("cover_url"):
                txt += f"\n<i>Обложка доступна</i>"
            await m.answer(txt, parse_mode="HTML")
        else:
            await m.answer(f"{EMOJI['check']} <b>Парсинг включён!</b>\n\n<i>Следующий трек появится после обновления</i>", parse_mode="HTML")
        logger.info("🎵 HQRadio парсинг включён")
        
    elif action in ["off", "false", "0", "выкл", "выключить"]:
        parse_hq_enabled = False
        await m.answer(f"{EMOJI['stop']} <b>Парсинг выключен</b>", parse_mode="HTML")
        logger.info("🎵 HQRadio парсинг выключен")
        
    elif action in ["status", "статус", "стат"]:
        status = "🟢 ВКЛ" if parse_hq_enabled else "🔴 ВЫКЛ"
        txt = f"{EMOJI['info']} <b>Статус парсинга HQRadio:</b> {status}\n\n"
        if last_parsed_track:  # ← Чтение без global — это ОК
            txt += f"🎵 Последний трек:\n"
            txt += f"• {safe_html_text(last_parsed_track.get('title', 'Unknown'))}\n"
            if last_parsed_track.get("artist"):
                txt += f"• {safe_html_text(last_parsed_track['artist'])}\n"
            if last_parsed_track.get("duration"):
                txt += f"• ⏱️ {last_parsed_track['duration']}\n"
            parsed_time = last_parsed_track.get("parsed_at")
            if parsed_time:
                txt += f"\n<i>Обновлено: {parsed_time.strftime('%H:%M:%S')} МСК</i>"
        else:
            txt += "<i>Треки ещё не парсились</i>"
        txt += f"\n\n<b>Управление:</b>\n<code>/parse on</code> — включить\n<code>/parse off</code> — выключить"
        await m.answer(txt, parse_mode="HTML")
        
    elif action in ["now", "сейчас", "текущий"]:
        await m.answer("⏳ Парсю страницу...", parse_mode="HTML")
        track = await fetch_hq_radio()
        if track:
            last_parsed_track = track  # ← Присваивание, global уже объявлен выше
            last_parse_time = time.time()
            txt = f"{PREMIUM_EMOJI['sparkle']} <b>Сейчас играет:</b>\n\n"
            txt += f"🎵 <b>{safe_html_text(track.get('title', 'Unknown'))}</b>\n"
            if track.get("artist"):
                txt += f"🎤 {safe_html_text(track['artist'])}\n"
            if track.get("duration"):
                txt += f"⏱️ {track['duration']}\n"
            if track.get("cover_url"):
                try:
                    await bot.send_photo(
                        chat_id=m.chat.id,
                        photo=track["cover_url"],
                        caption=txt,
                        parse_mode="HTML"
                    )
                    return
                except:
                    txt += f"\n\n<i>⚠️ Не удалось загрузить обложку</i>"
            await m.answer(txt, parse_mode="HTML")
        else:
            await m.answer(f"{EMOJI['warning']} Не удалось получить информацию о треке", parse_mode="HTML")
    else:
        await m.answer(
            f"{EMOJI['info']} <b>Управление парсингом HQRadio:</b>\n\n"
            f"<code>/parse on</code> — включить парсинг\n"
            f"<code>/parse off</code> — выключить парсинг\n"
            f"<code>/parse status</code> — показать статус и последний трек\n"
            f"<code>/parse now</code> — спарсить прямо сейчас",
            parse_mode="HTML"
        )

@dp.message(Command("track"))
async def cmd_track(m: Message):
    """Быстро отправить текущий трек в чат"""
    if m.from_user.id != OWNER_ID_INT:
        return
    
    if not last_parsed_track:
        await m.answer(f"{EMOJI['info']} Трек ещё не спаршен. Включи /parse on", parse_mode="HTML")
        return
    
    track = last_parsed_track
    txt = f"🎧 <b>Сейчас играет на Phonk Radio:</b>\n\n"
    txt += f"🎵 <b>{safe_html_text(track.get('title', 'Unknown'))}</b>\n"
    if track.get("artist"):
        txt += f"🎤 {safe_html_text(track['artist'])}\n"
    if track.get("duration"):
        txt += f"⏱️ {track['duration']}\n"
    txt += f"\n<i>via @looniesbot</i>"
    
    # Если есть обложка — отправляем фото
    if track.get("cover_url"):
        try:
            await bot.send_photo(chat_id=m.chat.id, photo=track["cover_url"], caption=txt, parse_mode="HTML")
            return
        except:
            pass
    
    await m.answer(txt, parse_mode="HTML")

@dp.message(Command("setdays"))
async def cmd_setdays(message: Message):
    if message.from_user.id != OWNER_ID_INT: return
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit() or int(parts[1]) < 0:
        await message.answer(EMOJI["warning"] + " Используй: <code>/setdays &lt;число&gt;</code>", parse_mode="HTML"); return
    update_settings(message.from_user.id, min_days=int(parts[1]))
    days_text = "все лоры" if int(parts[1])==0 else ">=" + parts[1] + " дней"
    await message.answer(EMOJI["check"] + f" Порог: <b>{days_text}</b>", parse_mode="HTML")

@dp.message(Command("addtag"))
async def cmd_addtag(message: Message):
    if message.from_user.id != OWNER_ID_INT: return
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].strip().lower().isalnum():
        await message.answer(EMOJI["warning"] + " Используй: <code>/addtag &lt;название&gt;</code>", parse_mode="HTML"); return
    new_tag = parts[1].strip().lower()
    settings = get_settings(message.from_user.id)
    if new_tag in [t.lower() for t in settings["tags"]]: await message.answer(EMOJI["warning"] + " Тег уже в списке", parse_mode="HTML"); return
    settings["tags"].append(new_tag)
    update_settings(message.from_user.id, tags=settings["tags"])
    await message.answer(EMOJI["check"] + f" Тег <b>{new_tag}</b> добавлен. Текущие: {', '.join(settings['tags'])}", parse_mode="HTML")

@dp.message(Command("rmtag"))
async def cmd_rmtag(message: Message):
    if message.from_user.id != OWNER_ID_INT: return
    parts = message.text.split()
    if len(parts) != 2: await message.answer(EMOJI["warning"] + " Используй: <code>/rmtag &lt;название&gt;</code>", parse_mode="HTML"); return
    tag_to_remove = parts[1].strip().lower()
    settings = get_settings(message.from_user.id)
    tag = next((t for t in settings["tags"] if t.lower() == tag_to_remove), None)
    if not tag: await message.answer(EMOJI["warning"] + " Тег не найден", parse_mode="HTML"); return
    settings["tags"].remove(tag)
    update_settings(message.from_user.id, tags=settings["tags"])
    await message.answer(EMOJI["check"] + f" Тег <b>{tag}</b> удалён. Текущие: {', '.join(settings['tags']) if settings['tags'] else 'нет'}", parse_mode="HTML")

@dp.message(Command("tags"))
async def cmd_tags(message: Message):
    if message.from_user.id != OWNER_ID_INT: return
    settings = get_settings(message.from_user.id)
    if not settings["tags"]: await message.answer(EMOJI["tag"] + " <b>Теги:</b>\n<i>нет</i>\n\nИспользуй /addtag &lt;тег&gt;", parse_mode="HTML"); return
    txt = EMOJI["tag"] + " <b>Теги:</b>\n" + "\n".join(f"{i}. <code>{t}</code>" for i,t in enumerate(settings["tags"], 1))
    await message.answer(txt, parse_mode="HTML")

@dp.message(Command("export"))
async def cmd_export(m: Message):
    """Экспортирует лоры из последнего поиска в файл (только если <50 лор)"""
    if m.from_user.id != OWNER_ID_INT:
        return
    
    global last_search_results, last_search_meta
    
    if not last_search_results or not last_search_meta:
        # ← ИСПРАВЛЕНО: &lt; вместо <
        await m.answer(
            f"{EMOJI['warning']} Нет данных для экспорта.\n"
            f"Сначала выполните <code>/check</code> с результатом &lt;50 лор.",
            parse_mode="HTML"
        )
        return
    
    content = make_export_file(
        last_search_results,
        last_search_meta["min_days"],
        last_search_meta["tags"]
    )
    
    timestamp = last_search_meta["timestamp"].strftime("%Y%m%d_%H%M")
    filename = f"loonie_export_{timestamp}.txt"
    file = BufferedInputFile(file=content, filename=filename)
    
    caption = f"{PREMIUM_EMOJI['sparkle']} <b>Экспорт лор</b>\n"
    caption += f"📅 {last_search_meta['timestamp'].strftime('%d.%m %H:%M')} МСК\n"
    caption += f"📊 Лор: {len(last_search_results)}\n"
    caption += f"🎯 Порог: >= {last_search_meta['min_days']} дней"
    if last_search_meta["tags"]:
        caption += f"\n🏷️ Теги: {', '.join(last_search_meta['tags'])}"
    caption += f"\n\n<i>Файл готов к использованию с /dellora</i>"
    
    await m.answer_document(document=file, caption=caption, parse_mode="HTML")
    logger.info(f"📤 Экспортировано {len(last_search_results)} лор в файл {filename}")


@dp.message(Command("status"))
async def cmd_status(message: Message):
    if message.from_user.id != OWNER_ID_INT: return
    settings = get_settings(message.from_user.id)
    moscow_time = datetime.now(timezone(timedelta(hours=3))).strftime('%Y-%m-%d %H:%M:%S')
    txt = f"{EMOJI['settings']} <b>Настройки:</b>\n🕐 МСК {moscow_time}\n"
    txt += EMOJI["days"] + f" Порог: <b>{('все лоры' if settings['min_days']==0 else '>=' + str(settings['min_days']) + ' дней')}</b>\n"
    txt += EMOJI["tag"] + f" Теги: <b>{(', '.join(settings['tags']) if settings['tags'] else 'нет (все лоры)')}</b>\n"
    can_use, remaining = check_cooldown(message.from_user.id)
    txt += f"⏱️ Кулдаун: <b>{'готов' if can_use else str(remaining) + ' сек'}</b>\n"
    txt += EMOJI["check" if bot_running else "stop"] + f" Бот: <b>{'Активен' if bot_running else 'ОСТАНОВЛЕН'}</b>"
    txt += f"\n👥 Пользователей: <b>{len(known_users)}</b>"
    if log_handler: txt += f"\n📊 Лог-уровень: <b>{logging.getLevelName(log_handler.min_level)}</b>"
    # ← ИСПРАВЛЕНО НИЖЕ:
    if db is not None:  # ← Было "if db:", стало "if db is not None:"
        txt += f"\n{PREMIUM_EMOJI['sparkle']} БД: <b>MongoDB подключена</b>"
    else:
        txt += f"\n{EMOJI['warning']} БД: <b>не подключена (данные сбросятся при рестарте)</b>"
    await message.answer(txt, parse_mode="HTML")

@dp.message(Command("stop"))
async def cmd_stop(message: Message):
    if message.from_user.id != OWNER_ID_INT: return
    parts = message.text.split()
    if len(parts) != 2 or parts[1] != STOP_PASSWORD:
        await message.answer(EMOJI["stop"] + f" Используй: <code>/stop {STOP_PASSWORD}</code>", parse_mode="HTML"); return
    global bot_running
    bot_running = False
    await message.answer(EMOJI["stop"] + " <b>БОТ ОСТАНОВЛЕН!</b>\nНапиши /start для запуска.", parse_mode="HTML")
    logger.warning("🛑 Бот остановлен владельцем")

@dp.message(Command("start"))
async def cmd_start(m: Message):
    if m.from_user.id == OWNER_ID_INT:
        
        global bot_running
        if not bot_running: bot_running = True; logger.info("🔄 Bot resumed by owner")
        await m.answer(f"{EMOJI['check']} <b>Бот активен!</b>\n/help — команды", parse_mode="HTML")
        return
    ru = "🇷🇺 Если есть вопросы или что-то подобное — пишите, отвечу по возможности! "
    en = "🇬🇧 If you have questions or anything like that — write, I'll respond if possible! "
    await m.answer(ru + "\n\n" + en, parse_mode="HTML")

@dp.message()
async def silent_ignore(message: Message):
    """Обрабатывает все необработанные сообщения"""
    if message.from_user.id != OWNER_ID_INT:
        # Для обычных пользователей — показываем обратную связь
        ru = "🇷🇺 Если есть вопросы или что-то подобное — пишите, отвечу по возможности! "
        en = "🇬🇧 If you have questions or anything like that — write, I'll respond if possible! "
        await message.answer(ru + "\n\n" + en, parse_mode="HTML")
        return
    
    # Для владельца — показываем справку по неизвестным командам
    await message.answer(EMOJI["info"] + " Неизвестная команда. /help — справка", parse_mode="HTML")

# ================= WEBHOOK SERVER =================
async def webhook_handler(request):
    try:
        update = await request.json()
        await dp.feed_webhook_update(bot, update)
        return web.Response(text="OK")
    except Exception as e:
        logger.error("Webhook error: " + str(e))
        return web.Response(text="Error", status=500)

async def health_handler(request):
    return web.Response(text="OK - " + ("running" if bot_running else "stopped"))

async def run_web_server():
    app = web.Application()
    webhook_path = "/webhook/" + BOT_TOKEN.split(":")[0]
    app.router.add_post(webhook_path, webhook_handler)
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", "8080"))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info("🌐 Server on port " + str(port))
    ext_url = os.getenv("RENDER_EXTERNAL_URL")
    if ext_url and bot_running:
        wh_url = ext_url + webhook_path
        await bot.set_webhook(wh_url)
        logger.info("✅ Webhook: " + wh_url)

# ================= MAIN =================
async def main():
    # Инициализация (функции уже определены выше)
    init_log_bot()
    mongo_ok = init_mongo()
    load_forwarded()
    load_users()
    load_settings()
    
    await run_web_server()
    moscow_time = datetime.now(timezone(timedelta(hours=3))).strftime('%Y-%m-%d %H:%M:%S')
    db_status = "✅ MongoDB" if mongo_ok else "❌ память"
    logger.info(f"🚀 Bot started! Owner: {OWNER_ID_INT} | Users: {len(known_users)} | Time: МСК {moscow_time} | DB: {db_status}")
    
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Bot stopped by user")