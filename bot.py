import os
import re
import asyncio
import logging
import requests
import time
import json
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, BufferedInputFile
from aiohttp import web

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

# === ЭМОДЗИ ===
EMOJI = {
    "brain": "🧠", "id": "🆔", "days": "🕸️", "delete": "🗑️", "search": "🔍",
    "stats": "📊", "settings": "⚙️", "tag": "🏷️", "clock": "⏰", "check": "✅",
    "warning": "⚠️", "error": "❌", "info": "ℹ️", "file": "📄", "stop": "🛑",
    "restart": "🔄", "lock": "🔒", "users": "👥", "log": "📜", "db": "🗄️"
}

# === ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ===
bot_running = True
user_settings = {}
awaiting_conversion = set()
forwarded_messages = {}
known_users = {}
log_handler = None
mongo_client = None
db = None

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
            count = 0
            for doc in db.forwarded.find():
                forwarded_messages[doc["message_id"]] = doc["user_id"]
                count += 1
            logger.info(f"📦 Загружено {count} пересланных сообщений из MongoDB")
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
        loras = parse_loras_from_html(html, min_days)
        pages_scanned += 1
        if loras:
            all_results.extend(loras)
            logger.info("Стр. " + str(page) + ": найдено " + str(len(loras)) + " лор")
        if not loras and page > 1:
            logger.info("Стр. " + str(page) + ": лор не найдено → завершаю")
            break
        if page < MAX_PAGES: time.sleep(1.0)
    logger.info("=== ВСЕГО === Стр: " + str(pages_scanned) + " | Лор: " + str(len(all_results)))
    return all_results, pages_scanned

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
    # Гарантируем, что user_id — это целое число
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
            "forwarded": False
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
    # Тоже гарантируем тип
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

# ================= ОБРАТНАЯ СВЯЗЬ =================
@dp.message(F.from_user.id != OWNER_ID_INT)
async def handle_user_message(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username or None
    full_name = message.from_user.full_name
    track_user(user_id, username, full_name)
    try:
        forwarded = await message.forward(chat_id=OWNER_ID_INT)
        forwarded_messages[forwarded.message_id] = user_id
        save_forwarded()
        mark_user_forwarded(user_id)
        moscow_time = datetime.now(timezone(timedelta(hours=3))).strftime('%H:%M')
        user_info = f"📬 <b>Сообщение от:</b>\n• Имя: {full_name}\n• Username: @{username or 'нет'}\n• ID: <code>{user_id}</code>\n• Время: 🕐 МСК {moscow_time}\n\n<i>Ответьте на пересланное сообщение чтобы ответить</i>"
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
                await bot.send_message(chat_id=user_id, text=f"📬 {message.text}", parse_mode="HTML")
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
        if not all_loras: await message.answer(EMOJI["check"] + " Лоры не найдены."); update_settings(user_id, is_checking=False, last_check=time.time()); return
        all_loras.sort(key=lambda x: x["days"], reverse=True)
        if len(all_loras) > EXPORT_THRESHOLD:
            await message.answer(EMOJI["file"] + f" Лор много (<b>{len(all_loras)}</b>), отправляю файлом...", parse_mode="HTML")
            await send_loras_as_file(message, all_loras, total_pages, min_days, tags)
        else:
            await send_loras_to_chat(message, all_loras, total_pages)
        update_settings(user_id, last_check=time.time())
        logger.info("✅ Поиск завершён: " + str(len(all_loras)) + " лор")
    except Exception as e:
        logger.error("❌ Ошибка в /check: " + str(e), exc_info=True)
        await message.answer(EMOJI["error"] + " Ошибка: " + str(e)[:100], parse_mode="HTML")
    finally: update_settings(user_id, is_checking=False)

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
        txt += f"\n{EMOJI['db']} БД: <b>MongoDB подключена</b>"
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