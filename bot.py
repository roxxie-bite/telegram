import os
import sys
import re
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
from dotenv import load_dotenv
from aiogram.types import InlineQuery, InlineQueryResultArticle, InputTextMessageContent, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, ChosenInlineResult



# === РУЧНАЯ ЗАГРУЗКА .env (без python-dotenv) ===
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")

if os.path.exists(_env_path):
    with open(_env_path, 'r', encoding='utf-8-sig') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                os.environ[key] = value
    print(f"✅ .env загружен: {_env_path}")
else:
    print(f"❌ .env НЕ НАЙДЕН: {_env_path}")
    print(f"   Текущая папка: {os.getcwd()}")
    print(f"   Папка скрипта: {os.path.dirname(os.path.abspath(__file__))}")

# Теперь проверяем
print(f"🔍 BOT_TOKEN: {'ДА (' + str(len(os.getenv('BOT_TOKEN',''))) + ' символов)' if os.getenv('BOT_TOKEN') else 'НЕТ'}")
print(f"🔍 OWNER_ID: {os.getenv('OWNER_ID') or 'НЕТ'}")


# ================= НАСТРОЙКИ =================
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = os.getenv("OWNER_ID")
STOP_PASSWORD = os.getenv("STOP_PASSWORD", "stop123")
MIN_DAYS_ENV = os.getenv("MIN_DAYS")
LOG_BOT_TOKEN = os.getenv("LOG_BOT_TOKEN")
LOG_CHAT_ID = os.getenv("LOG_CHAT_ID")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
SITE_BASE = "https://lynther.sytes.net"

EXTRA_ALLOWED_AI_USERS = {
    8371541704,
    5802195555  # gvno
}
    

def is_user_allowed(user_id: int, allowed_set: set) -> bool:
    """Проверяет, есть ли пользователь в списке разрешённых"""
    return user_id in allowed_set

# ================= OPENROUTER AI =================
# OpenRouter — единый OpenAI-совместимый шлюз к десяткам провайдеров
# (OpenAI, Anthropic, Google, DeepSeek, Meta, Mistral и т.д.), включая бесплатные модели.
# Это снимает проблему лимита запросов одного провайдера (например Gemini) —
# можно переключаться между моделями/провайдерами одним параметром.
# Документация: https://openrouter.ai/docs
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"

# ================= AI МОДЕЛИ (КАТАЛОГ OPENROUTER) =================
# ⚠️ "name" — это id модели в формате "провайдер/модель" (иногда с суффиксом ":free").
# Каталог OpenRouter часто меняется (особенно бесплатные модели) — актуальный список
# смотри на https://openrouter.ai/models и поправь нужные строки здесь при необходимости.
AVAILABLE_AI_MODELS = {
    # === Авто-роутер OpenRouter (бесплатно, сам выбирает доступную бесплатную модель) ===
    "auto-free": {
        "name": "openrouter/free",
        "display": "🎲 Auto Free",
        "desc": "Авто-выбор бесплатной модели — не зависит от лимита одного провайдера",
        "temp": 0.7,
        "max_tokens": 8192
    },

    # === OpenAI ===
    "gpt-4o": {
        "name": "openai/gpt-4o",
        "display": "🤖 GPT-4o",
        "desc": "Флагман OpenAI, универсальная",
        "temp": 0.7,
        "max_tokens": 16384
    },

    "nex-agi": {
        "name": "nex-agi/nex-n2-pro:free",
        "display": "🚀 NEX AGI N2 Pro",
        "desc": "Новая модель от OpenAI, хороша в коде и логике",
        "temp": 0.7,
        "max_tokens": 16384
    },

    "gpt-4o-mini": {
        "name": "openai/gpt-4o-mini",
        "display": "⚡ GPT-4o mini",
        "desc": "Быстрая и дешёвая версия GPT-4o",
        "temp": 0.7,
        "max_tokens": 16384
    },

    # === Anthropic ===
    "claude-sonnet": {
        "name": "anthropic/claude-sonnet-4.6",
        "display": "🧠 Claude Sonnet 4.6",
        "desc": "Сбалансированная флагманская модель Anthropic",
        "temp": 0.7,
        "max_tokens": 16384
    },
    "claude-haiku": {
        "name": "anthropic/claude-haiku-4.5",
        "display": "🪶 Claude Haiku 4.5",
        "desc": "Быстрая и лёгкая модель Anthropic",
        "temp": 0.7,
        "max_tokens": 16384
    },

    # === Google (через OpenRouter — без лимита прямого Gemini API) ===
    "gemini-flash": {
        "name": "google/gemini-2.5-flash",
        "display": "🚀 Gemini 2.5 Flash",
        "desc": "Google, доступ через OpenRouter",
        "temp": 0.7,
        "max_tokens": 16384
    },
    "gemini-pro": {
        "name": "google/gemini-3-pro-preview",
        "display": "✨ Gemini 3 Pro",
        "desc": "Google, доступ через OpenRouter",
        "temp": 0.7,
        "max_tokens": 16384
    },

    "owl": {
        "name": "openrouter/owl-alpha",
        "display": "🦉 Owl Alpha",
        "desc": "Модель от неизвестного провайдера, хороша в коде и логике",
        "temp": 0.7,
        "max_tokens": 16384
    },


    "qwen": {
        "name": "qwen/qwen3-coder:free",
        "display": "👨‍💻 Qwen3 Coder (free)",
        "desc": "Бесплатная модель, хороша в коде",
        "temp": 0.7,
        "max_tokens": 8192
    },

    "nvidia": {
        "name":"nvidia/nemotron-3-ultra-550b-a55b:free",
        "display": "🎮 NVIDIA NeMo 3 Ultra (free)",
        "desc": "Бесплатная модель от NVIDIA, хороша в коде и логике",
        "temp": 0.7,
        "max_tokens": 8192
    },

    "nvidia-nemotron-3-nano-omni-30b-a3b": {
        "name": "nvidia/nemotron-3-nano-omni-30b-a3b-reasoning:free",
        "display": "🎮 NVIDIA NeMo 3 Nano Omni 30B (free)",
        "desc": "Бесплатная модель от NVIDIA, хороша в коде и логике",
        "temp": 0.7,
        "max_tokens": 8192
    },


    # === DeepSeek ===
    "deepseek-r1": {
        "name": "deepseek/deepseek-r1",
        "display": "🔬 DeepSeek R1",
        "desc": "Рассуждающая модель, хороша в коде и логике",
        "temp": 0.7,
        "max_tokens": 16384
    },

    "deepseek-r1-free": {
        "name": "deepseek/deepseek-r1:free",
        "display": "🔬 DeepSeek R1 (free)",
        "desc": "Бесплатная версия (свой лимит запросов в минуту/день)",
        "temp": 0.7,
        "max_tokens": 8192
    },

    # === Meta Llama (бесплатная) ===
    "llama-3.3-70b-free": {
        "name": "meta-llama/llama-3.3-70b-instruct:free",
        "display": "🦙 Llama 3.3 70B (free)",
        "desc": "Бесплатная модель Meta",
        "temp": 0.7,
        "max_tokens": 8192
    },

    # === Qwen (бесплатная, заточена под код) ===
    "qwen-coder-free": {
        "name": "qwen/qwen3-coder:free",
        "display": "👨‍💻 Qwen3 Coder (free)",
        "desc": "Бесплатная модель, хороша в коде",
        "temp": 0.7,
        "max_tokens": 8192
    },

    # === Mistral (бесплатная) ===
    "mistral-small-free": {
        "name": "mistralai/mistral-small-3.1-24b-instruct:free",
        "display": "🌬️ Mistral Small (free)",
        "desc": "Бесплатная модель Mistral",
        "temp": 0.7,
        "max_tokens": 8192
    },
}

# Модель по умолчанию — бесплатный авто-роутер, чтобы из коробки не зависеть
# от лимита конкретного провайдера (именно то, от чего вы уходите с Gemini)
DEFAULT_AI_MODEL = "nvidia-nemotron-3-nano-omni-30b-a3b"  # ← Выберите модель по умолчанию из AVAILABLE_AI_MODELS



BASE_URL = SITE_BASE + "/?p=lora"
DEFAULT_MIN_DAYS = int(MIN_DAYS_ENV) if MIN_DAYS_ENV and MIN_DAYS_ENV.isdigit() else 0
DEFAULT_TAGS = []
MAX_PAGES = 50
EXPORT_THRESHOLD = 50
COOLDOWN_SECONDS = 20
FORWARDED_FILE = "forwarded.json"
USERS_FILE = "users.json"
# === Rate limiting для сайта с лорами ===
LAST_REQUEST_TIME = 0  # Время последнего запроса
REQUEST_DELAY = 30.0    # Мин. секунд между запросами (по просьбе владельца)



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

def markdown_to_html(text: str) -> str:
    """
    Конвертирует Markdown, который обычно присылают нейросети (заголовки,
    **жирный**, *курсив*, ~~зачёркнутый~~, `код`, блоки ```код```,
    [ссылки](url), > цитаты, списки - / *), в HTML-разметку, понятную Telegram
    (parse_mode="HTML").

    ВАЖНО: экранирование HTML (html.escape) выполняется ПОСЛЕ разбора
    markdown-синтаксиса — через служебные маркеры из непечатных символов,
    которые html.escape не трогает. Если экранировать текст раньше, то уже
    вставленные теги <b>, <code> и т.п. сами потом превратятся в &lt;b&gt; —
    именно так раньше ломалось форматирование ответов AI.
    """
    if not text:
        return ""

    code_blocks = []
    inline_codes = []
    links = []

    # 1) Блоки кода ```...``` — извлекаем целиком, экранируем содержимое и сразу
    #    собираем готовый HTML. Дальше эти куски никто не трогает.
    def stash_code_block(m):
        lang = (m.group(1) or "").strip()
        code = html.escape(m.group(2))
        lang_attr = f' class="language-{html.escape(lang)}"' if lang else ""
        code_blocks.append(f"<pre><code{lang_attr}>{code}</code></pre>")
        return f"\x00CB{len(code_blocks)-1}\x00"
    text = re.sub(r'```(\w*)\n?(.*?)```', stash_code_block, text, flags=re.DOTALL)

    # 2) Инлайн-код `...`
    def stash_inline_code(m):
        inline_codes.append(f"<code>{html.escape(m.group(1))}</code>")
        return f"\x00IC{len(inline_codes)-1}\x00"
    text = re.sub(r'`([^`\n]+?)`', stash_inline_code, text)

    # 3) Ссылки [текст](url) — тоже целиком защищаем, экранируя текст и url
    def stash_link(m):
        label, url = html.escape(m.group(1)), html.escape(m.group(2))
        links.append(f'<a href="{url}">{label}</a>')
        return f"\x00LK{len(links)-1}\x00"
    text = re.sub(r'\[([^\]]+)\]\((https?://[^\s\)]+)\)', stash_link, text)

    # 4) Заголовки "# Текст" -> жирная строка (Telegram не умеет в заголовки)
    text = re.sub(r'^#{1,6}[ \t]+(.+?)[ \t]*$', lambda m: f"\x01B\x01{m.group(1)}\x01b\x01", text, flags=re.MULTILINE)

    # 5) Цитаты "> текст" (склеиваем подряд идущие строки в один блок) -> <blockquote>
    def stash_blockquote(m):
        lines = [re.sub(r'^>[ \t]?', '', line) for line in m.group(0).split('\n')]
        return "\x01Q\x01" + "\n".join(lines) + "\x01q\x01"
    text = re.sub(r'^>.*(?:\n>.*)*', stash_blockquote, text, flags=re.MULTILINE)

    # 6) Маркированные списки "- item" / "* item" / "+ item" -> "• item"
    text = re.sub(r'^([ \t]*)[-*+][ \t]+', r'\1• ', text, flags=re.MULTILINE)

    # 7) Полужирный **text** / __text__
    text = re.sub(r'\*\*(.+?)\*\*', lambda m: f"\x01B\x01{m.group(1)}\x01b\x01", text, flags=re.DOTALL)
    text = re.sub(r'__(.+?)__', lambda m: f"\x01B\x01{m.group(1)}\x01b\x01", text, flags=re.DOTALL)

    # 8) Курсив *text* / _text_ (одиночные — двойные уже разобраны выше)
    text = re.sub(r'(?<!\*)\*([^\*\n]+?)\*(?!\*)', lambda m: f"\x01I\x01{m.group(1)}\x01i\x01", text)
    text = re.sub(r'(?<![\w_])_([^_\n]+?)_(?![\w_])', lambda m: f"\x01I\x01{m.group(1)}\x01i\x01", text)

    # 9) Зачёркнутый ~~text~~
    text = re.sub(r'~~(.+?)~~', lambda m: f"\x01S\x01{m.group(1)}\x01s\x01", text, flags=re.DOTALL)

    # 10) Экранируем весь оставшийся обычный текст (служебные маркеры — непечатные
    #     символы, html.escape их не трогает)
    text = html.escape(text)

    # 11) Превращаем служебные маркеры в реальные HTML-теги
    text = (text
            .replace('\x01B\x01', '<b>').replace('\x01b\x01', '</b>')
            .replace('\x01I\x01', '<i>').replace('\x01i\x01', '</i>')
            .replace('\x01S\x01', '<s>').replace('\x01s\x01', '</s>')
            .replace('\x01Q\x01', '<blockquote>').replace('\x01q\x01', '</blockquote>'))

    # 12) Возвращаем защищённые код-блоки, инлайн-код и ссылки
    text = re.sub(r'\x00CB(\d+)\x00', lambda m: code_blocks[int(m.group(1))], text)
    text = re.sub(r'\x00IC(\d+)\x00', lambda m: inline_codes[int(m.group(1))], text)
    text = re.sub(r'\x00LK(\d+)\x00', lambda m: links[int(m.group(1))], text)

    return text

# === ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ===
bot_running = True
user_settings = {}
awaiting_conversion = set()
forwarded_messages = {}
known_users = {}
allowed_ai_users = set()
if OWNER_ID:
    allowed_ai_users.add(int(OWNER_ID))
allowed_ai_users |= EXTRA_ALLOWED_AI_USERS

allowed_ai_users_env = os.getenv("ALLOWED_AI_USERS", "")
if allowed_ai_users_env:
    for uid_str in allowed_ai_users_env.split(","):
        uid_str = uid_str.strip()
        if uid_str:
            try:
                allowed_ai_users.add(int(uid_str))
            except ValueError:
                logger.warning(f"⚠️ Неверный ID в ALLOWED_AI_USERS: {uid_str}")
openrouter_session = None
current_ai_model = DEFAULT_AI_MODEL
ai_memory = {}
MEMORY_FILE = "ai_memory.json"
log_handler = None
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
    print("❌❌❌ КРИТИЧЕСКАЯ ОШИБКА ❌❌❌")
    print(f"BOT_TOKEN: {BOT_TOKEN}")
    print(f"OWNER_ID: {OWNER_ID}")
    print("Переменные окружения не загружены. Проверь .env файл.")
    print("Список всех env-переменных (только ключи):")
    for k in sorted(os.environ.keys()):
        print(f"  - {k}")
    sys.exit(1)  # чистый выход, не raise

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

# ================= OPENROUTER HTTP CLIENT =================
def init_openrouter_http():
    """Инициализирует HTTP-сессию для OpenRouter API"""
    global openrouter_session
    
    if not OPENROUTER_API_KEY:
        logger.warning("⚠️ OPENROUTER_API_KEY не задан — AI-функции недоступны")
        return False
    
    try:
        # Создаём сессию с настройками
        openrouter_session = requests.Session()
        openrouter_session.headers.update({
            "Content-Type": "application/json",
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            # Необязательные заголовки — OpenRouter использует их для статистики в дашборде
            "HTTP-Referer": "https://github.com/",
            "X-Title": "LoonieBot",
        })
        
        logger.info(f"✅ OpenRouter HTTP client инициализирован: модель по умолчанию {AVAILABLE_AI_MODELS[DEFAULT_AI_MODEL]['name']}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации OpenRouter HTTP: {e}")
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
                        f"📊 Лог-уровень: {LOG_LEVEL}",
                "parse_mode": "HTML"
            }
            requests.post(url, json=data, timeout=10)
        except Exception as e:
            logger.warning("⚠️ Лог-бот не подключён: " + str(e))
    else:
        logger.warning("⚠️ LOG_BOT_TOKEN или LOG_CHAT_ID не заданы")

# ================= ХРАНИЛИЩЕ (JSON-файлы) =================
def load_forwarded():
    global forwarded_messages
    try:
        if os.path.exists(FORWARDED_FILE):
            with open(FORWARDED_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            forwarded_messages = {int(k): v for k, v in data.items()}
            logger.info(f"📦 Загружено {len(forwarded_messages)} пересланных сообщений из файла")
    except Exception as e:
        logger.error("❌ Ошибка загрузки forwarded.json: " + str(e))
        forwarded_messages = {}

def save_forwarded():
    try:
        data = {str(k): v for k, v in forwarded_messages.items()}
        with open(FORWARDED_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.error("❌ Ошибка сохранения forwarded.json: " + str(e))

def load_users():
    global known_users
    try:
        if os.path.exists(USERS_FILE):
            with open(USERS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            known_users = {int(k): v for k, v in data.items()}
            logger.info(f"👥 Загружено {len(known_users)} пользователей из файла")
    except Exception as e:
        logger.error("❌ Ошибка загрузки users.json: " + str(e))
        known_users = {}

def save_users():
    try:
        data = {str(k): v for k, v in known_users.items()}
        with open(USERS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error("❌ Ошибка сохранения users.json: " + str(e))


# ================= НАСТРОЙКИ =================
SETTINGS_FILE = "settings.json"

def load_settings():
    """Загружает настройки владельца из файла"""
    global user_settings
    try:
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data:
                user_settings[OWNER_ID_INT] = data
                logger.info("⚙️ Загружены настройки из файла")
                return
    except Exception as e:
        logger.warning("⚠️ Ошибка загрузки настроек: " + str(e))
    logger.info("⚙️ Используем настройки по умолчанию")

def save_settings(user_id):
    """Сохраняет настройки владельца в файл"""
    if user_id == OWNER_ID_INT:
        try:
            settings = user_settings.get(user_id, {})
            with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
                json.dump(settings, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.warning("⚠️ Ошибка сохранения настроек: " + str(e))

    

def load_memory():
    global ai_memory
    try:
        if os.path.exists(MEMORY_FILE):
            with open(MEMORY_FILE, "r", encoding="utf-8") as f:
                ai_memory = json.load(f)
            logger.info(f"🧠 Загружена память AI для {len(ai_memory)} моделей")
    except Exception as e:
        logger.error("❌ Ошибка загрузки ai_memory.json: " + str(e))
        ai_memory = {}

def save_memory():
    try:
        with open(MEMORY_FILE, "w", encoding="utf-8") as f:
            json.dump(ai_memory, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error("❌ Ошибка сохранения ai_memory.json: " + str(e))


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
    if user_id == OWNER_ID_INT:
        save_settings(user_id)

def check_cooldown(user_id):
    settings = get_settings(user_id)
    elapsed = time.time() - settings.get("last_check", 0)
    if elapsed >= COOLDOWN_SECONDS:
        return True, 0
    return False, int(COOLDOWN_SECONDS - elapsed)

# ================= ЗАПРОСЫ И ПАРСИНГ =================
async def fetch_with_retry(url, max_retries=3):
    """Асинхронный запрос с rate limiting (1 запрос/сек)"""
    global LAST_REQUEST_TIME
    
    headers = {"User-Agent": "Mozilla/5.0"}
    
    for attempt in range(1, max_retries + 1):
        try:
            # 🔹 Ждём если прошло меньше REQUEST_DELAY секунд с последнего запроса
            now = time.time()
            time_since_last = now - LAST_REQUEST_TIME
            if time_since_last < REQUEST_DELAY:
                await asyncio.sleep(REQUEST_DELAY - time_since_last)
            
            # 🔹 Выполняем запрос в отдельном потоке (чтобы не блокировать event loop)
            response = await asyncio.to_thread(
                requests.get, url, headers=headers, timeout=20
            )
            response.raise_for_status()
            
            # 🔹 Обновляем время последнего запроса
            LAST_REQUEST_TIME = time.time()
            
            return response.text
            
        except requests.RequestException as e:
            logger.warning(f"Попытка {attempt} упала: {e}")
            if attempt == max_retries:
                return None
            # 🔹 Ждём перед повторной попыткой (экспоненциальная задержка)
            await asyncio.sleep(min(2 ** attempt, 10))

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


async def find_loras_by_tag(tag, min_days):
    all_results, pages_scanned = [], 0
    for page in range(1, MAX_PAGES + 1):
        if not bot_running: break
        url = BASE_URL + "&t=" + tag + ("&c="+str(page) if page>1 else "")
        logger.info("=== Тег: " + tag + " | Страница: " + str(page) + " ===")
        html = await fetch_with_retry(url)
        if not html: break
        loras = parse_loras_from_html(html, min_days)
        pages_scanned += 1
        if loras:
            all_results.extend(loras)
            logger.info("Стр. " + str(page) + ": найдено " + str(len(loras)) + " лор")
        else:
            logger.info("Стр. " + str(page) + ": лор не найдено")
            if page > 3: break
        if page < MAX_PAGES: 
            await asyncio.sleep(1.0)
    logger.info("=== Тег " + tag + " готов === Лор: " + str(len(all_results)) + " | Стр: " + str(pages_scanned))
    return all_results, pages_scanned

async def find_all_loras(min_days):
    all_results, pages_scanned = [], 0
    for page in range(1, MAX_PAGES + 1):
        if not bot_running: break
        url = BASE_URL if page == 1 else BASE_URL + "&c=" + str(page)
        logger.info("=== Все лоры | Страница: " + str(page) + " ===")
        html = await fetch_with_retry(url)
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
        
        if page < MAX_PAGES: 
            await asyncio.sleep(1.0)
    
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

# ================= РАЗБИЕНИЕ ДЛИННЫХ СООБЩЕНИЙ =================
MAX_MESSAGE_LENGTH = 4000  # Чуть меньше лимита 4096 для безопасности

def split_long_message(text: str, max_length: int = MAX_MESSAGE_LENGTH) -> list[str]:
    """
    Разбивает длинный текст на части для отправки в Telegram
    
    Args:
        text: Исходный текст
        max_length: Максимальная длина одной части (по умолчанию 4000)
    
    Returns:
        list[str]: Список частей текста, каждая <= max_length
    """
    if len(text) <= max_length:
        return [text]
    
    parts = []
    current = ""
    
    # Разбиваем по строкам чтобы не резать слова
    lines = text.split('\n')
    
    for line in lines:
        # Если одна строка длиннее лимита — режем её по словам
        if len(line) > max_length:
            # Если есть текущий буфер — сохраняем его
            if current:
                parts.append(current)
                current = ""
            
            # Режем длинную строку по словам
            words = line.split(' ')
            temp = ""
            for word in words:
                if len(temp + ' ' + word) <= max_length:
                    temp += (' ' if temp else '') + word
                else:
                    if temp:
                        parts.append(temp)
                    temp = word
            if temp:
                current = temp
        # Если строка помещается в текущую часть
        elif len(current) + len(line) + 1 <= max_length:
            current += ('\n' if current else '') + line
        # Если не помещается — начинаем новую часть
        else:
            if current:
                parts.append(current)
            current = line
    
    # Добавляем последнюю часть
    if current:
        parts.append(current)
    
    return parts

async def send_long_message(message: Message, text: str, parse_mode: str = "HTML", split_code: bool = True):
    """
    Отправляет длинное сообщение, разбивая его на части если нужно
    
    Args:
        message: Исходное сообщение для ответа
        text: Текст для отправки
        parse_mode: Режим парсинга ("HTML", "Markdown", None)
        split_code: Если True — стараться не разрывать код внутри ```
    """
    # Если текст короткий — отправляем как есть
    if len(text) <= MAX_MESSAGE_LENGTH:
        await message.answer(text, parse_mode=parse_mode)
        return
    
    # Разбиваем на части
    parts = split_long_message(text, MAX_MESSAGE_LENGTH)
    
    # Отправляем частями с небольшой задержкой
    for i, part in enumerate(parts, 1):
        # Добавляем индикатор "продолжение" если частей больше одной
        if len(parts) > 1:
            prefix = f"<i>({i}/{len(parts)})</i>\n" if parse_mode == "HTML" else f"({i}/{len(parts)})\n"
            part = prefix + part
        
        try:
            await message.answer(part, parse_mode=parse_mode)
            # Небольшая задержка чтобы не получить 429 от Telegram
            if i < len(parts):
                await asyncio.sleep(0.3)
        except Exception as e:
            logger.error(f"❌ Ошибка отправки части {i}: {e}")
            # Если не вышло с HTML — пробуем без парсинга
            if parse_mode:
                await message.answer(part, parse_mode=None)

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


async def ask_ai_http(prompt: str, history: list = None, model_key: str = None) -> dict:
    """
    Отправляет запрос к нейросети через OpenRouter (OpenAI-совместимый формат)

    Args:
        prompt: Текст запроса
        history: Опционально, история диалога — список {"role": "user"/"assistant", "text": "..."}
        model_key: Опционально, ключ модели из AVAILABLE_AI_MODELS (если None — используется current_ai_model)
    """
    # Определяем какую модель использовать
    model_key = model_key or current_ai_model
    model_info = AVAILABLE_AI_MODELS.get(model_key, AVAILABLE_AI_MODELS[DEFAULT_AI_MODEL])
    model_name = model_info["name"]
    
    if not openrouter_session or not OPENROUTER_API_KEY:
        return {"success": False, "error": "OpenRouter не инициализирован (нет OPENROUTER_API_KEY)"}
    
    
    try:
        
        # 🔹 Формируем сообщения в формате OpenAI chat (role/content)
        messages = []
        
        # Добавляем system prompt (память) для текущей модели
        memory_text = ai_memory.get(model_key, "")
        if memory_text:
            messages.append({"role": "system", "content": memory_text})

        if history:
            # Многоходовой чат с историей
            for msg in history:
                role = "user" if msg.get("role") == "user" else "assistant"
                messages.append({"role": role, "content": msg.get("text", "")})
        messages.append({"role": "user", "content": prompt})
        
        # 🔹 Формируем тело запроса
        payload = {
            "model": model_name,
            "messages": messages,
            "temperature": model_info.get("temp", 0.7),
            "max_tokens": model_info.get("max_tokens", 8192),
        }
        
        # 🔹 Выполняем запрос в отдельном потоке (requests блокирующий)
        def make_request():
            return openrouter_session.post(OPENROUTER_API_URL, json=payload, timeout=60)
        
        response = await asyncio.to_thread(make_request)
        
        # 🔹 Обрабатываем ответ
        if response.status_code == 200:
            data = response.json()
            
            # Извлекаем текст ответа (стандартный формат OpenAI chat completion)
            choices = data.get("choices", [])
            if choices:
                message = choices[0].get("message", {})
                text = (message.get("content") or "").strip()
                if text:
                    return {"success": True, "text": text}
                # Некоторые reasoning-модели кладут рассуждение в отдельное поле,
                # если content пустой — на всякий случай подстрахуемся
                reasoning = message.get("reasoning")
                if reasoning:
                    return {"success": True, "text": reasoning.strip()}
            
            return {"success": False, "error": "Пустой или неверный ответ от API"}
            
        elif response.status_code == 400:
            return {"success": False, "error": "❌ Неверный запрос. Попробуй перефразировать или смени модель."}
        elif response.status_code == 401:
            return {"success": False, "error": "🔒 Неверный или отсутствующий OPENROUTER_API_KEY."}
        elif response.status_code == 402:
            return {"success": False, "error": "💳 Недостаточно кредитов на балансе OpenRouter."}
        elif response.status_code == 404:
            return {"success": False, "error": f"❓ Модель не найдена: {model_name}. Проверь название на openrouter.ai/models."}
        elif response.status_code == 429:
            return {"success": False, "error": "🔄 Лимит запросов (особенно у бесплатных моделей). Подожди минуту или смени модель."}
        elif response.status_code >= 500:
            return {"success": False, "error": "⚠️ Серверная ошибка провайдера. Попробуй позже или смени модель."}
        else:
            return {"success": False, "error": f"⚠️ HTTP {response.status_code}: {response.text[:150]}"}
        
    except requests.exceptions.Timeout:
        return {"success": False, "error": "⏱️ Таймаут ответа. Попробуй позже."}
    except requests.exceptions.ConnectionError:
        return {"success": False, "error": "🌐 Ошибка соединения. Проверь интернет."}
    except Exception as e:
        logger.error(f"❌ OpenRouter HTTP error: {str(e)}")
        return {"success": False, "error": f"⚠️ Ошибка: {str(e)[:200]}"}

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

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

@dp.message(Command("allowai"))
async def cmd_allowai(m: Message):
    """Добавить пользователя в список доступа к /ai: /allowai <user_id>"""
    if m.from_user.id != OWNER_ID_INT:
        return
    
    parts = m.text.split()
    if len(parts) < 2:
        await m.answer(f"{EMOJI['info']} Используй: <code>/allowai &lt;user_id&gt;</code>", parse_mode="HTML")
        return
    
    try:
        new_user_id = int(parts[1])
        allowed_ai_users.add(new_user_id)
        
        # Получаем инфо о пользователе для лога
        try:
            user = await bot.get_chat(new_user_id)
            name = user.full_name
            username = f"@{user.username}" if user.username else "без ника"
        except:
            name = "Unknown"
            username = "Unknown"
        
        await m.answer(
            f"{EMOJI['check']} <b>Пользователь добавлен!</b>\n\n"
            f"👤 {safe_html_text(name)} ({username})\n"
            f"🆔 ID: <code>{new_user_id}</code>\n\n"
            f"<i>Теперь может использовать /ai</i>",
            parse_mode="HTML"
        )
        logger.info(f"✅ Добавлен доступ к /ai для {new_user_id} ({name})")
        
    except ValueError:
        await m.answer(f"{EMOJI['error']} Неверный формат ID. Используй числа.", parse_mode="HTML")
    except Exception as e:
        await m.answer(f"{EMOJI['error']} Ошибка: {safe_html_text(str(e)[:200])}", parse_mode="HTML")

# Команда для просмотра списка:
@dp.message(Command("aiallowed"))
async def cmd_aiallowed(m: Message):
    """Показать список пользователей с доступом к /ai"""
    if m.from_user.id != OWNER_ID_INT:
        return
    
    if not allowed_ai_users:
        await m.answer(f"{EMOJI['info']} Список пуст", parse_mode="HTML")
        return
    
    txt = f"{EMOJI['lock']} <b>Доступ к /ai ({len(allowed_ai_users)}):</b>\n\n"
    
    for uid in list(allowed_ai_users)[:20]:  # Показываем первые 20
        try:
            user = await bot.get_chat(uid)
            name = user.full_name
            username = f"@{user.username}" if user.username else ""
            txt += f"• <code>{uid}</code> — {safe_html_text(name)} {username}\n"
        except:
            txt += f"• <code>{uid}</code> — Unknown\n"
    
    if len(allowed_ai_users) > 20:
        txt += f"\n<i>...и ещё {len(allowed_ai_users) - 20} пользователей</i>"
    
    await m.answer(txt, parse_mode="HTML")



@dp.message(Command("ai"))
async def cmd_ai(m: Message):
    """Запрос к нейросети через OpenRouter: /ai <твой вопрос>"""
    
    # 🔹 ПРОВЕРКА ДОСТУПА
    user_id = m.from_user.id
    if not is_user_allowed(user_id, allowed_ai_users):
        logger.warning(f"🚫 Доступ к /ai запрещён для пользователя {user_id}")
        await m.answer(
            f"{EMOJI['lock']} <b>Доступ запрещён</b>\n\n"
            f"<i>Эта команда доступна только авторизованным пользователям</i>",
            parse_mode="HTML"
        )
        return
    
    prompt = m.text.split(maxsplit=1)[1] if len(m.text.split()) > 1 else ""
    
    if not prompt:
        model_display = AVAILABLE_AI_MODELS.get(current_ai_model, {}).get("display", current_ai_model)
        await send_long_message(m,
            f"{EMOJI['info']} <b>Нейросеть ({model_display}):</b>\n\n"
            f"<code>/ai &lt;твой вопрос или запрос&gt;</code>\n\n"
            f"<b>Примеры:</b>\n"
            f"• /ai Объясни квантовую физику простыми словами\n"
            f"• /ai Напиши код для сортировки списка на Python",
            parse_mode="HTML"
        )
        return
    
    # Отправляем "думаю..."
    status_msg = await m.answer(f"{EMOJI['brain']} <i>Думаю...</i>", parse_mode="HTML")
    
    # Запрашиваем ответ
    result = await ask_ai_http(prompt)
    
    if result["success"]:
        # Конвертируем Markdown от нейросети (заголовки, **жирный**, код, ссылки
        # и т.д.) в HTML, которое понимает Telegram parse_mode="HTML"
        answer = markdown_to_html(result["text"])
        
        # 🔹 Отправляем с автоматическим разбиением
        await send_long_message(status_msg,
            f"{PREMIUM_EMOJI['sparkle']} <b>AI:</b>\n\n{answer}",
            parse_mode="HTML"
        )
        logger.info(f"🤖 AI: '{prompt[:50]}...' → ответ ({len(answer)} символов)")
    else:
        await status_msg.edit_text(f"{EMOJI['error']} {result['error']}", parse_mode="HTML")
        logger.warning(f"⚠️ AI ошибка: {result['error']}")

# ================= ПРОВЕРКА МОДЕЛЕЙ =================
async def fetch_available_models() -> set[str]:
    """Запрашивает список доступных моделей у OpenRouter"""
    if not openrouter_session or not OPENROUTER_API_KEY:
        return set()
    try:
        def _get():
            r = openrouter_session.get(
                "https://openrouter.ai/api/v1/models",
                timeout=15
            )
            if r.status_code == 200:
                data = r.json()
                return {m.get("id", "") for m in data.get("data", [])}
            return set()
        return await asyncio.to_thread(_get)
    except Exception as e:
        logger.warning(f"⚠️ Не удалось получить список моделей OpenRouter: {e}")
        return set()

async def ping_model(model_name: str) -> tuple[bool, float]:
    """Тестовый пинг модели. Возвращает (успех, секунды)."""
    if not openrouter_session or not OPENROUTER_API_KEY:
        return False, 0.0
    payload = {
        "model": model_name,
        "messages": [{"role": "user", "content": "ping"}],
        "max_tokens": 1,
        "temperature": 0,
    }
    start = time.time()
    try:
        def _post():
            return openrouter_session.post(
                OPENROUTER_API_URL,
                json=payload,
                timeout=10
            )
        r = await asyncio.wait_for(asyncio.to_thread(_post), timeout=12)
        elapsed = time.time() - start
        
        if r.status_code == 200:
            return True, elapsed
        elif r.status_code in (429, 402):
            # Модель доступна, но лимит/кредиты — считаем доступной
            return True, elapsed
        else:
            return False, elapsed
    except asyncio.TimeoutError:
        return False, time.time() - start
    except Exception as e:
        logger.debug(f"Ping error {model_name}: {e}")
        return False, 0.0


@dp.message(Command("model"))
async def cmd_model(m: Message):
    """Показать или сменить AI-модель (OpenRouter): /model [ключ]"""
    if m.from_user.id != OWNER_ID_INT:
        await m.answer(f"{EMOJI['lock']} Только для владельца", parse_mode="HTML")
        return
    
    global current_ai_model
    parts = m.text.split()
    
    # === БЕЗ АРГУМЕНТА: просто показать список (без пинга) ===
    if len(parts) < 2:
        txt = f"{EMOJI['settings']} <b>Доступные AI-модели (OpenRouter):</b>\n\n"
        
        for key, info in AVAILABLE_AI_MODELS.items():
            current = "✅ " if key == current_ai_model else "• "
            is_free = ":free" in info["name"]
            free_badge = " <code>[free]</code>" if is_free else ""
            
            txt += f"{current}{info['display']}{free_badge}\n"
            txt += f"   <i>{info['desc']}</i>\n"
            txt += f"   <code>/model {key}</code>\n\n"
        
        txt += f"<b>Текущая:</b> <code>{current_ai_model}</code>\n"
        txt += f"<i>Используй /model &lt;ключ&gt; чтобы сменить. "
        txt += f"При смене модель будет проверена автоматически.</i>"
        
        await m.answer(txt, parse_mode="HTML")
        return
    
    # === С АРГУМЕНТОМ: проверяем и только потом меняем ===
    new_model_key = parts[1].lower()
    
    if new_model_key not in AVAILABLE_AI_MODELS:
        available = ", ".join(AVAILABLE_AI_MODELS.keys())
        await m.answer(
            f"{EMOJI['error']} <b>Неизвестная модель:</b> <code>{new_model_key}</code>\n\n"
            f"<b>Доступно:</b> <code>{available}</code>\n"
            f"<i>Используй /model без аргумента чтобы увидеть список</i>",
            parse_mode="HTML"
        )
        return
    
    new_model_info = AVAILABLE_AI_MODELS[new_model_key]
    
    # Проверяем доступность перед сменой
    check_msg = await m.answer(
        f"{EMOJI['settings']} Проверяю <b>{new_model_info['display']}</b>...",
        parse_mode="HTML"
    )
    ok, latency = await ping_model(new_model_info["name"])
    
    if not ok:
        await check_msg.edit_text(
            f"{EMOJI['warning']} <b>Модель недоступна!</b>\n\n"
            f"{new_model_info['display']}\n"
            f"<i>Не удалось получить ответ. Возможно, модель offline или достигнут лимит.</i>\n\n"
            f"<i>Смена отменена. Попробуй другую модель.</i>",
            parse_mode="HTML"
        )
        return
    
    # Сменяем модель
    old_model = current_ai_model
    current_ai_model = new_model_key
    
    mem_status = ""
    if new_model_key in ai_memory and ai_memory[new_model_key]:
        mem_status = f"\n🧠 <i>Память:</i> <code>{safe_html_text(ai_memory[new_model_key][:50])}...</code>"
    
    await check_msg.edit_text(
        f"{EMOJI['check']} <b>Модель сменена!</b>\n\n"
        f"🔄 Было: <code>{old_model}</code>\n"
        f"✅ Стало: {new_model_info['display']}\n"
        f"⏱️ Ответ: <b>{latency:.1f}с</b>\n"
        f"📝 <i>{new_model_info['desc']}</i>{mem_status}",
        parse_mode="HTML"
    )
    
    logger.info(f"🔄 AI-модель сменена: {old_model} → {new_model_key} (ping {latency:.1f}s)")


@dp.message(Command("setmemory"))
async def cmd_setmemory(m: Message):
    """Задать память (system prompt) для текущей модели: /setmemory <текст>"""
    if m.from_user.id != OWNER_ID_INT:
        return
    
    parts = m.text.split(maxsplit=1)
    if len(parts) < 2:
        current_mem = ai_memory.get(current_ai_model, "")
        await m.answer(
            f"{EMOJI['brain']} <b>Память AI (system prompt)</b>\n\n"
            f"🤖 Модель: <b>{AVAILABLE_AI_MODELS.get(current_ai_model, {}).get('display', current_ai_model)}</b>\n"
            f"📝 Текущая память:\n<code>{safe_html_text(current_mem) if current_mem else '(пусто)'}</code>\n\n"
            f"<b>Использование:</b>\n"
            f"<code>/setmemory Отвечай кратко и по делу</code>\n"
            f"<code>/setmemory Ты эксперт по Python</code>\n\n"
            f"<i>Память будет прикрепляться к каждому запросу как системная инструкция</i>",
            parse_mode="HTML"
        )
        return
    
    memory_text = parts[1].strip()
    ai_memory[current_ai_model] = memory_text
    save_memory()
    
    await m.answer(
        f"{EMOJI['check']} <b>Память задана!</b>\n\n"
        f"🤖 Модель: <b>{AVAILABLE_AI_MODELS.get(current_ai_model, {}).get('display', current_ai_model)}</b>\n"
        f"📝 Память: <code>{safe_html_text(memory_text[:200])}{'...' if len(memory_text) > 200 else ''}</code>\n\n"
        f"<i>Теперь каждый запрос к этой модели будет с этой инструкцией</i>",
        parse_mode="HTML"
    )
    logger.info(f"🧠 Память задана для {current_ai_model}: {memory_text[:50]}...")


@dp.message(Command("clearmemory"))
async def cmd_clearmemory(m: Message):
    """Очистить память для текущей модели"""
    if m.from_user.id != OWNER_ID_INT:
        return
    
    if current_ai_model in ai_memory:
        del ai_memory[current_ai_model]
        save_memory()
        await m.answer(
            f"{EMOJI['check']} <b>Память очищена!</b>\n\n"
            f"🤖 Модель: <b>{AVAILABLE_AI_MODELS.get(current_ai_model, {}).get('display', current_ai_model)}</b>\n\n"
            f"<i>Теперь модель работает без системной инструкции</i>",
            parse_mode="HTML"
        )
        logger.info(f"🧠 Память очищена для {current_ai_model}")
    else:
        await m.answer(
            f"{EMOJI['info']} Для этой модели память не задана.",
            parse_mode="HTML"
        )


@dp.message(Command("memory"))
async def cmd_memory(m: Message):
    """Показать память для всех моделей"""
    if m.from_user.id != OWNER_ID_INT:
        return
    
    if not ai_memory:
        await m.answer(
            f"{EMOJI['info']} <b>Память AI не задана ни для одной модели</b>\n\n"
            f"<i>Используй /setmemory &lt;текст&gt; для текущей модели</i>",
            parse_mode="HTML"
        )
        return
    
    txt = f"{EMOJI['brain']} <b>Память AI (system prompts):</b>\n\n"
    for key, mem in ai_memory.items():
        info = AVAILABLE_AI_MODELS.get(key, {})
        display = info.get('display', key)
        is_current = " ✅" if key == current_ai_model else ""
        mem_preview = mem[:150] + "…" if len(mem) > 150 else mem
        txt += f"🤖 <b>{display}</b>{is_current}\n"
        txt += f"<code>{safe_html_text(mem_preview)}</code>\n\n"
    
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
    """Поиск лор по настройкам"""
    if message.from_user.id != OWNER_ID_INT:
        return
    
    if not bot_running:
        await message.answer(EMOJI["error"] + " Бот остановлен.", parse_mode="HTML")
        return
    
    user_id = message.from_user.id
    settings = get_settings(user_id)
    
    if settings.get("is_checking"):
        await message.answer(EMOJI["lock"] + " Проверка уже запущена!", parse_mode="HTML")
        return
    
    can_use, remaining = check_cooldown(user_id)
    if not can_use:
        await message.answer(EMOJI["clock"] + f" Кулдаун! Повтори через <b>{remaining}</b> сек.", parse_mode="HTML")
        return
    
    try:
        update_settings(user_id, is_checking=True)
        await message.answer(EMOJI["search"] + " Поиск запущен...", parse_mode="HTML")
        
        min_days, tags = settings["min_days"], settings["tags"]
        
        if tags:
            all_loras, total_pages = [], 0
            for tag in tags:
                loras, pages = await find_loras_by_tag(tag, min_days)
                all_loras.extend(loras)
                total_pages += pages
        else:
            all_loras, total_pages = await find_all_loras(min_days)
        
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
        
        # 🗄️ Сохраняем в кэш если лор <50
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
        
        # ⏱️ Обновляем кулдаун
        update_settings(user_id, last_check=time.time())
        logger.info("✅ Поиск завершён: " + str(len(all_loras)) + " лор")
        
    except Exception as e:
        logger.error("❌ Ошибка в /check: " + str(e), exc_info=True)
        await message.answer(EMOJI["error"] + " Ошибка: " + str(e)[:100], parse_mode="HTML")
    finally:
        update_settings(user_id, is_checking=False)



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
    if message.from_user.id != OWNER_ID_INT: 
        return
    settings = get_settings(message.from_user.id)
    moscow_time = datetime.now(timezone(timedelta(hours=3))).strftime('%Y-%m-%d %H:%M:%S')
    txt = f"{EMOJI['settings']} <b>Настройки:</b>\n🕐 МСК {moscow_time}\n"
    txt += EMOJI["days"] + f" Порог: <b>{('все лоры' if settings['min_days']==0 else '>=' + str(settings['min_days']) + ' дней')}</b>\n"
    txt += EMOJI["tag"] + f" Теги: <b>{(', '.join(settings['tags']) if settings['tags'] else 'нет (все лоры)')}</b>\n"
    can_use, remaining = check_cooldown(message.from_user.id)
    txt += f"⏱️ Кулдаун: <b>{'готов' if can_use else str(remaining) + ' сек'}</b>\n"
    txt += EMOJI["check" if bot_running else "stop"] + f" Бот: <b>{'Активен' if bot_running else 'ОСТАНОВЛЕН'}</b>"
    
    txt += f"\n👥 Пользователей: <b>{len(known_users)}</b>"
    if log_handler: 
        txt += f"\n📊 Лог-уровень: <b>{logging.getLevelName(log_handler.min_level)}</b>"
    
    # Информация о модели (теперь добавляется ДО отправки)
    model_info = AVAILABLE_AI_MODELS.get(current_ai_model, {})
    txt += f"\n🤖 AI модель: <b>{model_info.get('display', current_ai_model)}</b>"
    txt += f"\n   <i>{model_info.get('desc', '')}</i>"
    
    mem = ai_memory.get(current_ai_model, "")
    if mem:
        txt += f"\n🧠 Память: <code>{safe_html_text(mem[:40])}{'...' if len(mem) > 40 else ''}</code>"

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
    ext_url = os.getenv("EXTERNAL_URL") or os.getenv("RENDER_EXTERNAL_URL")
    if ext_url and bot_running:
        wh_url = ext_url + webhook_path
        await bot.set_webhook(
            wh_url,
            allowed_updates=[
                "message", "edited_message", "callback_query",
                "inline_query", "chosen_inline_result",
            ],
        )
        logger.info("✅ Webhook: " + wh_url)




# ================= MAIN =================
async def main():
    # Инициализация (функции уже определены выше)
    init_log_bot()
    load_dotenv()
    init_openrouter_http()
    load_forwarded()
    load_users()
    load_settings()
    load_memory()
    
    await run_web_server()
    moscow_time = datetime.now(timezone(timedelta(hours=3))).strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"🚀 Bot started! Owner: {OWNER_ID_INT} | Users: {len(known_users)} | Time: МСК {moscow_time}")
    
    while True:
        await asyncio.sleep(3600)



# ================= INLINE AI MODE (ПОЛНЫЙ) =================
def parse_ai_inline_query(user_input: str) -> tuple[str, str]:
    """Разбирает текст инлайн-запроса вида 'режим: текст' на (режим, текст).
    Используется и в inline_query, и в chosen_inline_result — специально без
    общего состояния между ними (см. комментарий в on_inline_result_chosen)."""
    mode = "ask"
    text = user_input.strip()
    if ":" in text:
        parts = text.split(":", 1)
        mode = parts[0].strip().lower()
        text = parts[1].strip() if len(parts) > 1 else ""
    return mode, text


@dp.inline_query()
async def inline_search(query: InlineQuery):
    logger.info(f"🔥🔥🔥 INLINE HANDLER CALLED! Query: '{query.query}'") 
    """Умный inline: @looniesbot → варианты действий"""
    user = query.from_user
    user_input = query.query.strip()
    
    logger.info(f"🔍 Inline: user_id={user.id} query='{user_input}'")
    
    # 🔹 Пустой запрос — показываем меню
    if len(user_input) < 2:
        results = [
            InlineQueryResultArticle(
                id="mode_ask",
                title="❓ Спросить",
                thumbnail_url="https://img.magnific.com/free-photo/closeup-shot-cute-fox-lying-ground-with-fallen-autumn-leaves_181624-32660.jpg?semt=ais_hybrid&w=740&q=80",
                thumbnail_width=300,
                thumbnail_height=300,
                description="Задать вопрос нейросети",
                input_message_content=InputTextMessageContent(
                    message_text="❓ Напиши вопрос после выбора..."
                ),
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✍️ Задать вопрос", switch_inline_query_current_chat="ask: ")]
                ]),
            ),
            InlineQueryResultArticle(
                id="mode_explain",
                title="📚 Объяснить",
                thumbnail_url="https://99px.ru/sstorage/53/2022/10/mid_345671_479468.jpg",
                thumbnail_width=300,
                thumbnail_height=300,
                description="Объяснить текст просто",
                input_message_content=InputTextMessageContent(
                    message_text="📚 Введи текст для объяснения..."
                ),
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✍️ Ввести текст", switch_inline_query_current_chat="explain: ")]
                ]),
            ),
        ]
        await query.answer(results=results, cache_time=30, is_personal=True)
        return
    
    # 🔹 Есть ввод — парсим режим:текст
    mode, text = parse_ai_inline_query(user_input)
    
    # 🔹 Нет текста — подсказка
    if not text:
        results = [
            InlineQueryResultArticle(
                id=f"prompt_{mode}",
                title="✍️ Введи текст",
                thumbnail_url="https://99px.ru/sstorage/53/2018/12/mid_245780_602075.jpg",
                thumbnail_width=300,
                thumbnail_height=300,
                description="Продолжи писать после режима...",
                input_message_content=InputTextMessageContent(
                    message_text=f"✍️ Пиши: @looniesbot {mode}: твой текст"
                ),
            )
        ]
        await query.answer(results=results, cache_time=0, is_personal=True)
        return
    
    # 🔹 Есть режим и текст — отдаём ОДИН результат с плейсхолдером.
    # ВАЖНО: Telegram не даёт ответить на один и тот же inline_query дважды,
    # поэтому реальный запрос к нейросети переносим в обработчик chosen_inline_result
    # (срабатывает, когда пользователь реально выбрал этот вариант),
    # а готовый ответ доставляем через edit_message_text по inline_message_id.
    # reply_markup обязателен — без него Telegram не передаст inline_message_id.
    #
    # Принципиально НЕ храним (mode, text) в словаре в памяти процесса: между ответом
    # на inline_query и моментом, когда пользователь реально выберет результат, может
    # пройти много времени, а процесс бота может успеть перезапуститься (передеплой,
    # "усыпление" на бесплатном хостинге) — тогда словарь в памяти потеряется, и
    # сообщение зависнет на "Думаю" навсегда без единой ошибки в логах. Вместо этого
    # в chosen_inline_result заново парсим chosen.query — Telegram сам присылает
    # исходный текст запроса, так что это не зависит от состояния процесса.
    result_id = f"ai_{query.id}"

    results = [
        InlineQueryResultArticle(
            id=result_id,
            title="✨ Спросить у AI",
            thumbnail_url="https://st.aestatic.net/items-img-8/R/7/I/L/A39468cbf296f4d35a51de52f5d2e3e3f4.jpeg_960x960.jpg",
            thumbnail_width=300,   
            thumbnail_height=300,
            description=text[:64],
            input_message_content=InputTextMessageContent(message_text="⏳ <i>Думаю...</i>", parse_mode="HTML"),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⏳ Обновляется...", callback_data="noop")]
            ]),
        )
    ]
    await query.answer(results=results, cache_time=0, is_personal=True)


@dp.chosen_inline_result()
async def on_inline_result_chosen(chosen: ChosenInlineResult):
    """Срабатывает, когда пользователь реально выбрал результат из inline-меню."""
    logger.info(f"🔥🔥🔥 CHOSEN_INLINE_RESULT CALLED! result_id={chosen.result_id} query='{chosen.query}'")

    if not chosen.result_id.startswith("ai_"):
        return  # выбрали не AI-результат (например, пункт меню) — обрабатывать нечего

    if not chosen.inline_message_id:
        logger.warning("⚠️ Нет inline_message_id — не могу отредактировать сообщение")
        return

    mode, text = parse_ai_inline_query(chosen.query)
    if not text:
        logger.warning(f"⚠️ Пустой текст после парсинга chosen.query='{chosen.query}'")
        return

    prompts = {
        "explain": "Объясни просто на русском:",
        "summarize": "Кратко перескажи на русском в 2-3 предложениях:",
        "ask": "Ответь на русском:",
    }
    full_prompt = f"{prompts.get(mode, prompts['ask'])}\n\n{text}"

    logger.info(f"🤖 Запрос к AI (модель {current_ai_model}): mode={mode} text='{text[:60]}'")
    result = await ask_ai_http(full_prompt)
    logger.info(f"🤖 Ответ от AI: success={result['success']}" + ("" if result["success"] else f" error='{result['error']}'"))

    if result["success"]:
        answer = markdown_to_html(result["text"])
        # Показываем исходный запрос пользователя (обрезаем, если очень длинный)
        display_query = text[:400] + "…" if len(text) > 400 else text
        safe_query = safe_html_text(display_query)
        final_text = (
            f"✨ <b>AI:</b>\n\n"
            f"<b>Запрос:</b> <i>{safe_query}</i>\n\n"
            f"{answer}"
        )
    else:
        final_text = f"❌ {result['error']}"

    try:
        await bot.edit_message_text(
            text=final_text,
            inline_message_id=chosen.inline_message_id,
            parse_mode="HTML",
        )
        logger.info("✅ Inline-сообщение отредактировано с финальным ответом")
    except Exception as e:
        logger.error(f"❌ Не удалось отредактировать inline-сообщение: {e}")


@dp.callback_query(F.data == "noop")
async def on_noop_callback(callback: CallbackQuery):
    """Декоративная кнопка на время ожидания ответа — просто гасим спиннер."""
    await callback.answer()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Bot stopped by user")