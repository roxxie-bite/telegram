import os
import sys
import re
import threading
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



# ================= YANDEX MUSIC NOW PLAYING =================
YANDEX_MUSIC_TOKEN = os.getenv("YANDEX_MUSIC_TOKEN")
YM_TARGET_CHAT_ID = os.getenv("YM_TARGET_CHAT_ID")
YM_STATE_FILE = "ym_state.json"

ym_client = None
ym_last_track_id = None
ym_last_message_id = None
ym_task = None
ym_enabled = False


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


# === Разрешённые пользователи для E621 Wiki ===
allowed_e621_users = set()
if OWNER_ID:
    allowed_e621_users.add(int(OWNER_ID))

allowed_e621_users_env = os.getenv("ALLOWED_E621_USERS", 8371541704, 5802195555)
if allowed_e621_users_env:
    for uid_str in allowed_e621_users_env.split(","):
        uid_str = uid_str.strip()
        if uid_str:
            try:
                allowed_e621_users.add(int(uid_str))
            except ValueError:
                pass



# ================= E621 WIKI / TAG INFO (v3) =================
# ВСТАВЬ ЭТОТ БЛОК В bot.py (замени старый блок E621 WIKI)

E621_BASE_URL = "https://e621.net"
E621_USER_AGENT = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"

E621_TAG_CATEGORIES = {
    0: ("general", "🔹 Общий"),
    1: ("artist", "🎨 Художник"),
    3: ("copyright", "©️ Копирайт"),
    4: ("character", "👤 Персонаж"),
    5: ("species", "🐾 Вид"),
    6: ("invalid", "❌ Невалидный"),
    7: ("meta", "📋 Мета"),
    8: ("lore", "📖 Лор"),
}


def e621_category_name(cat_id: int) -> str:
    return E621_TAG_CATEGORIES.get(cat_id, ("unknown", "❓ Неизвестно"))[1]


def e621_category_emoji(cat_id: int) -> str:
    return E621_TAG_CATEGORIES.get(cat_id, ("unknown", "❓"))[1].split()[0]


def clean_dtext(text: str) -> str:
    """Улучшенная очистка DText разметки e621 для Telegram HTML.
    Wiki-ссылки и тег-ссылки оборачиваются в <code> для копирования."""
    if not text:
        return ""

    # Убираем thumb #id
    text = re.sub(r'thumb\s+#\d+(?:\s+#\d+)*', '', text)
    # [nodtext]
    text = re.sub(r'\[nodtext\](.+?)\[/nodtext\]', r'\1', text, flags=re.DOTALL)
    # section/subsection
    text = re.sub(r'\[section[^\]]*\](.+?)\[/section\]', r'\1', text, flags=re.DOTALL)
    text = re.sub(r'\[subsection[^\]]*\](.+?)\[/subsection\]', r'\1', text, flags=re.DOTALL)
    # Заголовки h1.-h6.
    text = re.sub(r'^h\d+\.\s*', '', text, flags=re.MULTILINE)
    # Форматирование → markdown
    text = re.sub(r'\[b\](.+?)\[/b\]', r'**\1**', text, flags=re.DOTALL)
    text = re.sub(r'\[i\](.+?)\[/i\]', r'*\1*', text, flags=re.DOTALL)
    text = re.sub(r'\[u\](.+?)\[/u\]', r'__\1__', text, flags=re.DOTALL)
    text = re.sub(r'\[s\](.+?)\[/s\]', r'~~\1~~', text, flags=re.DOTALL)
    text = re.sub(r'\[spoiler\](.+?)\[/spoiler\]', r'||\1||', text, flags=re.DOTALL)
    text = re.sub(r'\[code\](.+?)\[/code\]', r'`\1`', text, flags=re.DOTALL)
    text = re.sub(r'\[quote\](.+?)\[/quote\]', r'> \1', text, flags=re.DOTALL)
    # Таблицы
    text = re.sub(r'\[table\](.+?)\[/table\]', r'\1', text, flags=re.DOTALL)
    text = re.sub(r'\[tr\](.+?)\[/tr\]', r'\1\n', text, flags=re.DOTALL)
    text = re.sub(r'\[td\](.+?)\[/td\]', r'| \1 ', text, flags=re.DOTALL)
    # Wiki-ссылки → <code>тег</code> (чтобы можно было копировать)
    text = re.sub(r'\[\[([^\]|]+)\|([^\]]+)\]\]', r'<code>\1</code>', text)
    text = re.sub(r'\[\[([^\]]+)\]\]', r'<code>\1</code>', text)
    # Тег-ссылки → <code>тег</code>
    text = re.sub(r'\[tag:([^\]]+)\]', r'<code>\1</code>', text)
    # URL
    text = re.sub(r'\[url=([^\]]+)\](.+?)\[/url\]', r'[\2](\1)', text, flags=re.DOTALL)
    # Цвета
    text = re.sub(r'\[color=[^\]]*\](.+?)\[/color\]', r'\1', text, flags=re.DOTALL)
    # Пустые списки
    text = re.sub(r'^[\s•\*\-\+]+$', '', text, flags=re.MULTILINE)
    text = re.sub(r'^[\s|_\-\*\+•]+$', '', text, flags=re.MULTILINE)
    # Пустые строки
    text = re.sub(r'\n{3,}', '\n\n', text)

    return text.strip()


def make_e621_callback_data(tag_name: str) -> str:
    """Создаёт callback_data для inline-кнопки e621 тега. Ограничение: 64 байта."""
    prefix = "e621|"
    max_len = 64 - len(prefix)
    tag = tag_name.strip().lower()
    if len(tag) > max_len:
        tag = tag[:max_len]
    return prefix + tag


def parse_e621_callback_data(data: str) -> str | None:
    """Парсит callback_data обратно в имя тега."""
    if data.startswith("e621|"):
        return data[5:]
    return None


async def e621_api_get(endpoint: str, params: dict = None) -> dict:
    """Делает GET-запрос к e621 API с правильным User-Agent."""
    url = f"{E621_BASE_URL}{endpoint}"
    headers = {
        "User-Agent": E621_USER_AGENT,
        "Accept": "application/json",
    }
    try:
        def _get():
            return requests.get(url, headers=headers, params=params, timeout=15)
        response = await asyncio.to_thread(_get)
        if response.status_code == 200:
            return {"success": True, "data": response.json()}
        elif response.status_code == 404:
            return {"success": False, "error": "Не найдено"}
        elif response.status_code == 429:
            return {"success": False, "error": "🔄 Лимит запросов к e621. Подожди немного."}
        else:
            return {"success": False, "error": f"HTTP {response.status_code}"}
    except requests.exceptions.Timeout:
        return {"success": False, "error": "⏱️ Таймаут запроса к e621"}
    except Exception as e:
        logger.error(f"❌ e621 API error: {e}")
        return {"success": False, "error": f"Ошибка: {str(e)[:100]}"}


async def get_e621_tag_info(tag_name: str) -> dict:
    """Получает информацию о теге из e621 tags.json."""
    search_name = tag_name.strip().lower().replace(" ", "_")
    result = await e621_api_get("/tags.json", {
        "search[name_matches]": search_name,
        "limit": 1,
    })
    if not result["success"]:
        return result
    tags = result["data"]
    if not tags:
        return {"success": False, "error": f"Тег <code>{safe_html_text(tag_name)}</code> не найден на e621"}
    tag = tags[0]
    return {
        "success": True,
        "tag": {
            "id": tag.get("id"),
            "name": tag.get("name", search_name),
            "category": tag.get("category", 0),
            "post_count": tag.get("post_count", 0),
            "related_tags": tag.get("related_tags", ""),
            "is_locked": tag.get("is_locked", False),
        }
    }


async def get_e621_wiki_page(tag_name: str) -> dict:
    """Получает wiki-страницу тега из e621 wiki_pages.json."""
    search_name = tag_name.strip().lower().replace(" ", "_")
    result = await e621_api_get("/wiki_pages.json", {
        "search[title]": search_name,
        "limit": 1,
    })
    if not result["success"]:
        return result
    pages = result["data"]
    if not pages:
        return {"success": False, "error": "wiki_not_found"}
    page = pages[0]
    return {
        "success": True,
        "wiki": {
            "id": page.get("id"),
            "title": page.get("title", search_name),
            "body": page.get("body", ""),
            "is_locked": page.get("is_locked", False),
            "is_deleted": page.get("is_deleted", False),
        }
    }


async def get_e621_posts(tag_name: str, limit: int = 2) -> list[str]:
    """Возвращает список URL изображений (file_url) по тегу."""
    search_name = tag_name.strip().lower().replace(" ", "_")
    result = await e621_api_get("/posts.json", {
        "tags": search_name,
        "limit": limit,
    })
    if not result["success"]:
        return []
    posts = result["data"].get("posts", [])
    urls = []
    for post in posts:
        file_url = post.get("file", {}).get("url")
        if file_url:
            urls.append(file_url)
    return urls


async def get_e621_tag_suggestions(query: str, limit: int = 6) -> list[dict]:
    """Ищет похожие теги через wildcard. Возвращает список {name, post_count, category}.
    Фильтрует числа и мусор."""
    search_name = query.strip().lower().replace(" ", "_")
    results = []
    for pattern in [search_name, f"*{search_name}*"]:
        if len(pattern) > 40:
            pattern = search_name
        result = await e621_api_get("/tags.json", {
            "search[name_matches]": pattern,
            "search[order]": "count",
            "limit": limit,
        })
        if result["success"]:
            for tag in result["data"]:
                name = tag.get("name", "")
                # Фильтруем: не число, длина > 1, не дубль
                if name and not name.isdigit() and len(name) > 1:
                    t = {
                        "name": name,
                        "post_count": tag.get("post_count", 0),
                        "category": tag.get("category", 0),
                    }
                    if t not in results:
                        results.append(t)
            if len(results) >= limit:
                break
    return results[:limit]


def build_suggestions_keyboard(suggestions: list[dict]) -> InlineKeyboardMarkup:
    """Строит inline-клавиатуру из списка suggestions (только для 'не найдено')."""
    buttons = []
    for s in suggestions:
        display = f"{s['name'].replace('_', ' ')} ({s['post_count']:,})".replace(",", " ")
        if len(display) > 40:
            display = display[:37] + "..."
        cb = make_e621_callback_data(s["name"])
        buttons.append([InlineKeyboardButton(text=display, callback_data=cb)])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def format_tag_info(tag_data: dict, wiki_data: dict = None) -> str:
    """Форматирует информацию о теге для Telegram.
    Теги в Related tags обёрнуты в <code> для копирования."""
    name = tag_data.get("name", "unknown")
    cat_id = tag_data.get("category", 0)
    cat_name = e621_category_name(cat_id)
    post_count = tag_data.get("post_count", 0)
    related = tag_data.get("related_tags", "")
    is_locked = tag_data.get("is_locked", False)

    wiki_url = f"https://e621.net/wiki_pages/show_or_new?title={name}"
    search_url = f"https://e621.net/posts?tags={name}"

    lines = [
        f"{e621_category_emoji(cat_id)} <b>{safe_html_text(name)}</b>",
        f"",
        f"📂 Категория: <b>{cat_name}</b>",
        f"🖼️ Постов: <b>{post_count:,}</b>".replace(",", " "),
    ]

    if is_locked:
        lines.append(f"🔒 Тег заблокирован")

    lines.append(f"")
    lines.append(f"<a href='{wiki_url}'>📖 Wiki</a> | <a href='{search_url}'>🖼️ Посты</a>")

    # Related tags — списком с <code> для копирования
    if related:
        # Фильтруем числа и мусор
        related_list = [t for t in related.split() if not t.isdigit() and len(t) > 1][:10]
        if related_list:
            lines.append(f"")
            lines.append(f"🔗 <b>Related tags:</b>")
            for t in related_list:
                lines.append(f"• <code>{safe_html_text(t)}</code>")

    if wiki_data and wiki_data.get("body"):
        body = clean_dtext(wiki_data["body"])
        if body:
            if len(body) > 1200:
                body = body[:1200].rsplit(" ", 1)[0] + "..."
            body_html = markdown_to_html(body)
            lines.append(f"")
            lines.append(f"📝 <b>Описание:</b>")
            lines.append(body_html)
    elif wiki_data is None:
        lines.append(f"")
        lines.append(f"<i>Wiki-страница не найдена</i>")

    return "\n".join(lines)


# ================= FREELLM API =================
FREELLMAPI_API_KEY = os.getenv("FREELLMAPI_API_KEY")
FREELLMAPI_BASE_URL = os.getenv("FREELLMAPI_BASE_URL", "http://localhost:3001/v1").rstrip("/")
FREELLMAPI_IMAGE_URL = FREELLMAPI_BASE_URL + "/images/generations"
FREELLMAPI_AUDIO_SPEECH_URL = FREELLMAPI_BASE_URL + "/audio/speech"
FREELLMAPI_AUDIO_TRANSCRIPT_URL = FREELLMAPI_BASE_URL + "/audio/transcriptions"
FREELLMAPI_CHAT_URL = FREELLMAPI_BASE_URL + "/chat/completions"
FREELLMAPI_MODELS_URL = FREELLMAPI_BASE_URL + "/models"

# ================= AI МОДЕЛИ (КАТАЛОГ FREELLM API) =================
# FreeLLM API поддерживает "auto" для авто-роутинга, а также конкретные модели.
# Имена взяты из актуального каталога free-tier моделей.
STATIC_AI_MODELS = {
    "auto": {
        "name": "auto",
        "display": "🎲 Auto (FreeLLM Router)",
        "desc": "Авто-выбор лучшей доступной бесплатной модели",
        "temp": 0.7,
        "max_tokens": 8192
    },
    "gemini-2.5-flash": {
        "name": "gemini-2.5-flash",
        "display": "🚀 Gemini 2.5 Flash",
        "desc": "Google, быстрая и умная модель",
        "temp": 0.7,
        "max_tokens": 8192
    },
    "gemini-2.5-pro": {
        "name": "gemini-2.5-pro",
        "display": "✨ Gemini 2.5 Pro",
        "desc": "Google, флагманская модель",
        "temp": 0.7,
        "max_tokens": 8192
    },
    "gpt-4o": {
        "name": "gpt-4o",
        "display": "🤖 GPT-4o",
        "desc": "OpenAI через GitHub Models",
        "temp": 0.7,
        "max_tokens": 8192
    },
    "gpt-4.1": {
        "name": "gpt-4.1",
        "display": "🤖 GPT-4.1",
        "desc": "OpenAI через GitHub Models",
        "temp": 0.7,
        "max_tokens": 8192
    },
    "llama-3.3-70b": {
        "name": "llama-3.3-70b",
        "display": "🦙 Llama 3.3 70B",
        "desc": "Meta через Groq/Cerebras/NV",
        "temp": 0.7,
        "max_tokens": 8192
    },
    "llama-4-scout": {
        "name": "llama-4-scout",
        "display": "🦙 Llama 4 Scout",
        "desc": "Meta через Groq",
        "temp": 0.7,
        "max_tokens": 8192
    },
    "qwen3-235b": {
        "name": "qwen3-235b",
        "display": "🔥 Qwen3 235B",
        "desc": "Alibaba через Cerebras",
        "temp": 0.7,
        "max_tokens": 8192
    },
    "qwen3-coder-480b": {
        "name": "qwen3-coder-480b",
        "display": "👨‍💻 Qwen3 Coder 480B",
        "desc": "Кодерская модель через OpenRouter",
        "temp": 0.7,
        "max_tokens": 8192
    },
    "qwen3-32b": {
        "name": "qwen3-32b",
        "display": "👨‍💻 Qwen3 32B",
        "desc": "Быстрая модель через Groq",
        "temp": 0.7,
        "max_tokens": 8192
    },
    "mistral-large-3": {
        "name": "mistral-large-3",
        "display": "🌬️ Mistral Large 3",
        "desc": "Флагман Mistral",
        "temp": 0.7,
        "max_tokens": 8192
    },
    "codestral": {
        "name": "codestral",
        "display": "💻 Codestral",
        "desc": "Mistral, специализирована на коде",
        "temp": 0.7,
        "max_tokens": 8192
    },
    "devstral": {
        "name": "devstral",
        "display": "💻 Devstral",
        "desc": "Mistral, для разработки",
        "temp": 0.7,
        "max_tokens": 8192
    },
    "gpt-oss-120b": {
        "name": "gpt-oss-120b",
        "display": "🔓 GPT-OSS 120B",
        "desc": "Open-source GPT через Groq/Cloudflare",
        "temp": 0.7,
        "max_tokens": 8192
    },
    "gpt-oss-20b": {
        "name": "gpt-oss-20b",
        "display": "🔓 GPT-OSS 20B",
        "desc": "Лёгкая версия через Groq",
        "temp": 0.7,
        "max_tokens": 8192
    },
    "nemotron-3-super-120b": {
        "name": "nemotron-3-super-120b",
        "display": "🎮 Nemotron 3 Super 120B",
        "desc": "NVIDIA",
        "temp": 0.7,
        "max_tokens": 8192
    },
    "nemotron-3-nano-30b": {
        "name": "nemotron-3-nano-30b",
        "display": "🎮 Nemotron 3 Nano 30B",
        "desc": "NVIDIA / OpenRouter",
        "temp": 0.7,
        "max_tokens": 8192
    },
    "glm-4.7-flash": {
        "name": "glm-4.7-flash",
        "display": "⚡ GLM-4.7 Flash",
        "desc": "Zhipu (Z.ai) / Cloudflare",
        "temp": 0.7,
        "max_tokens": 8192
    },
    "glm-4.5-air": {
        "name": "glm-4.5-air",
        "display": "🎈 GLM-4.5 Air",
        "desc": "Zhipu (Z.ai)",
        "temp": 0.7,
        "max_tokens": 8192
    },
    "deepseek-r1": {
        "name": "deepseek-r1",
        "display": "🔬 DeepSeek R1",
        "desc": "Рассуждающая модель",
        "temp": 0.7,
        "max_tokens": 8192
    },
    "claude-sonnet-4-5": {
        "name": "claude-sonnet-4-5",
        "display": "🧠 Claude Sonnet 4.5",
        "desc": "Anthropic через FreeLLM proxy",
        "temp": 0.7,
        "max_tokens": 8192
    },
}

# Динамический каталог (загружается из FreeLLM API)
AVAILABLE_AI_MODELS = STATIC_AI_MODELS.copy()


DEFAULT_AI_MODEL = "auto"

BASE_URL = SITE_BASE + "/?p=lora"
DEFAULT_MIN_DAYS = int(MIN_DAYS_ENV) if MIN_DAYS_ENV and MIN_DAYS_ENV.isdigit() else 0
DEFAULT_TAGS = []
MAX_PAGES = 50
EXPORT_THRESHOLD = 50
COOLDOWN_SECONDS = 20
FORWARDED_FILE = "forwarded.json"
USERS_FILE = "users.json"
LAST_REQUEST_TIME = 0
REQUEST_DELAY = 30.0


# ================= ПРЕМИУМ ЭМОДЗИ =================
def premium_emoji(emoji_id: str, fallback: str = "⭐") -> str:
    return f'<tg-emoji emoji-id="{emoji_id}">{fallback}</tg-emoji>'

PREMIUM_EMOJI = {
    "sparkle": premium_emoji("5325819430553263482", "🤩"),
    "fire": premium_emoji("5364703953407018920", "🔥"),
    "heart": premium_emoji("5364703953407018919", "❤️"),
    "star": premium_emoji("5364703953407018921", "⭐"),
    "cool": premium_emoji("5364703953407018922", "😎"),
    "party": premium_emoji("5364703953407018923", "🎉"),
}

EMOJI = {
    "brain": "🧠", "id": "🆔", "days": "🕸️", "delete": "🗑️", "search": "🔍",
    "stats": "📊", "settings": "⚙️", "tag": "🏷️", "clock": "⏰", "check": "✅",
    "warning": "⚠️", "error": "❌", "info": "ℹ️", "file": "📄", "stop": "🛑",
    "restart": "🔄", "lock": "🔒", "users": "👥", "log": "📜", "db": "🗄️"
}

def safe_html_text(text: str) -> str:
    return html.escape(text)

def markdown_to_html(text: str) -> str:
    if not text:
        return ""
    code_blocks = []
    inline_codes = []
    links = []

    def stash_code_block(m):
        lang = (m.group(1) or "").strip()
        code = html.escape(m.group(2))
        lang_attr = f' class="language-{html.escape(lang)}"' if lang else ""
        code_blocks.append(f"<pre><code{lang_attr}>{code}</code></pre>")
        return f"\x00CB{len(code_blocks)-1}\x00"
    text = re.sub(r'```(\w*)\n?(.*?)```', stash_code_block, text, flags=re.DOTALL)

    def stash_inline_code(m):
        inline_codes.append(f"<code>{html.escape(m.group(1))}</code>")
        return f"\x00IC{len(inline_codes)-1}\x00"
    text = re.sub(r'`([^`\n]+?)`', stash_inline_code, text)

    def stash_link(m):
        label, url = html.escape(m.group(1)), html.escape(m.group(2))
        links.append(f'<a href="{url}">{label}</a>')
        return f"\x00LK{len(links)-1}\x00"
    text = re.sub(r'\[([^\]]+)\]\((https?://[^\s\)]+)\)', stash_link, text)

    text = re.sub(r'^#{1,6}[ \t]+(.+?)[ \t]*$', lambda m: f"\x01B\x01{m.group(1)}\x01b\x01", text, flags=re.MULTILINE)

    def stash_blockquote(m):
        lines = [re.sub(r'^>[ \t]?', '', line) for line in m.group(0).split('\n')]
        return "\x01Q\x01" + "\n".join(lines) + "\x01q\x01"
    text = re.sub(r'^>.*(?:\n>.*)*', stash_blockquote, text, flags=re.MULTILINE)

    text = re.sub(r'^([ \t]*)[-*+][ \t]+', r'\1• ', text, flags=re.MULTILINE)
    text = re.sub(r'\*\*(.+?)\*\*', lambda m: f"\x01B\x01{m.group(1)}\x01b\x01", text, flags=re.DOTALL)
    text = re.sub(r'__(.+?)__', lambda m: f"\x01B\x01{m.group(1)}\x01b\x01", text, flags=re.DOTALL)
    text = re.sub(r'(?<!\*)\*([^\*\n]+?)\*(?!\*)', lambda m: f"\x01I\x01{m.group(1)}\x01i\x01", text)
    text = re.sub(r'(?<![\w_])_([^_\n]+?)_(?![\w_])', lambda m: f"\x01I\x01{m.group(1)}\x01i\x01", text)
    text = re.sub(r'~~(.+?)~~', lambda m: f"\x01S\x01{m.group(1)}\x01s\x01", text, flags=re.DOTALL)
    text = html.escape(text)
    text = (text
            .replace('\x01B\x01', '<b>').replace('\x01b\x01', '</b>')
            .replace('\x01I\x01', '<i>').replace('\x01i\x01', '</i>')
            .replace('\x01S\x01', '<s>').replace('\x01s\x01', '</s>')
            .replace('\x01Q\x01', '<blockquote>').replace('\x01q\x01', '</blockquote>'))
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
ai_conversations = {}      # user_id -> list of {"role": "user"|"assistant", "text": str}
MAX_AI_HISTORY = 20        # храним последние 20 сообщений (10 пар)

allowed_ai_users_env = os.getenv("ALLOWED_AI_USERS", "")
if allowed_ai_users_env:
    for uid_str in allowed_ai_users_env.split(","):
        uid_str = uid_str.strip()
        if uid_str:
            try:
                allowed_ai_users.add(int(uid_str))
            except ValueError:
                pass
freellmapi_session = None
current_ai_model = DEFAULT_AI_MODEL
ai_memory = {}
MEMORY_FILE = "ai_memory.json"
log_handler = None
last_search_results = None
last_search_meta = None

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
    for k in sorted(os.environ.keys()):
        print(f"  - {k}")
    sys.exit(1)

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
        self._lock = threading.Lock()

    def set_level(self, level):
        self.setLevel(level)
        self.min_level = level
        logger.info(f"📊 Уровень логов изменён на: {logging.getLevelName(level)}")

    def _send(self, msg):
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            data = {"chat_id": self.chat_id, "text": msg, "parse_mode": "HTML"}
            requests.post(url, json=data, timeout=10)
        except Exception as e:
            print(f"Failed to send log to Telegram: {e}")

    def emit(self, record):
        try:
            with self._lock:
                now = time.time()
                if now - self.last_send < self.cooldown:
                    return
                self.last_send = now
            moscow_time = datetime.now(timezone(timedelta(hours=3))).strftime('%H:%M:%S')
            level_emoji = {"DEBUG": "🔍", "INFO": "ℹ️", "WARNING": "⚠️", "ERROR": "❌", "CRITICAL": "🚨"}.get(record.levelname, "📋")
            msg = f"{level_emoji} <b>{record.levelname}:</b>\n\n🕐 МСК {moscow_time}\n📋 <code>{record.getMessage()}</code>"
            thread = threading.Thread(target=self._send, args=(msg,))
            thread.daemon = True
            thread.start()
        except Exception as e:
            print(f"Failed to send log to Telegram: {e}")

# ================= FREELLM API HTTP CLIENT =================
def init_freellmapi_http():
    global freellmapi_session
    if not FREELLMAPI_API_KEY:
        logger.warning("⚠️ FREELLMAPI_API_KEY не задан — AI-функции недоступны")
        return False
    try:
        freellmapi_session = requests.Session()
        freellmapi_session.headers.update({
            "Content-Type": "application/json",
            "Authorization": f"Bearer {FREELLMAPI_API_KEY}",
        })
        logger.info(f"✅ FreeLLM API HTTP client инициализирован ({FREELLMAPI_BASE_URL})")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации FreeLLM API HTTP: {e}")
        return False

# ================= ИНИЦИАЛИЗАЦИЯ ЛОГ-БОТА =================
async def init_log_bot():
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
                "text": "🟢 <b>Бот запущен (polling)!</b>\n\n🕐 МСК " + moscow_time + "\n📊 Лог-уровень: " + LOG_LEVEL,
                "parse_mode": "HTML"
            }
            await asyncio.to_thread(requests.post, url, json=data, timeout=10)
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
            logger.info(f"📦 Загружено {len(forwarded_messages)} пересланных сообщений")
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
            logger.info(f"👥 Загружено {len(known_users)} пользователей")
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

SETTINGS_FILE = "settings.json"

def load_settings():
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
    global LAST_REQUEST_TIME
    headers = {"User-Agent": "Mozilla/5.0"}
    for attempt in range(1, max_retries + 1):
        try:
            now = time.time()
            time_since_last = now - LAST_REQUEST_TIME
            if time_since_last < REQUEST_DELAY:
                await asyncio.sleep(REQUEST_DELAY - time_since_last)
            response = await asyncio.to_thread(requests.get, url, headers=headers, timeout=20)
            response.raise_for_status()
            LAST_REQUEST_TIME = time.time()
            return response.text
        except requests.RequestException as e:
            logger.warning(f"Попытка {attempt} упала: {e}")
            if attempt == max_retries:
                return None
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
        soup = BeautifulSoup(html, "html.parser")
        raw_loras = soup.find_all("p", class_="lora_head")
        if not raw_loras:
            logger.info("Стр. " + str(page) + ": нет лор на странице → завершаю")
            break
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

MAX_MESSAGE_LENGTH = 4000

def split_long_message(text: str, max_length: int = MAX_MESSAGE_LENGTH) -> list[str]:
    if len(text) <= max_length:
        return [text]
    parts = []
    current = ""
    lines = text.split('\n')
    for line in lines:
        if len(line) > max_length:
            if current:
                parts.append(current)
                current = ""
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
        elif len(current) + len(line) + 1 <= max_length:
            current += ('\n' if current else '') + line
        else:
            if current:
                parts.append(current)
            current = line
    if current:
        parts.append(current)
    return parts

async def send_long_message(message: Message, text: str, parse_mode: str = "HTML", split_code: bool = True):
    if len(text) <= MAX_MESSAGE_LENGTH:
        await message.answer(text, parse_mode=parse_mode)
        return
    parts = split_long_message(text, MAX_MESSAGE_LENGTH)
    for i, part in enumerate(parts, 1):
        if len(parts) > 1:
            prefix = f"<i>({i}/{len(parts)})</i>\n" if parse_mode == "HTML" else f"({i}/{len(parts)})\n"
            part = prefix + part
        try:
            await message.answer(part, parse_mode=parse_mode)
            if i < len(parts):
                await asyncio.sleep(0.3)
        except Exception as e:
            logger.error(f"❌ Ошибка отправки части {i}: {e}")
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
            "blocked": False,
            "unsubscribed": False
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

# ================= FREELLM API HTTP CLIENT (расширенный) =================

async def ask_ai_http(prompt: str, history: list = None, model_key: str = None) -> dict:
    model_key = model_key or current_ai_model
    model_info = AVAILABLE_AI_MODELS.get(model_key, AVAILABLE_AI_MODELS[DEFAULT_AI_MODEL])
    model_name = model_info["name"]
    if not freellmapi_session or not FREELLMAPI_API_KEY:
        return {"success": False, "error": "FreeLLM API не инициализирован (нет FREELLMAPI_API_KEY)"}
    try:
        messages = []
        memory_text = ai_memory.get(model_key, "")
        if memory_text:
            messages.append({"role": "system", "content": memory_text})
        if history:
            for msg in history:
                role = "user" if msg.get("role") == "user" else "assistant"
                messages.append({"role": role, "content": msg.get("text", "")})
        messages.append({"role": "user", "content": prompt})
        payload = {
            "model": model_name,
            "messages": messages,
            "temperature": model_info.get("temp", 0.7),
            "max_tokens": model_info.get("max_tokens", 8192),
        }
        def make_request():
            return freellmapi_session.post(FREELLMAPI_CHAT_URL, json=payload, timeout=60)
        response = await asyncio.to_thread(make_request)
        if response.status_code == 200:
            data = response.json()
            choices = data.get("choices", [])
            if choices:
                message = choices[0].get("message", {})
                text = (message.get("content") or "").strip()
                if text:
                    return {"success": True, "text": text}
                reasoning = message.get("reasoning")
                if reasoning:
                    return {"success": True, "text": reasoning.strip()}
            return {"success": False, "error": "Пустой или неверный ответ от API"}
        elif response.status_code == 400:
            return {"success": False, "error": "❌ Неверный запрос. Попробуй перефразировать или смени модель."}
        elif response.status_code == 401:
            return {"success": False, "error": "🔒 Неверный или отсутствующий FREELLMAPI_API_KEY."}
        elif response.status_code == 402:
            return {"success": False, "error": "💳 Недостаточно кредитов (если используется платный тир)."}
        elif response.status_code == 404:
            return {"success": False, "error": f"❓ Модель не найдена: {model_name}. Проверь название в дашборде FreeLLM API."}
        elif response.status_code == 429:
            return {"success": False, "error": "🔄 Лимит запросов. Подожди минуту или смени модель."}
        elif response.status_code >= 500:
            return {"success": False, "error": "⚠️ Серверная ошибка провайдера. Попробуй позже или смени модель."}
        else:
            return {"success": False, "error": f"⚠️ HTTP {response.status_code}: {response.text[:150]}"}
    except requests.exceptions.Timeout:
        return {"success": False, "error": "⏱️ Таймаут ответа. Попробуй позже."}
    except requests.exceptions.ConnectionError:
        return {"success": False, "error": "🌐 Ошибка соединения. Проверь, что FreeLLM API запущен на " + FREELLMAPI_BASE_URL}
    except Exception as e:
        logger.error(f"❌ FreeLLM API HTTP error: {str(e)}")
        return {"success": False, "error": f"⚠️ Ошибка: {str(e)[:200]}"}


async def generate_image(prompt: str, model: str = "auto", size: str = "1024x1024") -> dict:
    """Генерация изображения через /v1/images/generations"""
    if not freellmapi_session or not FREELLMAPI_API_KEY:
        return {"success": False, "error": "FreeLLM API не инициализирован"}
    payload = {
        "model": model,
        "prompt": prompt,
        "n": 1,
        "size": size,
    }
    try:
        def _post():
            return freellmapi_session.post(FREELLMAPI_IMAGE_URL, json=payload, timeout=60)
        r = await asyncio.to_thread(_post)
        if r.status_code == 200:
            data = r.json()
            images = data.get("data", [])
            if images and images[0].get("url"):
                return {"success": True, "url": images[0]["url"], "revised_prompt": images[0].get("revised_prompt", "")}
            return {"success": False, "error": "Пустой ответ от API изображений"}
        elif r.status_code == 429:
            return {"success": False, "error": "🔄 Лимит генерации изображений. Подожди."}
        elif r.status_code >= 500:
            return {"success": False, "error": "⚠️ Серверная ошибка при генерации изображения."}
        else:
            return {"success": False, "error": f"⚠️ HTTP {r.status_code}: {r.text[:150]}"}
    except requests.exceptions.Timeout:
        return {"success": False, "error": "⏱️ Таймаут генерации изображения."}
    except Exception as e:
        logger.error(f"❌ Image generation error: {e}")
        return {"success": False, "error": f"⚠️ Ошибка: {str(e)[:200]}"}


async def generate_speech(text: str, model: str = "tts-1", voice: str = "alloy") -> dict:
    """Генерация аудио через /v1/audio/speech"""
    if not freellmapi_session or not FREELLMAPI_API_KEY:
        return {"success": False, "error": "FreeLLM API не инициализирован"}
    payload = {
        "model": model,
        "input": text,
        "voice": voice,
    }
    try:
        def _post():
            return freellmapi_session.post(FREELLMAPI_AUDIO_SPEECH_URL, json=payload, timeout=60)
        r = await asyncio.to_thread(_post)
        if r.status_code == 200:
            return {"success": True, "audio_bytes": r.content}
        elif r.status_code == 429:
            return {"success": False, "error": "🔄 Лимит TTS. Подожди."}
        elif r.status_code >= 500:
            return {"success": False, "error": "⚠️ Серверная ошибка TTS."}
        else:
            return {"success": False, "error": f"⚠️ HTTP {r.status_code}: {r.text[:150]}"}
    except requests.exceptions.Timeout:
        return {"success": False, "error": "⏱️ Таймаут генерации аудио."}
    except Exception as e:
        logger.error(f"❌ TTS error: {e}")
        return {"success": False, "error": f"⚠️ Ошибка: {str(e)[:200]}"}


async def transcribe_audio(file_path: str, model: str = "whisper-1") -> dict:
    """Распознавание речи через /v1/audio/transcriptions"""
    if not freellmapi_session or not FREELLMAPI_API_KEY:
        return {"success": False, "error": "FreeLLM API не инициализирован"}
    try:
        def _post():
            with open(file_path, "rb") as f:
                files = {"file": f}
                data = {"model": model}
                return freellmapi_session.post(FREELLMAPI_AUDIO_TRANSCRIPT_URL, files=files, data=data, timeout=60)
        r = await asyncio.to_thread(_post)
        if r.status_code == 200:
            data = r.json()
            text = data.get("text", "").strip()
            if text:
                return {"success": True, "text": text}
            return {"success": False, "error": "Пустой результат транскрипции"}
        elif r.status_code == 429:
            return {"success": False, "error": "🔄 Лимит распознавания речи. Подожди."}
        elif r.status_code >= 500:
            return {"success": False, "error": "⚠️ Серверная ошибка распознавания."}
        else:
            return {"success": False, "error": f"⚠️ HTTP {r.status_code}: {r.text[:150]}"}
    except requests.exceptions.Timeout:
        return {"success": False, "error": "⏱️ Таймаут распознавания речи."}
    except Exception as e:
        logger.error(f"❌ Transcription error: {e}")
        return {"success": False, "error": f"⚠️ Ошибка: {str(e)[:200]}"}

# ================= ОБРАТНАЯ СВЯЗЬ =================

# 1. Сначала ответы владельца на пересланные сообщения
@dp.message(F.from_user.id == OWNER_ID_INT, F.reply_to_message)
async def handle_owner_reply(message: Message):
    """Обрабатывает ТОЛЬКО ответы владельца на пересланные сообщения"""
    reply_msg_id = message.reply_to_message.message_id
    logger.info(f"📨 Владелец ответил на message_id={reply_msg_id}")
    if reply_msg_id in forwarded_messages:
        user_id = forwarded_messages[reply_msg_id]
        logger.info(f"✅ Найдено соответствие: message_id={reply_msg_id} → user_id={user_id}")
        try:
            if message.text:
                await bot.send_message(chat_id=user_id, text=f"{PREMIUM_EMOJI['sparkle']} {message.text}", parse_mode="HTML")
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
            del forwarded_messages[reply_msg_id]
            save_forwarded()
            return
        except Exception as e:
            logger.error("Ошибка отправки ответа: " + str(e))
            await message.answer(f"{EMOJI['error']} Не удалось отправить: {str(e)[:100]}", parse_mode="HTML")
            return
    else:
        logger.info(f"⚠️ message_id={reply_msg_id} не найден в forwarded_messages")


# 2. Затем диалог с AI (чтобы перехватывать сообщения до пересылки владельцу)
@dp.message(lambda m: m.from_user.id in ai_conversations and m.text and not m.text.startswith('/'))
async def handle_ai_conversation(m: Message):
    user_id = m.from_user.id
    prompt = m.text.strip()
    if not prompt:
        return

    ai_conversations[user_id].append({"role": "user", "text": prompt})
    if len(ai_conversations[user_id]) > MAX_AI_HISTORY:
        ai_conversations[user_id] = ai_conversations[user_id][-MAX_AI_HISTORY:]

    status_msg = await m.answer(f"{EMOJI['brain']} <i>Думаю...</i>", parse_mode="HTML")

    history = ai_conversations[user_id][:-1]
    result = await ask_ai_http(prompt, history=history)

    if result["success"]:
        answer_text = result["text"]
        ai_conversations[user_id].append({"role": "assistant", "text": answer_text})
        answer_html = markdown_to_html(answer_text)

        if len(answer_html) <= MAX_MESSAGE_LENGTH:
            # Просто редактируем "Думаю..." в ответ
            try:
                await status_msg.edit_text(answer_html, parse_mode="HTML")
            except Exception as e:
                logger.warning(f"⚠️ Не удалось отредактировать сообщение: {e}")
                await status_msg.delete()
                await send_long_message(m, answer_html, parse_mode="HTML")
        else:
            # Ответ длинный — удаляем "Думаю..." и шлём частями
            try:
                await status_msg.delete()
            except Exception:
                pass
            await send_long_message(m, answer_html, parse_mode="HTML")

        logger.info(f"🤖 AI диалог [{user_id}]: '{prompt[:50]}...' → ответ ({len(answer_html)} символов)")
    else:
        await status_msg.edit_text(f"{EMOJI['error']} {result['error']}", parse_mode="HTML")
        logger.warning(f"⚠️ AI диалог [{user_id}] ошибка: {result['error']}")

@dp.message(lambda m: m.from_user.id in ai_conversations and m.voice)
async def handle_ai_voice(m: Message):
    """Распознавание голоса в режиме AI-диалога"""
    user_id = m.from_user.id
    voice = m.voice

    # Скачиваем голосовое
    status_msg = await m.answer(f"{EMOJI['brain']} <i>Слушаю...</i>", parse_mode="HTML")
    try:
        file = await bot.get_file(voice.file_id)
        file_path = f"/tmp/ai_voice_{user_id}_{voice.file_unique_id}.ogg"
        await bot.download_file(file.file_path, file_path)

        # Конвертируем в mp3 если нужно (whisper принимает mp3, m4a, wav, ogg)
        # OGG от Telegram обычно работает напрямую
        result = await transcribe_audio(file_path)

        # Удаляем временный файл
        try:
            os.remove(file_path)
        except:
            pass

        if not result["success"]:
            await status_msg.edit_text(f"{EMOJI['error']} {result['error']}", parse_mode="HTML")
            return

        transcribed_text = result["text"]
        await status_msg.edit_text(f"🎤 <i>{safe_html_text(transcribed_text[:200])}</i>", parse_mode="HTML")

        # Отправляем распознанный текст в тот же AI-диалог
        ai_conversations[user_id].append({"role": "user", "text": transcribed_text})
        if len(ai_conversations[user_id]) > MAX_AI_HISTORY:
            ai_conversations[user_id] = ai_conversations[user_id][-MAX_AI_HISTORY:]

        think_msg = await m.answer(f"{EMOJI['brain']} <i>Думаю...</i>", parse_mode="HTML")
        history = ai_conversations[user_id][:-1]
        ai_result = await ask_ai_http(transcribed_text, history=history)

        if ai_result["success"]:
            answer_text = ai_result["text"]
            ai_conversations[user_id].append({"role": "assistant", "text": answer_text})
            answer_html = markdown_to_html(answer_text)

            if len(answer_html) <= MAX_MESSAGE_LENGTH:
                try:
                    await think_msg.edit_text(answer_html, parse_mode="HTML")
                except Exception as e:
                    logger.warning(f"⚠️ Не удалось отредактировать: {e}")
                    await think_msg.delete()
                    await send_long_message(m, answer_html, parse_mode="HTML")
            else:
                try:
                    await think_msg.delete()
                except Exception:
                    pass
                await send_long_message(m, answer_html, parse_mode="HTML")

            logger.info(f"🤖 AI голос [{user_id}]: '{transcribed_text[:50]}...' → ответ ({len(answer_html)} символов)")
        else:
            await think_msg.edit_text(f"{EMOJI['error']} {ai_result['error']}", parse_mode="HTML")
            logger.warning(f"⚠️ AI голос [{user_id}] ошибка: {ai_result['error']}")

    except Exception as e:
        logger.error(f"❌ Ошибка обработки голосового: {e}")
        await status_msg.edit_text(f"{EMOJI['error']} Ошибка обработки голоса: {str(e)[:100]}", parse_mode="HTML")
        try:
            os.remove(file_path)
        except:
            pass


# 3. И только потом пересылка обычных сообщений не-владельцев
@dp.message(F.from_user.id != OWNER_ID_INT)
async def handle_user_message(message: Message):
    user_id = message.from_user.id
    if user_id in known_users and known_users[user_id].get("blocked", False):
        logger.info(f"🚫 Игнорировано сообщение от заблокированного пользователя {user_id}")
        return
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
    if m.from_user.id != OWNER_ID_INT:
        return
    parts = m.text.split(maxsplit=2)
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
        if target_user_id not in known_users:
            await m.answer(
                f"{EMOJI['warning']} Пользователь <code>{target_user_id}</code> не найден в базе.\n"
                f"<i>Он никогда не писал боту или данные были сброшены</i>",
                parse_mode="HTML"
            )
            return
        user_info = known_users[target_user_id]
        username = user_info.get("username", "нет")
        name = user_info.get("full_name", "Unknown")
        await bot.send_message(chat_id=target_user_id, text=f"{PREMIUM_EMOJI['sparkle']} {message_text}", parse_mode="HTML")
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
    if m.from_user.id != OWNER_ID_INT:
        return
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
    await m.answer(
        f"{EMOJI['info']} <b>Рассылка запущена!</b>\n\n"
        f"👥 Получателей: <b>{total_users}</b>\n"
        f"📝 Текст: <i>{message_text[:100]}{'...' if len(message_text) > 100 else ''}</i>\n\n"
        f"<i>Отчёт будет отправлен после завершения</i>",
        parse_mode="HTML"
    )
    sent_count = 0
    failed_count = 0
    blocked_count = 0
    for user_id, user_data in known_users.items():
        try:
            username = user_data.get("username", "нет")
            await bot.send_message(chat_id=user_id, text=f"💥 {message_text}", parse_mode="HTML")
            sent_count += 1
            logger.info(f"📤 Рассылка: отправлено пользователю {user_id} (@{username})")
            await asyncio.sleep(0.1)
        except Exception as e:
            error_str = str(e).lower()
            if "blocked" in error_str or "bot was blocked" in error_str:
                blocked_count += 1
                logger.warning(f"🚫 Рассылка: пользователь {user_id} заблокировал бота")
            else:
                failed_count += 1
                logger.error(f"❌ Рассылка: ошибка отправки пользователю {user_id}: {e}")
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
            await m.answer(f"{EMOJI['warning']} Пользователь <code>{target_user_id}</code> не найден в базе.", parse_mode="HTML")
            return
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
            await m.answer(f"{EMOJI['warning']} Пользователь <code>{target_user_id}</code> не найден в базе.", parse_mode="HTML")
            return
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
    if m.from_user.id != OWNER_ID_INT:
        return
    blocked_users = [(uid, data) for uid, data in known_users.items() if data.get("blocked", False)]
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

@dp.message(Command("allowai"))
async def cmd_allowai(m: Message):
    if m.from_user.id != OWNER_ID_INT:
        return
    parts = m.text.split()
    if len(parts) < 2:
        await m.answer(f"{EMOJI['info']} Используй: <code>/allowai &lt;user_id&gt;</code>", parse_mode="HTML")
        return
    try:
        new_user_id = int(parts[1])
        allowed_ai_users.add(new_user_id)
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

@dp.message(Command("aiallowed"))
async def cmd_aiallowed(m: Message):
    if m.from_user.id != OWNER_ID_INT:
        return
    if not allowed_ai_users:
        await m.answer(f"{EMOJI['info']} Список пуст", parse_mode="HTML")
        return
    txt = f"{EMOJI['lock']} <b>Доступ к /ai ({len(allowed_ai_users)}):</b>\n\n"
    for uid in list(allowed_ai_users)[:20]:
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
    user_id = m.from_user.id
    if not is_user_allowed(user_id, allowed_ai_users):
        logger.warning(f"🚫 Доступ к /ai запрещён для пользователя {user_id}")
        await m.answer(
            f"{EMOJI['lock']} <b>Доступ запрещён</b>\n\n"
            f"<i>Эта команда доступна только авторизованным пользователям</i>",
            parse_mode="HTML"
        )
        return

    # Если уже в режиме диалога — завершаем
    if user_id in ai_conversations:
        del ai_conversations[user_id]
        await m.answer(
            f"{EMOJI['check']} <b>Диалог с AI завершён</b>\n\n"
            f"<i>История очищена. Используй /ai чтобы начать новый разговор</i>",
            parse_mode="HTML"
        )
        logger.info(f"🛑 AI-диалог завершён для пользователя {user_id}")
        return

    prompt = m.text.split(maxsplit=1)[1] if len(m.text.split()) > 1 else ""

    # Если есть текст — одноразовый запрос (старая логика)
    if prompt:
        status_msg = await m.answer(f"{EMOJI['brain']} <i>Думаю...</i>", parse_mode="HTML")
        result = await ask_ai_http(prompt)
        if result["success"]:
            answer = markdown_to_html(result["text"])
            if len(answer) <= MAX_MESSAGE_LENGTH:
                await status_msg.edit_text(f"{PREMIUM_EMOJI['sparkle']} <b>AI:</b>\n\n{answer}", parse_mode="HTML")
            else:
                await status_msg.delete()
                await send_long_message(m, f"{PREMIUM_EMOJI['sparkle']} <b>AI:</b>\n\n{answer}", parse_mode="HTML")
            logger.info(f"🤖 AI: '{prompt[:50]}...' → ответ ({len(answer)} символов)")
        else:
            await status_msg.edit_text(f"{EMOJI['error']} {result['error']}", parse_mode="HTML")
            logger.warning(f"⚠️ AI ошибка: {result['error']}")
        return
    
    # Начинаем режим диалога
    ai_conversations[user_id] = []
    model_display = AVAILABLE_AI_MODELS.get(current_ai_model, {}).get("display", current_ai_model)
    await m.answer(
        f"{EMOJI['brain']} <b>Режим диалога с AI включён</b>\n\n"
        f"🤖 Модель: <b>{model_display}</b>\n"
        f"💬 Просто пиши сообщения — я буду отвечать с учётом контекста.\n\n"
        f"<i>Напиши /ai ещё раз чтобы завершить разговор</i>",
        parse_mode="HTML"
    )
    logger.info(f"🟢 AI-диалог начат для пользователя {user_id}")


# ================= ДИНАМИЧЕСКАЯ ЗАГРУЗКА МОДЕЛЕЙ =================
async def refresh_available_models():
    """Загружает список моделей из FreeLLM API /models и обновляет AVAILABLE_AI_MODELS."""
    global AVAILABLE_AI_MODELS
    if not freellmapi_session or not FREELLMAPI_API_KEY:
        logger.warning("⚠️ FreeLLM API не инициализирован — используем статический список моделей")
        AVAILABLE_AI_MODELS = STATIC_AI_MODELS.copy()
        return
    try:
        def _get():
            r = freellmapi_session.get(FREELLMAPI_MODELS_URL, timeout=15)
            if r.status_code == 200:
                return r.json()
            logger.warning(f"⚠️ /models вернул {r.status_code}: {r.text[:200]}")
            return None
        data = await asyncio.to_thread(_get)
        if not data or "data" not in data:
            logger.warning("⚠️ Неверный ответ от /models — используем статический список")
            AVAILABLE_AI_MODELS = STATIC_AI_MODELS.copy()
            return
        raw_models = data.get("data", [])
        new_models = {}
        # Сначала добавляем auto (роутер), если API его не вернул
        if "auto" in STATIC_AI_MODELS:
            new_models["auto"] = STATIC_AI_MODELS["auto"]
        for m in raw_models:
            model_id = m.get("id", "")
            if not model_id:
                continue
            key = model_id.lower()
            if key in STATIC_AI_MODELS:
                new_models[key] = STATIC_AI_MODELS[key]
            else:
                # Автоматическое описание для неизвестной модели
                new_models[key] = {
                    "name": model_id,
                    "display": f"🤖 {model_id}",
                    "desc": "Модель из FreeLLM API",
                    "temp": 0.7,
                    "max_tokens": 8192
                }
        if new_models:
            AVAILABLE_AI_MODELS = new_models
            logger.info(f"✅ Загружено {len(AVAILABLE_AI_MODELS)} моделей из FreeLLM API")
        else:
            logger.warning("⚠️ API вернул пустой список моделей — используем статический")
            AVAILABLE_AI_MODELS = STATIC_AI_MODELS.copy()
    except Exception as e:
        logger.warning(f"⚠️ Ошибка загрузки моделей из API: {e} — используем статический список")
        AVAILABLE_AI_MODELS = STATIC_AI_MODELS.copy()

async def fetch_available_models() -> set[str]:
    if not freellmapi_session or not FREELLMAPI_API_KEY:
        return set()
    try:
        def _get():
            r = freellmapi_session.get(FREELLMAPI_MODELS_URL, timeout=15)
            if r.status_code == 200:
                data = r.json()
                return {m.get("id", "") for m in data.get("data", [])}
            return set()
        return await asyncio.to_thread(_get)
    except Exception as e:
        logger.warning(f"⚠️ Не удалось получить список моделей FreeLLM API: {e}")
        return set()

async def ping_model(model_name: str) -> tuple[bool, float]:
    if not freellmapi_session or not FREELLMAPI_API_KEY:
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
            return freellmapi_session.post(FREELLMAPI_CHAT_URL, json=payload, timeout=10)
        r = await asyncio.wait_for(asyncio.to_thread(_post), timeout=12)
        elapsed = time.time() - start
        if r.status_code == 200:
            return True, elapsed
        elif r.status_code in (429, 402):
            return True, elapsed
        else:
            return False, elapsed
    except asyncio.TimeoutError:
        return False, time.time() - start
    except Exception as e:
        logger.debug(f"Ping error {model_name}: {e}")
        return False, 0.0


# ================= КОМАНДЫ E621 WIKI =================

@dp.message(Command("taginfo", "wiki"))
async def cmd_taginfo(m: Message):
    """Показывает wiki-информацию о теге e621 + 1-2 примера изображений.
    Теги в тексте обёрнуты в <code> для копирования."""
    if not is_user_allowed(m.from_user.id, allowed_e621_users):
        return

    parts = m.text.split(maxsplit=1)
    if len(parts) < 2:
        await m.answer(
            f"{EMOJI['info']} <b>Поиск по вики e621</b>\n\n"
            f"<code>/taginfo &lt;тег&gt;</code>\n"
            f"<code>/wiki &lt;тег&gt;</code>\n\n"
            f"<b>Примеры:</b>\n"
            f"<code>/taginfo canine</code>\n"
            f"<code>/wiki anthro</code>\n\n"
            f"<i>• Теги в тексте можно выделить и скопировать</i>\n"
            f"<i>• Если тег не найден — бот предложит похожие варианты</i>",
            parse_mode="HTML"
        )
        return

    tag_name = parts[1].strip()
    status_msg = await m.answer(
        f"{EMOJI['search']} <i>Ищу <code>{safe_html_text(tag_name)}</code> на e621...</i>",
        parse_mode="HTML"
    )

    tag_task = asyncio.create_task(get_e621_tag_info(tag_name))
    wiki_task = asyncio.create_task(get_e621_wiki_page(tag_name))
    posts_task = asyncio.create_task(get_e621_posts(tag_name, limit=2))

    tag_result = await tag_task
    wiki_result = await wiki_task
    image_urls = await posts_task

    if not tag_result["success"]:
        # Тег не найден — предлагаем похожие inline-кнопками
        suggestions = await get_e621_tag_suggestions(tag_name, limit=6)
        if suggestions:
            kb = build_suggestions_keyboard(suggestions)
            await status_msg.edit_text(
                f"{EMOJI['warning']} Тег <code>{safe_html_text(tag_name)}</code> не найден на e621.\n\n"
                f"<b>Возможно, вы имели в виду:</b>",
                parse_mode="HTML",
                reply_markup=kb
            )
        else:
            await status_msg.edit_text(
                f"{EMOJI['error']} Тег <code>{safe_html_text(tag_name)}</code> не найден на e621, "
                f"и похожих тегов тоже не нашлось.",
                parse_mode="HTML"
            )
        return

    tag_data = tag_result["tag"]
    wiki_data = wiki_result.get("wiki") if wiki_result.get("success") else None

    text = format_tag_info(tag_data, wiki_data)

    # Отправляем текст (без inline-кнопок, теги в <code>)
    if len(text) > MAX_MESSAGE_LENGTH:
        parts_msg = split_long_message(text, MAX_MESSAGE_LENGTH)
        await status_msg.delete()
        for i, part in enumerate(parts_msg, 1):
            if len(parts_msg) > 1:
                part = f"<i>({i}/{len(parts_msg)})</i>\n" + part
            await m.answer(part, parse_mode="HTML")
            if i < len(parts_msg):
                await asyncio.sleep(0.3)
    else:
        await status_msg.edit_text(text, parse_mode="HTML")

    # Отправляем 1-2 примера изображений
    if image_urls:
        for url in image_urls:
            try:
                await m.answer_photo(photo=url)
                await asyncio.sleep(0.3)
            except Exception as e:
                logger.warning(f"⚠️ Не удалось отправить пример: {e}")
    else:
        await m.answer("<i>Примеров изображений не найдено</i>", parse_mode="HTML")

    logger.info(f"📖 Tag info e621: {tag_data['name']} (posts: {tag_data['post_count']}, images: {len(image_urls)})")


@dp.message(Command("tag"))
async def cmd_tag(m: Message):
    """Краткая информация о теге e621 (только тег, без wiki и картинок)."""
    if not is_user_allowed(m.from_user.id, allowed_e621_users):
        return

    parts = m.text.split(maxsplit=1)
    if len(parts) < 2:
        await m.answer(
            f"{EMOJI['info']} <b>Инфо о теге e621</b>\n\n"
            f"<code>/tag &lt;тег&gt;</code>\n\n"
            f"<b>Пример:</b> <code>/tag fox</code>",
            parse_mode="HTML"
        )
        return

    tag_name = parts[1].strip()
    status_msg = await m.answer(
        f"{EMOJI['search']} <i>Ищу <code>{safe_html_text(tag_name)}</code>...</i>",
        parse_mode="HTML"
    )

    result = await get_e621_tag_info(tag_name)
    if not result["success"]:
        suggestions = await get_e621_tag_suggestions(tag_name, limit=6)
        if suggestions:
            kb = build_suggestions_keyboard(suggestions)
            await status_msg.edit_text(
                f"{EMOJI['warning']} Тег <code>{safe_html_text(tag_name)}</code> не найден.\n\n"
                f"<b>Возможно, вы имели в виду:</b>",
                parse_mode="HTML",
                reply_markup=kb
            )
        else:
            await status_msg.edit_text(
                f"{EMOJI['error']} {result['error']}",
                parse_mode="HTML"
            )
        return

    tag_data = result["tag"]
    text = format_tag_info(tag_data, wiki_data=None)
    await status_msg.edit_text(text, parse_mode="HTML")
    logger.info(f"🏷️ Tag quick info e621: {tag_data['name']}")


@dp.message(Command("refreshmodels"))
async def cmd_refreshmodels(m: Message):
    if m.from_user.id != OWNER_ID_INT:
        return
    status = await m.answer(f"{EMOJI['settings']} Обновляю список моделей из FreeLLM API...", parse_mode="HTML")
    await refresh_available_models()
    model_keys = list(AVAILABLE_AI_MODELS.keys())
    # Формируем список моделей, разбивая на строки чтобы не превысить лимит
    model_lines = []
    for k in model_keys:
        info = AVAILABLE_AI_MODELS.get(k, {})
        current = "✅" if k == current_ai_model else "•"
        model_lines.append(f"{current} <code>{k}</code> — {info.get('display', k)}")

    txt = f"{EMOJI['check']} <b>Список моделей обновлён!</b>\n\n"
    txt += f"📊 Доступно: <b>{len(model_keys)}</b> моделей\n\n"
    txt += "\n".join(model_lines)
    txt += f"\n\n<i>Используй /model &lt;ключ&gt; чтобы выбрать модель</i>"
    await send_long_message(m, txt, parse_mode="HTML")
    logger.info(f"🔄 Список моделей обновлён вручную: {len(model_keys)} моделей")





# ================= YANDEX MUSIC: ИНИЦИАЛИЗАЦИЯ =================
async def init_yandex_music():
    global ym_client
    load_ym_state()
    if not YANDEX_MUSIC_TOKEN:
        logger.warning("⚠️ YANDEX_MUSIC_TOKEN не задан — Now Playing недоступен")
        return False
    try:
        from yandex_music import ClientAsync
        ym_client = await ClientAsync(YANDEX_MUSIC_TOKEN).init()
        me = ym_client.me
        logger.info(f"✅ Yandex Music: авторизован как {me.account.display_name or me.account.login}")
        return True
    except ImportError:
        logger.warning("⚠️ yandex-music не установлен. Установи: pip install -U --pre 'yandex-music[ynison]'")
        return False
    except Exception as e:
        logger.error(f"❌ Yandex Music init error: {e}")
        return False

def load_ym_state():
    global ym_last_track_id, ym_last_message_id
    try:
        if os.path.exists(YM_STATE_FILE):
            with open(YM_STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            ym_last_track_id = data.get("last_track_id")
            ym_last_message_id = data.get("last_message_id")
            logger.info(f"🎵 YM state loaded: track={ym_last_track_id}, msg={ym_last_message_id}")
    except Exception as e:
        logger.warning(f"⚠️ Ошибка загрузки ym_state: {e}")
        ym_last_track_id = None
        ym_last_message_id = None

def save_ym_state():
    try:
        data = {
            "last_track_id": ym_last_track_id,
            "last_message_id": ym_last_message_id,
        }
        with open(YM_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения ym_state: {e}")


# ================= YANDEX MUSIC: ФОНОВЫЙ МОНИТОРИНГ =================
async def ym_now_playing_loop():
    """Периодически опрашивает Ynison и шлёт 'Сейчас слушает' в группу"""
    global ym_last_track_id, ym_enabled
    if not YANDEX_MUSIC_TOKEN or not YM_TARGET_CHAT_ID:
        return

    try:
        from yandex_music.ynison import simple_async
    except ImportError:
        logger.warning("⚠️ Ynison недоступен. Установи: pip install -U --pre 'yandex-music[ynison]'")
        return

    ym_enabled = True
    logger.info("🎵 Now Playing: цикл запущен")

    while bot_running and ym_enabled:
        try:
            track = await simple_async.get_current_track(YANDEX_MUSIC_TOKEN, timeout=10.0)
            if track and track.playable_id:
                if track.playable_id != ym_last_track_id:
                    ym_last_track_id = track.playable_id
                    await send_now_playing(track)
            else:
                ym_last_track_id = None
                save_ym_state()
        except Exception as e:
            logger.warning(f"⚠️ YM polling error: {e}")

        await asyncio.sleep(10)

    logger.info("🎵 Now Playing: цикл остановлен")


async def send_now_playing(playable):
    """Удаляет старый пост и отправляет новый 'Сейчас слушает'"""
    global ym_client, ym_last_message_id

    if not ym_client:
        return

    # Сначала удаляем старый пост
    if ym_last_message_id and YM_TARGET_CHAT_ID:
        try:
            await bot.delete_message(chat_id=YM_TARGET_CHAT_ID, message_id=ym_last_message_id)
            logger.info(f"🗑️ Старый пост удалён: msg_id={ym_last_message_id}")
        except Exception as e:
            # Сообщение уже удалено или недоступно — игнорируем
            logger.debug(f"⚠️ Не удалось удалить старый пост: {e}")
        ym_last_message_id = None
        save_ym_state()

    try:
        # Получаем полную инфу о треке
        full_tracks = await ym_client.tracks([playable.playable_id])
        if not full_tracks:
            return
        track = full_tracks[0]

        title = track.title or "Unknown"
        artists = ", ".join([a.name for a in track.artists]) if track.artists else "Unknown Artist"

        # Ищем обложку
        cover_uri = None
        if track.cover_uri:
            cover_uri = track.cover_uri
        elif track.albums and track.albums[0].cover_uri:
            cover_uri = track.albums[0].cover_uri

        cover_url = None
        if cover_uri:
            cover_url = cover_uri.replace("%%", "400x400")
            if not cover_url.startswith("http"):
                cover_url = f"https://{cover_url}"

        # Формируем текст
        text = (
            f"🎵 <b>Сейчас слушает</b>\n\n"
            f"🎤 <b>{safe_html_text(title)}</b>\n"
            f"👤 {safe_html_text(artists)}\n\n"
            f"<a href='https://music.yandex.ru/track/{track.id}'>🔗 Открыть в Яндекс.Музыке</a>"
        )

        # Отправляем новый пост
        sent_msg = None
        if cover_url:
            try:
                img_resp = await asyncio.to_thread(requests.get, cover_url, timeout=15)
                img_resp.raise_for_status()
                photo = BufferedInputFile(file=img_resp.content, filename="cover.jpg")
                sent_msg = await bot.send_photo(
                    chat_id=YM_TARGET_CHAT_ID,
                    photo=photo,
                    caption=text,
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.warning(f"⚠️ Обложка не загрузилась, шлём текстом: {e}")
                sent_msg = await bot.send_message(
                    chat_id=YM_TARGET_CHAT_ID,
                    text=text,
                    parse_mode="HTML"
                )
        else:
            sent_msg = await bot.send_message(
                chat_id=YM_TARGET_CHAT_ID,
                text=text,
                parse_mode="HTML"
            )

        if sent_msg:
            ym_last_message_id = sent_msg.message_id
            save_ym_state()

        logger.info(f"🎵 Now playing sent: {artists} — {title} (msg_id={ym_last_message_id})")

    except Exception as e:
        logger.error(f"❌ send_now_playing error: {e}")


# ================= КОМАНДЫ YANDEX MUSIC =================
@dp.message(Command("ymnow"))
async def cmd_ymnow(m: Message):
    """Вручную запросить текущий трек"""
    if m.from_user.id != OWNER_ID_INT:
        return
    if not YANDEX_MUSIC_TOKEN:
        await m.answer(f"{EMOJI['error']} YANDEX_MUSIC_TOKEN не задан", parse_mode="HTML")
        return

    status = await m.answer(f"{EMOJI['brain']} <i>Проверяю, что сейчас играет...</i>", parse_mode="HTML")

    try:
        from yandex_music.ynison import simple_async
        track = await simple_async.get_current_track(YANDEX_MUSIC_TOKEN, timeout=10.0)
        if track:
            await send_now_playing(track)
            await status.edit_text(f"{EMOJI['check']} Отправлено в группу!", parse_mode="HTML")
        else:
            await status.edit_text(f"{EMOJI['warning']} Сейчас ничего не играет", parse_mode="HTML")
    except Exception as e:
        await status.edit_text(f"{EMOJI['error']} Ошибка: {str(e)[:100]}", parse_mode="HTML")


@dp.message(Command("ymstart"))
async def cmd_ymstart(m: Message):
    """Запустить фоновый мониторинг треков"""
    global ym_task, ym_enabled
    if m.from_user.id != OWNER_ID_INT:
        return
    if not YANDEX_MUSIC_TOKEN or not YM_TARGET_CHAT_ID:
        await m.answer(f"{EMOJI['error']} Не задан YANDEX_MUSIC_TOKEN или YM_TARGET_CHAT_ID", parse_mode="HTML")
        return
    if ym_task and not ym_task.done():
        await m.answer(f"{EMOJI['info']} Мониторинг уже запущен", parse_mode="HTML")
        return

    ym_enabled = True
    ym_task = asyncio.create_task(ym_now_playing_loop())
    await m.answer(f"{EMOJI['check']} <b>Now Playing запущен!</b>\n\nЦелевая группа: <code>{YM_TARGET_CHAT_ID}</code>", parse_mode="HTML")
    logger.info("🎵 Now Playing запущен вручную")


@dp.message(Command("ymstop"))
async def cmd_ymstop(m: Message):
    """Остановить фоновый мониторинг"""
    global ym_task, ym_enabled, ym_last_track_id, ym_last_message_id
    if m.from_user.id != OWNER_ID_INT:
        return
    ym_enabled = False
    if ym_task:
        ym_task.cancel()
        ym_task = None
    ym_last_track_id = None
    ym_last_message_id = None
    save_ym_state()
    await m.answer(f"{EMOJI['check']} <b>Now Playing остановлен</b>\n\nСостояние сброшено.", parse_mode="HTML")
    logger.info("🎵 Now Playing остановлен вручную, состояние сброшено")


@dp.message(Command("ymstatus"))
async def cmd_ymstatus(m: Message):
    """Показать статус Yandex Music интеграции"""
    if m.from_user.id != OWNER_ID_INT:
        return
    txt = f"{EMOJI['settings']} <b>Yandex Music:</b>\n\n"
    txt += f"🔑 Токен: {'✅' if YANDEX_MUSIC_TOKEN else '❌'}\n"
    txt += f"💬 Группа: <code>{YM_TARGET_CHAT_ID or 'не задана'}</code>\n"
    txt += f"🎵 Мониторинг: <b>{'▶️ Активен' if ym_task and not ym_task.done() else '⏹️ Остановлен'}</b>\n"
    if ym_last_track_id:
        txt += f"📝 Последний трек ID: <code>{ym_last_track_id}</code>\n"
    await m.answer(txt, parse_mode="HTML")


@dp.message(Command("model"))
async def cmd_model(m: Message):
    if m.from_user.id != OWNER_ID_INT:
        await m.answer(f"{EMOJI['lock']} Только для владельца", parse_mode="HTML")
        return
    global current_ai_model
    parts = m.text.split()
    if len(parts) < 2:
        txt = f"{EMOJI['settings']} <b>Доступные AI-модели (FreeLLM API):</b>\n\n"
        for key, info in AVAILABLE_AI_MODELS.items():
            current = "✅ " if key == current_ai_model else "• "
            txt += f"{current}{info['display']}\n"
            txt += f"   <i>{info['desc']}</i>\n"
            txt += f"   <code>/model {key}</code>\n\n"
        txt += f"<b>Текущая:</b> <code>{current_ai_model}</code>\n"
        txt += f"<i>Используй /model &lt;ключ&gt; чтобы сменить. При смене модель будет проверена автоматически.</i>"
        await send_long_message(m, txt, parse_mode="HTML")
        return
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
    check_msg = await m.answer(f"{EMOJI['settings']} Проверяю <b>{new_model_info['display']}</b>...", parse_mode="HTML")
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
        await m.answer(f"{EMOJI['info']} Для этой модели память не задана.", parse_mode="HTML")

@dp.message(Command("memory"))
async def cmd_memory(m: Message):
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


@dp.message(Command("image", "img"))
async def cmd_image(m: Message):
    if m.from_user.id != OWNER_ID_INT:
        return
    prompt = m.text.split(maxsplit=1)[1] if len(m.text.split()) > 1 else ""
    if not prompt:
        await m.answer(
            f"{EMOJI['info']} <b>Генерация изображения</b>\n\n"
            f"<code>/image &lt;описание&gt;</code>\n\n"
            f"<b>Пример:</b>\n"
            f"<code>/image красный лис в осеннем лесу, цифровое искусство</code>",
            parse_mode="HTML"
        )
        return
    status_msg = await m.answer(f"{EMOJI['brain']} <i>Рисую...</i>", parse_mode="HTML")
    result = await generate_image(prompt)
    if result["success"]:
        try:
            # Скачиваем изображение
            img_response = await asyncio.to_thread(requests.get, result["url"], timeout=30)
            img_response.raise_for_status()
            photo = BufferedInputFile(file=img_response.content, filename="generated.png")
            caption = f"🎨 <b>Сгенерировано:</b>\n<i>{safe_html_text(prompt[:200])}</i>"
            if result.get("revised_prompt"):
                caption += f"\n\n<i>Уточнённый промпт:</i> <code>{safe_html_text(result['revised_prompt'][:150])}</code>"
            await m.answer_photo(photo=photo, caption=caption, parse_mode="HTML")
            await status_msg.delete()
            logger.info(f"🎨 Изображение сгенерировано: '{prompt[:50]}...'")
        except Exception as e:
            logger.error(f"❌ Ошибка скачивания/отправки изображения: {e}")
            await status_msg.edit_text(
                f"{EMOJI['check']} <b>Готово!</b>\n\n"
                f"<a href='{result['url']}'>🔗 Открыть изображение</a>\n\n"
                f"<i>{safe_html_text(prompt[:200])}</i>",
                parse_mode="HTML"
            )
    else:
        await status_msg.edit_text(f"{EMOJI['error']} {result['error']}", parse_mode="HTML")
        logger.warning(f"⚠️ Image generation error: {result['error']}")


@dp.message(Command("say", "tts"))
async def cmd_say(m: Message):
    if m.from_user.id != OWNER_ID_INT:
        return
    parts = m.text.split(maxsplit=1)
    if len(parts) < 2:
        await m.answer(
            f"{EMOJI['info']} <b>Генерация голоса (TTS)</b>\n\n"
            f"<code>/say &lt;текст&gt;</code>\n\n"
            f"<b>Пример:</b>\n"
            f"<code>/say Привет, мир! Как дела?</code>",
            parse_mode="HTML"
        )
        return
    text = parts[1].strip()
    status_msg = await m.answer(f"{EMOJI['brain']} <i>Генерирую аудио...</i>", parse_mode="HTML")
    result = await generate_speech(text)
    if result["success"]:
        voice = BufferedInputFile(file=result["audio_bytes"], filename="speech.mp3")
        await m.answer_voice(voice=voice, caption=f"🗣️ <i>{safe_html_text(text[:100])}</i>", parse_mode="HTML")
        await status_msg.delete()
        logger.info(f"🗣️ TTS сгенерировано: '{text[:50]}...'")
    else:
        await status_msg.edit_text(f"{EMOJI['error']} {result['error']}", parse_mode="HTML")
        logger.warning(f"⚠️ TTS error: {result['error']}")


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
        if len(all_loras) > EXPORT_THRESHOLD:
            await message.answer(EMOJI["file"] + f" Лор много (<b>{len(all_loras)}</b>), отправляю файлом...", parse_mode="HTML")
            await send_loras_as_file(message, all_loras, total_pages, min_days, tags)
        else:
            await send_loras_to_chat(message, all_loras, total_pages)
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
    if m.from_user.id != OWNER_ID_INT:
        return
    global last_search_results, last_search_meta
    if not last_search_results or not last_search_meta:
        await m.answer(
            f"{EMOJI['warning']} Нет данных для экспорта.\n"
            f"Сначала выполните <code>/check</code> с результатом &lt;50 лор.",
            parse_mode="HTML"
        )
        return
    content = make_export_file(last_search_results, last_search_meta["min_days"], last_search_meta["tags"])
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
    txt += f"\n🎨 <b>Медиа API:</b> {'✅' if freellmapi_session else '❌'}"
    txt += f"\n   /image — генерация изображений"
    txt += f"\n   /say — текст в голос"
    txt += f"\n   🎤 — распознавание голоса в /ai"
    txt += f"\n🎵 <b>Yandex Music:</b> {'✅' if YANDEX_MUSIC_TOKEN else '❌'} | Группа: {YM_TARGET_CHAT_ID or '—'}"
    txt += EMOJI["check" if bot_running else "stop"] + f" Бот: <b>{'Активен' if bot_running else 'ОСТАНОВЛЕН'}</b>"
    txt += f"\n👥 Пользователей: <b>{len(known_users)}</b>"
    if log_handler: 
        txt += f"\n📊 Лог-уровень: <b>{logging.getLevelName(log_handler.min_level)}</b>"
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
    if message.from_user.id != OWNER_ID_INT:
        ru = "🇷🇺 Если есть вопросы или что-то подобное — пишите, отвечу по возможности! "
        en = "🇬🇧 If you have questions or anything like that — write, I'll respond if possible! "
        await message.answer(ru + "\n\n" + en, parse_mode="HTML")
        return
    await message.answer(EMOJI["info"] + " Неизвестная команда. /help — справка", parse_mode="HTML")


# ================= ОБРАБОТЧИК INLINE-КНОПОК E621 (только для suggestions) =================

@dp.callback_query(lambda c: c.data and c.data.startswith("e621|"))
async def on_e621_tag_callback(callback: CallbackQuery):
    """Обрабатывает нажатия на inline-кнопки suggestions."""
    tag_name = parse_e621_callback_data(callback.data)
    if not tag_name:
        await callback.answer("❌ Ошибка", show_alert=False)
        return

    await callback.answer(f"🔍 {tag_name}...")

    tag_task = asyncio.create_task(get_e621_tag_info(tag_name))
    wiki_task = asyncio.create_task(get_e621_wiki_page(tag_name))
    posts_task = asyncio.create_task(get_e621_posts(tag_name, limit=2))

    tag_result = await tag_task
    wiki_result = await wiki_task
    image_urls = await posts_task

    if not tag_result["success"]:
        suggestions = await get_e621_tag_suggestions(tag_name, limit=6)
        if suggestions:
            kb = build_suggestions_keyboard(suggestions)
            await callback.message.answer(
                f"{EMOJI['warning']} Тег <code>{safe_html_text(tag_name)}</code> не найден.\n\n"
                f"<b>Возможно, вы имели в виду:</b>",
                parse_mode="HTML",
                reply_markup=kb
            )
        else:
            await callback.message.answer(
                f"{EMOJI['error']} Тег <code>{safe_html_text(tag_name)}</code> не найден и похожих тоже нет.",
                parse_mode="HTML"
            )
        return

    tag_data = tag_result["tag"]
    wiki_data = wiki_result.get("wiki") if wiki_result.get("success") else None

    text = format_tag_info(tag_data, wiki_data)

    if len(text) > MAX_MESSAGE_LENGTH:
        parts_msg = split_long_message(text, MAX_MESSAGE_LENGTH)
        for i, part in enumerate(parts_msg, 1):
            if len(parts_msg) > 1:
                part = f"<i>({i}/{len(parts_msg)})</i>\n" + part
            await callback.message.answer(part, parse_mode="HTML")
            if i < len(parts_msg):
                await asyncio.sleep(0.3)
    else:
        await callback.message.answer(text, parse_mode="HTML")

    if image_urls:
        for url in image_urls:
            try:
                await callback.message.answer_photo(photo=url)
                await asyncio.sleep(0.3)
            except Exception as e:
                logger.warning(f"⚠️ Не удалось отправить пример: {e}")

    logger.info(f"📖 Tag info e621 (callback): {tag_data['name']}")


# ================= INLINE AI MODE =================
def parse_ai_inline_query(user_input: str) -> tuple[str, str]:
    mode = "ask"
    text = user_input.strip()
    if ":" in text:
        parts = text.split(":", 1)
        mode = parts[0].strip().lower()
        text = parts[1].strip() if len(parts) > 1 else ""
    return mode, text

@dp.inline_query()
async def inline_search(query: InlineQuery):
    user = query.from_user
    user_input = query.query.strip()
    logger.info(f"🔍 Inline: user_id={user.id} username=@{user.username or 'none'} query='{user_input}'")

    # === РЕЗУЛЬТАТ С ИНФОЙ О ПОЛЬЗОВАТЕЛЕ ===
    user_info_lines = [f"👤 <b>Пользователь:</b>"]
    user_info_lines.append(f"🆔 ID: <code>{user.id}</code>")
    if user.username:
        user_info_lines.append(f"📛 @{user.username}")
    user_info_lines.append(f"📝 {safe_html_text(user.full_name)}")
    user_info_text = "\n".join(user_info_lines)

    results = [
        InlineQueryResultArticle(
            id="user_info",
            title=f"👤 Твой профиль  |  ID: {user.id}",
            description=f"@{user.username or 'нет ника'}  |  {user.full_name[:30]}",
            thumbnail_url="https://cdn-icons-png.flaticon.com/512/149/149071.png",
            thumbnail_width=128,
            thumbnail_height=128,
            input_message_content=InputTextMessageContent(
                message_text=user_info_text,
                parse_mode="HTML"
            ),
        )
    ]

    # === AI-РЕЖИМЫ (если запрос короткий) ===
    if len(user_input) < 2:
        results.extend([
            InlineQueryResultArticle(
                id="mode_ask",
                title="❓ Спросить AI",
                description="Задать вопрос нейросети",
                thumbnail_url="https://img.magnific.com/free-photo/closeup-shot-cute-fox-lying-ground-with-fallen-autumn-leaves_181624-32660.jpg?semt=ais_hybrid&w=740&q=80",
                thumbnail_width=300,
                thumbnail_height=300,
                input_message_content=InputTextMessageContent(message_text="❓ Напиши вопрос после выбора..."),
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✍️ Задать вопрос", switch_inline_query_current_chat="ask: ")]
                ]),
            ),
            InlineQueryResultArticle(
                id="mode_explain",
                title="📚 Объяснить AI",
                description="Объяснить текст просто",
                thumbnail_url="https://99px.ru/sstorage/53/2022/10/mid_345671_479468.jpg",
                thumbnail_width=300,
                thumbnail_height=300,
                input_message_content=InputTextMessageContent(message_text="📚 Введи текст для объяснения..."),
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✍️ Ввести текст", switch_inline_query_current_chat="explain: ")]
                ]),
            ),
        ])
        await query.answer(results=results, cache_time=30, is_personal=True)
        return

    # === AI-ЗАПРОС С ТЕКСТОМ ===
    mode, text = parse_ai_inline_query(user_input)
    if not text:
        results.append(
            InlineQueryResultArticle(
                id=f"prompt_{mode}",
                title="✍️ Введи текст",
                description="Продолжи писать после режима...",
                thumbnail_url="https://99px.ru/sstorage/53/2018/12/mid_245780_602075.jpg",
                thumbnail_width=300,
                thumbnail_height=300,
                input_message_content=InputTextMessageContent(message_text=f"✍️ Пиши: @{query.bot._me.username if query.bot._me else 'bot'} {mode}: твой текст"),
            )
        )
        await query.answer(results=results, cache_time=0, is_personal=True)
        return

    result_id = f"ai_{query.id}"
    results.append(
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
    )
    await query.answer(results=results, cache_time=0, is_personal=True)


@dp.chosen_inline_result()
async def on_inline_result_chosen(chosen: ChosenInlineResult):
    # Если выбран профиль — ничего не редактируем, он уже готов
    if chosen.result_id == "user_info":
        logger.info(f"👤 Пользователь {chosen.from_user.id} выбрал свой профиль")
        return

    if not chosen.result_id.startswith("ai_"):
        return

    if not chosen.inline_message_id:
        logger.warning("⚠️ Нет inline_message_id — не могу отредактировать сообщение")
        return

    user = chosen.from_user
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
    logger.info(f"🤖 Запрос к AI (модель {current_ai_model}): mode={mode} text='{text[:60]}' user={user.id}")
    result = await ask_ai_http(full_prompt)

    # Footer с ID/username отправителя
    footer_lines = ["", "─" * 20, f"👤 <b>Запросил:</b> <code>{user.id}</code>"]
    if user.username:
        footer_lines.append(f"📛 @{user.username}")
    footer = "\n".join(footer_lines)

    if result["success"]:
        answer = markdown_to_html(result["text"])
        display_query = text[:400] + "…" if len(text) > 400 else text
        safe_query = safe_html_text(display_query)
        final_text = f"✨ <b>AI:</b>\n\n<b>Запрос:</b> <i>{safe_query}</i>\n\n{answer}{footer}"
    else:
        final_text = f"❌ {result['error']}{footer}"

    try:
        await bot.edit_message_text(text=final_text, inline_message_id=chosen.inline_message_id, parse_mode="HTML")
        logger.info(f"✅ Inline-сообщение отредактировано для user={user.id}")
    except Exception as e:
        logger.error(f"❌ Не удалось отредактировать inline-сообщение: {e}")

# ================= MAIN (POLLING) =================
async def main():
    await init_log_bot()
    init_freellmapi_http()
    await refresh_available_models()
    load_forwarded()
    load_users()
    load_settings()
    load_memory()
    moscow_time = datetime.now(timezone(timedelta(hours=3))).strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"🚀 Bot started! Owner: {OWNER_ID_INT} | Users: {len(known_users)} | Time: МСК {moscow_time}")
            # Инициализация Yandex Music (опционально)
    await init_yandex_music()
    if YANDEX_MUSIC_TOKEN and YM_TARGET_CHAT_ID:
        ym_task = asyncio.create_task(ym_now_playing_loop())
        logger.info("🎵 Yandex Music Now Playing loop запущен")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Bot stopped by user")