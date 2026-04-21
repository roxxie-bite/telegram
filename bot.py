import os
import re
import asyncio
import logging
import requests
import time
import json
from datetime import datetime
from bs4 import BeautifulSoup
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from aiohttp import web
from supabase import create_client, Client

# ================= НАСТРОЙКИ =================
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = os.getenv("OWNER_ID")
STOP_PASSWORD = os.getenv("STOP_PASSWORD", "stop123")
MIN_DAYS_ENV = os.getenv("MIN_DAYS")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SITE_BASE = "https://lynther.sytes.net"
BASE_URL = SITE_BASE + "/?p=lora"
DEFAULT_MIN_DAYS = int(MIN_DAYS_ENV) if MIN_DAYS_ENV and MIN_DAYS_ENV.isdigit() else 0
DEFAULT_TAGS = []
CHECK_INTERVAL_HOURS = 6
MAX_PAGES = 50
EXPORT_THRESHOLD = 50
COOLDOWN_SECONDS = 60

# === СПЕЦИАЛЬНЫЕ ТЕГИ ===
SPECIAL_TAGS = {"xl": "tag_red", "style": "tag_purple", "character": "tag_green", "quality": "tag_gold"}

# === ЭМОДЗИ ===
EMOJI = {"brain": "🧠", "id": "🆔", "days": "🕸️", "delete": "🗑️", "search": "🔍", "stats": "📊",
         "settings": "⚙️", "tag": "🏷️", "clock": "⏰", "check": "✅", "warning": "⚠️", "error": "❌",
         "info": "ℹ️", "file": "📄", "chat": "💬", "stop": "🛑", "restart": "🔄", "lock": "🔒"}

# === ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ===
bot_running = True
periodic_task = None
supabase: Client = None
# =============================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

if not BOT_TOKEN or not OWNER_ID:
    raise ValueError("❌ Переменные BOT_TOKEN и OWNER_ID не заданы!")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ Переменные SUPABASE_URL и SUPABASE_KEY не заданы!")

OWNER_ID_INT = int(OWNER_ID)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ================= БАЗА ДАННЫХ (SUPABASE) =================
def init_supabase():
    """Инициализирует подключение к Supabase"""
    global supabase
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    logger.info("✅ Supabase подключена")

def get_user(user_id):
    """Получает настройки пользователя"""
    try:
        response = supabase.table("users").select("*").eq("user_id", user_id).execute()
        if response.data and len(response.data) > 0:
            user = response.data[0]
            return {
                "user_id": user["user_id"],
                "min_days": user["min_days"],
                "tags": json.loads(user["tags"]),
                "schedule": json.loads(user["schedule"]),
                "last_check": user["last_check"],
                "is_checking": user["is_checking"]
            }
        return None
    except Exception as e:
        logger.error("Ошибка get_user: " + str(e))
        return None

def create_user(user_id):
    """Создаёт нового пользователя"""
    try:
        supabase.table("users").insert({
            "user_id": user_id,
            "min_days": DEFAULT_MIN_DAYS,
            "tags": json.dumps(DEFAULT_TAGS),
            "schedule": json.dumps([]),
            "last_check": 0,
            "is_checking": False
        }).execute()
        logger.info("✅ Пользователь создан: " + str(user_id))
    except Exception as e:
        if "duplicate" not in str(e).lower():  # Игнорируем дубликаты
            logger.error("Ошибка create_user: " + str(e))

def update_user(user_id, **kwargs):
    """Обновляет настройки пользователя"""
    try:
        data = {}
        for key, value in kwargs.items():
            if key in ["tags", "schedule"]:
                value = json.dumps(value)
            data[key] = value
        supabase.table("users").update(data).eq("user_id", user_id).execute()
    except Exception as e:
        logger.error("Ошибка update_user: " + str(e))

def is_user_checking(user_id):
    """Проверяет, запущен ли уже /check у пользователя"""
    user = get_user(user_id)
    return user and user.get("is_checking", False)

def set_checking_status(user_id, is_checking):
    """Устанавливает статус проверки"""
    update_user(user_id, is_checking=is_checking)

def check_cooldown(user_id):
    """Проверяет кулдаун. Возвращает (можно_ли_использовать, секунд_осталось)"""
    user = get_user(user_id)
    if not user:
        return True, 0
    
    elapsed = time.time() - (user.get("last_check", 0) or 0)
    if elapsed >= COOLDOWN_SECONDS:
        return True, 0
    
    remaining = int(COOLDOWN_SECONDS - elapsed)
    return False, remaining

def update_last_check(user_id):
    """Обновляет время последнего использования /check"""
    update_user(user_id, last_check=time.time())

# ================= ЗАПРОСЫ =================
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

# ================= ПАРСЕР =================
def parse_loras_from_html(html, min_days):
    if html is None:
        return [], []
    try:
        soup = BeautifulSoup(html, "html.parser")
        all_on_page, filtered = [], []
        for head in soup.find_all("p", class_="lora_head"):
            try:
                text = head.get_text()
                id_match = re.search(r"#️⃣\s*(\d+)", text)
                days_match = re.search(r"🕸️\s*(\d+)\s*d", text, re.IGNORECASE)
                if not id_match or not days_match:
                    continue
                lora_id, lora_days = id_match.group(1), int(days_match.group(1))
                name_match = re.match(r'^\d+\.\s*(.+?)\s*\|\|', text.strip())
                lora_name = name_match.group(1).strip() if name_match else "Unknown"
                lora_url = SITE_BASE + "/?p=lora_d&lora_id=" + lora_id
                all_on_page.append({"id": lora_id, "days": lora_days, "name": lora_name, "url": lora_url})
                if lora_days >= min_days:
                    filtered.append({"id": lora_id, "days": lora_days, "name": lora_name, "url": lora_url})
            except Exception as e:
                logger.warning("Ошибка: " + str(e))
                continue
        return all_on_page, filtered
    except Exception as e:
        logger.error("Ошибка парсинга: " + str(e))
        return [], []

# ================= ПОИСК ПО СТРАНИЦАМ =================
def find_inactive_loonies_all_pages(base_url, min_days, active_tags, tag_name=None):
    all_results, pages_scanned = [], 0
    for search_tag in ([tag_name] if tag_name else active_tags):
        tag_results, tag_pages = [], 0
        for page in range(1, MAX_PAGES + 1):
            if not bot_running:
                break
            url = base_url + "&t=" + search_tag + ("&c=" + str(page) if page > 1 else "")
            html = fetch_with_retry(url)
            if not html:
                break
            all_on_page, filtered = parse_loras_from_html(html, min_days)
            tag_pages += 1
            tag_results.extend(filtered)
            if not all_on_page:
                break
            if page < MAX_PAGES:
                time.sleep(1.0)
        all_results.extend(tag_results)
        pages_scanned += tag_pages
    return all_results, pages_scanned

def format_message(lora):
    return "\n".join([
        EMOJI["brain"] + " <a href=\"" + lora["url"] + "\">" + lora["name"] + "</a>",
        EMOJI["id"] + " <code>ID: " + str(lora["id"]) + "</code>",
        EMOJI["days"] + " <b>" + str(lora["days"]) + " дней</b> без использования",
        EMOJI["delete"] + " <code>/dellora " + str(lora["id"]) + "</code>",
        "─" * 30
    ])

def make_export_file(loras, min_days, tags):
    lines = ["# Loonie Bot Export", "# Дата: " + datetime.now().strftime("%Y-%m-%d %H:%M"),
             "# Порог: >= " + str(min_days) + " дней", "# Теги: " + (", ".join(tags) if tags else "все"),
             "# Лор: " + str(len(loras)), ""]
    for l in loras:
        lines.append("/dellora " + l["id"] + "  # " + l["name"] + " (" + str(l["days"]) + " дней)")
    return "\n".join(lines).encode("utf-8")

# ================= УПРАВЛЕНИЕ ЗАДАЧАМИ =================
def cancel_periodic_task():
    global periodic_task
    if periodic_task and not periodic_task.done():
        periodic_task.cancel()
    periodic_task = None

def start_periodic_task():
    global periodic_task
    if not periodic_task or periodic_task.done():
        periodic_task = asyncio.create_task(periodic_check())

# ================= ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =================
async def _send_loras_to_chat(message, all_loras, total_pages, user_id):
    await message.answer(EMOJI["stats"] + " Найдено: <b>" + str(len(all_loras)) + "</b> лор", parse_mode="HTML")
    for i, lora in enumerate(all_loras, 1):
        await message.answer(format_message(lora), parse_mode="HTML")
        if i % 10 == 0:
            await asyncio.sleep(0.5)
        else:
            await asyncio.sleep(0.2)
    if all_loras:
        avg = sum(l["days"] for l in all_loras) // len(all_loras)
        mx = max(all_loras, key=lambda x: x["days"])
        mn = min(all_loras, key=lambda x: x["days"])
        stats = "\n" + EMOJI["stats"] + " <b>Статистика:</b>\n"
        stats += "• Страниц: <b>" + str(total_pages) + "</b>\n• Лор: <b>" + str(len(all_loras)) + "</b>\n"
        stats += "• Среднее: <b>" + str(avg) + "</b> дней\n• Мин: " + str(mn["days"]) + " | Макс: <b>" + str(mx["days"]) + "</b>"
        await message.answer(stats, parse_mode="HTML")

async def _send_loras_as_file(message, all_loras, total_pages, user_id):
    user = get_user(user_id)
    content = make_export_file(all_loras, user["min_days"], user["tags"])
    file = BufferedInputFile(file=content, filename="loonie_export_" + datetime.now().strftime("%Y%m%d_%H%M") + ".txt")
    caption = EMOJI["file"] + " <b>Экспорт лор</b>\n"
    caption += "Лор: " + str(len(all_loras)) + "\nПорог: >= " + str(user["min_days"]) + " дней"
    if user["tags"]:
        caption += "\nТеги: " + ", ".join(user["tags"])
    await message.answer_document(document=file, caption=caption, parse_mode="HTML")
    if all_loras:
        avg = sum(l["days"] for l in all_loras) // len(all_loras)
        mx = max(all_loras, key=lambda x: x["days"])
        stats = "\n" + EMOJI["stats"] + " <b>Статистика:</b>\n"
        stats += "• Страниц: <b>" + str(total_pages) + "</b>\n• Лор: <b>" + str(len(all_loras)) + "</b>\n"
        stats += "• Среднее: <b>" + str(avg) + "</b> дней\n• Макс: <b>" + str(mx["days"]) + "</b> дней"
        await message.answer(stats, parse_mode="HTML")

# ================= КОМАНДЫ =================
@dp.message(Command("help"))
async def cmd_help(message: Message):
    user_id = message.from_user.id
    create_user(user_id)
    is_owner = (user_id == OWNER_ID_INT)
    
    txt = EMOJI["info"] + " <b>Справка:</b>\n\n"
    txt += "<b>" + EMOJI["search"] + " Основные:</b>\n"
    txt += "/check — Проверить лоры по твоим настройкам\n"
    txt += "/status — Твои настройки\n"
    txt += "/help — Эта справка\n\n"
    txt += "<b>" + EMOJI["settings"] + " Настройки:</b>\n"
    txt += "/setdays N — Порог дней (0=все)\n"
    txt += "/addtag <тег> — Добавить тег\n"
    txt += "/rmtag <тег> — Удалить тег\n"
    txt += "/tags — Твои теги\n"
    txt += "/setschedule ЧЧ:ММ — Расписание (опционально)\n\n"
    
    if is_owner:
        txt += "<b>" + EMOJI["stop"] + " Владелец:</b>\n"
        txt += "/stop <пароль> — Остановить бота\n"
        txt += "/start — Запустить бота\n"
        txt += "/broadcast <текст> — Рассылка всем\n\n"
    
    txt += "<i>⏱️ Кулдаун /check: " + str(COOLDOWN_SECONDS) + " сек</i>"
    await message.answer(txt, parse_mode="HTML")

@dp.message(Command("check"))
async def cmd_check(message: Message):
    user_id = message.from_user.id
    create_user(user_id)
    
    if user_id != OWNER_ID_INT and not bot_running:
        await message.answer(EMOJI["error"] + " Бот временно остановлен.", parse_mode="HTML")
        return
    
    if is_user_checking(user_id):
        await message.answer(EMOJI["lock"] + " <b>Проверка уже запущена!</b>\n\nПодожди завершения текущей проверки.", parse_mode="HTML")
        return
    
    can_use, remaining = check_cooldown(user_id)
    if not can_use:
        await message.answer(EMOJI["clock"] + " <b>Кулдаун!</b>\n\nПовтори через <b>" + str(remaining) + "</b> сек.", parse_mode="HTML")
        return
    
    try:
        set_checking_status(user_id, True)
        await message.answer(EMOJI["search"] + " <b>Проверка запущена!</b>\n\nЭто займёт некоторое время...", parse_mode="HTML")
        
        user = get_user(user_id)
        logger.info("=== ПРОВЕРКА === User: " + str(user_id) + " | Теги: " + str(user["tags"]))
        
        if user["tags"]:
            all_loras, total_pages = [], 0
            for tag in user["tags"]:
                loras, pages = find_inactive_loonies_all_pages(BASE_URL, user["min_days"], user["tags"], tag_name=tag)
                all_loras.extend(loras)
                total_pages += pages
        else:
            all_loras, total_pages = find_inactive_loonies_all_pages(BASE_URL, user["min_days"], [])
        
        if not all_loras:
            await message.answer(EMOJI["check"] + " Лоры не найдены.")
            set_checking_status(user_id, False)
            update_last_check(user_id)
            return
        
        all_loras.sort(key=lambda x: x["days"], reverse=True)
        
        if len(all_loras) > EXPORT_THRESHOLD:
            await message.answer(EMOJI["file"] + " Лор много (<b>" + str(len(all_loras)) + "</b>), отправляю файлом...", parse_mode="HTML")
            await _send_loras_as_file(message, all_loras, total_pages, user_id)
        else:
            await _send_loras_to_chat(message, all_loras, total_pages, user_id)
        
        update_last_check(user_id)
        logger.info("✅ Проверка завершена для пользователя " + str(user_id))
        
    except Exception as e:
        logger.error("❌ Ошибка в /check: " + str(e), exc_info=True)
        await message.answer(EMOJI["error"] + " Ошибка при проверке.")
    finally:
        set_checking_status(user_id, False)

@dp.message(Command("setdays"))
async def cmd_setdays(message: Message):
    user_id = message.from_user.id
    create_user(user_id)
    
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit() or int(parts[1]) < 0:
        await message.answer(EMOJI["warning"] + " Используй: <code>/setdays &lt;число&gt;</code>", parse_mode="HTML")
        return
    
    new_days = int(parts[1])
    update_user(user_id, min_days=new_days)
    
    days_text = "все лоры" if new_days==0 else ">=" + str(new_days) + " дней"
    await message.answer(EMOJI["check"] + " Порог установлен: <b>" + days_text + "</b>", parse_mode="HTML")

@dp.message(Command("addtag"))
async def cmd_addtag(message: Message):
    user_id = message.from_user.id
    create_user(user_id)
    
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].strip().lower().isalnum():
        await message.answer(EMOJI["warning"] + " Используй: <code>/addtag &lt;название&gt;</code>", parse_mode="HTML")
        return
    
    new_tag = parts[1].strip().lower()
    user = get_user(user_id)
    
    if any(t.lower() == new_tag for t in user["tags"]):
        await message.answer(EMOJI["warning"] + " Тег уже в списке", parse_mode="HTML")
        return
    
    user["tags"].append(new_tag)
    update_user(user_id, tags=user["tags"])
    await message.answer(EMOJI["check"] + " Тег <b>" + new_tag + "</b> добавлен.", parse_mode="HTML")

@dp.message(Command("rmtag"))
async def cmd_rmtag(message: Message):
    user_id = message.from_user.id
    create_user(user_id)
    
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer(EMOJI["warning"] + " Используй: <code>/rmtag &lt;название&gt;</code>", parse_mode="HTML")
        return
    
    tag_to_remove = parts[1].strip().lower()
    user = get_user(user_id)
    tag = next((t for t in user["tags"] if t.lower() == tag_to_remove), None)
    
    if not tag or len(user["tags"]) <= 1:
        await message.answer(EMOJI["warning"] + " Тег не найден", parse_mode="HTML")
        return
    
    user["tags"].remove(tag)
    update_user(user_id, tags=user["tags"])
    await message.answer(EMOJI["check"] + " Тег <b>" + tag + "</b> удалён.", parse_mode="HTML")

@dp.message(Command("tags"))
async def cmd_tags(message: Message):
    user_id = message.from_user.id
    create_user(user_id)
    user = get_user(user_id)
    
    if not user["tags"]:
        await message.answer(EMOJI["tag"] + " <b>Твои теги:</b>\n<i>нет</i>\n\nИспользуй /addtag <тег>", parse_mode="HTML")
        return
    
    txt = EMOJI["tag"] + " <b>Твои теги:</b>\n" + "\n".join(f"{i}. <code>{t}</code>" for i,t in enumerate(user["tags"],1))
    await message.answer(txt, parse_mode="HTML")

@dp.message(Command("setschedule"))
async def cmd_setschedule(message: Message):
    user_id = message.from_user.id
    create_user(user_id)
    
    parts = message.text.split()
    if len(parts) < 2 or not all(re.match(r'^\d{2}:\d{2}$', t) for t in parts[1:]):
        await message.answer(EMOJI["warning"] + " Используй: <code>/setschedule 09:00 15:00</code>", parse_mode="HTML")
        return
    
    update_user(user_id, schedule=parts[1:])
    await message.answer(EMOJI["clock"] + " Расписание установлено.", parse_mode="HTML")

@dp.message(Command("status"))
async def cmd_status(message: Message):
    user_id = message.from_user.id
    create_user(user_id)
    user = get_user(user_id)
    is_owner = (user_id == OWNER_ID_INT)
    
    txt = EMOJI["settings"] + " <b>Твои настройки:</b>\n"
    txt += EMOJI["days"] + " Порог: <b>" + ("все лоры" if user["min_days"]==0 else ">=" + str(user["min_days"]) + " дней") + "</b>\n"
    txt += EMOJI["tag"] + " Теги: <b>" + (", ".join(user["tags"]) if user["tags"] else "нет (все лоры)") + "</b>\n"
    
    if user["schedule"]:
        txt += EMOJI["clock"] + " Расписание: <b>" + ", ".join(user["schedule"]) + "</b>\n"
    
    can_use, remaining = check_cooldown(user_id)
    if can_use:
        txt += "⏱️ Кулдаун: <b>готов</b>\n"
    else:
        txt += "⏱️ Кулдаун: <b>" + str(remaining) + " сек</b>\n"
    
    if is_user_checking(user_id):
        txt += EMOJI["lock"] + " Статус: <b>проверка идёт...</b>\n"
    
    if is_owner:
        txt += "\n" + EMOJI["check" if bot_running else "stop"] + " <b>Бот:</b> " + ("Активен" if bot_running else "ОСТАНОВЛЕН")
    
    await message.answer(txt, parse_mode="HTML")

# ================= ВЛАДЕЛЕЦ-КОМАНДЫ =================
@dp.message(Command("stop"))
async def cmd_stop(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    
    parts = message.text.split()
    if len(parts) != 2 or parts[1] != STOP_PASSWORD:
        await message.answer(EMOJI["stop"] + " <b>Остановка:</b>\nИспользуй: <code>/stop " + STOP_PASSWORD + "</code>", parse_mode="HTML")
        return
    
    global bot_running
    bot_running = False
    cancel_periodic_task()
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=EMOJI["restart"] + " Запустить", callback_data="restart_bot")]])
    await message.answer(EMOJI["stop"] + " <b>БОТ ОСТАНОВЛЕН!</b>\n\nНажми кнопку для запуска:", parse_mode="HTML", reply_markup=keyboard)
    logger.warning("🛑 БОТ ОСТАНОВЛЕН ВЛАДЕЛЬЦЕМ")

@dp.callback_query(F.data == "restart_bot")
async def handle_restart(callback: CallbackQuery):
    if callback.from_user.id != OWNER_ID_INT:
        await callback.answer("❌", show_alert=True)
        return
    
    global bot_running
    bot_running = True
    start_periodic_task()
    
    await callback.message.edit_text(EMOJI["check"] + " <b>Бот запущен!</b>", parse_mode="HTML")
    await callback.answer()
    logger.info("🔄 БОТ ЗАПУЩЕН ВЛАДЕЛЬЦЕМ")

@dp.message(Command("start"))
async def cmd_start(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    
    global bot_running
    if bot_running:
        await message.answer(EMOJI["info"] + " <b>Бот уже активен!</b>", parse_mode="HTML")
        return
    
    bot_running = True
    start_periodic_task()
    await message.answer(EMOJI["check"] + " <b>Бот запущен!</b>", parse_mode="HTML")
    logger.info("🔄 БОТ ЗАПУЩЕН ВЛАДЕЛЬЦЕМ")

@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    
    text = message.text.replace("/broadcast", "").strip()
    if not text:
        await message.answer(EMOJI["warning"] + " Используй: <code>/broadcast &lt;текст&gt;</code>", parse_mode="HTML")
        return
    
    try:
        response = supabase.table("users").select("user_id").execute()
        users = response.data
    except Exception as e:
        logger.error("Ошибка получения пользователей: " + str(e))
        await message.answer(EMOJI["error"] + " Ошибка базы данных", parse_mode="HTML")
        return
    
    sent = 0
    failed = 0
    for user in users:
        try:
            await bot.send_message(chat_id=user["user_id"], text=EMOJI["info"] + " <b>Сообщение от владельца:</b>\n\n" + text, parse_mode="HTML")
            sent += 1
            await asyncio.sleep(0.1)
        except Exception as e:
            failed += 1
            logger.error("Не удалось отправить пользователю " + str(user["user_id"]) + ": " + str(e))
    
    await message.answer(EMOJI["check"] + " Рассылка завершена!\n\nОтправлено: <b>" + str(sent) + "</b>\nНе удалось: <b>" + str(failed) + "</b>", parse_mode="HTML")

@dp.message()
async def silent_ignore(message: Message):
    pass

# ================= ФОНОВЫЕ ЗАДАЧИ =================
async def periodic_check():
    while bot_running:
        await asyncio.sleep(60)
        if not bot_running:
            break
        
        now = datetime.now().strftime("%H:%M")
        owner = get_user(OWNER_ID_INT)
        
        if owner and owner["schedule"] and now not in owner["schedule"]:
            continue
        
        logger.info("=== АВТОПРОВЕРКА (ВЛАДЕЛЕЦ) ===")
        
        if owner and owner["tags"]:
            all_loras, total_pages = [], 0
            for tag in owner["tags"]:
                loras, pages = find_inactive_loonies_all_pages(BASE_URL, owner["min_days"], owner["tags"], tag_name=tag)
                all_loras.extend(loras)
                total_pages += pages
        else:
            all_loras, total_pages = find_inactive_loonies_all_pages(BASE_URL, owner["min_days"] if owner else 0, [])
        
        if all_loras:
            all_loras.sort(key=lambda x: x["days"], reverse=True)
            if len(all_loras) > EXPORT_THRESHOLD:
                await _send_loras_as_file(bot, all_loras, total_pages, OWNER_ID_INT)
            else:
                for lora in all_loras:
                    if not bot_running:
                        break
                    try:
                        await bot.send_message(chat_id=OWNER_ID_INT, text=format_message(lora), parse_mode="HTML")
                        await asyncio.sleep(0.5)
                    except Exception as e:
                        logger.error("Ошибка отправки: " + str(e))

async def on_startup():
    init_supabase()
    create_user(OWNER_ID_INT)
    logger.info("🚀 Bot started. Owner: " + str(OWNER_ID_INT))
    if bot_running:
        start_periodic_task()

async def on_shutdown():
    logger.info("👋 Bot shutting down...")
    cancel_periodic_task()
    await bot.session.close()

# ================= WEBHOOK SERVER =================
async def webhook_handler(request):
    try:
        await dp.feed_webhook_update(bot, await request.json())
        return web.Response(text="OK")
    except Exception as e:
        logger.error("Ошибка вебхука: " + str(e))
        return web.Response(text="Error", status=500)

async def stop_handler(request):
    global bot_running
    action, password = request.query.get("action", "stop"), request.query.get("password", "")
    if password != STOP_PASSWORD:
        return web.Response(text="❌ Неверный пароль", status=403)
    if action == "stop":
        bot_running = False
        cancel_periodic_task()
        return web.Response(text="✅ Бот остановлен")
    elif action == "start":
        bot_running = True
        start_periodic_task()
        return web.Response(text="✅ Бот запущен")
    return web.Response(text="❌ Неизвестное действие", status=400)

async def health_handler(request):
    return web.Response(text="OK - Status: " + ("running" if bot_running else "stopped"))

async def run_web_server():
    app = web.Application()
    app.router.add_post("/webhook/" + BOT_TOKEN.split(":")[0], webhook_handler)
    app.router.add_get("/stop", stop_handler)
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", "8080"))
    await web.TCPSite(runner, "0.0.0.0", port).start()
    logger.info("🌐 Server on port " + str(port))
    webhook_url = os.getenv("RENDER_EXTERNAL_URL", "")
    if webhook_url and bot_running:
        await bot.set_webhook(webhook_url + "/webhook/" + BOT_TOKEN.split(":")[0])

async def main():
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    await run_web_server()
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Bot stopped by user")