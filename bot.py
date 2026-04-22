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
from aiogram.types import Message, BufferedInputFile
from aiohttp import web

# ================= НАСТРОЙКИ =================
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = os.getenv("OWNER_ID")
STOP_PASSWORD = os.getenv("STOP_PASSWORD", "stop123")
MIN_DAYS_ENV = os.getenv("MIN_DAYS")
SITE_BASE = "https://lynther.sytes.net"
BASE_URL = SITE_BASE + "/?p=lora"
DEFAULT_MIN_DAYS = int(MIN_DAYS_ENV) if MIN_DAYS_ENV and MIN_DAYS_ENV.isdigit() else 0
DEFAULT_TAGS = []
MAX_PAGES = 50
EXPORT_THRESHOLD = 50
COOLDOWN_SECONDS = 20

# === ЭМОДЗИ ===
EMOJI = {
    "brain": "🧠", "id": "🆔", "days": "🕸️", "delete": "🗑️", "search": "🔍",
    "stats": "📊", "settings": "⚙️", "tag": "🏷️", "clock": "⏰", "check": "✅",
    "warning": "⚠️", "error": "❌", "info": "ℹ️", "file": "📄", "stop": "🛑",
    "restart": "🔄", "lock": "🔒"
}

# === ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ===
bot_running = True
user_settings = {}  # Настройки владельца в памяти: {user_id: {min_days, tags, ...}}
# =============================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

if not BOT_TOKEN or not OWNER_ID:
    raise ValueError("❌ Переменные BOT_TOKEN и OWNER_ID не заданы!")

OWNER_ID_INT = int(OWNER_ID)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ================= НАСТРОЙКИ ПОЛЬЗОВАТЕЛЯ =================
def get_settings(user_id):
    """Получает настройки пользователя (или дефолтные)"""
    if user_id not in user_settings:
        user_settings[user_id] = {
            "min_days": DEFAULT_MIN_DAYS,
            "tags": DEFAULT_TAGS.copy(),
            "schedule": [],
            "last_check": 0,
            "is_checking": False
        }
    return user_settings[user_id]

def update_settings(user_id, **kwargs):
    """Обновляет настройки пользователя"""
    settings = get_settings(user_id)
    settings.update(kwargs)
    user_settings[user_id] = settings

def check_cooldown(user_id):
    """Проверяет кулдаун"""
    settings = get_settings(user_id)
    elapsed = time.time() - settings.get("last_check", 0)
    if elapsed >= COOLDOWN_SECONDS:
        return True, 0
    return False, int(COOLDOWN_SECONDS - elapsed)

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
    """Парсит лоры из HTML, фильтрует по min_days"""
    if html is None:
        return []
    try:
        soup = BeautifulSoup(html, "html.parser")
        results = []
        for head in soup.find_all("p", class_="lora_head"):
            try:
                text = head.get_text()
                # Ищем ID
                id_match = re.search(r"#️⃣\s*(\d+)", text)
                if not id_match:
                    continue
                lora_id = id_match.group(1)
                
                # Ищем дни
                days_match = re.search(r"🕸️\s*(\d+)\s*d", text, re.IGNORECASE)
                if not days_match:
                    continue
                lora_days = int(days_match.group(1))
                
                # Ищем название
                name_match = re.match(r'^\d+\.\s*(.+?)\s*\|\|', text.strip())
                lora_name = name_match.group(1).strip() if name_match else "Unknown"
                
                # Ссылка
                lora_url = SITE_BASE + "/?p=lora_d&lora_id=" + lora_id
                
                # Фильтр по дням
                if lora_days >= min_days:
                    results.append({
                        "id": lora_id,
                        "days": lora_days,
                        "name": lora_name,
                        "url": lora_url
                    })
            except Exception as e:
                logger.warning("Ошибка парсинга: " + str(e))
                continue
        return results
    except Exception as e:
        logger.error("Ошибка парсинга: " + str(e))
        return []

# ================= ПОИСК ПО СТРАНИЦАМ =================
def find_loras_by_tag(tag, min_days):
    """Ищет лоры по конкретному тегу через сайт"""
    all_results = []
    pages_scanned = 0
    
    for page in range(1, MAX_PAGES + 1):
        if not bot_running:
            break
        
        # Формируем URL с тегом
        if page == 1:
            url = BASE_URL + "&t=" + tag
        else:
            url = BASE_URL + "&t=" + tag + "&c=" + str(page)
        
        logger.info("=== Тег: " + tag + " | Страница: " + str(page) + " ===")
        html = fetch_with_retry(url)
        
        if not html:
            logger.warning("Страница не загрузилась")
            break
        
        loras = parse_loras_from_html(html, min_days)
        pages_scanned += 1
        
        if loras:
            all_results.extend(loras)
            logger.info("Стр. " + str(page) + ": найдено " + str(len(loras)) + " лор")
        else:
            # Если на странице нет лор — возможно, это последняя страница
            logger.info("Стр. " + str(page) + ": лор не найдено")
            # Но продолжаем, т.к. на следующих страницах могут быть лоры с этим тегом
            # Останавливаемся только если несколько страниц подряд пустые
            if page > 3:  # после 3 пустых страниц — стоп
                break
        
        if page < MAX_PAGES:
            time.sleep(1.0)
    
    logger.info("=== Тег " + tag + " готов === Лор: " + str(len(all_results)) + " | Стр: " + str(pages_scanned))
    return all_results, pages_scanned

def find_all_loras(min_days):
    """Ищет ВСЕ лоры без фильтра по тегу"""
    all_results = []
    pages_scanned = 0
    
    for page in range(1, MAX_PAGES + 1):
        if not bot_running:
            break
        
        url = BASE_URL if page == 1 else BASE_URL + "&c=" + str(page)
        logger.info("=== Все лоры | Страница: " + str(page) + " ===")
        
        html = fetch_with_retry(url)
        if not html:
            break
        
        loras = parse_loras_from_html(html, min_days)
        pages_scanned += 1
        
        if loras:
            all_results.extend(loras)
            logger.info("Стр. " + str(page) + ": найдено " + str(len(loras)) + " лор")
        
        if not loras and page > 1:
            logger.info("Стр. " + str(page) + ": лор не найдено → завершаю")
            break
        
        if page < MAX_PAGES:
            time.sleep(1.0)
    
    logger.info("=== ВСЕГО === Стр: " + str(pages_scanned) + " | Лор: " + str(len(all_results)))
    return all_results, pages_scanned

# ================= ФОРМАТИРОВАНИЕ =================
def format_message(lora):
    return "\n".join([
        EMOJI["brain"] + " <a href=\"" + lora["url"] + "\">" + lora["name"] + "</a>",
        EMOJI["id"] + " <code>ID: " + str(lora["id"]) + "</code>",
        EMOJI["days"] + " <b>" + str(lora["days"]) + " дней</b> без использования",
        EMOJI["delete"] + " <code>/dellora " + str(lora["id"]) + "</code>",
        "─" * 30
    ])

def make_export_file(loras, min_days, tags):
    lines = [
        "# Loonie Bot Export",
        "# Дата: " + datetime.now().strftime("%Y-%m-%d %H:%M"),
        "# Порог: >= " + str(min_days) + " дней",
        "# Теги: " + (", ".join(tags) if tags else "все"),
        "# Лор: " + str(len(loras)),
        ""
    ]
    for l in loras:
        lines.append("/dellora " + l["id"] + "  # " + l["name"] + " (" + str(l["days"]) + " дней)")
    return "\n".join(lines).encode("utf-8")

# ================= ОТПРАВКА =================
async def send_loras_to_chat(message, loras, total_pages):
    await message.answer(EMOJI["stats"] + " Найдено: <b>" + str(len(loras)) + "</b> лор", parse_mode="HTML")
    for i, lora in enumerate(loras, 1):
        await message.answer(format_message(lora), parse_mode="HTML")
        if i % 10 == 0:
            await asyncio.sleep(0.5)
        else:
            await asyncio.sleep(0.2)
    
    if loras:
        avg = sum(l["days"] for l in loras) // len(loras)
        mx = max(loras, key=lambda x: x["days"])
        mn = min(loras, key=lambda x: x["days"])
        stats = "\n" + EMOJI["stats"] + " <b>Статистика:</b>\n"
        stats += "• Страниц: <b>" + str(total_pages) + "</b>\n• Лор: <b>" + str(len(loras)) + "</b>\n"
        stats += "• Среднее: <b>" + str(avg) + "</b> дней\n• Мин: " + str(mn["days"]) + " | Макс: <b>" + str(mx["days"]) + "</b>"
        await message.answer(stats, parse_mode="HTML")

async def send_loras_as_file(message, loras, total_pages, min_days, tags):
    content = make_export_file(loras, min_days, tags)
    file = BufferedInputFile(file=content, filename="loonie_export_" + datetime.now().strftime("%Y%m%d_%H%M") + ".txt")
    caption = EMOJI["file"] + " <b>Экспорт лор</b>\n"
    caption += "Лор: " + str(len(loras)) + "\nПорог: >= " + str(min_days) + " дней"
    if tags:
        caption += "\nТеги: " + ", ".join(tags)
    await message.answer_document(document=file, caption=caption, parse_mode="HTML")
    
    if loras:
        avg = sum(l["days"] for l in loras) // len(loras)
        mx = max(loras, key=lambda x: x["days"])
        stats = "\n" + EMOJI["stats"] + " <b>Статистика:</b>\n"
        stats += "• Страниц: <b>" + str(total_pages) + "</b>\n• Лор: <b>" + str(len(loras)) + "</b>\n"
        stats += "• Среднее: <b>" + str(avg) + "</b> дней\n• Макс: <b>" + str(mx["days"]) + "</b> дней"
        await message.answer(stats, parse_mode="HTML")

# ================= КОМАНДЫ =================
@dp.message(Command("help"))
async def cmd_help(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    settings = get_settings(message.from_user.id)
    txt = EMOJI["info"] + " <b>Справка:</b>\n\n"
    txt += "<b>" + EMOJI["search"] + " Основные:</b>\n"
    txt += "/check — Найти лоры по твоим настройкам\n"
    txt += "/status — Показать настройки\n"
    txt += "/help — Эта справка\n\n"
    txt += "<b>" + EMOJI["settings"] + " Настройки:</b>\n"
    txt += "/setdays N — Порог дней (0 = все)\n"
    txt += "/addtag &lt;тег&gt; — Добавить тег для поиска\n"
    txt += "/rmtag &lt;тег&gt; — Удалить тег\n"
    txt += "/tags — Показать активные теги\n\n"
    txt += "<b>" + EMOJI["stop"] + " Управление:</b>\n"
    txt += "/stop &lt;пароль&gt; — Остановить бота\n"
    txt += "/start — Запустить бота\n\n"
    txt += "<i>⏱️ Кулдаун: " + str(COOLDOWN_SECONDS) + " сек</i>"
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
    
    # Проверка: не запущен ли уже check
    if settings.get("is_checking"):
        await message.answer(EMOJI["lock"] + " Проверка уже запущена!", parse_mode="HTML")
        return
    
    # Проверка кулдауна
    can_use, remaining = check_cooldown(user_id)
    if not can_use:
        await message.answer(EMOJI["clock"] + " Кулдаун! Повтори через <b>" + str(remaining) + "</b> сек.", parse_mode="HTML")
        return
    
    try:
        # Блокируем команду
        update_settings(user_id, is_checking=True)
        await message.answer(EMOJI["search"] + " Поиск запущен...", parse_mode="HTML")
        
        min_days = settings["min_days"]
        tags = settings["tags"]
        
        # Ищем лоры
        if tags:
            # Есть теги — ищем по каждому
            logger.info("=== Поиск по тегам: " + ", ".join(tags) + " ===")
            all_loras, total_pages = [], 0
            for tag in tags:
                loras, pages = find_loras_by_tag(tag, min_days)
                all_loras.extend(loras)
                total_pages += pages
        else:
            # Нет тегов — ищем все лоры
            logger.info("=== Поиск всех лор ===")
            all_loras, total_pages = find_all_loras(min_days)
        
        if not all_loras:
            await message.answer(EMOJI["check"] + " Лоры не найдены.")
            update_settings(user_id, is_checking=False, last_check=time.time())
            return
        
        # Сортируем: сначала самые старые
        all_loras.sort(key=lambda x: x["days"], reverse=True)
        
        # Отправляем
        if len(all_loras) > EXPORT_THRESHOLD:
            await message.answer(EMOJI["file"] + " Лор много (<b>" + str(len(all_loras)) + "</b>), отправляю файлом...", parse_mode="HTML")
            await send_loras_as_file(message, all_loras, total_pages, min_days, tags)
        else:
            await send_loras_to_chat(message, all_loras, total_pages)
        
        # Обновляем кулдаун
        update_settings(user_id, last_check=time.time())
        logger.info("✅ Поиск завершён: " + str(len(all_loras)) + " лор")
        
    except Exception as e:
        logger.error("❌ Ошибка в /check: " + str(e), exc_info=True)
        await message.answer(EMOJI["error"] + " Ошибка: " + str(e)[:100], parse_mode="HTML")
    finally:
        update_settings(user_id, is_checking=False)

@dp.message(Command("setdays"))
async def cmd_setdays(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit() or int(parts[1]) < 0:
        await message.answer(EMOJI["warning"] + " Используй: <code>/setdays &lt;число&gt;</code>", parse_mode="HTML")
        return
    new_days = int(parts[1])
    update_settings(message.from_user.id, min_days=new_days)
    days_text = "все лоры" if new_days == 0 else ">=" + str(new_days) + " дней"
    await message.answer(EMOJI["check"] + " Порог: <b>" + days_text + "</b>", parse_mode="HTML")

@dp.message(Command("addtag"))
async def cmd_addtag(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].strip().lower().isalnum():
        await message.answer(EMOJI["warning"] + " Используй: <code>/addtag &lt;название&gt;</code>", parse_mode="HTML")
        return
    new_tag = parts[1].strip().lower()
    settings = get_settings(message.from_user.id)
    if new_tag in [t.lower() for t in settings["tags"]]:
        await message.answer(EMOJI["warning"] + " Тег уже в списке", parse_mode="HTML")
        return
    settings["tags"].append(new_tag)
    update_settings(message.from_user.id, tags=settings["tags"])
    await message.answer(EMOJI["check"] + " Тег <b>" + new_tag + "</b> добавлен. Текущие: " + ", ".join(settings["tags"]), parse_mode="HTML")

@dp.message(Command("rmtag"))
async def cmd_rmtag(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer(EMOJI["warning"] + " Используй: <code>/rmtag &lt;название&gt;</code>", parse_mode="HTML")
        return
    tag_to_remove = parts[1].strip().lower()
    settings = get_settings(message.from_user.id)
    tag = next((t for t in settings["tags"] if t.lower() == tag_to_remove), None)
    if not tag:
        await message.answer(EMOJI["warning"] + " Тег не найден", parse_mode="HTML")
        return
    settings["tags"].remove(tag)
    update_settings(message.from_user.id, tags=settings["tags"])
    await message.answer(EMOJI["check"] + " Тег <b>" + tag + "</b> удалён. Текущие: " + (", ".join(settings["tags"]) if settings["tags"] else "нет"), parse_mode="HTML")

@dp.message(Command("tags"))
async def cmd_tags(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    settings = get_settings(message.from_user.id)
    if not settings["tags"]:
        await message.answer(EMOJI["tag"] + " <b>Теги:</b>\n<i>нет</i>\n\nИспользуй /addtag &lt;тег&gt;", parse_mode="HTML")
        return
    txt = EMOJI["tag"] + " <b>Теги:</b>\n" + "\n".join(f"{i}. <code>{t}</code>" for i, t in enumerate(settings["tags"], 1))
    await message.answer(txt, parse_mode="HTML")

@dp.message(Command("status"))
async def cmd_status(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    settings = get_settings(message.from_user.id)
    txt = EMOJI["settings"] + " <b>Настройки:</b>\n"
    txt += EMOJI["days"] + " Порог: <b>" + ("все лоры" if settings["min_days"]==0 else ">=" + str(settings["min_days"]) + " дней") + "</b>\n"
    txt += EMOJI["tag"] + " Теги: <b>" + (", ".join(settings["tags"]) if settings["tags"] else "нет (все лоры)") + "</b>\n"
    can_use, remaining = check_cooldown(message.from_user.id)
    txt += "⏱️ Кулдаун: <b>" + ("готов" if can_use else str(remaining) + " сек") + "</b>\n"
    txt += EMOJI["check" if bot_running else "stop"] + " Бот: <b>" + ("Активен" if bot_running else "ОСТАНОВЛЕН") + "</b>"
    await message.answer(txt, parse_mode="HTML")

@dp.message(Command("stop"))
async def cmd_stop(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    parts = message.text.split()
    if len(parts) != 2 or parts[1] != STOP_PASSWORD:
        await message.answer(EMOJI["stop"] + " Используй: <code>/stop " + STOP_PASSWORD + "</code>", parse_mode="HTML")
        return
    global bot_running
    bot_running = False
    await message.answer(EMOJI["stop"] + " <b>БОТ ОСТАНОВЛЕН!</b>\nНапиши /start для запуска.", parse_mode="HTML")
    logger.warning("🛑 Бот остановлен владельцем")

@dp.message(Command("start"))
async def cmd_start(m: Message):
    """Старт для всех: владелец → команды, другие → сообщение"""
    
    # 👑 ВЛАДЕЛЕЦ
    if m.from_user.id == OWNER_ID_INT:
        global bot_running
        if not bot_running:
            bot_running = True
            logger.info("🔄 Bot resumed by owner")
        await m.answer(f"{EMOJI['check']} <b>Бот активен!</b>\n/help — команды", parse_mode="HTML")
        return
    
    # 👥 ОБЫЧНЫЙ ПОЛЬЗОВАТЕЛЬ — простое двуязычное сообщение
    ru = "🇷🇺 Если есть вопросы или что-то подобное — пишите, отвечу по возможности! "
    en = "🇬🇧 If you have questions or anything like that — write, I'll respond if possible! "
    await m.answer(ru + "\n\n" + en, parse_mode="HTML")

@dp.message()
async def silent_ignore(message: Message):
    # Игнорируем все сообщения не от владельца
    if message.from_user.id != OWNER_ID_INT:
        return
    # Отвечаем владельцу на неизвестные команды
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
    status = "running" if bot_running else "stopped"
    return web.Response(text="OK - " + status)

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
    
    # Устанавливаем вебхук
    ext_url = os.getenv("RENDER_EXTERNAL_URL")
    if ext_url and bot_running:
        wh_url = ext_url + webhook_path
        await bot.set_webhook(wh_url)
        logger.info("✅ Webhook: " + wh_url)

async def main():
    await run_web_server()
    logger.info("🚀 Bot started! Owner: " + str(OWNER_ID_INT))
    # Держим сервер запущенным
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Bot stopped by user")
