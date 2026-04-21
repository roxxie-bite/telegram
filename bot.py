import os
import re
import asyncio
import logging
import requests
import time
import json
from datetime import datetime
from io import BytesIO
from bs4 import BeautifulSoup
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
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
CHECK_INTERVAL_HOURS = 6
MAX_PAGES = 50
CONFIG_FILE = "config.json"
EXPORT_THRESHOLD = 50  # Если больше — отправляем файлом

# === СПЕЦИАЛЬНЫЕ ТЕГИ ===
SPECIAL_TAGS = {"xl": "tag_red", "style": "tag_purple", "character": "tag_green", "quality": "tag_gold"}

# === ЭМОДЗИ ===
EMOJI = {"brain": "🧠", "id": "🆔", "days": "🕸️", "delete": "🗑️", "search": "🔍", "stats": "📊",
         "settings": "⚙️", "tag": "🏷️", "clock": "⏰", "check": "✅", "warning": "⚠️", "error": "❌",
         "info": "ℹ️", "file": "📄", "chat": "💬", "stop": "🛑", "restart": "🔄"}

# === ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ===
bot_running = True
periodic_task = None
# =============================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

if not BOT_TOKEN or not OWNER_ID:
    raise ValueError("❌ Переменные BOT_TOKEN и OWNER_ID не заданы!")

OWNER_ID_INT = int(OWNER_ID)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ================= КОНФИГ =================
def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"min_days": DEFAULT_MIN_DAYS, "tags": DEFAULT_TAGS.copy(), "schedule": []}

def save_config():
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(bot_state, f, indent=2)
    except Exception as e:
        logger.error("Не удалось сохранить config.json: " + str(e))

bot_state = load_config()

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
            if not all_on_page:  # ← Останавливаемся только если лор вообще нет
                break
            if page < MAX_PAGES:
                time.sleep(1.5)
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
async def _send_loras_to_chat(message, all_loras, total_pages):
    await message.answer(EMOJI["stats"] + " Найдено: <b>" + str(len(all_loras)) + "</b> лор", parse_mode="HTML")
    for lora in all_loras:
        await message.answer(format_message(lora), parse_mode="HTML")
        await asyncio.sleep(0.3)
    if all_loras:
        avg = sum(l["days"] for l in all_loras) // len(all_loras)
        mx = max(all_loras, key=lambda x: x["days"])
        mn = min(all_loras, key=lambda x: x["days"])
        stats = "\n" + EMOJI["stats"] + " <b>Статистика:</b>\n"
        stats += "• Страниц: <b>" + str(total_pages) + "</b>\n• Лор: <b>" + str(len(all_loras)) + "</b>\n"
        stats += "• Среднее: <b>" + str(avg) + "</b> дней\n• Мин: " + str(mn["days"]) + " | Макс: <b>" + str(mx["days"]) + "</b>"
        await message.answer(stats, parse_mode="HTML")

async def _send_loras_as_file(message, all_loras, total_pages):
    content = make_export_file(all_loras, bot_state["min_days"], bot_state["tags"])
    file = BytesIO(content)
    file.name = "loonie_export_" + datetime.now().strftime("%Y%m%d_%H%M") + ".txt"
    caption = EMOJI["file"] + " <b>Экспорт лор</b>\n"
    caption += "Лор: " + str(len(all_loras)) + "\nПорог: >= " + str(bot_state["min_days"]) + " дней"
    if bot_state["tags"]:
        caption += "\nТеги: " + ", ".join(bot_state["tags"])
    await message.answer_document(document=file, caption=caption, parse_mode="HTML")
    # Статистику отдельно
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
    if message.from_user.id != OWNER_ID_INT:
        return
    txt = EMOJI["info"] + " <b>Справка:</b>\n\n"
    txt += "<b>" + EMOJI["search"] + " Основные:</b>\n/check — Проверить лоры\n/status — Настройки\n/help — Справка\n\n"
    txt += "<b>" + EMOJI["settings"] + " Настройки:</b>\n/setdays N — Порог дней (0=все)\n"
    txt += "/addtag <тег> — Добавить тег\n/rmtag <тег> — Удалить тег\n/tags — Список тегов\n"
    txt += "/setschedule ЧЧ:ММ — Расписание проверок\n/schedule — Показать расписание\n\n"
    txt += "<b>" + EMOJI["stop"] + " Аварийные:</b>\n/stop <пароль> — Остановить бота (пароль: " + STOP_PASSWORD + ")\n\n"
    txt += "<i>Только для владельца</i>"
    await message.answer(txt, parse_mode="HTML")

@dp.message(Command("check"))
async def cmd_check(message: Message):
    if message.from_user.id != OWNER_ID_INT or not bot_running:
        return
    try:
        logger.info("=== ПРОВЕРКА ===")
        if bot_state["tags"]:
            all_loras, total_pages = [], 0
            for tag in bot_state["tags"]:
                loras, pages = find_inactive_loonies_all_pages(BASE_URL, bot_state["min_days"], bot_state["tags"], tag_name=tag)
                all_loras.extend(loras)
                total_pages += pages
        else:
            all_loras, total_pages = find_inactive_loonies_all_pages(BASE_URL, bot_state["min_days"], [])
        
        if not all_loras:
            await message.answer(EMOJI["check"] + " Лоры не найдены.")
            return
        
        all_loras.sort(key=lambda x: x["days"], reverse=True)
        
        # === АВТОМАТИЧЕСКИЙ ВЫБОР: файл если много, чат если мало ===
        if len(all_loras) > EXPORT_THRESHOLD:
            await message.answer(EMOJI["file"] + " Лор много (<b>" + str(len(all_loras)) + "</b>), отправляю файлом...", parse_mode="HTML")
            await _send_loras_as_file(message, all_loras, total_pages)
        else:
            await _send_loras_to_chat(message, all_loras, total_pages)
            
    except Exception as e:
        logger.error("❌ Ошибка в /check: " + str(e), exc_info=True)
        await message.answer(EMOJI["error"] + " Ошибка при проверке.")

@dp.message(Command("setdays"))
async def cmd_setdays(message: Message):
    if message.from_user.id != OWNER_ID_INT or not bot_running:
        return
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit() or int(parts[1]) < 0:
        await message.answer(EMOJI["warning"] + " Используй: <code>/setdays &lt;число&gt;</code>", parse_mode="HTML")
        return
    bot_state["min_days"] = int(parts[1])
    save_config()
    days_text = "все лоры" if bot_state["min_days"]==0 else ">=" + str(bot_state["min_days"]) + " дней"
    await message.answer(EMOJI["check"] + " Порог: <b>" + days_text + "</b>", parse_mode="HTML")

@dp.message(Command("addtag"))
async def cmd_addtag(message: Message):
    if message.from_user.id != OWNER_ID_INT or not bot_running:
        return
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].strip().lower().isalnum() or any(t.lower()==parts[1].strip().lower() for t in bot_state["tags"]):
        await message.answer(EMOJI["warning"] + " Используй: <code>/addtag &lt;название&gt;</code>", parse_mode="HTML")
        return
    bot_state["tags"].append(parts[1].strip().lower())
    save_config()
    await message.answer(EMOJI["check"] + " Тег <b>" + parts[1] + "</b> добавлен.", parse_mode="HTML")

@dp.message(Command("rmtag"))
async def cmd_rmtag(message: Message):
    if message.from_user.id != OWNER_ID_INT or not bot_running:
        return
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer(EMOJI["warning"] + " Используй: <code>/rmtag &lt;название&gt;</code>", parse_mode="HTML")
        return
    tag = next((t for t in bot_state["tags"] if t.lower()==parts[1].strip().lower()), None)
    if not tag or len(bot_state["tags"])<=1:
        await message.answer(EMOJI["warning"] + " Тег не найден", parse_mode="HTML")
        return
    bot_state["tags"].remove(tag)
    save_config()
    await message.answer(EMOJI["check"] + " Тег <b>" + tag + "</b> удалён.", parse_mode="HTML")

@dp.message(Command("tags"))
async def cmd_tags(message: Message):
    if message.from_user.id != OWNER_ID_INT or not bot_running:
        return
    if not bot_state["tags"]:
        await message.answer(EMOJI["tag"] + " <b>Теги:</b>\n<i>нет</i>", parse_mode="HTML")
        return
    txt = EMOJI["tag"] + " <b>Теги:</b>\n" + "\n".join(f"{i}. <code>{t}</code>" for i,t in enumerate(bot_state["tags"],1))
    await message.answer(txt, parse_mode="HTML")

@dp.message(Command("setschedule"))
async def cmd_setschedule(message: Message):
    if message.from_user.id != OWNER_ID_INT or not bot_running:
        return
    parts = message.text.split()
    if len(parts) < 2 or not all(re.match(r'^\d{2}:\d{2}$', t) for t in parts[1:]):
        await message.answer(EMOJI["warning"] + " Используй: <code>/setschedule 09:00 15:00</code>", parse_mode="HTML")
        return
    bot_state["schedule"] = parts[1:]
    save_config()
    await message.answer(EMOJI["clock"] + " Расписание установлено.", parse_mode="HTML")

@dp.message(Command("schedule"))
async def cmd_schedule(message: Message):
    if message.from_user.id != OWNER_ID_INT or not bot_running:
        return
    txt = EMOJI["clock"] + " <b>Расписание:</b>\n" + ("\n".join("• <code>"+t+"</code>" for t in bot_state["schedule"]) if bot_state["schedule"] else "<i>не установлено</i>")
    await message.answer(txt, parse_mode="HTML")

@dp.message(Command("status"))
async def cmd_status(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    txt = EMOJI["settings"] + " <b>Настройки:</b>\n"
    txt += EMOJI["check" if bot_running else "stop"] + " Статус: <b>" + ("Активен" if bot_running else "ОСТАНОВЛЕН") + "</b>\n"
    txt += EMOJI["days"] + " Порог: <b>" + ("все лоры" if bot_state["min_days"]==0 else ">=" + str(bot_state["min_days"]) + " дней") + "</b>\n"
    txt += EMOJI["tag"] + " Теги: <b>" + (", ".join(bot_state["tags"]) if bot_state["tags"] else "нет") + "</b>\n"
    txt += "🔄 Автопроверка: <b>" + str(CHECK_INTERVAL_HOURS) + "</b> ч.\n📄 Макс. страниц: <b>" + str(MAX_PAGES) + "</b>"
    if bot_state["schedule"]:
        txt += "\n" + EMOJI["clock"] + " Расписание: <b>" + ", ".join(bot_state["schedule"]) + "</b>"
    await message.answer(txt, parse_mode="HTML")

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

@dp.message(Command("start"))
async def cmd_start(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    global bot_running
    if bot_running:
        await message.answer(EMOJI["info"] + " <b>Бот активен!</b>\n/help — команды", parse_mode="HTML")
        return
    bot_running = True
    start_periodic_task()
    await message.answer(EMOJI["check"] + " <b>Бот запущен!</b>", parse_mode="HTML")

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
        if bot_state["schedule"] and now not in bot_state["schedule"]:
            continue
        logger.info("=== АВТОПРОВЕРКА ===")
        if bot_state["tags"]:
            all_loras, total_pages = [], 0
            for tag in bot_state["tags"]:
                loras, pages = find_inactive_loonies_all_pages(BASE_URL, bot_state["min_days"], bot_state["tags"], tag_name=tag)
                all_loras.extend(loras)
                total_pages += pages
        else:
            all_loras, total_pages = find_inactive_loonies_all_pages(BASE_URL, bot_state["min_days"], [])
        if all_loras:
            all_loras.sort(key=lambda x: x["days"], reverse=True)
            # Автопроверка всегда отправляет файлом если много
            if len(all_loras) > EXPORT_THRESHOLD:
                await _send_loras_as_file(bot, all_loras, total_pages)
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