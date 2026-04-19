import os
import re
import asyncio
import logging
import requests
import time
from bs4 import BeautifulSoup
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.exceptions import TelegramNetworkError, TelegramRetryAfter
from aiohttp import web

# ================= НАСТРОЙКИ =================
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = os.getenv("OWNER_ID")
MIN_DAYS_ENV = os.getenv("MIN_DAYS")
BASE_URL = "https://lynther.sytes.net/?p=lora&q=Loonie"
DEFAULT_MIN_DAYS = int(MIN_DAYS_ENV) if MIN_DAYS_ENV and MIN_DAYS_ENV.isdigit() else 25
CHECK_INTERVAL_HOURS = 6
MAX_PAGES = 20  # Максимум страниц для проверки
# =============================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

if not BOT_TOKEN or not OWNER_ID:
    raise ValueError("❌ Переменные BOT_TOKEN и OWNER_ID не заданы!")

OWNER_ID_INT = int(OWNER_ID)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# === СОСТОЯНИЕ В ПАМЯТИ ===
bot_state = {"min_days": DEFAULT_MIN_DAYS}

# ================= ЗАЩИЩЁННЫЙ ЗАПРОС =================
def fetch_with_retry(url, max_retries=3):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=20)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.warning("Попытка " + str(attempt) + "/" + str(max_retries) + " упала: " + str(e))
            if attempt == max_retries:
                return None
            time.sleep(2 ** attempt)

# ================= ПАРСЕР ОДНОЙ СТРАНИЦЫ =================
def parse_loras_from_html(html, min_days):
    if html is None:
        return []
    
    try:
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text()
        lines = text.split("\n")

        id_pattern = re.compile(r"#️⃣\s*(\d+)")
        days_pattern = re.compile(r"🕸️\s*(\d+)\s*days?")
        loonie_pattern = re.compile(r"\bLoonie\b", re.IGNORECASE)

        results = []
        current = {}

        for line in lines:
            line = line.strip()
            if not line:
                continue

            id_match = id_pattern.search(line)
            if id_match:
                if (current.get("id") and 
                    current.get("days") is not None and 
                    current.get("has_loonie")):
                    if current["days"] > min_days:
                        results.append({
                            "id": current["id"],
                            "days": current["days"],
                            "name": current.get("name", "Loonie")
                        })
                current = {
                    "id": id_match.group(1),
                    "days": None,
                    "has_loonie": bool(loonie_pattern.search(line)),
                    "name": "Loonie"
                }
                continue

            if current.get("id"):
                days_match = days_pattern.search(line)
                if days_match:
                    current["days"] = int(days_match.group(1))
                if loonie_pattern.search(line):
                    current["has_loonie"] = True

        if (current.get("id") and 
            current.get("days") is not None and 
            current.get("has_loonie")):
            if current["days"] > min_days:
                results.append({
                    "id": current["id"],
                    "days": current["days"],
                    "name": current.get("name", "Loonie")
                })

        return results
    except Exception as e:
        logger.error("Ошибка парсинга: " + str(e))
        return []

# ================= ПАРСЕР ВСЕХ СТРАНИЦ =================
def find_inactive_loonies_all_pages(base_url, min_days):
    all_results = []
    pages_scanned = 0
    
    for page in range(1, MAX_PAGES + 1):
        if page == 1:
            url = base_url
        else:
            url = base_url + "&c=" + str(page)
        
        logger.info("Сканирую страницу: " + str(page) + " | URL: " + url)
        html = fetch_with_retry(url)
        
        if html is None:
            logger.warning("Страница " + str(page) + " не загрузилась")
            break
        
        loras = parse_loras_from_html(html, min_days)
        pages_scanned += 1
        
        if loras:
            all_results.extend(loras)
            logger.info("Страница " + str(page) + ": найдено " + str(len(loras)) + " лор")
        else:
            logger.info("Страница " + str(page) + ": лор не найдено, завершаю")
            break
        
        # Пауза между страницами (чтобы не забанили)
        if page < MAX_PAGES:
            time.sleep(1.5)
    
    logger.info("=== ВСЕГО === Страниц: " + str(pages_scanned) + " | Лор: " + str(len(all_results)))
    return all_results

def format_message(lora):
    msg = []
    msg.append("🧠 <b>" + str(lora["name"]) + "</b>")
    msg.append("🆔 <code>ID: " + str(lora["id"]) + "</code>")
    msg.append("🕸️ <b>" + str(lora["days"]) + " дней</b> без использования")
    msg.append("🗑️ <code>/dellora " + str(lora["id"]) + "</code>")
    msg.append("─" * 30)
    return "\n".join(msg)

# ================= КОМАНДЫ =================
@dp.message(Command("check"), F.from_user.id == OWNER_ID_INT)
async def cmd_check(message: Message):
    try:
        logger.info("=== ПРОВЕРКА === Порог: " + str(bot_state["min_days"]))
        await message.answer("🔍 Сканирую все страницы (порог: " + str(bot_state["min_days"]) + " дней)...")
        
        loras = find_inactive_loonies_all_pages(BASE_URL, bot_state["min_days"])
        
        logger.info("Найдено лор: " + str(len(loras)))
        
        if not loras:
            await message.answer("✅ Лоры, соответствующие критериям, не найдены.")
            return
        
        await message.answer("📊 Найдено: <b>" + str(len(loras)) + "</b> лор", parse_mode="HTML")
        
        for lora in loras:
            await message.answer(format_message(lora), parse_mode="HTML")
            await asyncio.sleep(0.3)
            
    except Exception as e:
        logger.error("Ошибка в /check: " + str(e))
        await message.answer("❌ Произошла ошибка при проверке. Попробуй позже.")

@dp.message(Command("setdays"), F.from_user.id == OWNER_ID_INT)
async def cmd_setdays(message: Message):
    try:
        parts = message.text.split()
        if len(parts) != 2 or not parts[1].isdigit():
            await message.answer("⚠️ Использование: <code>/setdays &lt;число&gt;</code>", parse_mode="HTML")
            return
        new_days = int(parts[1])
        if new_days < 1:
            await message.answer("⚠️ Число должно быть больше 0")
            return
        
        bot_state["min_days"] = new_days
        logger.info("=== ПОРОГ ИЗМЕНЁН === Новый: " + str(new_days))
        
        await message.answer("✅ Порог установлен на <b>" + str(new_days) + "</b> дней.", parse_mode="HTML")
        
        await message.answer("🔍 Проверяю с новым порогом...")
        loras = find_inactive_loonies_all_pages(BASE_URL, bot_state["min_days"])
        if loras:
            await message.answer("📊 Найдено: <b>" + str(len(loras)) + "</b> лор", parse_mode="HTML")
        else:
            await message.answer("✅ Лоры не найдены")
            
    except Exception as e:
        logger.error("Ошибка в /setdays: " + str(e))
        await message.answer("❌ Не удалось изменить настройку.")

@dp.message(Command("status"), F.from_user.id == OWNER_ID_INT)
async def cmd_status(message: Message):
    try:
        txt = "⚙️ <b>Настройки бота:</b>\n"
        txt += "🕸️ Порог: <b>" + str(bot_state["min_days"]) + "</b> дней\n"
        txt += "🔄 Автопроверка: каждые <b>" + str(CHECK_INTERVAL_HOURS) + "</b> ч.\n"
        txt += "📄 Макс. страниц: <b>" + str(MAX_PAGES) + "</b>"
        await message.answer(txt, parse_mode="HTML")
    except Exception as e:
        logger.error("Ошибка в /status: " + str(e))

@dp.message(Command("debug"), F.from_user.id == OWNER_ID_INT)
async def cmd_debug(message: Message):
    try:
        txt = "🔍 <b>Отладка:</b>\n"
        txt += "Порог в памяти: " + str(bot_state["min_days"]) + "\n"
        txt += "Порог из env (MIN_DAYS): " + str(DEFAULT_MIN_DAYS) + "\n"
        txt += "Базовый URL: " + BASE_URL
        await message.answer(txt, parse_mode="HTML")
    except Exception as e:
        logger.error("Ошибка в /debug: " + str(e))

@dp.message()
async def silent_ignore(message: Message):
    pass

# ================= ГЛОБАЛЬНЫЙ ОБРАБОТЧИК ОШИБОК =================
@dp.errors()
async def global_error_handler(event, exception):
    logger.critical("Критическая ошибка aiogram: " + str(exception))
    if isinstance(exception, TelegramNetworkError):
        logger.warning("Проблемы с сетью Telegram. Ждём восстановления...")
    elif isinstance(exception, TelegramRetryAfter):
        logger.warning("Лимит Telegram. Ждём " + str(exception.retry_after) + " сек...")
        await asyncio.sleep(exception.retry_after)

# ================= ФОНОВЫЕ ЗАДАЧИ =================
async def periodic_check():
    await asyncio.sleep(60)
    while True:
        try:
            logger.info("=== АВТОПРОВЕРКА === Порог: " + str(bot_state["min_days"]))
            loras = find_inactive_loonies_all_pages(BASE_URL, bot_state["min_days"])
            if loras:
                logger.info("Найдено лор: " + str(len(loras)))
                for lora in loras:
                    try:
                        await bot.send_message(
                            chat_id=OWNER_ID_INT,
                            text=format_message(lora),
                            parse_mode="HTML"
                        )
                        await asyncio.sleep(0.5)
                    except TelegramNetworkError:
                        logger.warning("Сеть Telegram упала, повтор через 10 сек...")
                        await asyncio.sleep(10)
                    except Exception as e:
                        logger.error("Ошибка отправки сообщения: " + str(e))
            else:
                logger.info("Лор не найдено")
        except Exception as e:
            logger.error("Автопроверка упала: " + str(e))
        await asyncio.sleep(CHECK_INTERVAL_HOURS * 3600)

async def on_startup():
    logger.info("🚀 Bot started. Owner: " + str(OWNER_ID_INT))
    logger.info("🔒 Private mode | Порог: " + str(bot_state["min_days"]) + " | Страниц: до " + str(MAX_PAGES))
    asyncio.create_task(periodic_check())

# ================= WEB SERVER (RENDER) =================
async def health_handler(request):
    return web.Response(text="OK")

async def run_web_server():
    app = web.Application()
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", "8080"))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info("🌐 Health server running on port " + str(port))

async def main():
    dp.startup.register(on_startup)
    try:
        await asyncio.gather(
            dp.start_polling(bot),
            run_web_server(),
            return_exceptions=True
        )
    except Exception as e:
        logger.critical("Главный цикл упал: " + str(e))

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Bot stopped by user")