import os
import re
import json
import asyncio
import logging
import requests
from bs4 import BeautifulSoup
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message
from aiohttp import web

# ================= НАСТРОЙКИ =================
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = os.getenv("OWNER_ID")
TARGET_URL = "https://lynther.sytes.net/?p=lora&q=Loonie"
DEFAULT_MIN_DAYS = 25
CHECK_INTERVAL_HOURS = 6
CONFIG_FILE = "config.json"
# =============================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

if not BOT_TOKEN or not OWNER_ID:
    raise ValueError("❌ Переменные BOT_TOKEN и OWNER_ID не заданы в Render!")

OWNER_ID_INT = int(OWNER_ID)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ================= КОНФИГ =================
def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning("Не удалось загрузить config.json: " + str(e))
    return {"min_days": DEFAULT_MIN_DAYS}

def save_config(data):
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.error("Не удалось сохранить config.json: " + str(e))

bot_state = load_config()

# ================= ПАРСЕР =================
def find_inactive_loonies(url, min_days):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error("Ошибка запроса: " + str(e))
        return []

    soup = BeautifulSoup(response.text, "html.parser")
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

def format_message(lora):
    msg = []
    msg.append("🧠 <b>" + str(lora["name"]) + "</b>")
    msg.append("🆔 <code>ID: " + str(lora["id"]) + "</code>")
    msg.append("🕸️ <b>" + str(lora["days"]) + " дней</b> без использования")
    msg.append("🗑️ <code>/dellora " + str(lora["id"]) + "</code>")
    msg.append("─" * 30)
    return "\n".join(msg)

# ================= ПРИВАТНЫЕ КОМАНДЫ =================
@dp.message(Command("check"), F.from_user.id == OWNER_ID_INT)
async def cmd_check(message: Message):
    await message.answer("🔍 Начинаю сканирование...")
    loras = find_inactive_loonies(TARGET_URL, bot_state["min_days"])
    if not loras:
        await message.answer("✅ Лоры, соответствующие критериям, не найдены.")
        return
    for lora in loras:
        await message.answer(format_message(lora), parse_mode="HTML")
        await asyncio.sleep(0.3)

@dp.message(Command("setdays"), F.from_user.id == OWNER_ID_INT)
async def cmd_setdays(message: Message):
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("⚠️ Использование: <code>/setdays &lt;число&gt;</code>", parse_mode="HTML")
        return
    new_days = int(parts[1])
    if new_days < 1:
        await message.answer("⚠️ Число должно быть больше 0")
        return
    bot_state["min_days"] = new_days
    save_config({"min_days": new_days})
    await message.answer("✅ Порог установлен на <b>" + str(new_days) + "</b> дней.", parse_mode="HTML")

@dp.message(Command("status"), F.from_user.id == OWNER_ID_INT)
async def cmd_status(message: Message):
    txt = "⚙️ <b>Настройки бота:</b>\n"
    txt += "🕸️ Порог: <b>" + str(bot_state["min_days"]) + "</b> дней\n"
    txt += "🔄 Автопроверка: каждые <b>" + str(CHECK_INTERVAL_HOURS) + "</b> ч."
    await message.answer(txt, parse_mode="HTML")

# Тихий обработчик для всех остальных сообщений
@dp.message()
async def silent_ignore(message: Message):
    pass

# ================= ФОНОВЫЕ ЗАДАЧИ =================
async def periodic_check():
    await asyncio.sleep(60)
    while True:
        try:
            loras = find_inactive_loonies(TARGET_URL, bot_state["min_days"])
            if loras:
                for lora in loras:
                    await bot.send_message(
                        chat_id=OWNER_ID_INT,
                        text=format_message(lora),
                        parse_mode="HTML"
                    )
                    await asyncio.sleep(0.3)
        except Exception as e:
            logger.error("Ошибка автопроверки: " + str(e))
        await asyncio.sleep(CHECK_INTERVAL_HOURS * 3600)

async def on_startup():
    logger.info("🚀 Bot started. Owner: " + str(OWNER_ID_INT))
    logger.info("🔒 Private mode enabled")
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
    await asyncio.gather(
        dp.start_polling(bot),
        run_web_server()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Bot stopped by user")