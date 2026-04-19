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
from aiohttp import web

# ================= НАСТРОЙКИ =================
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = os.getenv("OWNER_ID")
MIN_DAYS_ENV = os.getenv("MIN_DAYS")
BASE_URL = "https://lynther.sytes.net/?p=lora&t=loonie"
SITE_BASE = "https://lynther.sytes.net"
DEFAULT_MIN_DAYS = int(MIN_DAYS_ENV) if MIN_DAYS_ENV and MIN_DAYS_ENV.isdigit() else 25
CHECK_INTERVAL_HOURS = 6
MAX_PAGES = 20
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

# ================= ПАРСЕР ПО lora_head =================
def parse_loras_from_html(html, min_days):
    if html is None:
        return []
    
    try:
        soup = BeautifulSoup(html, "html.parser")
        results = []
        
        lora_heads = soup.find_all("p", class_="lora_head")
        logger.info("Найдено lora_head: " + str(len(lora_heads)))
        
        for head in lora_heads:
            try:
                text = head.get_text()
                
                # Ищем ID: #️⃣123456
                id_match = re.search(r"#️⃣\s*(\d+)", text)
                if not id_match:
                    continue
                lora_id = id_match.group(1)
                
                # Ищем дни: 🕸️10 days или 🕸️1 day
                days_match = re.search(r"🕸️\s*(\d+)\s*d", text, re.IGNORECASE)
                if not days_match:
                    continue
                lora_days = int(days_match.group(1))
                
                # === ИЗВЛЕКАЕМ НАЗВАНИЕ (до "||") ===
                # Формат: "3. Uniro (style) [Illustrious] || ..."
                name_match = re.match(r'^\d+\.\s*(.+?)\s*\|\|', text.strip())
                if name_match:
                    lora_name = name_match.group(1).strip()
                else:
                    lora_name = "Loonie"
                
                # === ФОРМИРУЕМ ССЫЛКУ НА ЛОРУ ===
                lora_url = SITE_BASE + "/?p=lora&id=" + lora_id
                
                # === ПРОВЕРКА: >= min_days ===
                if lora_days >= min_days:
                    results.append({
                        "id": lora_id,
                        "days": lora_days,
                        "name": lora_name,
                        "url": lora_url
                    })
                    logger.info("✅ ID: " + lora_id + " | Дни: " + str(lora_days) + " | ВКЛЮЧЕНО")
                else:
                    logger.info("❌ ID: " + lora_id + " | Дни: " + str(lora_days) + " | ОТКЛОНЕНО")
                    
            except Exception as e:
                logger.warning("Ошибка обработки lora_head: " + str(e))
                continue
        
        logger.info("=== Страница готова === Лор: " + str(len(results)))
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
        
        logger.info("=== Страница: " + str(page) + " ===")
        html = fetch_with_retry(url)
        
        if html is None:
            logger.warning("Страница " + str(page) + " не загрузилась")
            break
        
        loras = parse_loras_from_html(html, min_days)
        pages_scanned += 1
        
        if loras:
            all_results.extend(loras)
            logger.info("Стр. " + str(page) + ": найдено " + str(len(loras)) + " лор")
        else:
            logger.info("Стр. " + str(page) + ": лор не найдено")
        
        if page < MAX_PAGES:
            time.sleep(1.5)
    
    logger.info("=== ВСЕГО === Стр: " + str(pages_scanned) + " | Лор: " + str(len(all_results)))
    return all_results

def format_message(lora):
    """Формирует сообщение с кликабельным названием лоры"""
    msg = []
    # Кликабельное название: <a href="URL">Name</a>
    msg.append("🧠 <a href=\"" + lora["url"] + "\">" + lora["name"] + "</a>")
    msg.append("🆔 <code>ID: " + str(lora["id"]) + "</code>")
    msg.append("🕸️ <b>" + str(lora["days"]) + " дней</b> без использования")
    msg.append("🗑️ <code>/dellora " + str(lora["id"]) + "</code>")
    msg.append("─" * 30)
    return "\n".join(msg)

# ================= КОМАНДЫ =================
@dp.message(Command("check"), F.from_user.id == OWNER_ID_INT)
async def cmd_check(message: Message):
    try:
        logger.info("=== ПРОВЕРКА === Порог: >= " + str(bot_state["min_days"]))
        await message.answer("🔍 Сканирую тег Loonie (порог: >= " + str(bot_state["min_days"]) + " дней)...")
        
        loras = find_inactive_loonies_all_pages(BASE_URL, bot_state["min_days"])
        
        if not loras:
            await message.answer("✅ Лоры не найдены.")
            return
        
        await message.answer("📊 Найдено: <b>" + str(len(loras)) + "</b> лор", parse_mode="HTML")
        
        for lora in loras:
            await message.answer(format_message(lora), parse_mode="HTML")
            await asyncio.sleep(0.3)
            
    except Exception as e:
        logger.error("Ошибка в /check: " + str(e))
        await message.answer("❌ Ошибка при проверке.")

@dp.message(Command("setdays"), F.from_user.id == OWNER_ID_INT)
async def cmd_setdays(message: Message):
    """Только сохраняет порог, без автоматической проверки"""
    try:
        parts = message.text.split()
        if len(parts) != 2 or not parts[1].isdigit():
            await message.answer("⚠️ Используй: <code>/setdays &lt;число&gt;</code>", parse_mode="HTML")
            return
        new_days = int(parts[1])
        if new_days < 1:
            await message.answer("⚠️ Число должно быть > 0")
            return
        
        bot_state["min_days"] = new_days
        logger.info("=== ПОРОГ ИЗМЕНЁН === Новый: >= " + str(new_days))
        
        # ✅ Только подтверждаем, без проверки
        await message.answer("✅ Порог установлен на <b>" + str(new_days) + "</b> дней (>=). Используй /check для поиска.", parse_mode="HTML")
        
    except Exception as e:
        logger.error("Ошибка в /setdays: " + str(e))
        await message.answer("❌ Не удалось изменить.")

@dp.message(Command("status"), F.from_user.id == OWNER_ID_INT)
async def cmd_status(message: Message):
    try:
        txt = "⚙️ <b>Настройки:</b>\n"
        txt += "🕸️ Порог: <b>" + str(bot_state["min_days"]) + "</b> дней (>=)\n"
        txt += "🔄 Автопроверка: <b>" + str(CHECK_INTERVAL_HOURS) + "</b> ч.\n"
        txt += "📄 Страниц: до <b>" + str(MAX_PAGES) + "</b>\n"
        await message.answer(txt, parse_mode="HTML")
    except Exception as e:
        logger.error("Ошибка в /status: " + str(e))

@dp.message()
async def silent_ignore(message: Message):
    pass

# ================= ФОНОВЫЕ ЗАДАЧИ =================
async def periodic_check():
    await asyncio.sleep(60)
    while True:
        try:
            logger.info("=== АВТОПРОВЕРКА === Порог: >= " + str(bot_state["min_days"]))
            loras = find_inactive_loonies_all_pages(BASE_URL, bot_state["min_days"])
            if loras:
                for lora in loras:
                    try:
                        await bot.send_message(
                            chat_id=OWNER_ID_INT,
                            text=format_message(lora),
                            parse_mode="HTML"
                        )
                        await asyncio.sleep(0.5)
                    except Exception as e:
                        logger.error("Ошибка отправки: " + str(e))
        except Exception as e:
            logger.error("Автопроверка упала: " + str(e))
        await asyncio.sleep(CHECK_INTERVAL_HOURS * 3600)

async def on_startup():
    logger.info("🚀 Bot started (WEBHOOK). Owner: " + str(OWNER_ID_INT))
    logger.info("📊 Порог: >= " + str(bot_state["min_days"]) + " дней | Парсер: lora_head")
    asyncio.create_task(periodic_check())

async def on_shutdown():
    logger.info("👋 Bot shutting down...")
    await bot.session.close()

# ================= WEBHOOK SERVER =================
async def webhook_handler(request):
    try:
        update = await request.json()
        await dp.feed_webhook_update(bot, update)
        return web.Response(text="OK")
    except Exception as e:
        logger.error("Ошибка вебхука: " + str(e))
        return web.Response(text="Error", status=500)

async def health_handler(request):
    return web.Response(text="OK")

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
    
    webhook_url = os.getenv("RENDER_EXTERNAL_URL", "")
    if webhook_url:
        webhook_full = webhook_url + webhook_path
        await bot.set_webhook(webhook_full)
        logger.info("✅ Webhook set: " + webhook_full)
    else:
        logger.warning("⚠️ RENDER_EXTERNAL_URL не задан!")

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
        logger.info("👋 Bot stopped")