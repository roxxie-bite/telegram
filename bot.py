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
from aiogram.types import Message
from aiohttp import web

# ================= НАСТРОЙКИ =================
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = os.getenv("OWNER_ID")
MIN_DAYS_ENV = os.getenv("MIN_DAYS")
TAG_ENV = os.getenv("TAG", "loonie")
SITE_BASE = "https://lynther.sytes.net"
DEFAULT_MIN_DAYS = int(MIN_DAYS_ENV) if MIN_DAYS_ENV and MIN_DAYS_ENV.isdigit() else 25
DEFAULT_TAGS = [TAG_ENV] if TAG_ENV else ["loonie"]
CHECK_INTERVAL_HOURS = 6
MAX_PAGES = 20
CONFIG_FILE = "config.json"

# === СПЕЦИАЛЬНЫЕ ТЕГИ С ЦВЕТНЫМИ КЛАССАМИ ===
# Только для этих тегов проверяем конкретный цвет класса
SPECIAL_TAGS = {
    "xl": "tag_red",
    "style": "tag_purple",
    "character": "tag_green",
    "quality": "tag_gold",
}
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
def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {
        "min_days": DEFAULT_MIN_DAYS,
        "tags": DEFAULT_TAGS.copy(),
        "schedule": []
    }

def save_config():
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(bot_state, f, indent=2)
    except Exception as e:
        logger.error("Не удалось сохранить config.json: " + str(e))

bot_state = load_config()

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
def parse_loras_from_html(html, min_days, tag_name):
    if html is None:
        return []
    
    try:
        soup = BeautifulSoup(html, "html.parser")
        results = []
        
        lora_heads = soup.find_all("p", class_="lora_head")
        logger.info("Найдено lora_head: " + str(len(lora_heads)))
        
        # Проверяем, есть ли специальный класс для этого тега
        special_class = SPECIAL_TAGS.get(tag_name.lower())
        
        for head in lora_heads:
            try:
                text = head.get_text()
                
                # === ПРОВЕРКА ТЕГА ===
                has_tag = False
                
                if special_class:
                    # Для специальных тегов проверяем цветной класс
                    tag_span = head.find("span", class_=special_class)
                    if tag_span and tag_name.lower() in tag_span.get_text().lower():
                        has_tag = True
                else:
                    # Для обычных тегов ищем название тега в любом span class="tag_*"
                    for span in head.find_all("span", class_=re.compile(r"^tag_")):
                        if tag_name.lower() in span.get_text().lower():
                            has_tag = True
                            break
                
                if not has_tag:
                    continue  # Пропускаем, если нет нужного тега
                
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
                name_match = re.match(r'^\d+\.\s*(.+?)\s*\|\|', text.strip())
                if name_match:
                    lora_name = name_match.group(1).strip()
                else:
                    lora_name = tag_name.capitalize()
                
                # === ФОРМИРУЕМ ССЫЛКУ НА ЛОРУ ===
                lora_url = SITE_BASE + "/?p=lora_d&lora_id=" + lora_id
                
                # === ПРОВЕРКА: >= min_days ===
                if lora_days >= min_days:
                    results.append({
                        "id": lora_id,
                        "days": lora_days,
                        "name": lora_name,
                        "url": lora_url
                    })
                    logger.info("✅ ID: " + lora_id + " | Дни: " + str(lora_days) + " | Тег: " + tag_name + " | ВКЛЮЧЕНО")
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
def find_inactive_loonies_all_pages(base_url, min_days, tag_name):
    all_results = []
    pages_scanned = 0
    
    for page in range(1, MAX_PAGES + 1):
        if page == 1:
            url = base_url
        else:
            url = base_url + "&c=" + str(page)
        
        logger.info("=== Страница: " + str(page) + " | Тег: " + tag_name + " ===")
        html = fetch_with_retry(url)
        
        if html is None:
            logger.warning("Страница " + str(page) + " не загрузилась")
            break
        
        loras = parse_loras_from_html(html, min_days, tag_name)
        pages_scanned += 1
        
        if loras:
            all_results.extend(loras)
            logger.info("Стр. " + str(page) + ": найдено " + str(len(loras)) + " лор")
        else:
            logger.info("Стр. " + str(page) + ": лор не найдено → завершаю поиск")
            break
        
        if page < MAX_PAGES:
            time.sleep(1.5)
    
    logger.info("=== ВСЕГО === Стр: " + str(pages_scanned) + " | Лор: " + str(len(all_results)))
    return all_results, pages_scanned

def format_message(lora):
    msg = []
    msg.append("🧠 <a href=\"" + lora["url"] + "\">" + lora["name"] + "</a>")
    msg.append("🆔 <code>ID: " + str(lora["id"]) + "</code>")
    msg.append("🕸️ <b>" + str(lora["days"]) + " дней</b> без использования")
    msg.append("🗑️ <code>/dellora " + str(lora["id"]) + "</code>")
    msg.append("─" * 30)
    return "\n".join(msg)

# ================= КОМАНДЫ =================
@dp.message(Command("help"))
async def cmd_help(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    try:
        txt = "📖 <b>Справка по командам:</b>\n\n"
        txt += "<b>🔍 Основные:</b>\n"
        txt += "/check — Проверить все теги на лоры\n"
        txt += "/status — Показать текущие настройки\n"
        txt += "/help — Эта справка\n\n"
        txt += "<b>⚙️ Настройки:</b>\n"
        txt += "/setdays <N> — Установить порог дней (>= N)\n"
        txt += "/addtag <тег> — Добавить тег для поиска\n"
        txt += "/rmtag <тег> — Удалить тег из списка\n"
        txt += "/tags — Показать все активные теги\n"
        txt += "/setschedule <время> — Установить расписание\n"
        txt += "/schedule — Показать расписание проверок\n\n"
        txt += "<b>📊 Специальные теги:</b>\n"
        txt += "xl (красный), style (фиолетовый),\n"
        txt += "character (зелёный), quality (золотой)\n"
        txt += "<i>Остальные теги работают автоматически</i>\n\n"
        txt += "<i>Все команды доступны только тебе (владелец)</i>"
        await message.answer(txt, parse_mode="HTML")
    except Exception as e:
        logger.error("Ошибка в /help: " + str(e))

@dp.message(Command("check"))
async def cmd_check(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    try:
        logger.info("=== ПРОВЕРКА === Порог: >= " + str(bot_state["min_days"]) + " | Теги: " + ", ".join(bot_state["tags"]))
        await message.answer("🔍 Сканирую теги: <b>" + ", ".join(bot_state["tags"]) + "</b> (порог: >= " + str(bot_state["min_days"]) + " дней)...", parse_mode="HTML")
        
        all_loras = []
        total_pages = 0
        
        for tag in bot_state["tags"]:
            base_url = SITE_BASE + "/?p=lora&t=" + tag
            loras, pages = find_inactive_loonies_all_pages(base_url, bot_state["min_days"], tag)
            all_loras.extend(loras)
            total_pages += pages
        
        if not all_loras:
            await message.answer("✅ Лоры не найдены.")
            return
        
        all_loras.sort(key=lambda x: x["days"], reverse=True)
        
        await message.answer("📊 Найдено: <b>" + str(len(all_loras)) + "</b> лор", parse_mode="HTML")
        
        for lora in all_loras:
            await message.answer(format_message(lora), parse_mode="HTML")
            await asyncio.sleep(0.3)
        
        if all_loras:
            avg_days = sum(l["days"] for l in all_loras) // len(all_loras)
            max_lora = max(all_loras, key=lambda x: x["days"])
            min_lora = min(all_loras, key=lambda x: x["days"])
            
            stats = "\n📊 <b>Статистика:</b>\n"
            stats += "• Страниц просканировано: <b>" + str(total_pages) + "</b>\n"
            stats += "• Лор найдено: <b>" + str(len(all_loras)) + "</b>\n"
            stats += "• Средний простой: <b>" + str(avg_days) + "</b> дней\n"
            stats += "• Минимум: " + str(min_lora["days"]) + " дней\n"
            stats += "• Максимум: <b>" + str(max_lora["days"]) + "</b> дней (ID: <code>" + max_lora["id"] + "</code>)"
            
            await message.answer(stats, parse_mode="HTML")
            
    except Exception as e:
        logger.error("Ошибка в /check: " + str(e))
        await message.answer("❌ Ошибка при проверке.")

@dp.message(Command("setdays"))
async def cmd_setdays(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
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
        save_config()
        logger.info("=== ПОРОГ ИЗМЕНЁН === Новый: >= " + str(new_days))
        
        await message.answer("✅ Порог установлен на <b>" + str(new_days) + "</b> дней (>=). Используй /check для поиска.", parse_mode="HTML")
        
    except Exception as e:
        logger.error("Ошибка в /setdays: " + str(e))
        await message.answer("❌ Не удалось изменить.")

@dp.message(Command("addtag"))
async def cmd_addtag(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    try:
        parts = message.text.split()
        if len(parts) != 2:
            await message.answer("⚠️ Используй: <code>/addtag &lt;название&gt;</code>\nПример: <code>/addtag xl</code>", parse_mode="HTML")
            return
        
        new_tag = parts[1].strip().lower()
        
        if not new_tag.isalnum():
            await message.answer("⚠️ Тег должен содержать только буквы и цифры", parse_mode="HTML")
            return
        
        if new_tag in bot_state["tags"]:
            await message.answer("⚠️ Тег <b>" + new_tag + "</b> уже в списке", parse_mode="HTML")
            return
        
        bot_state["tags"].append(new_tag)
        save_config()
        
        # Проверяем, специальный ли тег
        if new_tag in SPECIAL_TAGS:
            tag_info = " (спец: " + SPECIAL_TAGS[new_tag] + ")"
        else:
            tag_info = " (обычный)"
        
        logger.info("=== ТЕГ ДОБАВЛЕН === " + new_tag + tag_info)
        
        await message.answer("✅ Тег <b>" + new_tag + "</b> добавлен" + tag_info + ".\nТекущие теги: " + ", ".join(bot_state["tags"]), parse_mode="HTML")
        
    except Exception as e:
        logger.error("Ошибка в /addtag: " + str(e))
        await message.answer("❌ Не удалось добавить тег.")

@dp.message(Command("rmtag"))
async def cmd_rmtag(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    try:
        parts = message.text.split()
        if len(parts) != 2:
            await message.answer("⚠️ Используй: <code>/rmtag &lt;название&gt;</code>", parse_mode="HTML")
            return
        
        tag_to_remove = parts[1].strip().lower()
        
        if tag_to_remove not in bot_state["tags"]:
            await message.answer("⚠️ Тег <b>" + tag_to_remove + "</b> не найден", parse_mode="HTML")
            return
        
        if len(bot_state["tags"]) <= 1:
            await message.answer("⚠️ Должен остаться хотя бы один тег", parse_mode="HTML")
            return
        
        bot_state["tags"].remove(tag_to_remove)
        save_config()
        logger.info("=== ТЕГ УДАЛЁН === " + tag_to_remove)
        
        await message.answer("✅ Тег <b>" + tag_to_remove + "</b> удалён. Текущие теги: " + ", ".join(bot_state["tags"]), parse_mode="HTML")
        
    except Exception as e:
        logger.error("Ошибка в /rmtag: " + str(e))
        await message.answer("❌ Не удалось удалить тег.")

@dp.message(Command("tags"))
async def cmd_tags(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    try:
        txt = "🏷️ <b>Активные теги:</b>\n"
        for i, tag in enumerate(bot_state["tags"], 1):
            if tag in SPECIAL_TAGS:
                txt += str(i) + ". <code>" + tag + "</code> (спец: " + SPECIAL_TAGS[tag] + ")\n"
            else:
                txt += str(i) + ". <code>" + tag + "</code> (обычный)\n"
        txt += "\n<i>Всего: " + str(len(bot_state["tags"])) + "</i>"
        await message.answer(txt, parse_mode="HTML")
    except Exception as e:
        logger.error("Ошибка в /tags: " + str(e))

@dp.message(Command("setschedule"))
async def cmd_setschedule(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    try:
        parts = message.text.split()
        if len(parts) < 2:
            await message.answer(
                "⚠️ Используй: <code>/setschedule &lt;время&gt; [&lt;время&gt;...]</code>\n"
                "Пример: <code>/setschedule 09:00 15:00 21:00</code>\n"
                "Формат: ЧЧ:ММ (24 часа)",
                parse_mode="HTML"
            )
            return
        
        times = []
        for t in parts[1:]:
            if re.match(r'^\d{2}:\d{2}$', t):
                times.append(t)
            else:
                await message.answer("⚠️ Неверный формат времени: <code>" + t + "</code>\nИспользуй ЧЧ:ММ (например, 09:00)", parse_mode="HTML")
                return
        
        bot_state["schedule"] = times
        save_config()
        logger.info("=== РАСПИСАНИЕ === Установлено: " + ", ".join(times))
        
        if times:
            await message.answer("✅ Расписание установлено: <b>" + ", ".join(times) + "</b>\nАвтопроверки будут в это время.", parse_mode="HTML")
        else:
            await message.answer("✅ Расписание очищено. Автопроверки отключены.", parse_mode="HTML")
        
    except Exception as e:
        logger.error("Ошибка в /setschedule: " + str(e))
        await message.answer("❌ Не удалось установить расписание.")

@dp.message(Command("schedule"))
async def cmd_schedule(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    try:
        if bot_state["schedule"]:
            txt = "⏰ <b>Расписание автопроверок:</b>\n"
            for t in bot_state["schedule"]:
                txt += "• <code>" + t + "</code>\n"
            txt += "\n<i>Следующая проверка в ближайшее время из списка</i>"
        else:
            txt = "⏰ <b>Расписание:</b> не установлено\n"
            txt += "<i>Используй /setschedule 09:00 15:00 21:00</i>"
        await message.answer(txt, parse_mode="HTML")
    except Exception as e:
        logger.error("Ошибка в /schedule: " + str(e))

@dp.message(Command("status"))
async def cmd_status(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    try:
        txt = "⚙️ <b>Настройки:</b>\n"
        txt += "🕸️ Порог: <b>" + str(bot_state["min_days"]) + "</b> дней (>=)\n"
        txt += "🏷️ Теги: <b>" + ", ".join(bot_state["tags"]) + "</b>\n"
        txt += "🔄 Автопроверка: <b>" + str(CHECK_INTERVAL_HOURS) + "</b> ч.\n"
        txt += "📄 Макс. страниц: <b>" + str(MAX_PAGES) + "</b>\n"
        if bot_state["schedule"]:
            txt += "⏰ Расписание: <b>" + ", ".join(bot_state["schedule"]) + "</b>\n"
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
            now = datetime.now()
            current_time = now.strftime("%H:%M")
            
            if bot_state["schedule"]:
                if current_time not in bot_state["schedule"]:
                    await asyncio.sleep(60)
                    continue
            
            logger.info("=== АВТОПРОВЕРКА === Порог: >= " + str(bot_state["min_days"]) + " | Теги: " + ", ".join(bot_state["tags"]))
            
            all_loras = []
            total_pages = 0
            
            for tag in bot_state["tags"]:
                base_url = SITE_BASE + "/?p=lora&t=" + tag
                loras, pages = find_inactive_loonies_all_pages(base_url, bot_state["min_days"], tag)
                all_loras.extend(loras)
                total_pages += pages
            
            if all_loras:
                all_loras.sort(key=lambda x: x["days"], reverse=True)
                
                for lora in all_loras:
                    try:
                        await bot.send_message(
                            chat_id=OWNER_ID_INT,
                            text=format_message(lora),
                            parse_mode="HTML"
                        )
                        await asyncio.sleep(0.5)
                    except Exception as e:
                        logger.error("Ошибка отправки: " + str(e))
                
                logger.info("✅ Автопроверка завершена: " + str(len(all_loras)) + " лор | Страниц: " + str(total_pages))
            
            await asyncio.sleep(60)
            
        except Exception as e:
            logger.error("Автопроверка упала: " + str(e))
            await asyncio.sleep(60)

async def on_startup():
    logger.info("🚀 Bot started (WEBHOOK). Owner: " + str(OWNER_ID_INT))
    logger.info("📊 Порог: >= " + str(bot_state["min_days"]) + " дней | Теги: " + ", ".join(bot_state["tags"]))
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