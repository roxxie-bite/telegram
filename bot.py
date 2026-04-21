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
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
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
EXPORT_THRESHOLD = 50

# === СПЕЦИАЛЬНЫЕ ТЕГИ ===
SPECIAL_TAGS = {
    "xl": "tag_red",
    "style": "tag_purple",
    "character": "tag_green",
    "quality": "tag_gold",
}

# === ЭМОДЗИ ===
EMOJI = {
    "brain": "🧠",
    "id": "🆔",
    "days": "🕸️",
    "delete": "🗑️",
    "search": "🔍",
    "stats": "📊",
    "settings": "⚙️",
    "tag": "🏷️",
    "clock": "⏰",
    "check": "✅",
    "warning": "⚠️",
    "error": "❌",
    "info": "ℹ️",
    "file": "📄",
    "chat": "💬",
    "stop": "🛑",
    "restart": "🔄",
}

# === ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ===
bot_running = True
periodic_task = None  # Ссылка на задачу автопроверки
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

# ================= ПРОВЕРКА ТЕГА =================
def has_tag_in_head(head, tag_name):
    tag_lower = tag_name.lower()
    
    if tag_lower in SPECIAL_TAGS:
        special_class = SPECIAL_TAGS[tag_lower]
        for span in head.find_all("span", class_=special_class):
            if tag_lower in span.get_text().strip().lower():
                return True
        return False
    
    for span in head.find_all("span", class_=re.compile(r"^tag_", re.IGNORECASE)):
        span_text = span.get_text().strip().lower()
        if tag_lower == span_text or tag_lower in span_text:
            return True
    return False


# ================= ПАРСЕР =================
def parse_loras_from_html(html, min_days, current_tag):
    """Возвращает (все_лоры_на_странице, отфильтрованные_лоры)"""
    if html is None:
        return [], []
    
    try:
        soup = BeautifulSoup(html, "html.parser")
        all_on_page = []  # Все лоры на странице (независимо от дней)
        filtered = []     # Только те, что >= min_days
        
        lora_heads = soup.find_all("p", class_="lora_head")
        
        for head in lora_heads:
            try:
                text = head.get_text()
                
                id_match = re.search(r"#️⃣\s*(\d+)", text)
                if not id_match:
                    continue
                lora_id = id_match.group(1)
                
                days_match = re.search(r"🕸️\s*(\d+)\s*d", text, re.IGNORECASE)
                if not days_match:
                    continue
                lora_days = int(days_match.group(1))
                
                name_match = re.match(r'^\d+\.\s*(.+?)\s*\|\|', text.strip())
                lora_name = name_match.group(1).strip() if name_match else "Unknown"
                
                lora_url = SITE_BASE + "/?p=lora_d&lora_id=" + lora_id
                
                # Сохраняем ВСЕ лоры (для проверки, есть ли ещё страницы)
                all_on_page.append({
                    "id": lora_id,
                    "days": lora_days,
                    "name": lora_name,
                    "url": lora_url
                })
                
                # Фильтруем по дням
                if lora_days >= min_days:
                    filtered.append({
                        "id": lora_id,
                        "days": lora_days,
                        "name": lora_name,
                        "url": lora_url
                    })
                    logger.info("✅ ID: " + lora_id + " | Дни: " + str(lora_days) + " | ВКЛЮЧЕНО")
                else:
                    logger.info("❌ ID: " + lora_id + " | Дни: " + str(lora_days) + " | ОТКЛОНЕНО")
                    
            except Exception as e:
                logger.warning("Ошибка: " + str(e))
                continue
        
        return all_on_page, filtered
        
    except Exception as e:
        logger.error("Ошибка парсинга: " + str(e))
        return [], []

# ================= ПАРСЕР ВСЕХ СТРАНИЦ =================
def find_inactive_loonies_all_pages(base_url, min_days, active_tags, tag_name=None):
    all_results = []
    pages_scanned = 0
    
    tags_to_search = [tag_name] if tag_name else active_tags
    
    for search_tag in tags_to_search:
        tag_results = []
        tag_pages = 0
        
        for page in range(1, MAX_PAGES + 1):
            if not bot_running:
                logger.info("🛑 Остановка")
                break
            
            if page == 1:
                url = base_url + "&t=" + search_tag
            else:
                url = base_url + "&t=" + search_tag + "&c=" + str(page)
            
            logger.info("=== Страница: " + str(page) + " | Тег: " + search_tag + " ===")
            html = fetch_with_retry(url)
            
            if html is None:
                logger.warning("Страница не загрузилась")
                break
            
            # === ИСПРАВЛЕНИЕ: получаем и все лоры, и отфильтрованные ===
            all_on_page, filtered = parse_loras_from_html(html, min_days, search_tag)
            tag_pages += 1
            
            # Добавляем только отфильтрованные
            tag_results.extend(filtered)
            
            if filtered:
                logger.info("Стр. " + str(page) + ": найдено " + str(len(filtered)) + " лор (из " + str(len(all_on_page)) + ")")
            
            # === ИСПРАВЛЕНИЕ: прерываем только если на странице вообще нет лор ===
            if not all_on_page:
                logger.info("Стр. " + str(page) + ": лор нет → завершаю поиск")
                break
            else:
                logger.info("Стр. " + str(page) + ": есть лоры, продолжаю...")
            
            if page < MAX_PAGES:
                time.sleep(1.5)
        
        all_results.extend(tag_results)
        pages_scanned += tag_pages
        logger.info("=== Тег " + search_tag + " готов === Лор: " + str(len(tag_results)))
    
    logger.info("=== ВСЕГО === Стр: " + str(pages_scanned) + " | Лор: " + str(len(all_results)))
    return all_results, pages_scanned



def format_message(lora):
    msg = []
    msg.append(EMOJI["brain"] + " <a href=\"" + lora["url"] + "\">" + lora["name"] + "</a>")
    msg.append(EMOJI["id"] + " <code>ID: " + str(lora["id"]) + "</code>")
    msg.append(EMOJI["days"] + " <b>" + str(lora["days"]) + " дней</b> без использования")
    msg.append(EMOJI["delete"] + " <code>/dellora " + str(lora["id"]) + "</code>")
    msg.append("─" * 30)
    return "\n".join(msg)

def make_export_file(loras, min_days, tags):
    content = "# Loonie Bot Export\n"
    content += "# Дата: " + datetime.now().strftime("%Y-%m-%d %H:%M") + "\n"
    content += "# Порог: >= " + str(min_days) + " дней\n"
    content += "# Теги: " + (", ".join(tags) if tags else "все") + "\n"
    content += "# Лор: " + str(len(loras)) + "\n\n"
    for lora in loras:
        content += "/dellora " + lora["id"] + "  # " + lora["name"] + " (" + str(lora["days"]) + " дней)\n"
    return content.encode("utf-8")

# ================= УПРАВЛЕНИЕ ЗАДАЧАМИ =================
def cancel_periodic_task():
    """Отменяет задачу автопроверки"""
    global periodic_task
    if periodic_task and not periodic_task.done():
        periodic_task.cancel()
        logger.info("🛑 Задача автопроверки отменена")
    periodic_task = None

def start_periodic_task():
    """Запускает задачу автопроверки"""
    global periodic_task
    if periodic_task and not periodic_task.done():
        logger.warning("⚠️ Задача автопроверки уже запущена")
        return
    periodic_task = asyncio.create_task(periodic_check())
    logger.info("🔄 Задача автопроверки запущена")

# ================= КОМАНДЫ =================
@dp.message(Command("help"))
async def cmd_help(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    try:
        txt = EMOJI["info"] + " <b>Справка по командам:</b>\n\n"
        txt += "<b>" + EMOJI["search"] + " Основные:</b>\n"
        txt += "/check — Проверить все лоры по активным тегам\n"
        txt += "/status — Показать текущие настройки\n"
        txt += "/help — Эта справка\n\n"
        txt += "<b>" + EMOJI["settings"] + " Настройки:</b>\n"
        txt += "/setdays <N> — Порог дней (>= N), 0 = все лоры\n"
        txt += "/addtag <тег> — Добавить тег (регистр не важен)\n"
        txt += "/rmtag <тег> — Удалить тег из списка\n"
        txt += "/tags — Показать все активные теги\n"
        txt += "/setschedule <время> — Установить расписание\n"
        txt += "/schedule — Показать расписание проверок\n\n"
        txt += "<b>" + EMOJI["stop"] + " Аварийные:</b>\n"
        txt += "/stop <пароль> — Остановить бота\n"
        txt += "<i>Пароль по умолчанию: stop123</i>\n\n"
        txt += "<i>Все команды доступны только тебе (владелец)</i>"
        await message.answer(txt, parse_mode="HTML")
    except Exception as e:
        logger.error("Ошибка в /help: " + str(e))

# ================= КОМАНДА /check (ИСПРАВЛЕНА) =================
@dp.message(Command("check"))
async def cmd_check(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    if not bot_running:
        await message.answer(EMOJI["error"] + " Бот остановлен.", parse_mode="HTML")
        return
    try:
        logger.info("=== ПРОВЕРКА ===")
        
        # Собираем лоры
        if bot_state["tags"]:
            tag_info = "по тегам: <b>" + ", ".join(bot_state["tags"]) + "</b>"
            all_loras = []
            total_pages = 0
            for tag in bot_state["tags"]:
                loras, pages = find_inactive_loonies_all_pages(BASE_URL, bot_state["min_days"], bot_state["tags"], tag_name=tag)
                all_loras.extend(loras)
                total_pages += pages
        else:
            tag_info = "<b>все лоры</b>"
            all_loras, total_pages = find_inactive_loonies_all_pages(BASE_URL, bot_state["min_days"], [])
        
        days_info = " (>= " + str(bot_state["min_days"]) + " дней)" if bot_state["min_days"] > 0 else ""
        await message.answer(EMOJI["search"] + " Сканирую " + tag_info + days_info + "...", parse_mode="HTML")
        
        if not all_loras:
            await message.answer(EMOJI["check"] + " Лоры не найдены.")
            return
        
        all_loras.sort(key=lambda x: x["days"], reverse=True)
        
        # === ИСПРАВЛЕНИЕ: выбор отправки через временный файл, а не storage ===
        if len(all_loras) > EXPORT_THRESHOLD:
            # Сохраняем во временный файл на диске (работает на Render)
            temp_file = "/tmp/loonie_pending_" + str(OWNER_ID_INT) + ".json"
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump({"loras": all_loras, "pages": total_pages}, f)
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text=EMOJI["chat"] + " В чат (" + str(len(all_loras)) + ")", callback_data="send_chat"),
                    InlineKeyboardButton(text=EMOJI["file"] + " Файлом", callback_data="send_file")
                ]
            ])
            await message.answer(
                EMOJI["stats"] + " Найдено: <b>" + str(len(all_loras)) + "</b> лор (много!)\n\n"
                "Как отправить?",
                parse_mode="HTML",
                reply_markup=keyboard
            )
            return  # ← Ждём выбора пользователя
        
        # Если лор мало — отправляем сразу
        await _send_loras_to_chat(message, all_loras, total_pages)
            
    except Exception as e:
        logger.error("Ошибка в /check: " + str(e))
        await message.answer(EMOJI["error"] + " Ошибка при проверке.")

async def _send_loras_to_chat(message, all_loras, total_pages):
    """Вспомогательная функция для отправки лор в чат"""
    await message.answer(EMOJI["stats"] + " Найдено: <b>" + str(len(all_loras)) + "</b> лор", parse_mode="HTML")
    for lora in all_loras:
        await message.answer(format_message(lora), parse_mode="HTML")
        await asyncio.sleep(0.3)
    
    avg_days = sum(l["days"] for l in all_loras) // len(all_loras)
    max_lora = max(all_loras, key=lambda x: x["days"])
    min_lora = min(all_loras, key=lambda x: x["days"])
    
    stats = "\n" + EMOJI["stats"] + " <b>Статистика:</b>\n"
    stats += "• Страниц просканировано: <b>" + str(total_pages) + "</b>\n"
    stats += "• Лор найдено: <b>" + str(len(all_loras)) + "</b>\n"
    stats += "• Средний простой: <b>" + str(avg_days) + "</b> дней\n"
    stats += "• Минимум: " + str(min_lora["days"]) + " дней\n"
    stats += "• Максимум: <b>" + str(max_lora["days"]) + "</b> дней (ID: <code>" + max_lora["id"] + "</code>)"
    await message.answer(stats, parse_mode="HTML")

# ================= CALLBACK (ИСПРАВЛЕН) =================
@dp.callback_query(F.data.in_({"send_chat", "send_file"}))
async def handle_send_choice(callback: CallbackQuery):
    if callback.from_user.id != OWNER_ID_INT:
        await callback.answer("❌ Не авторизован", show_alert=True)
        return
    if not bot_running:
        await callback.answer("❌ Бот остановлен", show_alert=True)
        return
    
    # === ИСПРАВЛЕНИЕ: читаем из файла, а не из storage ===
    temp_file = "/tmp/loonie_pending_" + str(OWNER_ID_INT) + ".json"
    if not os.path.exists(temp_file):
        await callback.answer("❌ Данные устарели, сделай /check заново", show_alert=True)
        return
    
    try:
        with open(temp_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        all_loras = data["loras"]
        total_pages = data["pages"]
        os.remove(temp_file)  # Удаляем временный файл
    except Exception as e:
        logger.error("Ошибка чтения файла: " + str(e))
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return
    
    await callback.message.edit_reply_markup(reply_markup=None)
    
    if callback.data == "send_chat":
        await callback.message.answer(EMOJI["chat"] + " Отправляю в чат...")
        await _send_loras_to_chat(callback.message, all_loras, total_pages)
    else:
        await callback.message.answer(EMOJI["file"] + " Готовлю файл...")
        content = make_export_file(all_loras, bot_state["min_days"], bot_state["tags"])
        file = BytesIO(content)
        file.name = "loonie_export_" + datetime.now().strftime("%Y%m%d_%H%M") + ".txt"
        await callback.message.answer_document(
            document=file,
            caption=EMOJI["file"] + " <b>Экспорт лор</b>\n" +
                   "Лор: " + str(len(all_loras)) + "\n" +
                   "Порог: >= " + str(bot_state["min_days"]) + " дней",
            parse_mode="HTML"
        )
        # Статистику в файл не пишем, чтобы не загромождать
    
    await callback.answer()

@dp.message(Command("stop"))
async def cmd_stop(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    try:
        parts = message.text.split()
        if len(parts) != 2:
            await message.answer(
                EMOJI["stop"] + " <b>Аварийная остановка бота</b>\n\n"
                "Используй: <code>/stop &lt;пароль&gt;</code>\n"
                "Пароль по умолчанию: <code>stop123</code>",
                parse_mode="HTML"
            )
            return
        
        password = parts[1]
        if password != STOP_PASSWORD:
            await message.answer(EMOJI["error"] + " Неверный пароль!", parse_mode="HTML")
            return
        
        global bot_running
        bot_running = False
        
        # === ОТМЕНЯЕМ ЗАДАЧУ АВТОПРОВЕРКИ ===
        cancel_periodic_task()
        
        logger.warning("🛑 БОТ ОСТАНОВЛЕН ПО ЗАПРОСУ ВЛАДЕЛЬЦА")
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=EMOJI["restart"] + " Запустить снова", callback_data="restart_bot")]
        ])
        
        await message.answer(
            EMOJI["stop"] + " <b>БОТ ОСТАНОВЛЕН!</b>\n\n"
            "• Автопроверки отключены\n"
            "• Поиск лор остановлен\n"
            "• Команды не работают (кроме /start)\n\n"
            "Нажми кнопку для запуска:",
            parse_mode="HTML",
            reply_markup=keyboard
        )
        
    except Exception as e:
        logger.error("Ошибка в /stop: " + str(e))
        await message.answer(EMOJI["error"] + " Ошибка при остановке.", parse_mode="HTML")

@dp.callback_query(F.data == "restart_bot")
async def handle_restart(callback: CallbackQuery):
    if callback.from_user.id != OWNER_ID_INT:
        await callback.answer("❌ Не авторизован", show_alert=True)
        return
    
    global bot_running
    bot_running = True
    
    # === ЗАПУСКАЕМ ЗАДАЧУ АВТОПРОВЕРКИ ===
    start_periodic_task()
    
    logger.info("🔄 БОТ ЗАПУЩЕН СНОВА")
    
    await callback.message.edit_text(
        EMOJI["check"] + " <b>Бот запущен!</b>\n\n"
        "• Автопроверки активны\n"
        "• Все команды работают",
        parse_mode="HTML"
    )
    await callback.answer()

@dp.message(Command("start"))
async def cmd_start(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    
    global bot_running
    
    # Если бот уже работает
    if bot_running:
        await message.answer(
            EMOJI["info"] + " <b>Бот уже активен!</b>\n\n"
            "Используй /help для списка команд.",
            parse_mode="HTML"
        )
        return
    
    # Запускаем бота
    bot_running = True
    start_periodic_task()
    
    logger.info("🔄 БОТ ЗАПУЩЕН ПО ЗАПРОСУ ВЛАДЕЛЬЦА")
    
    await message.answer(
        EMOJI["check"] + " <b>Бот запущен!</b>\n\n"
        "• Автопроверки активны\n"
        "• Все команды работают",
        parse_mode="HTML"
    )

@dp.message(Command("setdays"))
async def cmd_setdays(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    if not bot_running:
        await message.answer(EMOJI["error"] + " Бот остановлен.", parse_mode="HTML")
        return
    try:
        parts = message.text.split()
        if len(parts) != 2 or not parts[1].isdigit():
            await message.answer(EMOJI["warning"] + " Используй: <code>/setdays &lt;число&gt;</code>", parse_mode="HTML")
            return
        new_days = int(parts[1])
        if new_days < 0:
            await message.answer(EMOJI["warning"] + " Число должно быть >= 0", parse_mode="HTML")
            return
        
        bot_state["min_days"] = new_days
        save_config()
        
        days_text = "все лоры" if new_days == 0 else ">= " + str(new_days) + " дней"
        await message.answer(EMOJI["check"] + " Порог установлен: <b>" + days_text + "</b>.", parse_mode="HTML")
        
    except Exception as e:
        logger.error("Ошибка в /setdays: " + str(e))
        await message.answer(EMOJI["error"] + " Не удалось изменить.")

@dp.message(Command("addtag"))
async def cmd_addtag(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    if not bot_running:
        await message.answer(EMOJI["error"] + " Бот остановлен.", parse_mode="HTML")
        return
    try:
        parts = message.text.split()
        if len(parts) != 2:
            await message.answer(EMOJI["warning"] + " Используй: <code>/addtag &lt;название&gt;</code>", parse_mode="HTML")
            return
        
        new_tag = parts[1].strip().lower()
        if not new_tag.isalnum():
            await message.answer(EMOJI["warning"] + " Тег должен содержать только буквы и цифры", parse_mode="HTML")
            return
        
        if any(t.lower() == new_tag for t in bot_state["tags"]):
            await message.answer(EMOJI["warning"] + " Тег уже в списке", parse_mode="HTML")
            return
        
        bot_state["tags"].append(new_tag)
        save_config()
        
        await message.answer(EMOJI["check"] + " Тег <b>" + new_tag + "</b> добавлен.", parse_mode="HTML")
        
    except Exception as e:
        logger.error("Ошибка в /addtag: " + str(e))
        await message.answer(EMOJI["error"] + " Не удалось добавить тег.")

@dp.message(Command("rmtag"))
async def cmd_rmtag(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    if not bot_running:
        await message.answer(EMOJI["error"] + " Бот остановлен.", parse_mode="HTML")
        return
    try:
        parts = message.text.split()
        if len(parts) != 2:
            await message.answer(EMOJI["warning"] + " Используй: <code>/rmtag &lt;название&gt;</code>", parse_mode="HTML")
            return
        
        tag_to_remove = parts[1].strip().lower()
        tag_found = None
        for t in bot_state["tags"]:
            if t.lower() == tag_to_remove:
                tag_found = t
                break
        
        if not tag_found:
            await message.answer(EMOJI["warning"] + " Тег не найден", parse_mode="HTML")
            return
        
        bot_state["tags"].remove(tag_found)
        save_config()
        
        await message.answer(EMOJI["check"] + " Тег <b>" + tag_found + "</b> удалён.", parse_mode="HTML")
        
    except Exception as e:
        logger.error("Ошибка в /rmtag: " + str(e))
        await message.answer(EMOJI["error"] + " Не удалось удалить тег.")

@dp.message(Command("tags"))
async def cmd_tags(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    if not bot_running:
        await message.answer(EMOJI["error"] + " Бот остановлен.", parse_mode="HTML")
        return
    try:
        if not bot_state["tags"]:
            await message.answer(EMOJI["tag"] + " <b>Активные теги:</b>\n<i>нет</i>", parse_mode="HTML")
            return
        
        txt = EMOJI["tag"] + " <b>Активные теги:</b>\n"
        for i, tag in enumerate(bot_state["tags"], 1):
            tag_type = " (спец)" if tag in SPECIAL_TAGS else " (обычный)"
            txt += str(i) + ". <code>" + tag + "</code>" + tag_type + "\n"
        await message.answer(txt, parse_mode="HTML")
    except Exception as e:
        logger.error("Ошибка в /tags: " + str(e))

@dp.message(Command("setschedule"))
async def cmd_setschedule(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    if not bot_running:
        await message.answer(EMOJI["error"] + " Бот остановлен.", parse_mode="HTML")
        return
    try:
        parts = message.text.split()
        if len(parts) < 2:
            await message.answer(EMOJI["warning"] + " Используй: <code>/setschedule &lt;время&gt;</code>", parse_mode="HTML")
            return
        
        times = []
        for t in parts[1:]:
            if re.match(r'^\d{2}:\d{2}$', t):
                times.append(t)
            else:
                await message.answer(EMOJI["warning"] + " Неверный формат времени", parse_mode="HTML")
                return
        
        bot_state["schedule"] = times
        save_config()
        
        await message.answer(EMOJI["clock"] + " Расписание установлено.", parse_mode="HTML")
        
    except Exception as e:
        logger.error("Ошибка в /setschedule: " + str(e))
        await message.answer(EMOJI["error"] + " Не удалось установить.")

@dp.message(Command("schedule"))
async def cmd_schedule(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    if not bot_running:
        await message.answer(EMOJI["error"] + " Бот остановлен.", parse_mode="HTML")
        return
    try:
        if bot_state["schedule"]:
            txt = EMOJI["clock"] + " <b>Расписание:</b>\n"
            for t in bot_state["schedule"]:
                txt += "• <code>" + t + "</code>\n"
            await message.answer(txt, parse_mode="HTML")
        else:
            await message.answer(EMOJI["clock"] + " Расписание не установлено.", parse_mode="HTML")
    except Exception as e:
        logger.error("Ошибка в /schedule: " + str(e))

@dp.message(Command("status"))
async def cmd_status(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    try:
        status_icon = EMOJI["check"] if bot_running else EMOJI["stop"]
        status_text = "Активен" if bot_running else "ОСТАНОВЛЕН"
        
        days_text = "все лоры" if bot_state["min_days"] == 0 else ">= " + str(bot_state["min_days"]) + " дней"
        tags_text = ", ".join(bot_state["tags"]) if bot_state["tags"] else "нет (все лоры)"
        
        txt = EMOJI["settings"] + " <b>Настройки:</b>\n"
        txt += status_icon + " Статус: <b>" + status_text + "</b>\n"
        txt += EMOJI["days"] + " Порог: <b>" + days_text + "</b>\n"
        txt += EMOJI["tag"] + " Теги: <b>" + tags_text + "</b>\n"
        txt += "🔄 Автопроверка: <b>" + str(CHECK_INTERVAL_HOURS) + "</b> ч.\n"
        txt += "📄 Макс. страниц: <b>" + str(MAX_PAGES) + "</b>\n"
        if bot_state["schedule"]:
            txt += EMOJI["clock"] + " Расписание: <b>" + ", ".join(bot_state["schedule"]) + "</b>\n"
        await message.answer(txt, parse_mode="HTML")
    except Exception as e:
        logger.error("Ошибка в /status: " + str(e))

@dp.message()
async def silent_ignore(message: Message):
    pass

# ================= ФОНОВЫЕ ЗАДАЧИ =================
async def periodic_check():
    """Задача автопроверки — запускается только когда bot_running=True"""
    while bot_running:
        try:
            # Проверяем каждую минуту, не пора ли по расписанию
            await asyncio.sleep(60)
            
            if not bot_running:
                break
            
            now = datetime.now()
            current_time = now.strftime("%H:%M")
            
            # Если есть расписание — проверяем по нему
            if bot_state["schedule"]:
                if current_time not in bot_state["schedule"]:
                    continue
            
            logger.info("=== АВТОПРОВЕРКА ===")
            
            # Если теги заданы — ищем по каждому, если нет — ищем все лоры
            if bot_state["tags"]:
                all_loras = []
                total_pages = 0
                for tag in bot_state["tags"]:
                    loras, pages = find_inactive_loonies_all_pages(BASE_URL, bot_state["min_days"], bot_state["tags"], tag_name=tag)
                    all_loras.extend(loras)
                    total_pages += pages
            else:
                all_loras, total_pages = find_inactive_loonies_all_pages(BASE_URL, bot_state["min_days"], [])
            
            if all_loras:
                all_loras.sort(key=lambda x: x["days"], reverse=True)
                for lora in all_loras:
                    if not bot_running:
                        logger.info("🛑 Остановка во время отправки")
                        break
                    try:
                        await bot.send_message(chat_id=OWNER_ID_INT, text=format_message(lora), parse_mode="HTML")
                        await asyncio.sleep(0.5)
                    except Exception as e:
                        logger.error("Ошибка отправки: " + str(e))
                
                logger.info("✅ Автопроверка завершена: " + str(len(all_loras)) + " лор")
            
        except asyncio.CancelledError:
            logger.info("🛑 Задача автопроверки отменена")
            break
        except Exception as e:
            logger.error("Автопроверка упала: " + str(e))
            if not bot_running:
                break
            await asyncio.sleep(60)

async def on_startup():
    logger.info("🚀 Bot started. Owner: " + str(OWNER_ID_INT))
    logger.info("📊 Статус: " + ("Активен" if bot_running else "Остановлен"))
    # Запускаем автопроверку только если бот активен
    if bot_running:
        start_periodic_task()

async def on_shutdown():
    logger.info("👋 Bot shutting down...")
    cancel_periodic_task()
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

async def stop_handler(request):
    """Вебхук для остановки/запуска"""
    global bot_running
    
    action = request.query.get("action", "stop")
    password = request.query.get("password", "")
    
    if password != STOP_PASSWORD:
        return web.Response(text="❌ Неверный пароль", status=403)
    
    if action == "stop":
        bot_running = False
        cancel_periodic_task()
        logger.warning("🛑 БОТ ОСТАНОВЛЕН ЧЕРЕЗ ВЕБХУК")
        return web.Response(text="✅ Бот остановлен")
    
    elif action == "start":
        bot_running = True
        start_periodic_task()
        logger.info("🔄 БОТ ЗАПУЩЕН ЧЕРЕЗ ВЕБХУК")
        return web.Response(text="✅ Бот запущен")
    
    return web.Response(text="❌ Неизвестное действие", status=400)

async def health_handler(request):
    status = "running" if bot_running else "stopped"
    task_status = "active" if periodic_task and not periodic_task.done() else "inactive"
    return web.Response(text="OK - Status: " + status + " | Task: " + task_status)

async def run_web_server():
    app = web.Application()
    
    webhook_path = "/webhook/" + BOT_TOKEN.split(":")[0]
    app.router.add_post(webhook_path, webhook_handler)
    
    app.router.add_get("/stop", stop_handler)
    
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)
    
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", "8080"))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info("🌐 Server on port " + str(port))
    logger.info("🛑 Stop URL: https://ТВОЙ_ДОМЕН.onrender.com/stop?password=" + STOP_PASSWORD)
    
    webhook_url = os.getenv("RENDER_EXTERNAL_URL", "")
    if webhook_url and bot_running:
        webhook_full = webhook_url + webhook_path
        await bot.set_webhook(webhook_full)
        logger.info("✅ Webhook set: " + webhook_full)

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