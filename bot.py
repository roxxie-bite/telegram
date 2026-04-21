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
MIN_DAYS_ENV = os.getenv("MIN_DAYS")
SITE_BASE = "https://lynther.sytes.net"
BASE_URL = SITE_BASE + "/?p=lora"
DEFAULT_MIN_DAYS = int(MIN_DAYS_ENV) if MIN_DAYS_ENV and MIN_DAYS_ENV.isdigit() else 0
DEFAULT_TAGS = []
CHECK_INTERVAL_HOURS = 6
MAX_PAGES = 50
CONFIG_FILE = "config.json"
EXPORT_THRESHOLD = 50  # Если лор больше — предлагать файл

# === СПЕЦИАЛЬНЫЕ ТЕГИ (регистр не важен) ===
SPECIAL_TAGS = {
    "xl": "tag_red",
    "style": "tag_purple",
    "character": "tag_green",
    "quality": "tag_gold",
}

# === ОБЫЧНЫЕ ЭМОДЗИ ===
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

# ================= ПРОВЕРКА ТЕГА (БЕЗ УЧЁТА РЕГИСТРА) =================
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

# ================= ПАРСЕР ПО lora_head =================
def parse_loras_from_html(html, min_days, active_tags):
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
                
                if active_tags:
                    has_any_tag = False
                    for tag in active_tags:
                        if has_tag_in_head(head, tag):
                            has_any_tag = True
                            break
                    if not has_any_tag:
                        continue
                
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
def find_inactive_loonies_all_pages(base_url, min_days, active_tags):
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
        
        loras = parse_loras_from_html(html, min_days, active_tags)
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
        txt += "<b>" + EMOJI["tag"] + " Специальные теги:</b>\n"
        txt += "xl (красный), style (фиолетовый),\n"
        txt += "character (зелёный), quality (золотой)\n"
        txt += "<i>Остальные теги ищутся по названию</i>\n\n"
        txt += "<i>Все команды доступны только тебе (владелец)</i>"
        await message.answer(txt, parse_mode="HTML")
    except Exception as e:
        logger.error("Ошибка в /help: " + str(e))

@dp.message(Command("check"))
async def cmd_check(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    try:
        logger.info("=== ПРОВЕРКА === Порог: >= " + str(bot_state["min_days"]) + " | Теги: " + (", ".join(bot_state["tags"]) if bot_state["tags"] else "ВСЕ"))
        
        tag_info = "по тегам: <b>" + ", ".join(bot_state["tags"]) + "</b>" if bot_state["tags"] else "<b>все лоры</b>"
        days_info = " (>= " + str(bot_state["min_days"]) + " дней)" if bot_state["min_days"] > 0 else ""
        await message.answer(EMOJI["search"] + " Сканирую " + tag_info + days_info + "...", parse_mode="HTML")
        
        all_loras, total_pages = find_inactive_loonies_all_pages(BASE_URL, bot_state["min_days"], bot_state["tags"])
        
        if not all_loras:
            await message.answer(EMOJI["check"] + " Лоры не найдены.")
            return
        
        all_loras.sort(key=lambda x: x["days"], reverse=True)
        
        # Если лор много — спрашиваем, как отправить
        if len(all_loras) > EXPORT_THRESHOLD:
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
            # Сохраняем лоры во временное хранилище для обработки в callback
            dp.storage.set_data(chat_id=message.chat.id, user_id=message.from_user.id, key="pending_loras", value=all_loras)
            dp.storage.set_data(chat_id=message.chat.id, user_id=message.from_user.id, key="pending_pages", value=total_pages)
            return
        
        # Если лор мало — отправляем сразу в чат
        await message.answer(EMOJI["stats"] + " Найдено: <b>" + str(len(all_loras)) + "</b> лор", parse_mode="HTML")
        for lora in all_loras:
            await message.answer(format_message(lora), parse_mode="HTML")
            await asyncio.sleep(0.3)
        
        # Статистика
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
            
    except Exception as e:
        logger.error("Ошибка в /check: " + str(e))
        await message.answer(EMOJI["error"] + " Ошибка при проверке.")

@dp.callback_query(F.data.in_({"send_chat", "send_file"}))
async def handle_send_choice(callback: CallbackQuery):
    if callback.from_user.id != OWNER_ID_INT:
        await callback.answer("❌ Не авторизован", show_alert=True)
        return
    
    # Получаем сохранённые лоры
    all_loras = dp.storage.get_data(chat_id=callback.message.chat.id, user_id=callback.from_user.id, key="pending_loras")
    total_pages = dp.storage.get_data(chat_id=callback.message.chat.id, user_id=callback.from_user.id, key="pending_pages")
    
    if not all_loras:
        await callback.answer("❌ Данные устарели, сделай /check заново", show_alert=True)
        return
    
    # Очищаем хранилище
    dp.storage.set_data(chat_id=callback.message.chat.id, user_id=callback.from_user.id, key="pending_loras", value=None)
    dp.storage.set_data(chat_id=callback.message.chat.id, user_id=callback.from_user.id, key="pending_pages", value=None)
    
    await callback.message.edit_reply_markup(reply_markup=None)
    
    if callback.data == "send_chat":
        # Отправляем в чат
        await callback.message.answer(EMOJI["chat"] + " Отправляю в чат...")
        for lora in all_loras:
            await callback.message.answer(format_message(lora), parse_mode="HTML")
            await asyncio.sleep(0.3)
    else:
        # Отправляем файлом
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
    
    # Статистика (в любом случае)
    avg_days = sum(l["days"] for l in all_loras) // len(all_loras)
    max_lora = max(all_loras, key=lambda x: x["days"])
    min_lora = min(all_loras, key=lambda x: x["days"])
    
    stats = "\n" + EMOJI["stats"] + " <b>Статистика:</b>\n"
    stats += "• Страниц просканировано: <b>" + str(total_pages) + "</b>\n"
    stats += "• Лор найдено: <b>" + str(len(all_loras)) + "</b>\n"
    stats += "• Средний простой: <b>" + str(avg_days) + "</b> дней\n"
    stats += "• Минимум: " + str(min_lora["days"]) + " дней\n"
    stats += "• Максимум: <b>" + str(max_lora["days"]) + "</b> дней (ID: <code>" + max_lora["id"] + "</code>)"
    await callback.message.answer(stats, parse_mode="HTML")
    
    await callback.answer()

@dp.message(Command("setdays"))
async def cmd_setdays(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    try:
        parts = message.text.split()
        if len(parts) != 2 or not parts[1].isdigit():
            await message.answer(EMOJI["warning"] + " Используй: <code>/setdays &lt;число&gt;</code>\nПример: <code>/setdays 10</code> или <code>/setdays 0</code> (все лоры)", parse_mode="HTML")
            return
        new_days = int(parts[1])
        if new_days < 0:
            await message.answer(EMOJI["warning"] + " Число должно быть >= 0", parse_mode="HTML")
            return
        
        bot_state["min_days"] = new_days
        save_config()
        logger.info("=== ПОРОГ ИЗМЕНЁН === Новый: >= " + str(new_days))
        
        days_text = "все лоры" if new_days == 0 else ">= " + str(new_days) + " дней"
        await message.answer(EMOJI["check"] + " Порог установлен: <b>" + days_text + "</b>. Используй /check для поиска.", parse_mode="HTML")
        
    except Exception as e:
        logger.error("Ошибка в /setdays: " + str(e))
        await message.answer(EMOJI["error"] + " Не удалось изменить.")

@dp.message(Command("addtag"))
async def cmd_addtag(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    try:
        parts = message.text.split()
        if len(parts) != 2:
            await message.answer(EMOJI["warning"] + " Используй: <code>/addtag &lt;название&gt;</code>\nПример: <code>/addtag anime</code>", parse_mode="HTML")
            return
        
        new_tag = parts[1].strip().lower()
        if not new_tag.isalnum():
            await message.answer(EMOJI["warning"] + " Тег должен содержать только буквы и цифры", parse_mode="HTML")
            return
        
        if any(t.lower() == new_tag for t in bot_state["tags"]):
            await message.answer(EMOJI["warning"] + " Тег <b>" + new_tag + "</b> уже в списке", parse_mode="HTML")
            return
        
        bot_state["tags"].append(new_tag)
        save_config()
        
        tag_type = " (спец: " + SPECIAL_TAGS.get(new_tag, "") + ")" if new_tag in SPECIAL_TAGS else " (обычный)"
        logger.info("=== ТЕГ ДОБАВЛЕН === " + new_tag + tag_type)
        
        tags_list = ", ".join(bot_state["tags"]) if bot_state["tags"] else "<i>нет (будут все лоры)</i>"
        await message.answer(EMOJI["check"] + " Тег <b>" + new_tag + "</b> добавлен" + tag_type + ".\nТекущие теги: " + tags_list, parse_mode="HTML")
        
    except Exception as e:
        logger.error("Ошибка в /addtag: " + str(e))
        await message.answer(EMOJI["error"] + " Не удалось добавить тег.")

@dp.message(Command("rmtag"))
async def cmd_rmtag(message: Message):
    if message.from_user.id != OWNER_ID_INT:
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
            await message.answer(EMOJI["warning"] + " Тег <b>" + tag_to_remove + "</b> не найден", parse_mode="HTML")
            return
        
        bot_state["tags"].remove(tag_found)
        save_config()
        logger.info("=== ТЕГ УДАЛЁН === " + tag_found)
        
        tags_list = ", ".join(bot_state["tags"]) if bot_state["tags"] else "<i>нет (будут все лоры)</i>"
        await message.answer(EMOJI["check"] + " Тег <b>" + tag_found + "</b> удалён.\nТекущие теги: " + tags_list, parse_mode="HTML")
        
    except Exception as e:
        logger.error("Ошибка в /rmtag: " + str(e))
        await message.answer(EMOJI["error"] + " Не удалось удалить тег.")

@dp.message(Command("tags"))
async def cmd_tags(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    try:
        if not bot_state["tags"]:
            await message.answer(EMOJI["tag"] + " <b>Активные теги:</b>\n<i>нет</i>\n\n<i>Будут показаны ВСЕ лоры</i>", parse_mode="HTML")
            return
        
        txt = EMOJI["tag"] + " <b>Активные теги:</b>\n"
        for i, tag in enumerate(bot_state["tags"], 1):
            tag_type = " (спец)" if tag in SPECIAL_TAGS else " (обычный)"
            txt += str(i) + ". <code>" + tag + "</code>" + tag_type + "\n"
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
                EMOJI["warning"] + " Используй: <code>/setschedule &lt;время&gt; [&lt;время&gt;...]</code>\n"
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
                await message.answer(EMOJI["warning"] + " Неверный формат времени: <code>" + t + "</code>\nИспользуй ЧЧ:ММ (например, 09:00)", parse_mode="HTML")
                return
        
        bot_state["schedule"] = times
        save_config()
        logger.info("=== РАСПИСАНИЕ === Установлено: " + ", ".join(times))
        
        if times:
            await message.answer(EMOJI["clock"] + " Расписание установлено: <b>" + ", ".join(times) + "</b>\nАвтопроверки будут в это время.", parse_mode="HTML")
        else:
            await message.answer(EMOJI["clock"] + " Расписание очищено. Автопроверки отключены.", parse_mode="HTML")
        
    except Exception as e:
        logger.error("Ошибка в /setschedule: " + str(e))
        await message.answer(EMOJI["error"] + " Не удалось установить расписание.")

@dp.message(Command("schedule"))
async def cmd_schedule(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    try:
        if bot_state["schedule"]:
            txt = EMOJI["clock"] + " <b>Расписание автопроверок:</b>\n"
            for t in bot_state["schedule"]:
                txt += "• <code>" + t + "</code>\n"
            txt += "\n<i>Следующая проверка в ближайшее время из списка</i>"
        else:
            txt = EMOJI["clock"] + " <b>Расписание:</b> не установлено\n"
            txt += "<i>Используй /setschedule 09:00 15:00 21:00</i>"
        await message.answer(txt, parse_mode="HTML")
    except Exception as e:
        logger.error("Ошибка в /schedule: " + str(e))

@dp.message(Command("status"))
async def cmd_status(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    try:
        days_text = "все лоры" if bot_state["min_days"] == 0 else ">= " + str(bot_state["min_days"]) + " дней"
        tags_text = ", ".join(bot_state["tags"]) if bot_state["tags"] else "нет (все лоры)"
        
        txt = EMOJI["settings"] + " <b>Настройки:</b>\n"
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
    await asyncio.sleep(60)
    while True:
        try:
            now = datetime.now()
            current_time = now.strftime("%H:%M")
            
            if bot_state["schedule"]:
                if current_time not in bot_state["schedule"]:
                    await asyncio.sleep(60)
                    continue
            
            logger.info("=== АВТОПРОВЕРКА === Порог: >= " + str(bot_state["min_days"]) + " | Теги: " + (", ".join(bot_state["tags"]) if bot_state["tags"] else "ВСЕ"))
            
            all_loras, total_pages = find_inactive_loonies_all_pages(BASE_URL, bot_state["min_days"], bot_state["tags"])
            
            if all_loras:
                all_loras.sort(key=lambda x: x["days"], reverse=True)
                
                # Автопроверка всегда отправляет в чат (без выбора)
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
    tags_info = ", ".join(bot_state["tags"]) if bot_state["tags"] else "ВСЕ"
    logger.info("🚀 Bot started (WEBHOOK). Owner: " + str(OWNER_ID_INT))
    logger.info("📊 Порог: >= " + str(bot_state["min_days"]) + " дней | Теги: " + tags_info)
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