--- bot.py (原始)
import os
import re
import asyncio
import logging
import sqlite3
import requests
import time
import json
from datetime import datetime
from bs4 import BeautifulSoup
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message
from aiohttp import web
from aiohttp.web_response import json_response

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
DB_FILE = "bot_data.db"
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

# === SQLITE БАЗА ДАННЫХ ===
def init_db():
    """Инициализация базы данных SQLite"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Таблица настроек
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    ''')
    
    # Таблица тегов
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tag TEXT UNIQUE NOT NULL
        )
    ''')
    
    # Таблица расписания
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS schedule (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            time TEXT NOT NULL
        )
    ''')
    
    # Таблица истории проверок
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS check_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            loras_found INTEGER DEFAULT 0,
            pages_scanned INTEGER DEFAULT 0
        )
    ''')
    
    # Таблица чёрного списка лор
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS blacklist (
            lora_id TEXT PRIMARY KEY,
            added_at TEXT NOT NULL,
            reason TEXT
        )
    ''')
    
    conn.commit()
    
    # Инициализация значений по умолчанию
    cursor.execute("SELECT COUNT(*) FROM settings")
    if cursor.fetchone()[0] == 0:
        cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", 
                      ("min_days", str(DEFAULT_MIN_DAYS)))
    
    cursor.execute("SELECT COUNT(*) FROM tags")
    if cursor.fetchone()[0] == 0:
        for tag in DEFAULT_TAGS:
            cursor.execute("INSERT OR IGNORE INTO tags (tag) VALUES (?)", (tag,))
    
    conn.commit()
    conn.close()
    logger.info("✅ База данных инициализирована: " + DB_FILE)

def get_db_connection():
    """Получение соединения с базой данных"""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def get_setting(key, default=None):
    """Получение настройки из БД"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM settings WHERE key = ?", (key,))
    row = cursor.fetchone()
    conn.close()
    return row["value"] if row else default

def set_setting(key, value):
    """Сохранение настройки в БД"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, str(value)))
    conn.commit()
    conn.close()

def get_all_tags():
    """Получение всех тегов"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT tag FROM tags ORDER BY id")
    tags = [row["tag"] for row in cursor.fetchall()]
    conn.close()
    return tags if tags else DEFAULT_TAGS.copy()

def add_tag(tag):
    """Добавление тега"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO tags (tag) VALUES (?)", (tag,))
        conn.commit()
        success = True
    except sqlite3.IntegrityError:
        success = False
    conn.close()
    return success

def remove_tag(tag):
    """Удаление тега"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM tags")
    count = cursor.fetchone()[0]
    if count <= 1:
        conn.close()
        return False
    cursor.execute("DELETE FROM tags WHERE tag = ?", (tag,))
    conn.commit()
    conn.close()
    return True

def get_schedule():
    """Получение расписания"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT time FROM schedule ORDER BY id")
    times = [row["time"] for row in cursor.fetchall()]
    conn.close()
    return times

def set_schedule(times):
    """Установка расписания"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM schedule")
    for t in times:
        cursor.execute("INSERT INTO schedule (time) VALUES (?)", (t,))
    conn.commit()
    conn.close()

def add_check_history(loras_count, pages_count):
    """Добавление записи в историю проверок"""
    conn = get_db_connection()
    cursor = conn.cursor()
    timestamp = datetime.now().isoformat()
    cursor.execute("INSERT INTO check_history (timestamp, loras_found, pages_scanned) VALUES (?, ?, ?)",
                  (timestamp, loras_count, pages_count))
    conn.commit()
    conn.close()

def is_blacklisted(lora_id):
    """Проверка наличия лоры в чёрном списке"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM blacklist WHERE lora_id = ?", (lora_id,))
    result = cursor.fetchone() is not None
    conn.close()
    return result

def add_to_blacklist(lora_id, reason=""):
    """Добавление лоры в чёрный список"""
    conn = get_db_connection()
    cursor = conn.cursor()
    timestamp = datetime.now().isoformat()
    cursor.execute("INSERT OR REPLACE INTO blacklist (lora_id, added_at, reason) VALUES (?, ?, ?)",
                  (lora_id, timestamp, reason))
    conn.commit()
    conn.close()

def remove_from_blacklist(lora_id):
    """Удаление лоры из чёрного списка"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM blacklist WHERE lora_id = ?", (lora_id,))
    deleted = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return deleted

def get_blacklist():
    """Получение чёрного списка"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT lora_id, added_at, reason FROM blacklist ORDER BY added_at DESC")
    items = cursor.fetchall()
    conn.close()
    return items

# Инициализация БД при старте
init_db()

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

                id_match = re.search(r"#️⃣\s*(\d+)", text)
                if not id_match:
                    continue
                lora_id = id_match.group(1)

                days_match = re.search(r"🕸️\s*(\d+)\s*d", text, re.IGNORECASE)
                if not days_match:
                    continue
                lora_days = int(days_match.group(1))

                name_match = re.match(r'^\d+\.\s*(.+?)\s*\|\|', text.strip())
                if name_match:
                    lora_name = name_match.group(1).strip()
                else:
                    lora_name = "Loonie"

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
        txt += "<b>🗑️ Чёрный список:</b>\n"
        txt += "/blacklist — Показать чёрный список\n"
        txt += "/addblack <ID> [причина] — Добавить в ЧС\n"
        txt += "/rmblack <ID> — Удалить из ЧС\n\n"
        txt += "<b>📊 История:</b>\n"
        txt += "/history — Показать историю проверок\n\n"
        txt += "<i>Все команды доступны только тебе (владелец)</i>"
        await message.answer(txt, parse_mode="HTML")
    except Exception as e:
        logger.error("Ошибка в /help: " + str(e))

@dp.message(Command("check"))
async def cmd_check(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    try:
<<<<<<< HEAD
        logger.info("=== ПРОВЕРКА === Порог: >= " + str(bot_state["min_days"]) + " | Теги: " + ", ".join(bot_state["tags"]))
        await message.answer("🔍 Сканирую теги: <b>" + ", ".join(bot_state["tags"]) + "</b> (порог: >= " + str(bot_state["min_days"]) + " дней)...", parse_mode="HTML")

        all_loras = []
        total_pages = 0

        for tag in bot_state["tags"]:
=======
        min_days = int(get_setting("min_days", DEFAULT_MIN_DAYS))
        tags = get_all_tags()
        
        logger.info("=== ПРОВЕРКА === Порог: >= " + str(min_days) + " | Теги: " + ", ".join(tags))
        await message.answer("🔍 Сканирую теги: <b>" + ", ".join(tags) + "</b> (порог: >= " + str(min_days) + " дней)...", parse_mode="HTML")
        
        all_loras = []
        total_pages = 0
        
        for tag in tags:
>>>>>>> d8296f47bac30f1cb00ce48f5340dd9309a72d7b
            base_url = SITE_BASE + "/?p=lora&t=" + tag
            loras, pages = find_inactive_loonies_all_pages(base_url, min_days)
            # Фильтрация чёрного списка
            filtered_loras = [l for l in loras if not is_blacklisted(l["id"])]
            all_loras.extend(filtered_loras)
            total_pages += pages
<<<<<<< HEAD

=======
        
        # Сохраняем в историю
        add_check_history(len(all_loras), total_pages)
        
>>>>>>> d8296f47bac30f1cb00ce48f5340dd9309a72d7b
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
<<<<<<< HEAD

        bot_state["min_days"] = new_days
        save_config()
=======
        
        set_setting("min_days", new_days)
>>>>>>> d8296f47bac30f1cb00ce48f5340dd9309a72d7b
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
            await message.answer("⚠️ Используй: <code>/addtag &lt;название&gt;</code>\nПример: <code>/addtag anime</code>", parse_mode="HTML")
            return

        new_tag = parts[1].strip().lower()
        if not new_tag.isalnum():
            await message.answer("⚠️ Тег должен содержать только буквы и цифры", parse_mode="HTML")
            return
<<<<<<< HEAD

        if new_tag in bot_state["tags"]:
            await message.answer("⚠️ Тег <b>" + new_tag + "</b> уже в списке", parse_mode="HTML")
            return

        bot_state["tags"].append(new_tag)
        save_config()
        logger.info("=== ТЕГ ДОБАВЛЕН === " + new_tag)

        await message.answer("✅ Тег <b>" + new_tag + "</b> добавлен. Текущие теги: " + ", ".join(bot_state["tags"]), parse_mode="HTML")

=======
        
        if add_tag(new_tag):
            logger.info("=== ТЕГ ДОБАВЛЕН === " + new_tag)
            tags = get_all_tags()
            await message.answer("✅ Тег <b>" + new_tag + "</b> добавлен. Текущие теги: " + ", ".join(tags), parse_mode="HTML")
        else:
            await message.answer("⚠️ Тег <b>" + new_tag + "</b> уже в списке", parse_mode="HTML")
        
>>>>>>> d8296f47bac30f1cb00ce48f5340dd9309a72d7b
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
<<<<<<< HEAD

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

=======
        
        if remove_tag(tag_to_remove):
            logger.info("=== ТЕГ УДАЛЁН === " + tag_to_remove)
            tags = get_all_tags()
            await message.answer("✅ Тег <b>" + tag_to_remove + "</b> удалён. Текущие теги: " + ", ".join(tags), parse_mode="HTML")
        else:
            await message.answer("⚠️ Тег <b>" + tag_to_remove + "</b> не найден или это последний тег", parse_mode="HTML")
        
>>>>>>> d8296f47bac30f1cb00ce48f5340dd9309a72d7b
    except Exception as e:
        logger.error("Ошибка в /rmtag: " + str(e))
        await message.answer("❌ Не удалось удалить тег.")

@dp.message(Command("tags"))
async def cmd_tags(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    try:
        tags = get_all_tags()
        txt = "🏷️ <b>Активные теги:</b>\n"
        for i, tag in enumerate(tags, 1):
            txt += str(i) + ". <code>" + tag + "</code>\n"
        txt += "\n<i>Всего: " + str(len(tags)) + "</i>"
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
<<<<<<< HEAD

        bot_state["schedule"] = times
        save_config()
=======
        
        set_schedule(times)
>>>>>>> d8296f47bac30f1cb00ce48f5340dd9309a72d7b
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
        schedule = get_schedule()
        if schedule:
            txt = "⏰ <b>Расписание автопроверок:</b>\n"
            for t in schedule:
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
        min_days = int(get_setting("min_days", DEFAULT_MIN_DAYS))
        tags = get_all_tags()
        schedule = get_schedule()
        
        txt = "⚙️ <b>Настройки:</b>\n"
        txt += "🕸️ Порог: <b>" + str(min_days) + "</b> дней (>=)\n"
        txt += "🏷️ Теги: <b>" + ", ".join(tags) + "</b>\n"
        txt += "🔄 Автопроверка: <b>" + str(CHECK_INTERVAL_HOURS) + "</b> ч.\n"
        txt += "📄 Макс. страниц: <b>" + str(MAX_PAGES) + "</b>\n"
        if schedule:
            txt += "⏰ Расписание: <b>" + ", ".join(schedule) + "</b>\n"
        await message.answer(txt, parse_mode="HTML")
    except Exception as e:
        logger.error("Ошибка в /status: " + str(e))

# ================= ЧЁРНЫЙ СПИСОК =================
@dp.message(Command("blacklist"))
async def cmd_blacklist(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    try:
        blacklist = get_blacklist()
        if not blacklist:
            await message.answer("🗑️ <b>Чёрный список пуст</b>", parse_mode="HTML")
            return
        
        txt = "🗑️ <b>Чёрный список:</b>\n"
        for item in blacklist[:20]:  # Ограничим вывод
            reason = item["reason"] if item["reason"] else "без причины"
            txt += "• ID: <code>" + item["lora_id"] + "</code> (" + reason + ")\n"
        
        if len(blacklist) > 20:
            txt += "\n... и ещё " + str(len(blacklist) - 20) + " записей"
        
        await message.answer(txt, parse_mode="HTML")
    except Exception as e:
        logger.error("Ошибка в /blacklist: " + str(e))

@dp.message(Command("addblack"))
async def cmd_addblack(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    try:
        parts = message.text.split(maxsplit=2)
        if len(parts) < 2:
            await message.answer("⚠️ Используй: <code>/addblack &lt;ID&gt; [причина]</code>", parse_mode="HTML")
            return
        
        lora_id = parts[1]
        reason = parts[2] if len(parts) > 2 else ""
        
        add_to_blacklist(lora_id, reason)
        logger.info("=== ЧЁРНЫЙ СПИСОК === Добавлен ID: " + lora_id)
        
        msg = "✅ ID <code>" + lora_id + "</code> добавлен в чёрный список"
        if reason:
            msg += "\nПричина: " + reason
        await message.answer(msg, parse_mode="HTML")
    except Exception as e:
        logger.error("Ошибка в /addblack: " + str(e))
        await message.answer("❌ Не удалось добавить в чёрный список.")

@dp.message(Command("rmblack"))
async def cmd_rmblack(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    try:
        parts = message.text.split()
        if len(parts) != 2:
            await message.answer("⚠️ Используй: <code>/rmblack &lt;ID&gt;</code>", parse_mode="HTML")
            return
        
        lora_id = parts[1]
        
        if remove_from_blacklist(lora_id):
            logger.info("=== ЧЁРНЫЙ СПИСОК === Удалён ID: " + lora_id)
            await message.answer("✅ ID <code>" + lora_id + "</code> удалён из чёрного списка", parse_mode="HTML")
        else:
            await message.answer("⚠️ ID <code>" + lora_id + "</code> не найден в чёрном списке", parse_mode="HTML")
    except Exception as e:
        logger.error("Ошибка в /rmblack: " + str(e))
        await message.answer("❌ Не удалось удалить из чёрного списка.")

# ================= ИСТОРИЯ =================
@dp.message(Command("history"))
async def cmd_history(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT timestamp, loras_found, pages_scanned FROM check_history ORDER BY id DESC LIMIT 10")
        records = cursor.fetchall()
        conn.close()
        
        if not records:
            await message.answer("📊 <b>История пуста</b>", parse_mode="HTML")
            return
        
        txt = "📊 <b>История проверок (последние 10):</b>\n"
        for rec in records:
            ts = rec["timestamp"][:16].replace("T", " ")
            txt += "• " + ts + ": <b>" + str(rec["loras_found"]) + "</b> лор, " + str(rec["pages_scanned"]) + " стр.\n"
        
        await message.answer(txt, parse_mode="HTML")
    except Exception as e:
        logger.error("Ошибка в /history: " + str(e))
        await message.answer("❌ Не удалось получить историю.")

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
<<<<<<< HEAD

            if bot_state["schedule"]:
                if current_time not in bot_state["schedule"]:
                    await asyncio.sleep(60)
                    continue

            logger.info("=== АВТОПРОВЕРКА === Порог: >= " + str(bot_state["min_days"]) + " | Теги: " + ", ".join(bot_state["tags"]))

            all_loras = []
            total_pages = 0

            for tag in bot_state["tags"]:
=======
            
            schedule = get_schedule()
            min_days = int(get_setting("min_days", DEFAULT_MIN_DAYS))
            tags = get_all_tags()
            
            if schedule:
                if current_time not in schedule:
                    await asyncio.sleep(60)
                    continue
            
            logger.info("=== АВТОПРОВЕРКА === Порог: >= " + str(min_days) + " | Теги: " + ", ".join(tags))
            
            all_loras = []
            total_pages = 0
            
            for tag in tags:
>>>>>>> d8296f47bac30f1cb00ce48f5340dd9309a72d7b
                base_url = SITE_BASE + "/?p=lora&t=" + tag
                loras, pages = find_inactive_loonies_all_pages(base_url, min_days)
                # Фильтрация чёрного списка
                filtered_loras = [l for l in loras if not is_blacklisted(l["id"])]
                all_loras.extend(filtered_loras)
                total_pages += pages
<<<<<<< HEAD

=======
            
            # Сохраняем в историю
            add_check_history(len(all_loras), total_pages)
            
>>>>>>> d8296f47bac30f1cb00ce48f5340dd9309a72d7b
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
    min_days = int(get_setting("min_days", DEFAULT_MIN_DAYS))
    tags = get_all_tags()
    logger.info("📊 Порог: >= " + str(min_days) + " дней | Теги: " + ", ".join(tags))
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

# ================= WEB PANEL API =================
WEB_ADMIN_KEY = os.getenv("WEB_ADMIN_KEY", "admin_secret_key_change_me")

async def api_auth(request):
    """Проверка авторизации для веб-панели"""
    try:
        data = await request.json()
        key = data.get("key", "")
        if key == WEB_ADMIN_KEY:
            return json_response({"success": True, "message": "Authorized"})
        return json_response({"success": False, "message": "Invalid key"}, status=401)
    except Exception as e:
        logger.error("Ошибка авторизации: " + str(e))
        return json_response({"success": False, "message": "Error"}, status=500)

async def api_get_status(request):
    """Получение текущего статуса бота"""
    try:
        auth_header = request.headers.get("Authorization", "")
        if auth_header != "Bearer " + WEB_ADMIN_KEY:
            return json_response({"error": "Unauthorized"}, status=401)
        
        min_days = int(get_setting("min_days", DEFAULT_MIN_DAYS))
        tags = get_all_tags()
        schedule = get_schedule()
        blacklist_count = len(get_blacklist())
        history = []
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT timestamp, loras_found, pages_scanned FROM check_history ORDER BY id DESC LIMIT 10")
        for row in cursor.fetchall():
            history.append({"timestamp": row["timestamp"], "loras_found": row["loras_found"], "pages_scanned": row["pages_scanned"]})
        conn.close()
        
        return json_response({
            "success": True,
            "data": {
                "min_days": min_days,
                "tags": tags,
                "schedule": schedule,
                "blacklist_count": blacklist_count,
                "history": history
            }
        })
    except Exception as e:
        logger.error("Ошибка получения статуса: " + str(e))
        return json_response({"error": str(e)}, status=500)

async def api_set_days(request):
    """Установка порога дней"""
    try:
        auth_header = request.headers.get("Authorization", "")
        if auth_header != "Bearer " + WEB_ADMIN_KEY:
            return json_response({"error": "Unauthorized"}, status=401)
        
        data = await request.json()
        new_days = data.get("days")
        if not isinstance(new_days, int) or new_days < 1:
            return json_response({"error": "Invalid days value"}, status=400)
        
        set_setting("min_days", new_days)
        logger.info("=== ПОРОГ ИЗМЕНЁН ЧЕРЕЗ WEB === Новый: >= " + str(new_days))
        return json_response({"success": True, "message": "Days updated to " + str(new_days)})
    except Exception as e:
        logger.error("Ошибка установки дней: " + str(e))
        return json_response({"error": str(e)}, status=500)

async def api_add_tag(request):
    """Добавление тега"""
    try:
        auth_header = request.headers.get("Authorization", "")
        if auth_header != "Bearer " + WEB_ADMIN_KEY:
            return json_response({"error": "Unauthorized"}, status=401)
        
        data = await request.json()
        tag = data.get("tag", "").strip()
        if not tag:
            return json_response({"error": "Empty tag"}, status=400)
        
        if add_tag(tag):
            logger.info("=== ТЕГ ДОБАВЛЕН ЧЕРЕЗ WEB === " + tag)
            return json_response({"success": True, "message": "Tag added"})
        return json_response({"error": "Tag already exists"}, status=400)
    except Exception as e:
        logger.error("Ошибка добавления тега: " + str(e))
        return json_response({"error": str(e)}, status=500)

async def api_remove_tag(request):
    """Удаление тега"""
    try:
        auth_header = request.headers.get("Authorization", "")
        if auth_header != "Bearer " + WEB_ADMIN_KEY:
            return json_response({"error": "Unauthorized"}, status=401)
        
        data = await request.json()
        tag = data.get("tag", "").strip()
        if not tag:
            return json_response({"error": "Empty tag"}, status=400)
        
        if remove_tag(tag):
            logger.info("=== ТЕГ УДАЛЁН ЧЕРEZ WEB === " + tag)
            return json_response({"success": True, "message": "Tag removed"})
        return json_response({"error": "Cannot remove last tag or tag not found"}, status=400)
    except Exception as e:
        logger.error("Ошибка удаления тега: " + str(e))
        return json_response({"error": str(e)}, status=500)

async def api_run_check(request):
    """Запуск проверки лор"""
    try:
        auth_header = request.headers.get("Authorization", "")
        if auth_header != "Bearer " + WEB_ADMIN_KEY:
            return json_response({"error": "Unauthorized"}, status=401)
        
        # Запускаем проверку в фоне
        asyncio.create_task(run_web_check())
        return json_response({"success": True, "message": "Check started"})
    except Exception as e:
        logger.error("Ошибка запуска проверки: " + str(e))
        return json_response({"error": str(e)}, status=500)

async def run_web_check():
    """Фоновая проверка для веб-панели"""
    try:
        min_days = int(get_setting("min_days", DEFAULT_MIN_DAYS))
        tags = get_all_tags()
        logger.info("=== ВЕБ-ПРОВЕРКА === Порог: >= " + str(min_days) + " | Теги: " + ", ".join(tags))
        
        all_loras = []
        total_pages = 0
        
        for tag in tags:
            base_url = SITE_BASE + "/?p=lora&t=" + tag
            loras, pages = find_inactive_loonies_all_pages(base_url, min_days)
            filtered_loras = [l for l in loras if not is_blacklisted(l["id"])]
            all_loras.extend(filtered_loras)
            total_pages += pages
        
        add_check_history(len(all_loras), total_pages)
        
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
        
        logger.info("✅ Веб-проверка завершена: " + str(len(all_loras)) + " лор")
    except Exception as e:
        logger.error("Веб-проверка упала: " + str(e))

async def api_get_blacklist(request):
    """Получение чёрного списка"""
    try:
        auth_header = request.headers.get("Authorization", "")
        if auth_header != "Bearer " + WEB_ADMIN_KEY:
            return json_response({"error": "Unauthorized"}, status=401)
        
        items = get_blacklist()
        bl_list = [{"lora_id": item["lora_id"], "added_at": item["added_at"], "reason": item["reason"]} for item in items]
        return json_response({"success": True, "data": bl_list})
    except Exception as e:
        logger.error("Ошибка получения ЧС: " + str(e))
        return json_response({"error": str(e)}, status=500)

async def api_add_blacklist(request):
    """Добавление в чёрный список"""
    try:
        auth_header = request.headers.get("Authorization", "")
        if auth_header != "Bearer " + WEB_ADMIN_KEY:
            return json_response({"error": "Unauthorized"}, status=401)
        
        data = await request.json()
        lora_id = data.get("lora_id", "").strip()
        reason = data.get("reason", "")
        
        if not lora_id:
            return json_response({"error": "Empty lora_id"}, status=400)
        
        add_to_blacklist(lora_id, reason)
        logger.info("=== ДОБАВЛЕНО В ЧС ЧЕРЕЗ WEB === ID: " + lora_id)
        return json_response({"success": True, "message": "Added to blacklist"})
    except Exception as e:
        logger.error("Ошибка добавления в ЧС: " + str(e))
        return json_response({"error": str(e)}, status=500)

async def api_remove_blacklist(request):
    """Удаление из чёрного списка"""
    try:
        auth_header = request.headers.get("Authorization", "")
        if auth_header != "Bearer " + WEB_ADMIN_KEY:
            return json_response({"error": "Unauthorized"}, status=401)
        
        data = await request.json()
        lora_id = data.get("lora_id", "").strip()
        
        if not lora_id:
            return json_response({"error": "Empty lora_id"}, status=400)
        
        if remove_from_blacklist(lora_id):
            logger.info("=== УДАЛЕНО ИЗ ЧС ЧЕРЕЗ WEB === ID: " + lora_id)
            return json_response({"success": True, "message": "Removed from blacklist"})
        return json_response({"error": "Not found in blacklist"}, status=404)
    except Exception as e:
        logger.error("Ошибка удаления из ЧС: " + str(e))
        return json_response({"error": str(e)}, status=500)

async def web_panel_handler(request):
    """Отдача HTML страницы веб-панели"""
    html = '''<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Bot Control Panel</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: Arial, sans-serif; background: #1a1a2e; color: #eee; padding: 20px; }
        .container { max-width: 900px; margin: 0 auto; }
        h1 { text-align: center; margin-bottom: 30px; color: #4ecca3; }
        .card { background: #16213e; border-radius: 10px; padding: 20px; margin-bottom: 20px; }
        .card h2 { color: #4ecca3; margin-bottom: 15px; font-size: 1.3em; }
        .stat-row { display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid #0f3460; }
        .stat-row:last-child { border-bottom: none; }
        button { background: #4ecca3; color: #1a1a2e; border: none; padding: 10px 20px; border-radius: 5px; cursor: pointer; font-weight: bold; margin: 5px; }
        button:hover { background: #45b393; }
        button.danger { background: #e74c3c; }
        button.danger:hover { background: #c0392b; }
        input, select { background: #0f3460; border: 1px solid #4ecca3; color: #eee; padding: 8px; border-radius: 5px; margin: 5px; }
        .tag-item { display: inline-block; background: #0f3460; padding: 5px 10px; border-radius: 15px; margin: 3px; }
        .tag-remove { color: #e74c3c; cursor: pointer; margin-left: 5px; font-weight: bold; }
        #login-section { text-align: center; }
        #panel-section { display: none; }
        .hidden { display: none !important; }
        .log-entry { font-size: 0.85em; color: #aaa; padding: 3px 0; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🤖 Bot Control Panel</h1>
        
        <div id="login-section" class="card">
            <h2>🔐 Login</h2>
            <input type="password" id="admin-key" placeholder="Enter admin key">
            <button onclick="login()">Login</button>
            <p id="login-error" style="color: #e74c3c; margin-top: 10px;"></p>
        </div>
        
        <div id="panel-section">
            <div class="card">
                <h2>📊 Status</h2>
                <div class="stat-row"><span>Min Days:</span><span id="stat-days">-</span></div>
                <div class="stat-row"><span>Tags:</span><span id="stat-tags">-</span></div>
                <div class="stat-row"><span>Schedule:</span><span id="stat-schedule">-</span></div>
                <div class="stat-row"><span>Blacklist:</span><span id="stat-blacklist">-</span></div>
                <button onclick="refreshStatus()">🔄 Refresh</button>
                <button onclick="runCheck()">🔍 Run Check Now</button>
            </div>
            
            <div class="card">
                <h2>⚙️ Settings</h2>
                <div>
                    <label>Min Days: </label>
                    <input type="number" id="new-days" min="1" value="25">
                    <button onclick="setDays()">Set</button>
                </div>
            </div>
            
            <div class="card">
                <h2>🏷️ Tags</h2>
                <div id="tags-list"></div>
                <div style="margin-top: 15px;">
                    <input type="text" id="new-tag" placeholder="New tag name">
                    <button onclick="addTag()">Add Tag</button>
                </div>
            </div>
            
            <div class="card">
                <h2>⬛ Blacklist</h2>
                <div id="blacklist-list"></div>
                <div style="margin-top: 15px;">
                    <input type="text" id="bl-id" placeholder="LoRA ID">
                    <input type="text" id="bl-reason" placeholder="Reason (optional)">
                    <button onclick="addToBlacklist()">Add to BL</button>
                </div>
            </div>
            
            <div class="card">
                <h2>📜 Recent History</h2>
                <div id="history-list"></div>
            </div>
        </div>
    </div>
    
    <script>
        let adminKey = '';
        
        async function login() {
            const key = document.getElementById('admin-key').value;
            try {
                const res = await fetch('/api/auth', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({key: key})
                });
                const data = await res.json();
                if (data.success) {
                    adminKey = key;
                    document.getElementById('login-section').classList.add('hidden');
                    document.getElementById('panel-section').style.display = 'block';
                    refreshStatus();
                } else {
                    document.getElementById('login-error').textContent = data.message;
                }
            } catch (e) {
                document.getElementById('login-error').textContent = 'Connection error';
            }
        }
        
        async function apiRequest(endpoint, method='GET', data=null) {
            const options = {
                method: method,
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': 'Bearer ' + adminKey
                }
            };
            if (data) options.body = JSON.stringify(data);
            const res = await fetch(endpoint, options);
            return await res.json();
        }
        
        async function refreshStatus() {
            const data = await apiRequest('/api/status');
            if (data.data) {
                document.getElementById('stat-days').textContent = data.data.min_days;
                document.getElementById('stat-tags').textContent = data.data.tags.join(', ');
                document.getElementById('stat-schedule').textContent = data.data.schedule.join(', ') || 'None';
                document.getElementById('stat-blacklist').textContent = data.data.blacklist_count + ' items';
                
                // Tags
                const tagsList = document.getElementById('tags-list');
                tagsList.innerHTML = '';
                data.data.tags.forEach(tag => {
                    const span = document.createElement('span');
                    span.className = 'tag-item';
                    span.innerHTML = tag + ' <span class="tag-remove" onclick="removeTag(\\'' + tag + '\\')">×</span>';
                    tagsList.appendChild(span);
                });
                
                // History
                const histList = document.getElementById('history-list');
                histList.innerHTML = '';
                data.data.history.forEach(h => {
                    const div = document.createElement('div');
                    div.className = 'log-entry';
                    div.textContent = h.timestamp + ' | Pages: ' + h.pages_scanned + ' | LoRAs: ' + h.loras_found;
                    histList.appendChild(div);
                });
                
                loadBlacklist();
            }
        }
        
        async function loadBlacklist() {
            const data = await apiRequest('/api/blacklist');
            const blList = document.getElementById('blacklist-list');
            blList.innerHTML = '';
            if (data.data) {
                data.data.forEach(item => {
                    const div = document.createElement('div');
                    div.className = 'stat-row';
                    div.innerHTML = '<span>ID: ' + item.lora_id + ' (' + item.reason + ')</span><button class="danger" onclick="removeFromBlacklist(\\'' + item.lora_id + '\\')">Remove</button>';
                    blList.appendChild(div);
                });
            }
        }
        
        async function setDays() {
            const days = parseInt(document.getElementById('new-days').value);
            const res = await apiRequest('/api/setdays', 'POST', {days: days});
            if (res.success) { alert('Updated!'); refreshStatus(); }
            else { alert('Error: ' + res.error); }
        }
        
        async function addTag() {
            const tag = document.getElementById('new-tag').value.trim();
            if (!tag) return;
            const res = await apiRequest('/api/addtag', 'POST', {tag: tag});
            if (res.success) { document.getElementById('new-tag').value = ''; refreshStatus(); }
            else { alert('Error: ' + res.error); }
        }
        
        async function removeTag(tag) {
            if (!confirm('Remove tag ' + tag + '?')) return;
            const res = await apiRequest('/api/rmtag', 'POST', {tag: tag});
            if (res.success) { refreshStatus(); }
            else { alert('Error: ' + res.error); }
        }
        
        async function runCheck() {
            const res = await apiRequest('/api/check', 'POST');
            if (res.success) { alert('Check started! Results will be sent to Telegram.'); }
            else { alert('Error: ' + res.error); }
        }
        
        async function addToBlacklist() {
            const lora_id = document.getElementById('bl-id').value.trim();
            const reason = document.getElementById('bl-reason').value.trim();
            if (!lora_id) return;
            const res = await apiRequest('/api/blacklist/add', 'POST', {lora_id: lora_id, reason: reason});
            if (res.success) { document.getElementById('bl-id').value = ''; document.getElementById('bl-reason').value = ''; loadBlacklist(); }
            else { alert('Error: ' + res.error); }
        }
        
        async function removeFromBlacklist(lora_id) {
            const res = await apiRequest('/api/blacklist/remove', 'POST', {lora_id: lora_id});
            if (res.success) { loadBlacklist(); refreshStatus(); }
            else { alert('Error: ' + res.error); }
        }
    </script>
</body>
</html>'''
    return web.Response(text=html, content_type='text/html')

async def run_web_server():
    app = web.Application()

    webhook_path = "/webhook/" + BOT_TOKEN.split(":")[0]
    app.router.add_post(webhook_path, webhook_handler)

    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)
<<<<<<< HEAD

=======
    app.router.add_get("/panel", web_panel_handler)
    
    # API endpoints
    app.router.add_post("/api/auth", api_auth)
    app.router.add_get("/api/status", api_get_status)
    app.router.add_post("/api/setdays", api_set_days)
    app.router.add_post("/api/addtag", api_add_tag)
    app.router.add_post("/api/rmtag", api_remove_tag)
    app.router.add_post("/api/check", api_run_check)
    app.router.add_get("/api/blacklist", api_get_blacklist)
    app.router.add_post("/api/blacklist/add", api_add_blacklist)
    app.router.add_post("/api/blacklist/remove", api_remove_blacklist)
    
>>>>>>> d8296f47bac30f1cb00ce48f5340dd9309a72d7b
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

+++ bot.py (修改后)
import os
import re
import asyncio
import logging
import sqlite3
import requests
import time
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
DB_FILE = "bot_data.db"
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

# === SQLITE БАЗА ДАННЫХ ===
def init_db():
    """Инициализация базы данных SQLite"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Таблица настроек
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    ''')

    # Таблица тегов
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tag TEXT UNIQUE NOT NULL
        )
    ''')

    # Таблица расписания
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS schedule (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            time TEXT NOT NULL
        )
    ''')

    # Таблица истории проверок
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS check_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            loras_found INTEGER DEFAULT 0,
            pages_scanned INTEGER DEFAULT 0
        )
    ''')

    # Таблица чёрного списка лор
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS blacklist (
            lora_id TEXT PRIMARY KEY,
            added_at TEXT NOT NULL,
            reason TEXT
        )
    ''')

    conn.commit()

    # Инициализация значений по умолчанию
    cursor.execute("SELECT COUNT(*) FROM settings")
    if cursor.fetchone()[0] == 0:
        cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
                      ("min_days", str(DEFAULT_MIN_DAYS)))

    cursor.execute("SELECT COUNT(*) FROM tags")
    if cursor.fetchone()[0] == 0:
        for tag in DEFAULT_TAGS:
            cursor.execute("INSERT OR IGNORE INTO tags (tag) VALUES (?)", (tag,))

    conn.commit()
    conn.close()
    logger.info("✅ База данных инициализирована: " + DB_FILE)

def get_db_connection():
    """Получение соединения с базой данных"""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def get_setting(key, default=None):
    """Получение настройки из БД"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM settings WHERE key = ?", (key,))
    row = cursor.fetchone()
    conn.close()
    return row["value"] if row else default

def set_setting(key, value):
    """Сохранение настройки в БД"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, str(value)))
    conn.commit()
    conn.close()

def get_all_tags():
    """Получение всех тегов"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT tag FROM tags ORDER BY id")
    tags = [row["tag"] for row in cursor.fetchall()]
    conn.close()
    return tags if tags else DEFAULT_TAGS.copy()

def add_tag(tag):
    """Добавление тега"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO tags (tag) VALUES (?)", (tag,))
        conn.commit()
        success = True
    except sqlite3.IntegrityError:
        success = False
    conn.close()
    return success

def remove_tag(tag):
    """Удаление тега"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM tags")
    count = cursor.fetchone()[0]
    if count <= 1:
        conn.close()
        return False
    cursor.execute("DELETE FROM tags WHERE tag = ?", (tag,))
    conn.commit()
    conn.close()
    return True

def get_schedule():
    """Получение расписания"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT time FROM schedule ORDER BY id")
    times = [row["time"] for row in cursor.fetchall()]
    conn.close()
    return times

def set_schedule(times):
    """Установка расписания"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM schedule")
    for t in times:
        cursor.execute("INSERT INTO schedule (time) VALUES (?)", (t,))
    conn.commit()
    conn.close()

def add_check_history(loras_count, pages_count):
    """Добавление записи в историю проверок"""
    conn = get_db_connection()
    cursor = conn.cursor()
    timestamp = datetime.now().isoformat()
    cursor.execute("INSERT INTO check_history (timestamp, loras_found, pages_scanned) VALUES (?, ?, ?)",
                  (timestamp, loras_count, pages_count))
    conn.commit()
    conn.close()

def is_blacklisted(lora_id):
    """Проверка наличия лоры в чёрном списке"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM blacklist WHERE lora_id = ?", (lora_id,))
    result = cursor.fetchone() is not None
    conn.close()
    return result

def add_to_blacklist(lora_id, reason=""):
    """Добавление лоры в чёрный список"""
    conn = get_db_connection()
    cursor = conn.cursor()
    timestamp = datetime.now().isoformat()
    cursor.execute("INSERT OR REPLACE INTO blacklist (lora_id, added_at, reason) VALUES (?, ?, ?)",
                  (lora_id, timestamp, reason))
    conn.commit()
    conn.close()

def remove_from_blacklist(lora_id):
    """Удаление лоры из чёрного списка"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM blacklist WHERE lora_id = ?", (lora_id,))
    deleted = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return deleted

def get_blacklist():
    """Получение чёрного списка"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT lora_id, added_at, reason FROM blacklist ORDER BY added_at DESC")
    items = cursor.fetchall()
    conn.close()
    return items

# Инициализация БД при старте
init_db()

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

                id_match = re.search(r"#️⃣\s*(\d+)", text)
                if not id_match:
                    continue
                lora_id = id_match.group(1)

                days_match = re.search(r"🕸️\s*(\d+)\s*d", text, re.IGNORECASE)
                if not days_match:
                    continue
                lora_days = int(days_match.group(1))

                name_match = re.match(r'^\d+\.\s*(.+?)\s*\|\|', text.strip())
                if name_match:
                    lora_name = name_match.group(1).strip()
                else:
                    lora_name = "Loonie"

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
        txt += "<b>🗑️ Чёрный список:</b>\n"
        txt += "/blacklist — Показать чёрный список\n"
        txt += "/addblack <ID> [причина] — Добавить в ЧС\n"
        txt += "/rmblack <ID> — Удалить из ЧС\n\n"
        txt += "<b>📊 История:</b>\n"
        txt += "/history — Показать историю проверок\n\n"
        txt += "<i>Все команды доступны только тебе (владелец)</i>"
        await message.answer(txt, parse_mode="HTML")
    except Exception as e:
        logger.error("Ошибка в /help: " + str(e))

@dp.message(Command("check"))
async def cmd_check(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    try:
        min_days = int(get_setting("min_days", DEFAULT_MIN_DAYS))
        tags = get_all_tags()

        logger.info("=== ПРОВЕРКА === Порог: >= " + str(min_days) + " | Теги: " + ", ".join(tags))
        await message.answer("🔍 Сканирую теги: <b>" + ", ".join(tags) + "</b> (порог: >= " + str(min_days) + " дней)...", parse_mode="HTML")

        all_loras = []
        total_pages = 0

        for tag in tags:
            base_url = SITE_BASE + "/?p=lora&t=" + tag
            loras, pages = find_inactive_loonies_all_pages(base_url, min_days)
            # Фильтрация чёрного списка
            filtered_loras = [l for l in loras if not is_blacklisted(l["id"])]
            all_loras.extend(filtered_loras)
            total_pages += pages

        # Сохраняем в историю
        add_check_history(len(all_loras), total_pages)

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

        set_setting("min_days", new_days)
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
            await message.answer("⚠️ Используй: <code>/addtag &lt;название&gt;</code>\nПример: <code>/addtag anime</code>", parse_mode="HTML")
            return

        new_tag = parts[1].strip().lower()
        if not new_tag.isalnum():
            await message.answer("⚠️ Тег должен содержать только буквы и цифры", parse_mode="HTML")
            return

        if add_tag(new_tag):
            logger.info("=== ТЕГ ДОБАВЛЕН === " + new_tag)
            tags = get_all_tags()
            await message.answer("✅ Тег <b>" + new_tag + "</b> добавлен. Текущие теги: " + ", ".join(tags), parse_mode="HTML")
        else:
            await message.answer("⚠️ Тег <b>" + new_tag + "</b> уже в списке", parse_mode="HTML")

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

        if remove_tag(tag_to_remove):
            logger.info("=== ТЕГ УДАЛЁН === " + tag_to_remove)
            tags = get_all_tags()
            await message.answer("✅ Тег <b>" + tag_to_remove + "</b> удалён. Текущие теги: " + ", ".join(tags), parse_mode="HTML")
        else:
            await message.answer("⚠️ Тег <b>" + tag_to_remove + "</b> не найден или это последний тег", parse_mode="HTML")

    except Exception as e:
        logger.error("Ошибка в /rmtag: " + str(e))
        await message.answer("❌ Не удалось удалить тег.")

@dp.message(Command("tags"))
async def cmd_tags(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    try:
        tags = get_all_tags()
        txt = "🏷️ <b>Активные теги:</b>\n"
        for i, tag in enumerate(tags, 1):
            txt += str(i) + ". <code>" + tag + "</code>\n"
        txt += "\n<i>Всего: " + str(len(tags)) + "</i>"
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

        set_schedule(times)
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
        schedule = get_schedule()
        if schedule:
            txt = "⏰ <b>Расписание автопроверок:</b>\n"
            for t in schedule:
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
        min_days = int(get_setting("min_days", DEFAULT_MIN_DAYS))
        tags = get_all_tags()
        schedule = get_schedule()

        txt = "⚙️ <b>Настройки:</b>\n"
        txt += "🕸️ Порог: <b>" + str(min_days) + "</b> дней (>=)\n"
        txt += "🏷️ Теги: <b>" + ", ".join(tags) + "</b>\n"
        txt += "🔄 Автопроверка: <b>" + str(CHECK_INTERVAL_HOURS) + "</b> ч.\n"
        txt += "📄 Макс. страниц: <b>" + str(MAX_PAGES) + "</b>\n"
        if schedule:
            txt += "⏰ Расписание: <b>" + ", ".join(schedule) + "</b>\n"
        await message.answer(txt, parse_mode="HTML")
    except Exception as e:
        logger.error("Ошибка в /status: " + str(e))

# ================= ЧЁРНЫЙ СПИСОК =================
@dp.message(Command("blacklist"))
async def cmd_blacklist(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    try:
        blacklist = get_blacklist()
        if not blacklist:
            await message.answer("🗑️ <b>Чёрный список пуст</b>", parse_mode="HTML")
            return

        txt = "🗑️ <b>Чёрный список:</b>\n"
        for item in blacklist[:20]:  # Ограничим вывод
            reason = item["reason"] if item["reason"] else "без причины"
            txt += "• ID: <code>" + item["lora_id"] + "</code> (" + reason + ")\n"

        if len(blacklist) > 20:
            txt += "\n... и ещё " + str(len(blacklist) - 20) + " записей"

        await message.answer(txt, parse_mode="HTML")
    except Exception as e:
        logger.error("Ошибка в /blacklist: " + str(e))

@dp.message(Command("addblack"))
async def cmd_addblack(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    try:
        parts = message.text.split(maxsplit=2)
        if len(parts) < 2:
            await message.answer("⚠️ Используй: <code>/addblack &lt;ID&gt; [причина]</code>", parse_mode="HTML")
            return

        lora_id = parts[1]
        reason = parts[2] if len(parts) > 2 else ""

        add_to_blacklist(lora_id, reason)
        logger.info("=== ЧЁРНЫЙ СПИСОК === Добавлен ID: " + lora_id)

        msg = "✅ ID <code>" + lora_id + "</code> добавлен в чёрный список"
        if reason:
            msg += "\nПричина: " + reason
        await message.answer(msg, parse_mode="HTML")
    except Exception as e:
        logger.error("Ошибка в /addblack: " + str(e))
        await message.answer("❌ Не удалось добавить в чёрный список.")

@dp.message(Command("rmblack"))
async def cmd_rmblack(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    try:
        parts = message.text.split()
        if len(parts) != 2:
            await message.answer("⚠️ Используй: <code>/rmblack &lt;ID&gt;</code>", parse_mode="HTML")
            return

        lora_id = parts[1]

        if remove_from_blacklist(lora_id):
            logger.info("=== ЧЁРНЫЙ СПИСОК === Удалён ID: " + lora_id)
            await message.answer("✅ ID <code>" + lora_id + "</code> удалён из чёрного списка", parse_mode="HTML")
        else:
            await message.answer("⚠️ ID <code>" + lora_id + "</code> не найден в чёрном списке", parse_mode="HTML")
    except Exception as e:
        logger.error("Ошибка в /rmblack: " + str(e))
        await message.answer("❌ Не удалось удалить из чёрного списка.")

# ================= ИСТОРИЯ =================
@dp.message(Command("history"))
async def cmd_history(message: Message):
    if message.from_user.id != OWNER_ID_INT:
        return
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT timestamp, loras_found, pages_scanned FROM check_history ORDER BY id DESC LIMIT 10")
        records = cursor.fetchall()
        conn.close()

        if not records:
            await message.answer("📊 <b>История пуста</b>", parse_mode="HTML")
            return

        txt = "📊 <b>История проверок (последние 10):</b>\n"
        for rec in records:
            ts = rec["timestamp"][:16].replace("T", " ")
            txt += "• " + ts + ": <b>" + str(rec["loras_found"]) + "</b> лор, " + str(rec["pages_scanned"]) + " стр.\n"

        await message.answer(txt, parse_mode="HTML")
    except Exception as e:
        logger.error("Ошибка в /history: " + str(e))
        await message.answer("❌ Не удалось получить историю.")

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

            schedule = get_schedule()
            min_days = int(get_setting("min_days", DEFAULT_MIN_DAYS))
            tags = get_all_tags()

            if schedule:
                if current_time not in schedule:
                    await asyncio.sleep(60)
                    continue

            logger.info("=== АВТОПРОВЕРКА === Порог: >= " + str(min_days) + " | Теги: " + ", ".join(tags))

            all_loras = []
            total_pages = 0

            for tag in tags:
                base_url = SITE_BASE + "/?p=lora&t=" + tag
                loras, pages = find_inactive_loonies_all_pages(base_url, min_days)
                # Фильтрация чёрного списка
                filtered_loras = [l for l in loras if not is_blacklisted(l["id"])]
                all_loras.extend(filtered_loras)
                total_pages += pages

            # Сохраняем в историю
            add_check_history(len(all_loras), total_pages)

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
    min_days = int(get_setting("min_days", DEFAULT_MIN_DAYS))
    tags = get_all_tags()
    logger.info("📊 Порог: >= " + str(min_days) + " дней | Теги: " + ", ".join(tags))
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