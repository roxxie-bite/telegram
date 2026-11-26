"""
sticker_mods.py — модуль для бота (aiogram 3.x): превращение присланных видео/GIF
(как файла) в формат анимированных стикеров Telegram.

Формат анимированного стикера Telegram (WebM, VP9, ≤512x512, ≤3 сек, ≤256 КБ, 1 fps?):
  - контейнер: WebM
  - кодек: VP9
  - размер: одна сторона ровно 512 px (вторая <= 512)
  - длительность: не более 3 секунд
  - вес: не более 256 КБ
  - без звуковой дорожки

Модуль использует ffmpeg через subprocess (асинхронно), ничего не скачивая сам —
файл из Telegram скачивает бот, конвертирует и отправляет результат как видеосообщение
(кружок) или обычным документом .webm.

Поддерживаемые входные форматы: любое видео/гиф, которое умеет читать ffmpeg
(mp4, webm, mkv, gif, mov, avi ...). GIF'ки тоже принимаются, даже если Telegram
показывает их как документ.

Использование:
    from sticker_mods import router as sticker_router
    dp.include_router(sticker_router)

Команды:
    /sticker  — ответить на видео/GIF-сообщение этой командой, чтобы получить стикер-формат
    /stickers — помощь по модулю

Либо вызов функции напрямую из своего кода:
    from sticker_mods import convert_to_telegram_sticker
    webm_bytes = await convert_to_telegram_sticker(input_bytes)   # -> bytes (webm vp9)
"""

from __future__ import annotations
import asyncio
import functools
import logging
import os
import shutil
import tempfile
from typing import Optional
from aiogram import F, Router
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.types import BufferedInputFile, Message

logger = logging.getLogger(__name__)
router = Router(name="sticker")

# ================= PARSE_MODE (HTML) =================
# В aiogram 3.x Router не умеет задавать parse_mode, поэтому оборачиваем методы Message/Bot:
# если parse_mode явно не передан, подставляем HTML. Так <b>, <code> и т.д. перестают
# печататься как текст. Если у бота уже задан DefaultBotProperties(parse_mode=HTML),
# эти обёртки просто ничего не меняют.

from aiogram import Bot  # noqa: E402

_PATCHED_TARGETS = (Message, Bot)
_PATCHED_ATTRS = (
    "answer",
    "reply",
    "forward",
    "copy_to_chat",
    "edit_text",
    "edit_caption",
    "answer_photo",
    "answer_video",
    "answer_animation",
    "answer_document",
    "send_message",
    "send_photo",
    "send_video",
    "send_animation",
    "send_document",
)


def _wrap_with_html(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        if kwargs.get("parse_mode") is None:
            kwargs["parse_mode"] = ParseMode.HTML
        return await func(*args, **kwargs)

    return wrapper


def _patch_parse_mode() -> None:
    for cls in _PATCHED_TARGETS:
        for name in _PATCHED_ATTRS:
            orig = getattr(cls, name, None)
            if orig is None or getattr(orig, "_sticker_html_patched", False):
                continue
            patched = _wrap_with_html(orig)
            patched._sticker_html_patched = True
            setattr(cls, name, patched)


_patch_parse_mode()


def _safe_html(text: str) -> str:
    """Экранирует текст, чтобы вставки (ошибки ffmpeg и т.п.) не ломали HTML-разметку."""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )

# ================= НАСТРОЙКИ ФОРМАТА СТИКЕРОВ TELEGRAM =================
STICKER_MAX_SIDE = 512          # px, максимальная сторона
STICKER_DURATION = 3.0          # сек, максимальная длительность
STICKER_MAX_BYTES = 256 * 1024  # 256 КБ — лимит Telegram для анимированных стикеров
TARGET_FPS = 30                 # fps результата (Telegram допускает до 30)

FFMPEG_BIN = os.getenv("FFMPEG_BIN", "ffmpeg")

HELP_TEXT = (
    "🎨 <b>Конвертер в формат стикеров Telegram</b>\n\n"
    "Пришли видео или GIF <b>как файл</b> (или ответь на такое сообщение командой /sticker)\n"
    "— я сконвертирую его в формат анимированного стикера:\n"
    "WebM • VP9 • 512px • до 3 секунд • без звука • до 256 КБ.\n\n"
    "Команды:\n"
    "/sticker — сконвертировать видео/GIF (можно указать таймкод: <code>/sticker 2.5</code> — начать с 2.5 сек)\n"
    "/stickers — эта справка\n\n"
    "💡 Готовый файл можно добавить в набор стикеров через @Stickers."
)


# ================= УТИЛИТЫ FFMPEG =================

def ffmpeg_available() -> bool:
    return shutil.which(FFMPEG_BIN) is not None


async def _run_ffmpeg(args: list[str], timeout: float = 120.0) -> tuple[int, str, str]:
    """Запускает ffmpeg асинхронно, возвращает (returncode, stdout, stderr)."""
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        raise RuntimeError("ffmpeg: превышено время ожидания конвертации")
    return proc.returncode or 0, out.decode(errors="ignore"), err.decode(errors="ignore")


async def get_media_info(path: str) -> dict:
    """Возвращает {'duration': float|None, 'width': int|None, 'height': int|None} через ffprobe."""
    ffprobe = os.getenv("FFPROBE_BIN", "ffprobe")
    if shutil.which(ffprobe) is None:
        # ffprobe может отсутствовать — тогда попробуем определить длительность парсингом логов ffmpeg
        return {"duration": None, "width": None, "height": None}
    args = [
        ffprobe, "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=width,height:format=duration",
        "-of", "default=noprint_wrappers=1:nokey=0",
        path,
    ]
    rc, out, _ = await _run_ffmpeg(args, timeout=30)
    info = {"duration": None, "width": None, "height": None}
    if rc != 0:
        return info
    flat = " ".join(out.split())  # 'width = N height = N duration = X'
    import re
    m = re.search(r"width\s*=\s*(\d+)", flat)
    if m:
        info["width"] = int(m.group(1))
    m = re.search(r"height\s*=\s*(\d+)", flat)
    if m:
        info["height"] = int(m.group(1))
    m = re.search(r"duration\s*=\s*([\d.]+)", flat)
    if m:
        info["duration"] = float(m.group(1))
    return info


def _scale_filter() -> str:
    """
    Масштабирование под требования Telegram: бо́льшая сторона = 512 px,
    меньшая — пропорционально; обе стороны кратны 2 (требование VP9).
    """
    s = STICKER_MAX_SIDE
    return f"scale='if(gt(iw,ih),{s},-2)':'if(gt(iw,ih),-2,{s})'"


def _build_encode_cmd(src: str, dst: str, crf: int, start: Optional[float] = None) -> list[str]:
    args = [FFMPEG_BIN, "-hide_banner", "-loglevel", "error", "-y"]
    if start is not None and start > 0:
        args += ["-ss", f"{start:.3f}"]
    args += [
        "-i", src,
        "-t", f"{STICKER_DURATION:.3f}",      # обрезаем до 3 секунд
        "-map", "0:v:0",                       # только видео (без звука)
        "-an",
        "-vf", _scale_filter() + ",fps=" + str(TARGET_FPS),
        "-c:v", "libvpx-vp9",
        "-crf", str(crf),
        "-b:v", "0",
        "-deadline", "good",
        "-cpu-used", "4",
        "-row-mt", "1",
        "-auto-alt-ref", "0",                   # важно для корректной зацикленной анимации
        "-pix_fmt", "yuva420p",                 # поддержка прозрачности (для гиф с альфа-каналом)
        "-metadata", "title=",
        dst,
    ]
    return args


async def _encode_size_ok(src: str, dst: str, crf: int, start: Optional[float]) -> int:
    """Кодирует с заданным CRF, возвращает размер файла в байтах."""
    if os.path.exists(dst):
        os.remove(dst)
    rc, _, err = await _run_ffmpeg(_build_encode_cmd(src, dst, crf, start))
    if rc != 0 or not os.path.exists(dst):
        raise RuntimeError(f"ffmpeg error: {err.strip() or 'unknown'}")
    return os.path.getsize(dst)


async def convert_to_telegram_sticker(
    data: bytes,
    suffix: str = ".bin",
    start: Optional[float] = None,
) -> bytes:
    """
    Конвертирует байты видео/GIF в формат анимированного стикера Telegram
    (WebM/VP9, 512px, <=3s, <=256KB, без звука).

    :param data: исходные байты файла
    :param suffix: расширение входного файла (на ffmpeg почти не влияет, но помогает гифам)
    :param start: с какой секунды начинать (обрезка начала), None = с начала
    :return: байты готового .webm
    :raises RuntimeError: если ffmpeg недоступен или конвертация не удалась
    """
    if not ffmpeg_available():
        raise RuntimeError("ffmpeg не найден в системе (установите его или задайте FFMPEG_BIN)")

    tmpdir = tempfile.mkdtemp(prefix="stk_")
    src = os.path.join(tmpdir, "input" + (suffix if suffix.startswith(".") else "." + suffix))
    dst = os.path.join(tmpdir, "sticker.webm")
    try:
        with open(src, "wb") as f:
            f.write(data)

        # Проверим, что внутри вообще есть видео
        info = await get_media_info(src)
        if info["width"] is None and info["duration"] == 0:
            raise RuntimeError("в файле не найдено видео")

        # Подбираем качество: начинаем с CRF 33 и повышаем, пока не уложимся в 256 КБ
        crf = 33
        size = await _encode_size_ok(src, dst, crf, start)
        while size > STICKER_MAX_BYTES and crf < 56:
            crf += 3
            size = await _encode_size_ok(src, dst, crf, start)

        # Если даже при максимальном сжатии не влезаем — режем fps и пересобираем
        if size > STICKER_MAX_BYTES:
            low = dst + ".low.webm"
            rc, _, err = await _run_ffmpeg([
                FFMPEG_BIN, "-hide_banner", "-loglevel", "error", "-y",
                "-i", dst,
                "-vf", f"fps=15,scale=trunc(iw/2)*2:trunc(ih/2)*2",
                "-c:v", "libvpx-vp9", "-crf", "45", "-b:v", "0",
                "-deadline", "good", "-cpu-used", "5", "-row-mt", "1",
                "-auto-alt-ref", "0", "-pix_fmt", "yuva420p",
                low,
            ])
            if rc == 0 and os.path.getsize(low) < size:
                shutil.move(low, dst)
                size = os.path.getsize(dst)

        logger.info("sticker converted: crf=%s size=%s bytes", crf, size)
        with open(dst, "rb") as f:
            return f.read()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ================= ХЭНДЛЕРЫ AIogram =================

def _is_video_or_gif(msg: Message) -> bool:
    return bool(
        msg.video
        or msg.animation          # GIF, отправленный как документ/анимация
        or msg.video_note
    )


def _guess_suffix(msg: Message) -> str:
    if msg.animation and msg.animation.file_name:
        return os.path.splitext(msg.animation.file_name)[1] or ".gif"
    if msg.video and msg.video.file_name:
        return os.path.splitext(msg.video.file_name)[1] or ".mp4"
    return ".mp4"


@router.message(Command("stickers"))
async def cmd_stickers_help(message: Message) -> None:
    await message.answer(HELP_TEXT)


@router.message(Command("sticker"))
async def cmd_sticker(message: Message, command: CommandObject) -> None:
    """Ответить на видео/GIF командой /sticker [start_sec]."""
    target = message.reply_to_message or message
    if not _is_video_or_gif(target):
        await message.answer("↩️ Ответь командой /sticker на сообщение с видео или GIF (файлом).")
        return

    if not ffmpeg_available():
        await message.answer("⚠️ На сервере не установлен ffmpeg — конвертация невозможна.")
        return

    start: Optional[float] = None
    if command and command.args:
        try:
            start = float(command.args.split()[0].replace(",", "."))
        except ValueError:
            await message.answer("❌ Не понял таймкод. Пример: <code>/sticker 2.5</code>")
            return

    file_ref = target.animation or target.video or target.video_note
    tg_file = await message.bot.get_file(file_ref.file_id)
    data = await tg_file.download_as_bytearray()

    wait = await message.answer("🔄 Конвертирую в формат стикера (WebM/VP9, 512px, ≤3с)...")
    try:
        webm = await convert_to_telegram_sticker(bytes(data), suffix=_guess_suffix(target), start=start)
    except Exception as e:
        logger.exception("sticker conversion failed")
        await wait.edit_text(f"❌ Не удалось сконвертировать: <code>{_safe_html(e)}</code>")
        return

    filename = "sticker.webm"
    await wait.delete()
    # Отправляем как видеосообщение (оно тоже webm) И как документ — пользователь выберет,
    # либо просто сохраним файл. Здесь: документ .webm (его можно загрузить в @Stickers).
    await message.answer_document(
        BufferedInputFile(webm, filename=filename),
        caption=(
            f"✅ Формат анимированного стикера Telegram\n"
            f"WebM • VP9 • ≤{STICKER_MAX_SIDE}px • ≤{STICKER_DURATION:.0f}с • "
            f"{len(webm) // 1024} КБ из {STICKER_MAX_BYTES // 1024}\n\n"
            f"Отправь этот файл боту @Stickers, чтобы создать стикер."
        ),
    )


@router.message(F.content_type.in_(["video", "animation", "video_note"]))
async def auto_convert(message: Message) -> None:
    """Автоконвертация любого присланного видео/GIF (файлом)."""
    if message.from_user is None:
        return
    # Защита от рекурсии: не конвертим сообщения, отправленные ботами
    if getattr(message, "sender_bot", None) is not None:
        return

    if not ffmpeg_available():
        return  # тихо пропускаем, если ffmpeg нет

    file_ref = message.animation or message.video or message.video_note
    try:
        tg_file = await message.bot.get_file(file_ref.file_id)
        data = await tg_file.download_as_bytearray()
        wait = await message.answer("🔄 Конвертирую в формат стикера...")
        webm = await convert_to_telegram_sticker(
            bytes(data), suffix=_guess_suffix(message)
        )
        await wait.delete()
        await message.answer_document(
            BufferedInputFile(webm, filename="sticker.webm"),
            caption=(
                f"🎨 WebM/VP9 • ≤{STICKER_MAX_SIDE}px • ≤{STICKER_DURATION:.0f}с • "
                f"{len(webm) // 1024} КБ — формат анимированного стикера.\n"
                f"Добавить в набор: отправь файл @Stickers."
            ),
        )
    except Exception as e:
        logger.exception("auto sticker conversion failed")
        try:
            await message.answer(f"❌ Ошибка конвертации: <code>{_safe_html(e)}</code>")
        except Exception:
            pass
