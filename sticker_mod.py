"""
sticker_mods.py — модуль для бота (aiogram 3.x): превращение присланных видео/GIF
в формат анимированных стикеров Telegram.
"""
from __future__ import annotations

import asyncio
import functools
import io
import logging
import os
import re
import shutil
import tempfile
from typing import Optional

from aiogram import Bot, F, Router
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import BufferedInputFile, Message

logger = logging.getLogger(__name__)
router = Router(name="sticker")


class StickerStates(StatesGroup):
    waiting_file = State()


_PATCHED_TARGETS = (Message, Bot)
_PATCHED_ATTRS = (
    "answer", "reply", "forward", "copy_to_chat",
    "edit_text", "edit_caption",
    "answer_photo", "answer_video", "answer_animation", "answer_document",
    "send_message", "send_photo", "send_video", "send_animation", "send_document",
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
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


STICKER_MAX_SIDE = 512
STICKER_DURATION = 3.0
STICKER_MAX_BYTES = 256 * 1024
TARGET_FPS = 30
FFMPEG_BIN = os.getenv("FFMPEG_BIN", "ffmpeg")

HELP_TEXT = (
    "🎨 <b>Конвертер в формат стикеров Telegram</b>\n\n"
    "Отправь команду /sticker — я перейду в режим ожидания,\n"
    "а затем пришли мне видео или GIF (файлом или как видео).\n\n"
    "Результат будет в формате анимированного стикера:\n"
    "WebM • VP9 • 512px • до 3 секунд • без звука • до 256 КБ.\n\n"
    "💡 Готовый файл можно добавить в набор через @Stickers.\n"
    "Чтобы отменить ожидание — отправь /cancel."
)


def ffmpeg_available() -> bool:
    return shutil.which(FFMPEG_BIN) is not None


async def _run_ffmpeg(args: list[str], timeout: float = 120.0) -> tuple[int, str, str]:
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
    ffprobe = os.getenv("FFPROBE_BIN", "ffprobe")
    if shutil.which(ffprobe) is None:
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

    flat = " ".join(out.split())
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
    s = STICKER_MAX_SIDE
    return f"scale='if(gt(iw,ih),{s},-2)':'if(gt(iw,ih),-2,{s})'"


def _build_encode_cmd(src: str, dst: str, crf: int, start: Optional[float] = None) -> list[str]:
    args = [FFMPEG_BIN, "-hide_banner", "-loglevel", "error", "-y"]
    if start is not None and start > 0:
        args += ["-ss", f"{start:.3f}"]
    args += [
        "-i", src,
        "-t", f"{STICKER_DURATION:.3f}",
        "-map", "0:v:0",
        "-an",
        "-vf", _scale_filter() + ",fps=" + str(TARGET_FPS),
        "-c:v", "libvpx-vp9",
        "-crf", str(crf),
        "-b:v", "0",
        "-deadline", "good",
        "-cpu-used", "4",
        "-row-mt", "1",
        "-auto-alt-ref", "0",
        "-pix_fmt", "yuva420p",
        "-metadata", "title=",
        dst,
    ]
    return args


async def _encode_size_ok(src: str, dst: str, crf: int, start: Optional[float]) -> int:
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
    if not ffmpeg_available():
        raise RuntimeError("ffmpeg не найден в системе (установите его или задайте FFMPEG_BIN)")

    tmpdir = tempfile.mkdtemp(prefix="stk_")
    src = os.path.join(tmpdir, "input" + (suffix if suffix.startswith(".") else "." + suffix))
    dst = os.path.join(tmpdir, "sticker.webm")
    try:
        with open(src, "wb") as f:
            f.write(data)

        info = await get_media_info(src)
        if info["width"] is None and info["duration"] == 0:
            raise RuntimeError("в файле не найдено видео")

        crf = 33
        size = await _encode_size_ok(src, dst, crf, start)
        while size > STICKER_MAX_BYTES and crf < 56:
            crf += 3
            size = await _encode_size_ok(src, dst, crf, start)

        if size > STICKER_MAX_BYTES:
            low = dst + ".low.webm"
            rc, _, err = await _run_ffmpeg([
                FFMPEG_BIN, "-hide_banner", "-loglevel", "error", "-y",
                "-i", dst,
                "-vf", "fps=15,scale=trunc(iw/2)*2:trunc(ih/2)*2",
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


def _is_video_document(msg: Message) -> bool:
    if not msg.document:
        return False
    mime = msg.document.mime_type or ""
    if mime.startswith("video/"):
        return True
    if msg.document.file_name:
        ext = os.path.splitext(msg.document.file_name)[1].lower()
        if ext in {".mp4", ".webm", ".mkv", ".mov", ".avi", ".gif"}:
            return True
    return False


def _guess_suffix_from_message(msg: Message) -> str:
    if msg.document and msg.document.file_name:
        ext = os.path.splitext(msg.document.file_name)[1]
        if ext:
            return ext
    if msg.animation and msg.animation.file_name:
        return os.path.splitext(msg.animation.file_name)[1] or ".gif"
    if msg.video and msg.video.file_name:
        return os.path.splitext(msg.video.file_name)[1] or ".mp4"
    return ".mp4"


@router.message(Command("stickers"))
async def cmd_stickers_help(message: Message) -> None:
    await message.answer(HELP_TEXT)


@router.message(Command("cancel"), StateFilter(StickerStates.waiting_file))
async def cmd_cancel(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer("❌ Ожидание отменено.")


@router.message(Command("sticker"))
async def cmd_sticker(message: Message, state: FSMContext) -> None:
    if not ffmpeg_available():
        await message.answer("⚠️ На сервере не установлен ffmpeg — конвертация невозможна.")
        return

    await state.set_state(StickerStates.waiting_file)
    await message.answer(
        "📥 Жду видео или GIF (файлом или как видео).\n"
        "Пришли его следующим сообщением — я сконвертирую в формат стикера.\n\n"
        "Чтобы отменить — отправь /cancel."
    )


@router.message(StickerStates.waiting_file, F.video | F.animation | F.video_note)
async def on_sticker_file(message: Message, state: FSMContext) -> None:
    file_ref = message.animation or message.video or message.video_note
    tg_file = await message.bot.get_file(file_ref.file_id)
    buffer = io.BytesIO()
    await message.bot.download_file(tg_file.file_path, buffer)
    data = buffer.getvalue()
    wait = await message.answer("🔄 Конвертирую в формат стикера (WebM/VP9, 512px, ≤3с)...")
    try:
        webm = await convert_to_telegram_sticker(
            bytes(data), suffix=_guess_suffix_from_message(message)
        )
    except Exception as e:
        logger.exception("sticker conversion failed")
        await wait.edit_text(f"❌ Не удалось сконвертировать: <code>{_safe_html(e)}</code>")
        await state.clear()
        return

    await wait.delete()
    await state.clear()

    await message.answer_document(
        BufferedInputFile(webm, filename="sticker.webm"),
        caption=(
            f"✅ Формат анимированного стикера Telegram\n"
            f"WebM • VP9 • ≤{STICKER_MAX_SIDE}px • ≤{STICKER_DURATION:.0f}с • "
            f"{len(webm) // 1024} КБ из {STICKER_MAX_BYTES // 1024}\n\n"
            f"Отправь этот файл боту @Stickers, чтобы создать стикер."
        ),
    )


@router.message(StickerStates.waiting_file, F.document)
async def on_sticker_document(message: Message, state: FSMContext) -> None:
    if not _is_video_document(message):
        await message.answer(
            "🤔 Этот документ не похож на видео.\n"
            "Пришли видеофайл (mp4, webm, mkv, mov, avi) или GIF, либо отмени ожидание командой /cancel."
        )
        return

    doc = message.document
    tg_file = await message.bot.get_file(doc.file_id)
    buffer = io.BytesIO()
    await message.bot.download_file(tg_file.file_path, buffer)
    data = buffer.getvalue()

    wait = await message.answer("🔄 Конвертирую в формат стикера (WebM/VP9, 512px, ≤3с)...")
    try:
        webm = await convert_to_telegram_sticker(
            bytes(data), suffix=_guess_suffix_from_message(message)
        )
    except Exception as e:
        logger.exception("sticker conversion failed")
        await wait.edit_text(f"❌ Не удалось сконвертировать: <code>{_safe_html(e)}</code>")
        await state.clear()
        return

    await wait.delete()
    await state.clear()

    await message.answer_document(
        BufferedInputFile(webm, filename="sticker.webm"),
        caption=(
            f"✅ Формат анимированного стикера Telegram\n"
            f"WebM • VP9 • ≤{STICKER_MAX_SIDE}px • ≤{STICKER_DURATION:.0f}с • "
            f"{len(webm) // 1024} КБ из {STICKER_MAX_BYTES // 1024}\n\n"
            f"Отправь этот файл боту @Stickers, чтобы создать стикер."
        ),
    )


@router.message(StickerStates.waiting_file)
async def on_sticker_wrong_type(message: Message) -> None:
    await message.answer(
        "🤔 Это не похоже на видео или GIF.\n"
        "Пришли именно видео или GIF (файлом или как видео), либо отмени ожидание командой /cancel."
    )