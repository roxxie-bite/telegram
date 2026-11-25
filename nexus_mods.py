"""
Nexus Mods API integration for the Telegram bot.

This module is intentionally separated from bot.py so the Nexus functionality can be
developed/tested independently. It contains:
  * NexusClient — small async-friendly wrapper over the Nexus Mods v1 REST API;
  * WatchlistStore — persistent JSON watchlist with baseline mod/file metadata;
  * aiogram Router — owner-only commands and optional automatic update watcher.

Setup in .env:
    NEXUS_API_KEY=your_personal_api_key
    NEXUS_APP_NAME=LoonieBot
    NEXUS_APP_VERSION=1.0.0
    NEXUS_WATCHLIST_FILE=nexus_watchlist.json
    NEXUS_DEFAULT_GAME=mountandblade2bannerlord   # optional
    NEXUS_MIN_REQUEST_INTERVAL=1.0               # seconds, be polite
    NEXUS_AUTO_CHECK_MINUTES=0                   # 0 = disabled by default

In bot.py:
    from nexus_mods import router as nexus_router, start_nexus_watcher
    dp.include_router(nexus_router)
    # in main(), before dp.start_polling(...):
    await start_nexus_watcher(bot, OWNER_ID_INT, logger)
"""

from __future__ import annotations

import asyncio
import html
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import quote

import requests
from aiogram import Bot, Router
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

logger = logging.getLogger(__name__)

API_BASE = "https://api.nexusmods.com"
WEB_BASE = "https://www.nexusmods.com"
VALID_PERIODS = {"1d", "1w", "1m"}
GAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{1,80}$", re.IGNORECASE)
MOD_ID_RE = re.compile(r"(?:\A|/)(\d{1,12})(?:\Z|[/?#])")


# =========================
# Exceptions
# =========================
class NexusError(RuntimeError):
    """Base Nexus integration error."""


class NexusConfigError(NexusError):
    """API key or required configuration is missing."""


class NexusAuthError(NexusError):
    """HTTP 401 or invalid API key."""


class NexusNotFoundError(NexusError):
    """Requested game/mod/file was not found."""


class NexusRateLimitError(NexusError):
    """HTTP 429 from Nexus."""

    def __init__(self, message: str, retry_after: Optional[float] = None, headers: Optional[dict[str, str]] = None):
        super().__init__(message)
        self.retry_after = retry_after
        self.headers = headers or {}


# =========================
# Small helpers
# =========================
def _safe(value: Any) -> str:
    return html.escape(str(value if value is not None else "-"))


def _format_ts(ts: Any) -> str:
    try:
        ts_i = int(ts)
        if ts_i <= 0:
            return "-"
        return datetime.fromtimestamp(ts_i, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    except (TypeError, ValueError, OSError):
        return "-"


def _mod_url(game: str, mod_id: int | str) -> str:
    return f"{WEB_BASE}/{quote(game.lower())}/mods/{int(mod_id)}"


def parse_mod_reference(text: str, default_game: Optional[str] = None) -> tuple[Optional[str], Optional[int]]:
    """Parses '12345' or a Nexus URL into (game_domain, mod_id)."""
    text = (text or "").strip()
    if not text:
        return default_game, None

    lowered = text.lower()
    m = re.search(r"nexusmods\.com/([a-z0-9_-]+)/mods/(\d+)", lowered)
    if m:
        return m.group(1), int(m.group(2))

    m = MOD_ID_RE.search(text)
    if m:
        return default_game, int(m.group(1))

    return default_game, None


def _game_or_raise(game: Optional[str]) -> str:
    game = (game or "").strip().lower()
    if not game:
        raise NexusConfigError("Не задана игра. Используй /nexusgame или укажи игру в ссылке.")
    if not GAME_RE.match(game):
        raise NexusError(f"Некорректный game domain: {game}")
    return game


def _read_rate_limit_headers(headers: requests.structures.CaseInsensitiveDict) -> dict[str, str]:
    keys = (
        "X-RL-Hourly-Limit", "X-RL-Hourly-Remaining", "X-RL-Hourly-Reset",
        "X-RL-Daily-Limit", "X-RL-Daily-Remaining", "X-RL-Daily-Reset",
        "Retry-After",
    )
    return {key: str(headers[key]) for key in keys if key in headers}


# =========================
# Nexus API client
# =========================
class NexusClient:
    """Thin client for the stable Nexus Mods v1 REST API."""

    def __init__(
        self,
        api_key: str,
        app_name: str = "TelegramNexusBot",
        app_version: str = "1.0.0",
        min_interval: float = 1.0,
        timeout: tuple[float, float] = (10.0, 30.0),
    ):
        api_key = (api_key or "").strip()
        if not api_key:
            raise NexusConfigError("NEXUS_API_KEY не задан")
        self.api_key = api_key
        self.app_name = app_name     
        self.app_version = app_version   
        self.min_interval = max(0.2, float(min_interval))
        self.timeout = timeout
        self.session = requests.Session()
        self._last_request_at = 0.0
        self.last_rate_limits: dict[str, str] = {}

    @property
    def headers(self) -> dict[str, str]:
        return {
            "apikey": self.api_key,
            "Application-Name": self.app_name,
            "Application-Version": self.app_version,
            "User-Agent": f"{self.app_name}/{self.app_version} (Telegram bot; Nexus updates)",
            "Accept": "application/json",
        }

    @property
    def app_name(self) -> str:
        return self._app_name

    @app_name.setter
    def app_name(self, value: str) -> None:
        self._app_name = (value or "TelegramNexusBot").strip()[:80]

    @property
    def app_version(self) -> str:
        return self._app_version

    @app_version.setter
    def app_version(self, value: str) -> None:
        self._app_version = (value or "1.0.0").strip()[:40]

    def _sync_request(self, method: str, path: str, **kwargs: Any) -> Any:
        now = time.monotonic()
        elapsed = now - self._last_request_at
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)

        url = API_BASE + path
        self._last_request_at = time.monotonic()
        response = self.session.request(
            method,
            url,
            headers=self.headers,
            timeout=self.timeout,
            **kwargs,
        )
        self.last_rate_limits = _read_rate_limit_headers(response.headers)

        if response.status_code == 401:
            raise NexusAuthError("Nexus API вернул 401: неверный или отозванный API-ключ")
        if response.status_code == 404:
            raise NexusNotFoundError(f"Nexus API: ресурс не найден ({path})")
        if response.status_code == 429:
            retry_after = None
            try:
                retry_after = float(response.headers.get("Retry-After", "") or 0) or None
            except ValueError:
                retry_after = None
            raise NexusRateLimitError(
                "Nexus API rate limit exceeded",
                retry_after=retry_after,
                headers=self.last_rate_limits,
            )
        if response.status_code >= 400:
            detail = response.text[:300].replace("\n", " ")
            raise NexusError(f"Nexus API HTTP {response.status_code}: {detail}")

        if not response.content:
            return None
        try:
            return response.json()
        except ValueError as exc:
            raise NexusError(f"Nexus API вернул не JSON: {response.text[:200]}") from exc

    async def request(self, method: str, path: str, **kwargs: Any) -> Any:
        return await asyncio.to_thread(self._sync_request, method, path, **kwargs)

    async def validate_key(self) -> dict[str, Any]:
        return await self.request("GET", "/v1/users/validate.json")

    async def games(self, include_unapproved: bool = False) -> list[dict[str, Any]]:
        data = await self.request(
            "GET", "/v1/games.json", params={"include_unapproved": str(include_unapproved).lower()}
        )
        return data if isinstance(data, list) else []

    async def game(self, game_domain: str) -> dict[str, Any]:
        game_domain = _game_or_raise(game_domain)
        data = await self.request("GET", f"/v1/games/{quote(game_domain)}.json")
        return data if isinstance(data, dict) else {}

    async def updated_mods(self, game_domain: str, period: str = "1d") -> list[dict[str, Any]]:
        game_domain = _game_or_raise(game_domain)
        period = period.lower()
        if period not in VALID_PERIODS:
            raise NexusError("period должен быть одним из: 1d, 1w, 1m")
        data = await self.request(
            "GET",
            f"/v1/games/{quote(game_domain)}/mods/updated.json",
            params={"period": period},
        )
        return data if isinstance(data, list) else []

    async def latest_updated(self, game_domain: str) -> list[dict[str, Any]]:
        game_domain = _game_or_raise(game_domain)
        data = await self.request("GET", f"/v1/games/{quote(game_domain)}/mods/latest_updated.json")
        return data if isinstance(data, list) else []

    async def latest_added(self, game_domain: str) -> list[dict[str, Any]]:
        game_domain = _game_or_raise(game_domain)
        data = await self.request("GET", f"/v1/games/{quote(game_domain)}/mods/latest_added.json")
        return data if isinstance(data, list) else []

    async def trending(self, game_domain: str) -> list[dict[str, Any]]:
        game_domain = _game_or_raise(game_domain)
        data = await self.request("GET", f"/v1/games/{quote(game_domain)}/mods/trending.json")
        return data if isinstance(data, list) else []

    async def mod(self, game_domain: str, mod_id: int | str) -> dict[str, Any]:
        game_domain = _game_or_raise(game_domain)
        data = await self.request("GET", f"/v1/games/{quote(game_domain)}/mods/{int(mod_id)}.json")
        return data if isinstance(data, dict) else {}

    async def files(self, game_domain: str, mod_id: int | str, category: Optional[str] = None) -> list[dict[str, Any]]:
        game_domain = _game_or_raise(game_domain)
        params = {}
        if category:
            params["category"] = category
        data = await self.request(
            "GET",
            f"/v1/games/{quote(game_domain)}/mods/{int(mod_id)}/files.json",
            params=params,
        )
        if isinstance(data, dict) and isinstance(data.get("files"), list):
            return data["files"]
        return data if isinstance(data, list) else []

    async def changelogs(self, game_domain: str, mod_id: int | str) -> dict[str, Any]:
        game_domain = _game_or_raise(game_domain)
        data = await self.request(
            "GET", f"/v1/games/{quote(game_domain)}/mods/{int(mod_id)}/changelogs.json"
        )
        return data if isinstance(data, dict) else {}

    async def download_link(self, game_domain: str, mod_id: int | str, file_id: int | str) -> list[dict[str, str]]:
        """Получает прямые ссылки на скачивание для конкретного файла мода."""
        game_domain = _game_or_raise(game_domain)
        data = await self.request(
            "GET", f"/v1/games/{quote(game_domain)}/mods/{int(mod_id)}/files/{int(file_id)}/download_link.json"
        )
        # API возвращает список словарей: [{"name": "CDN (US)", "URI": "https://..."}, ...]
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and "files" in data:
            return data["files"]
        return []


# =========================
# Watchlist storage
# =========================
@dataclass
class NexusWatchItem:
    game: str
    mod_id: int
    name: str = ""
    url: str = ""
    summary: str = ""
    version: str = ""
    latest_file_id: Optional[int] = None
    latest_file_name: str = ""
    latest_file_version: str = ""
    latest_file_uploaded: Optional[int] = None
    mod_updated_timestamp: Optional[int] = None
    added_at: int = field(default_factory=lambda: int(time.time()))
    last_checked_at: Optional[int] = None
    muted: bool = False

    @property
    def key(self) -> str:
        return f"{self.game.lower()}:{self.mod_id}"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "NexusWatchItem":
        return cls(
            game=str(data.get("game", "")).lower(),
            mod_id=int(data.get("mod_id", 0)),
            name=str(data.get("name", "")),
            url=str(data.get("url", "")),
            summary=str(data.get("summary", "")),
            version=str(data.get("version", "")),
            latest_file_id=data.get("latest_file_id"),
            latest_file_name=str(data.get("latest_file_name", "")),
            latest_file_version=str(data.get("latest_file_version", "")),
            latest_file_uploaded=data.get("latest_file_uploaded"),
            mod_updated_timestamp=data.get("mod_updated_timestamp"),
            added_at=int(data.get("added_at", time.time())),
            last_checked_at=data.get("last_checked_at"),
            muted=bool(data.get("muted", False)),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "game": self.game,
            "mod_id": self.mod_id,
            "name": self.name,
            "url": self.url,
            "summary": self.summary,
            "version": self.version,
            "latest_file_id": self.latest_file_id,
            "latest_file_name": self.latest_file_name,
            "latest_file_version": self.latest_file_version,
            "latest_file_uploaded": self.latest_file_uploaded,
            "mod_updated_timestamp": self.mod_updated_timestamp,
            "added_at": self.added_at,
            "last_checked_at": self.last_checked_at,
            "muted": self.muted,
        }


class WatchlistStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.items: dict[str, NexusWatchItem] = {}
        self.meta: dict[str, Any] = {
            "default_game": None,
            "auto_check_enabled": False,
            "auto_check_interval_minutes": 60,
            "last_auto_check": None,
        }
        self.load()

    def load(self) -> None:
        if not self.path.exists():
            return
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            self.meta.update(raw.get("meta", {}))
            for item in raw.get("items", []):
                obj = NexusWatchItem.from_dict(item)
                if obj.game and obj.mod_id:
                    self.items[obj.key] = obj
        except Exception as exc:
            logger.error("❌ Не удалось загрузить Nexus watchlist %s: %s", self.path, exc)
            self.items = {}

    def save(self) -> None:
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        payload = {
            "meta": self.meta,
            "items": [item.to_dict() for item in sorted(self.items.values(), key=lambda x: (x.game, x.mod_id))],
        }
        tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(self.path)

    def upsert(self, item: NexusWatchItem) -> None:
        self.items[item.key] = item
        self.save()

    def remove(self, game: str, mod_id: int | str) -> bool:
        key = f"{game.lower()}:{int(mod_id)}"
        existed = key in self.items
        self.items.pop(key, None)
        if existed:
            self.save()
        return existed

    def clear(self) -> None:
        self.items.clear()
        self.save()

    def list(self, game: Optional[str] = None) -> list[NexusWatchItem]:
        values = list(self.items.values())
        if game:
            values = [item for item in values if item.game == game.lower()]
        return sorted(values, key=lambda x: (x.game, x.mod_id))


# =========================
# Config/state
# =========================
_client: Optional[NexusClient] = None
_store: Optional[WatchlistStore] = None
_bot_instance: Optional[Bot] = None
_owner_id: Optional[int] = None
_watcher_task: Optional[asyncio.Task] = None
_watcher_stop = asyncio.Event()


def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def init_nexus(bot: Bot, owner_id: int) -> None:
    """Задать владельца и bot instance без запуска watcher'а."""
    global _bot_instance, _owner_id
    _bot_instance = bot
    _owner_id = int(owner_id)


def _owner_check(m: Message) -> bool:
    uid = _owner_id
    if uid is None:
        raw = os.getenv("OWNER_ID", "").strip()
        uid = int(raw) if raw.isdigit() else None
    return uid is not None and m.from_user is not None and m.from_user.id == uid


def get_client() -> NexusClient:
    global _client
    if _client is not None:
        return _client
    api_key = os.getenv("NEXUS_API_KEY", "").strip()
    if not api_key:
        raise NexusConfigError(
            "NEXUS_API_KEY не задан. Добавь ключ в .env: NEXUS_API_KEY=..."
        )
    _client = NexusClient(
        api_key=api_key,
        app_name=os.getenv("NEXUS_APP_NAME", "LoonieBot"),
        app_version=os.getenv("NEXUS_APP_VERSION", "1.0.0"),
        min_interval=env_float("NEXUS_MIN_REQUEST_INTERVAL", 1.0),
    )
    return _client


def get_store() -> WatchlistStore:
    global _store
    if _store is None:
        path = os.getenv("NEXUS_WATCHLIST_FILE", "nexus_watchlist.json")
        _store = WatchlistStore(path)
    return _store


def default_game() -> Optional[str]:
    store = get_store()
    game = store.meta.get("default_game") or os.getenv("NEXUS_DEFAULT_GAME")
    return str(game).lower() if game else None


def set_default_game(game: Optional[str]) -> None:
    store = get_store()
    store.meta["default_game"] = game.lower() if game else None
    store.save()


def auto_interval_minutes() -> int:
    store = get_store()
    env_value = env_int("NEXUS_AUTO_CHECK_MINUTES", 0)
    configured = store.meta.get("auto_check_interval_minutes")
    try:
        configured = int(configured)
    except (TypeError, ValueError):
        configured = 60
    return configured if configured > 0 else env_value


def rate_limit_text(client: Optional[NexusClient] = None) -> str:
    client = client or _client
    if not client or not client.last_rate_limits:
        return ""
    rl = client.last_rate_limits
    daily = f"{rl.get('X-RL-Daily-Remaining', '?')}/{rl.get('X-RL-Daily-Limit', '?')}"
    hourly = f"{rl.get('X-RL-Hourly-Remaining', '?')}/{rl.get('X-RL-Hourly-Limit', '?')}"
    return f"\n\n<i>Rate limit: день {daily}, час {hourly}</i>"


# =========================
# Business logic
# =========================
def _pick_latest_file(files: Iterable[dict[str, Any]]) -> Optional[dict[str, Any]]:
    candidates = list(files or [])
    if not candidates:
        return None

    def ts(item: dict[str, Any]) -> int:
        try:
            return int(item.get("uploaded_timestamp", 0) or 0)
        except (TypeError, ValueError):
            return 0

    # Primary main files first, then newest upload time.
    return max(
        candidates,
        key=lambda item: (
            bool(item.get("is_primary")),
            str(item.get("category_name", "")).upper() == "MAIN",
            ts(item),
        ),
    )


def _baseline_snapshot(mod: dict[str, Any], latest_file: Optional[dict[str, Any]]) -> dict[str, Any]:
    return {
        "version": str(mod.get("version", "") or ""),
        "mod_updated_timestamp": mod.get("updated_timestamp"),
        "latest_file_id": latest_file.get("id") if latest_file else None,
        "latest_file_name": latest_file.get("name", "") if latest_file else "",
        "latest_file_version": latest_file.get("version", "") or latest_file.get("mod_version", "") if latest_file else "",
        "latest_file_uploaded": latest_file.get("uploaded_timestamp") if latest_file else None,
    }


def _has_update(old: Optional[dict[str, Any]], new: dict[str, Any]) -> bool:
    if not old:
        return False
    keys = (
        "version",
        "mod_updated_timestamp",
        "latest_file_id",
        "latest_file_name",
        "latest_file_version",
        "latest_file_uploaded",
    )
    return any(str(old.get(key, "")) != str(new.get(key, "")) for key in keys)


async def check_mod_update(game: str, mod_id: int | str, baseline: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    """Fetches current mod state and compares it with a saved baseline."""
    client = get_client()
    game = _game_or_raise(game)
    mod_id = int(mod_id)

    mod = await client.mod(game, mod_id)
    if not mod:
        raise NexusNotFoundError(f"Мод {mod_id} не найден в игре {game}")

    files = await client.files(game, mod_id)
    latest_file = _pick_latest_file(files)
    current = _baseline_snapshot(mod, latest_file)
    update_available = _has_update(baseline, current)

    changed_fields: list[str] = []
    if baseline:
        labels = {
            "version": "версия мода",
            "mod_updated_timestamp": "дата обновления мода",
            "latest_file_id": "ID файла",
            "latest_file_name": "имя файла",
            "latest_file_version": "версия файла",
            "latest_file_uploaded": "дата загрузки файла",
        }
        changed_fields = [labels[key] for key in labels if str(baseline.get(key, "")) != str(current.get(key, ""))]

    return {
        "ok": True,
        "game": game,
        "mod_id": mod_id,
        "mod": mod,
        "files": files,
        "latest_file": latest_file,
        "current": current,
        "update_available": update_available,
        "is_new": not bool(baseline),
        "changed_fields": changed_fields,
        "url": mod.get("mod_page_uri") or mod.get("url") or _mod_url(game, mod_id),
    }


def apply_snapshot(item: NexusWatchItem, result: dict[str, Any]) -> bool:
    """Updates item baseline. Returns True if update was detected."""
    current = result.get("current", {})
    update_available = bool(result.get("update_available"))
    item.name = str(result.get("mod", {}).get("name", item.name))
    item.url = str(result.get("url", item.url))
    item.summary = str(result.get("mod", {}).get("summary", item.summary) or "")
    item.version = str(current.get("version", ""))
    item.latest_file_id = current.get("latest_file_id")
    item.latest_file_name = str(current.get("latest_file_name", ""))
    item.latest_file_version = str(current.get("latest_file_version", ""))
    item.latest_file_uploaded = current.get("latest_file_uploaded")
    item.mod_updated_timestamp = current.get("mod_updated_timestamp")
    item.last_checked_at = int(time.time())
    return update_available


# =========================
# Formatting
# =========================
def format_mod_info(result: dict[str, Any], include_files: bool = False) -> str:
    mod = result.get("mod", {})
    latest = result.get("latest_file") or {}
    
    lines = [
        f"🎮 <b><a href=\"{_safe(result.get('url'))}\">{_safe(mod.get('name', 'Неизвестный мод'))}</a></b>",
        f"🆔 <code>{result.get('game')} / {result.get('mod_id')}</code>",
        "┅" * 20
    ]
    
    if mod.get("version"):
        lines.append(f"🏷️ <b>Версия мода:</b> {_safe(mod.get('version'))}")
    
    if mod.get("summary"):
        lines.append(f"📝 <i>{_safe(str(mod.get('summary'))[:300])}</i>")
        
    lines.extend([
        "",
        f"🔄 <b>Обновлён:</b> {_format_ts(mod.get('updated_timestamp'))}",
        f"⬇️ <b>Загрузок:</b> {int(mod.get('mod_downloads', 0) or 0):,} | 👍 <b>Одобрений:</b> {int(mod.get('endorsements', 0) or 0):,}"
    ])
    
    if result.get("update_available"):
        fields = ", ".join(result.get("changed_fields", [])) or "есть изменения"
        lines.extend(["", f"🔔 <b>Есть обновление!</b> Изменилось: {_safe(fields)}"])
    elif result.get("is_new"):
        lines.extend(["", "🆕 <i>Мод добавлен в список наблюдения.</i>"])
    else:
        lines.extend(["", "✅ Обновлений не найдено."])
        
    if latest:
        lines.extend([
            "",
            "┅" * 20,
            f"📦 <b>Последний файл:</b> {_safe(latest.get('name', '-'))}",
            f"🗂️ <b>Категория:</b> {_safe(latest.get('category_name', '-'))}",
            f"⏫ <b>Загружен:</b> {_format_ts(latest.get('uploaded_timestamp'))}"
        ])
        if latest.get("version") or latest.get("mod_version"):
            lines.append(f"🏷️ <b>Версия файла:</b> {_safe(latest.get('version') or latest.get('mod_version'))}")
            
    if include_files and result.get("files"):
        lines.extend(["", "┅" * 20, "<b>Последние файлы:</b>"])
        files = sorted(
            result["files"],
            key=lambda f: int(f.get("uploaded_timestamp", 0) or 0),
            reverse=True,
        )[:5]
        for file_item in files:
            lines.append(
                f"• <code>{file_item.get('id')}</code> {_safe(file_item.get('name', '-'))} "
                f"({_safe(file_item.get('category_name', '-'))}, {_format_ts(file_item.get('uploaded_timestamp'))})"
            )
            
    return "\n".join(lines)


def format_watch_item(item: NexusWatchItem) -> str:
    title = item.name or f"Mod {item.mod_id}"
    lines = [
        f"🎮 <a href=\"{_safe(item.url or _mod_url(item.game, item.mod_id))}\">{_safe(title)}</a>",
        f"🆔 <code>{item.game} / {item.mod_id}</code>",
        f"🏷️ Версия: <b>{_safe(item.version or '-')}</b>",
        f"📦 Файл: <code>{item.latest_file_id or '-'}</code> {_safe(item.latest_file_name or '-')}",
        f"🔄 Обновлён: <b>{_format_ts(item.mod_updated_timestamp)}</b>",
        f"🕒 Проверен: <b>{_format_ts(item.last_checked_at)}</b>",
    ]
    return "\n".join(lines)

def _build_mod_keyboard(result: dict[str, Any], download_url: Optional[str] = None) -> InlineKeyboardMarkup:
    """Строит инлайн-клавиатуру для сообщения о моде.
    Если есть прямая ссылка на скачивание (Premium) — добавляет её первой кнопкой.
    Всегда добавляет кнопки на страницу мода и вкладку файлов.
    """
    game = result.get("game", "")
    mod_id = result.get("mod_id")
    mod_url = result.get("url") or _mod_url(game, mod_id)
    files_url = f"{mod_url}?tab=files"
    
    rows: list[list[InlineKeyboardButton]] = []
    
    # Прямая ссылка на скачивание (работает только с Premium)
    if download_url:
        rows.append([InlineKeyboardButton(text="📥 Скачать последнюю версию", url=download_url)])
    
    # Всегда работающие кнопки
    rows.append([
        InlineKeyboardButton(text="🌐 Страница мода", url=mod_url),
        InlineKeyboardButton(text="📦 Все файлы", url=files_url),
    ])
    
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _try_get_download_url(game: str, mod_id: int, latest_file: Optional[dict[str, Any]]) -> Optional[str]:
    """Пытается получить прямую CDN-ссылку. Возвращает None, если не удалось (например, нет Premium)."""
    if not latest_file or not latest_file.get("id"):
        return None
    try:
        links = await get_client().download_link(game, mod_id, latest_file["id"])
        if links and isinstance(links, list) and len(links) > 0:
            return links[0].get("URI")
    except Exception as e:
        logger.debug("Прямая ссылка недоступна: %s", e)
    return None

# =========================
# Bot commands
# =========================
router = Router(name="nexus")


def _owner_check(m: Message) -> bool:
    return _owner_id is not None and m.from_user.id == _owner_id


async def _reply_env_error(m: Message, exc: Exception) -> None:
    await m.answer(f"⚠️ Nexus: <i>{_safe(exc)}</i>", parse_mode="HTML")


@router.message(Command("nexushelp"))
async def cmd_nexus_help(m: Message):
    if not _owner_check(m):
        return
    text = """
<b>Nexus Mods команды (только владелец):</b>

<code>/nexusauth</code> — проверить API-ключ
<code>/nexusgame [domain]</code> — показать/задать игру по умолчанию
<code>/nexusadd &lt;id или ссылка&gt;</code> — добавить мод в watchlist
<code>/nexusdel &lt;id|all&gt;</code> — удалить мод или очистить список
<code>/nexuslist</code> — список отслеживаемых модов
<code>/nexuscheck [all|id|ссылка]</code> — проверить обновления
<code>/nexusinfo &lt;id|ссылка&gt;</code> — информация о моде
<code>/nexusfiles &lt;id|ссылка&gt;</code> — информация + последние файлы
<code>/nexuslatest [game]</code> — последние обновлённые моды игры
<code>/nexusadded [game]</code> — последние добавленные моды игры
<code>/nexustrending [game]</code> — трендовые моды игры
<code>/nexusauto [on|off|минуты]</code> — авто-проверка watchlist

Примеры:
<code>/nexusgame mountandblade2bannerlord</code>
<code>/nexusadd https://www.nexusmods.com/mountandblade2bannerlord/mods/1234</code>
<code>/nexuscheck all</code>
<code>/nexusauto 120</code>
""".strip()
    await m.answer(text, parse_mode="HTML", disable_web_page_preview=True)


@router.message(Command("nexusauth"))
async def cmd_nexus_auth(m: Message):
    if not _owner_check(m):
        return
    try:
        data = await get_client().validate_key()
        username = data.get("name") or data.get("username") or "пользователь"
        is_premium = data.get("is_premium")
        lines = [
            "✅ <b>Nexus API-ключ действителен</b>",
            f"👤 Пользователь: <b>{_safe(username)}</b>",
        ]
        if is_premium is not None:
            lines.append(f"💎 Premium: <b>{'да' if is_premium else 'нет'}</b>")
        lines.append(rate_limit_text())
        await m.answer("\n".join(lines), parse_mode="HTML", disable_web_page_preview=True)
    except Exception as exc:
        await _reply_env_error(m, exc)


@router.message(Command("nexusgame"))
async def cmd_nexus_game(m: Message):
    if not _owner_check(m):
        return
    parts = m.text.split(maxsplit=1)
    if len(parts) == 1:
        game = default_game()
        await m.answer(
            f"🎮 Игра по умолчанию: <code>{_safe(game or 'не задана')}</code>\n"
            f"<i>Задать: /nexusgame mountandblade2bannerlord</i>",
            parse_mode="HTML",
        )
        return
    game = _game_or_raise(parts[1])
    try:
        info = await get_client().game(game)
        name = info.get("name", game)
        set_default_game(game)
        await m.answer(
            f"✅ Игра по умолчанию: <b>{_safe(name)}</b> <code>{_safe(game)}</code>",
            parse_mode="HTML",
        )
    except Exception as exc:
        await _reply_env_error(m, exc)


@router.message(Command("nexusadd"))
async def cmd_nexus_add(m: Message):
    if not _owner_check(m):
        return
    parts = m.text.split(maxsplit=1)
    if len(parts) < 2:
        await m.answer("⚠️ Используй: <code>/nexusadd &lt;id или ссылка&gt;</code>", parse_mode="HTML")
        return

    game, mod_id = parse_mod_reference(parts[1], default_game())
    if not mod_id:
        await m.answer("⚠️ Не смог определить ID мода. Пришли ссылку вида <code>.../mods/12345</code>", parse_mode="HTML")
        return
    try:
        result = await check_mod_update(game, mod_id)
        item = NexusWatchItem(game=result["game"], mod_id=result["mod_id"])
        apply_snapshot(item, result)
        get_store().upsert(item)
        await m.answer(
            "✅ <b>Мод добавлен в watchlist</b>\n\n" + format_watch_item(item),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception as exc:
        await _reply_env_error(m, exc)


@router.message(Command("nexusdel"))
async def cmd_nexus_del(m: Message):
    if not _owner_check(m):
        return
    parts = m.text.split(maxsplit=1)
    if len(parts) < 2:
        await m.answer("⚠️ Используй: <code>/nexusdel &lt;id|all&gt;</code>", parse_mode="HTML")
        return

    arg = parts[1].strip().lower()
    if arg == "all":
        count = len(get_store().items)
        get_store().clear()
        await m.answer(f"🗑️ Watchlist очищен. Удалено модов: <b>{count}</b>", parse_mode="HTML")
        return

    game, mod_id = parse_mod_reference(arg, default_game())
    if not game or not mod_id:
        await m.answer("⚠️ Не смог определить игру и ID мода.", parse_mode="HTML")
        return
    removed = get_store().remove(game, mod_id)
    if removed:
        await m.answer(f"🗑️ Удалён из watchlist: <code>{_safe(game)} / {mod_id}</code>", parse_mode="HTML")
    else:
        await m.answer("⚠️ Такого мода нет в watchlist.", parse_mode="HTML")


@router.message(Command("nexuslist"))
async def cmd_nexus_list(m: Message):
    if not _owner_check(m):
        return
    items = get_store().list(default_game()) if default_game() else get_store().list()
    if not items:
        await m.answer("ℹ️ Watchlist пуст. Добавь мод: <code>/nexusadd &lt;id или ссылка&gt;</code>", parse_mode="HTML")
        return

    chunks: list[str] = []
    current = f"🎮 <b>Nexus watchlist ({len(items)}):</b>\n\n"
    for item in items:
        block = format_watch_item(item) + "\n\n"
        if len(current) + len(block) > 3800:
            chunks.append(current.rstrip())
            current = ""
        current += block
    if current:
        chunks.append(current.rstrip())

    for i, chunk in enumerate(chunks, 1):
        if len(chunks) > 1:
            chunk = f"<i>({i}/{len(chunks)})</i>\n" + chunk
        await m.answer(chunk, parse_mode="HTML", disable_web_page_preview=True)


async def _check_one(game: str, mod_id: int) -> tuple[dict[str, Any], NexusWatchItem]:
    store = get_store()
    key = f"{game.lower()}:{int(mod_id)}"
    old_item = store.items.get(key)
    baseline = old_item.to_dict() if old_item else None
    result = await check_mod_update(game, mod_id, baseline=baseline)
    item = old_item or NexusWatchItem(game=result["game"], mod_id=result["mod_id"])
    apply_snapshot(item, result)
    store.upsert(item)
    return result, item


@router.message(Command("nexuscheck"))
async def cmd_nexus_check(m: Message):
    if not _owner_check(m):
        return
    parts = m.text.split(maxsplit=1)
    arg = parts[1].strip() if len(parts) > 1 else "all"
    store = get_store()

    # Одиночная проверка конкретного мода
    if arg.lower() != "all":
        game, mod_id = parse_mod_reference(arg, default_game())
        if not mod_id:
            await m.answer(
                "⚠️ Используй: <code>/nexuscheck all</code> или <code>/nexuscheck &lt;id|ссылка&gt;</code>",
                parse_mode="HTML",
            )
            return
        try:
            result, _ = await _check_one(game, mod_id)
            latest = result.get("latest_file")
            download_url = await _try_get_download_url(game, mod_id, latest)
            keyboard = _build_mod_keyboard(result, download_url=download_url)
            await m.answer(
                format_mod_info(result),
                parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=keyboard,
            )
        except Exception as exc:
            await _reply_env_error(m, exc)
        return

    # Массовая проверка всего watchlist
    items = store.list(default_game()) if default_game() else store.list()
    if not items:
        await m.answer("ℹ️ Watchlist пуст.", parse_mode="HTML")
        return

    status = await m.answer(f"🔍 Проверяю Nexus watchlist ({len(items)} модов)...", parse_mode="HTML")
    updated: list[dict[str, Any]] = []
    errors: list[str] = []

    for item in items:
        try:
            result, _ = await _check_one(item.game, item.mod_id)
            if result.get("update_available") and not item.muted:
                updated.append(result)
        except Exception as exc:
            logger.warning("⚠️ Nexus check failed for %s/%s: %s", item.game, item.mod_id, exc)
            errors.append(f"{item.game}/{item.mod_id}: {exc}")

    get_store().save()

    lines = [f"✅ Проверка завершена. Модов: <b>{len(items)}</b>."]
    if updated:
        lines.append(f"🔔 Обновилось: <b>{len(updated)}</b>.")
    if errors:
        lines.append(f"⚠️ Ошибок: <b>{len(errors)}</b>.")
    lines.append(rate_limit_text())
    await status.edit_text("\n".join(lines), parse_mode="HTML")

    # Отправляем каждое обновление с кнопками
    for result in updated:
        latest = result.get("latest_file")
        download_url = await _try_get_download_url(result["game"], result["mod_id"], latest)
        keyboard = _build_mod_keyboard(result, download_url=download_url)
        await m.answer(
            "🔔 <b>Обновление Nexus Mods</b>\n\n" + format_mod_info(result),
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=keyboard,
        )

    if errors:
        preview = "\n".join(errors[:5])
        await m.answer(f"⚠️ <b>Ошибки проверки:</b>\n<code>{_safe(preview)}</code>", parse_mode="HTML")


@router.message(Command("nexusinfo", "nexusfiles"))
async def cmd_nexus_info(m: Message):
    if not _owner_check(m):
        return
    parts = m.text.split(maxsplit=1)
    if len(parts) < 2:
        await m.answer("⚠️ Используй: <code>/nexusinfo <id|ссылка></code>", parse_mode="HTML")
        return
        
    game, mod_id = parse_mod_reference(parts[1], default_game())
    if not mod_id:
        await m.answer("⚠️ Не смог определить ID мода.", parse_mode="HTML")
        return
        
    try:
        status = await m.answer("⏳ Запрашиваю информацию у Nexus Mods...", parse_mode="HTML")
        result = await check_mod_update(game, mod_id)
        include_files = m.text.startswith("/nexusfiles")
        latest = result.get("latest_file")
        
        # Пытаемся получить прямую ссылку (работает только с Premium)
        download_url = await _try_get_download_url(game, mod_id, latest)
        
        # Строим клавиатуру: всегда есть кнопки на страницу и файлы,
        # а если Premium — сверху кнопка прямого скачивания
        keyboard = _build_mod_keyboard(result, download_url=download_url)
        
        await status.edit_text(
            format_mod_info(result, include_files=include_files),
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=keyboard,
        )
    except Exception as exc:
        await _reply_env_error(m, exc)


async def _send_mod_list(m: Message, title: str, game: str, mods: list[dict[str, Any]]) -> None:
    if not mods:
        await m.answer(f"ℹ️ {title}: список пуст.", parse_mode="HTML")
        return
    lines = [f"🎮 <b>{title}</b> <code>{_safe(game)}</code>\n"]
    for mod in mods[:10]:
        mod_id = mod.get("mod_id") or mod.get("id") or "?"
        name = mod.get("name", f"Mod {mod_id}")
        lines.append(f"• <a href=\"{_mod_url(game, mod_id)}\">{_safe(name)}</a> <code>{mod_id}</code>")
        if mod.get("version"):
            lines[-1] += f" — <i>{_safe(mod.get('version'))}</i>"
        if mod.get("summary"):
            lines.append(f"  <i>{_safe(str(mod.get('summary'))[:120])}</i>")
    lines.append(rate_limit_text())
    await m.answer("\n".join(lines), parse_mode="HTML", disable_web_page_preview=True)


@router.message(Command("nexuslatest"))
async def cmd_nexus_latest(m: Message):
    if not _owner_check(m):
        return
    parts = m.text.split(maxsplit=1)
    game = _game_or_raise(parts[1]) if len(parts) > 1 else default_game()
    try:
        mods = await get_client().latest_updated(game)
        await _send_mod_list(m, "Последние обновлённые моды", game, mods)
    except Exception as exc:
        await _reply_env_error(m, exc)


@router.message(Command("nexusadded"))
async def cmd_nexus_added(m: Message):
    if not _owner_check(m):
        return
    parts = m.text.split(maxsplit=1)
    game = _game_or_raise(parts[1]) if len(parts) > 1 else default_game()
    try:
        mods = await get_client().latest_added(game)
        await _send_mod_list(m, "Последние добавленные моды", game, mods)
    except Exception as exc:
        await _reply_env_error(m, exc)


@router.message(Command("nexustrending"))
async def cmd_nexus_trending(m: Message):
    if not _owner_check(m):
        return
    parts = m.text.split(maxsplit=1)
    game = _game_or_raise(parts[1]) if len(parts) > 1 else default_game()
    try:
        mods = await get_client().trending(game)
        await _send_mod_list(m, "Трендовые моды", game, mods)
    except Exception as exc:
        await _reply_env_error(m, exc)


# =========================
# Automatic watcher
# =========================
def _watcher_enabled() -> bool:
    if os.getenv("NEXUS_AUTO_CHECK_MINUTES", "").strip() == "0":
        return False
    return bool(get_store().meta.get("auto_check_enabled", False)) or auto_interval_minutes() > 0


async def _watcher_loop() -> None:
    logger.info("▶️ Nexus watcher started")
    while not _watcher_stop.is_set():
        try:
            interval = auto_interval_minutes()
            if interval <= 0 or not _bot_instance or not _owner_id:
                await asyncio.sleep(30)
                continue

            items = get_store().list()
            for item in items:
                if _watcher_stop.is_set():
                    break
                try:
                    result, _ = await _check_one(item.game, item.mod_id)
                    if result.get("update_available") and not item.muted:
                        # Пытаемся получить прямую ссылку (работает только с Premium)
                        latest = result.get("latest_file")
                        download_url = await _try_get_download_url(item.game, item.mod_id, latest)
                        # Строим клавиатуру: всегда есть кнопки на страницу и файлы,
                        # а если Premium — сверху кнопка прямого скачивания
                        keyboard = _build_mod_keyboard(result, download_url=download_url)
                        await _bot_instance.send_message(
                            _owner_id,
                            "🔔 <b>Nexus Mods: найдено обновление</b>\n\n" + format_mod_info(result),
                            parse_mode="HTML",
                            disable_web_page_preview=True,
                            reply_markup=keyboard,
                        )
                        await asyncio.sleep(0.5)
                except Exception as exc:
                    logger.warning("⚠️ Nexus watcher failed for %s/%s: %s", item.game, item.mod_id, exc)
            get_store().meta["last_auto_check"] = int(time.time())
            get_store().save()
            logger.info("✅ Nexus watcher check finished: %s mods", len(items))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("❌ Nexus watcher error: %s", exc)
        try:
            await asyncio.wait_for(_watcher_stop.wait(), timeout=max(60, auto_interval_minutes() * 60))
        except asyncio.TimeoutError:
            pass
    logger.info("⏹️ Nexus watcher stopped")


async def start_nexus_watcher(bot: Bot, owner_id: int, log: Optional[logging.Logger] = None) -> None:
    """Starts the automatic watcher. Safe to call even when NEXUS_API_KEY is missing."""
    global _bot_instance, _owner_id, _watcher_task
    if log is not None:
        # Make module logs go through bot logger as well if desired.
        pass
    _bot_instance = bot
    _owner_id = int(owner_id)

    if not os.getenv("NEXUS_API_KEY"):
        logger.warning("⚠️ NEXUS_API_KEY не задан — Nexus watcher не запущен")
        return
    if not _watcher_enabled():
        logger.info("ℹ️ Nexus watcher отключён (NEXUS_AUTO_CHECK_MINUTES=0 или auto_check_enabled=false)")
        return
    if _watcher_task is None or _watcher_task.done():
        _watcher_stop.clear()
        _watcher_task = asyncio.create_task(_watcher_loop())


async def stop_nexus_watcher() -> None:
    global _watcher_task
    _watcher_stop.set()
    if _watcher_task and not _watcher_task.done():
        _watcher_task.cancel()
        try:
            await _watcher_task
        except asyncio.CancelledError:
            pass
    _watcher_task = None


@router.message(Command("nexusauto"))
async def cmd_nexus_auto(m: Message):
    if not _owner_check(m):
        return
    store = get_store()
    parts = m.text.split(maxsplit=1)
    arg = parts[1].strip().lower() if len(parts) > 1 else ""

    if not arg:
        enabled = bool(store.meta.get("auto_check_enabled", False))
        interval = auto_interval_minutes()
        last = _format_ts(store.meta.get("last_auto_check"))
        state = "включена" if enabled and interval > 0 else "выключена"
        await m.answer(
            f"⚙️ <b>Nexus auto-check:</b> {state}\n"
            f"⏱️ Интервал: <b>{interval}</b> мин.\n"
            f"🕒 Последняя проверка: <b>{last}</b>\n\n"
            f"<i>Управление: /nexusauto on | /nexusauto off | /nexusauto 120</i>",
            parse_mode="HTML",
        )
        return

    if arg in {"on", "вкл", "включить"}:
        interval = auto_interval_minutes() or 60
        store.meta["auto_check_enabled"] = True
        store.meta["auto_check_interval_minutes"] = interval
        store.save()
        await start_nexus_watcher(_bot_instance, _owner_id) if _bot_instance else None
        await m.answer(f"✅ Nexus auto-check включена. Интервал: <b>{interval}</b> мин.", parse_mode="HTML")
        return

    if arg in {"off", "выкл", "выключить"}:
        store.meta["auto_check_enabled"] = False
        store.save()
        await stop_nexus_watcher()
        await m.answer("⏹️ Nexus auto-check выключена.", parse_mode="HTML")
        return

    if arg.isdigit():
        interval = int(arg)
        if interval < 5:
            await m.answer("⚠️ Интервал должен быть не меньше 5 минут — у Nexus есть rate limit.", parse_mode="HTML")
            return
        store.meta["auto_check_enabled"] = True
        store.meta["auto_check_interval_minutes"] = interval
        store.save()
        await start_nexus_watcher(_bot_instance, _owner_id) if _bot_instance else None
        await m.answer(f"✅ Nexus auto-check включена. Интервал: <b>{interval}</b> мин.", parse_mode="HTML")
        return

    await m.answer("⚠️ Используй: <code>/nexusauto on|off|минуты</code>", parse_mode="HTML")