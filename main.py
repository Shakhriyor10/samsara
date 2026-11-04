"""Telegram notifications for Samsara harsh-braking safety events."""

from __future__ import annotations

import asyncio
import html
import logging
import os
import shutil
import sqlite3
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramNetworkError, TelegramRetryAfter, TelegramServerError
from aiogram.filters import Command
from aiogram.types import BotCommand, BufferedInputFile, CallbackQuery, ChatMemberUpdated, InlineKeyboardButton, InlineKeyboardMarkup, Message, WebAppInfo
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))
INITIAL_LOOKBACK_MINUTES = int(os.getenv("INITIAL_LOOKBACK_MINUTES", "10"))
# Display all event times in New York time, including automatic notices and history.
EVENT_TIMEZONE = "America/New_York"
HISTORY_LOOKBACK_DAYS = int(os.getenv("HISTORY_LOOKBACK_DAYS", "6"))
_state_db_config = Path(os.getenv("STATE_DB", "samsara_bot.sqlite3").strip())
STATE_DB = str(_state_db_config if _state_db_config.is_absolute() else Path(__file__).resolve().parent / _state_db_config)
API_URL = "https://api.samsara.com/safety-events/stream"
VEHICLE_STATS_URL = "https://api.samsara.com/fleet/vehicles/stats"
VEHICLE_STATS_HISTORY_URL = "https://api.samsara.com/fleet/vehicles/stats/history"
DRIVER_ASSIGNMENTS_URL = "https://api.samsara.com/fleet/driver-vehicle-assignments"
DRIVER_URL = "https://api.samsara.com/fleet/drivers"
MAX_TELEGRAM_VIDEO_BYTES = 50_000_000
MAX_SOURCE_VIDEO_BYTES = 200 * 1024 * 1024
VIDEO_TELEMATICS_OVERLAY = os.getenv("VIDEO_TELEMATICS_OVERLAY", "true").strip().lower() in {"1", "true", "yes", "on"}
WEBAPP_URL = os.getenv("WEBAPP_URL", "").strip()
WEBAPP_HOST = os.getenv("WEBAPP_HOST", "127.0.0.1").strip()
WEBAPP_PORT = int(os.getenv("WEBAPP_PORT", "8080"))
HISTORY_TRUCKS: dict[int, list[dict[str, Any]]] = {}
HISTORY_EVENTS: dict[tuple[int, str, str], dict[str, Any]] = {}
HISTORY_EVENT_LISTS: dict[int, list[dict[str, Any]]] = {}
HISTORY_SELECTED_TRUCK: dict[int, str] = {}
HISTORY_TRUCK_PAGE: dict[int, int] = {}
SETUP_SESSIONS: dict[int, dict[str, Any]] = {}

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("samsara_bot")


async def telegram_call_with_retry(method: Any, *args: Any, **kwargs: Any) -> Any:
    """Retry Telegram flood-control responses using the server-provided delay."""
    for attempt in range(3):
        try:
            return await method(*args, **kwargs)
        except TelegramRetryAfter as error:
            if attempt == 2:
                raise
            delay = max(float(error.retry_after), 1.0) + 0.5
            logger.warning("Telegram flood control; retrying %s in %.1f seconds", getattr(method, "__name__", "request"), delay)
            await asyncio.sleep(delay)
        except (TelegramNetworkError, TelegramServerError):
            if attempt == 2:
                raise
            delay = float(attempt + 1)
            logger.warning("Temporary Telegram error; retrying %s in %.1f seconds", getattr(method, "__name__", "request"), delay, exc_info=True)
            await asyncio.sleep(delay)


async def edit_text_with_retry(message: Message, text: str, **kwargs: Any) -> Any:
    return await telegram_call_with_retry(message.edit_text, text, **kwargs)


async def edit_reply_markup_with_retry(message: Message, **kwargs: Any) -> Any:
    return await telegram_call_with_retry(message.edit_reply_markup, **kwargs)


@dataclass(frozen=True)
class SamsaraAccount:
    """Credentials for one Samsara organization."""

    account_id: str
    api_key: str


ACCOUNTS: tuple[SamsaraAccount, ...] = ()


def account_label(account_id: str) -> str:
    return {"1": "U.Z. Freight", "2": "Safe & Secure"}.get(account_id, f"Samsara {account_id}")


def configured_accounts() -> tuple[SamsaraAccount, ...]:
    primary_key = os.getenv("API_KEY", "").strip()
    secondary_key = os.getenv("API_KEY2", "").strip()
    if not primary_key:
        raise RuntimeError("Set API_KEY in .env")
    accounts = [SamsaraAccount("1", primary_key)]
    if secondary_key:
        accounts.append(SamsaraAccount("2", secondary_key))
    return tuple(accounts)


def utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def init_state() -> None:
    with sqlite3.connect(STATE_DB) as conn:
        # WAL lets the bot and the Mini App API read while the other writes,
        # and makes each small commit cheaper. The setting persists in the file.
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE IF NOT EXISTS sent_events (event_id TEXT PRIMARY KEY, sent_at TEXT NOT NULL)")
        conn.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        conn.execute("CREATE TABLE IF NOT EXISTS admins (user_id INTEGER PRIMARY KEY, added_at TEXT NOT NULL)")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS group_trucks (chat_id INTEGER NOT NULL, account_id TEXT NOT NULL, "
            "truck_id TEXT NOT NULL, truck_name TEXT NOT NULL, PRIMARY KEY(chat_id, account_id, truck_id))"
        )
        # Telegram has no "list my chats" method, so the bot remembers the groups it is in.
        conn.execute(
            "CREATE TABLE IF NOT EXISTS bot_groups (chat_id INTEGER PRIMARY KEY, title TEXT NOT NULL, "
            "chat_type TEXT NOT NULL, is_member INTEGER NOT NULL DEFAULT 1, updated_at TEXT NOT NULL)"
        )
        conn.execute(
            "INSERT OR IGNORE INTO bot_groups(chat_id, title, chat_type, is_member, updated_at) "
            "SELECT DISTINCT chat_id, CAST(chat_id AS TEXT), 'supergroup', 1, ? FROM group_trucks",
            (utc_iso(datetime.now(timezone.utc)),),
        )
    with sqlite3.connect(STATE_DB) as conn:
        for user_id in bootstrap_admin_ids():
            conn.execute("INSERT OR IGNORE INTO admins(user_id, added_at) VALUES(?, ?)", (user_id, utc_iso(datetime.now(timezone.utc))))
    reset_admin_cache()


def bootstrap_admin_ids() -> set[int]:
    values = os.getenv("BOT_ADMIN_IDS", "").split(",")
    result: set[int] = set()
    for value in values:
        value = value.strip()
        if value:
            try:
                result.add(int(value))
            except ValueError as error:
                raise RuntimeError("BOT_ADMIN_IDS must contain comma-separated Telegram IDs") from error
    return result


_ADMIN_IDS: frozenset[int] | None = None


def is_admin(user_id: int | None) -> bool:
    # Checked on every command and Mini App request; the admins table changes
    # only through /add_admin and /remove_admin, which reset this cache.
    global _ADMIN_IDS
    if user_id is None:
        return False
    if _ADMIN_IDS is None:
        with sqlite3.connect(STATE_DB) as conn:
            _ADMIN_IDS = frozenset(row[0] for row in conn.execute("SELECT user_id FROM admins"))
    return user_id in _ADMIN_IDS


def reset_admin_cache() -> None:
    global _ADMIN_IDS
    _ADMIN_IDS = None


def is_linked_driver(user_id: int | None) -> bool:
    if user_id is None:
        return False
    try:
        with sqlite3.connect(STATE_DB) as conn:
            return conn.execute(
                "SELECT 1 FROM driver_accounts WHERE telegram_user_id = ?", (user_id,)
            ).fetchone() is not None
    except sqlite3.OperationalError:
        return False


_KNOWN_BOT_GROUPS: dict[int, tuple[str, str, bool]] = {}


def remember_bot_group(chat_id: int, title: str | None, chat_type: str, is_member: bool) -> None:
    """Record a group the bot belongs to (or has left) so the Mini App can offer it in a picker."""
    # Called for every group message: write only when something changed.
    state = ((title or str(chat_id))[:255], chat_type, is_member)
    if _KNOWN_BOT_GROUPS.get(chat_id) == state:
        return
    _KNOWN_BOT_GROUPS[chat_id] = state
    with sqlite3.connect(STATE_DB) as conn:
        conn.execute(
            "INSERT INTO bot_groups(chat_id, title, chat_type, is_member, updated_at) VALUES(?, ?, ?, ?, ?) "
            "ON CONFLICT(chat_id) DO UPDATE SET title=excluded.title, chat_type=excluded.chat_type, "
            "is_member=excluded.is_member, updated_at=excluded.updated_at",
            (chat_id, state[0], chat_type, 1 if is_member else 0, utc_iso(datetime.now(timezone.utc))),
        )


def forget_bot_groups(chat_ids: list[int]) -> None:
    """Drop cached group state after the Mini App marks groups as left."""
    for chat_id in chat_ids:
        _KNOWN_BOT_GROUPS.pop(chat_id, None)


async def bot_membership_changed(update: ChatMemberUpdated) -> None:
    chat = update.chat
    if chat.type not in {"group", "supergroup"}:
        return
    is_member = update.new_chat_member.status in {"member", "administrator", "restricted"}
    remember_bot_group(chat.id, chat.title, chat.type, is_member)
    logger.info("Bot %s group %s (%s)", "joined" if is_member else "left", chat.id, chat.title)


async def track_group_messages(handler, event: Message, data: dict[str, Any]):
    """Outer middleware: any message the bot sees in a group keeps that group in the picker."""
    try:
        chat = event.chat
        if chat.type in {"group", "supergroup"}:
            if event.migrate_to_chat_id:
                remember_bot_group(chat.id, chat.title, chat.type, False)
                remember_bot_group(event.migrate_to_chat_id, chat.title, "supergroup", True)
            elif event.left_chat_member is None or event.left_chat_member.id != event.bot.id:
                remember_bot_group(chat.id, chat.title, chat.type, True)
    except Exception:
        logger.exception("Could not record Telegram group %s", getattr(event.chat, "id", "?"))
    return await handler(event, data)


def configured_trucks(chat_id: int, account_id: str | None = None) -> list[dict[str, str]]:
    query = "SELECT account_id, truck_id, truck_name FROM group_trucks WHERE chat_id = ?"
    values: tuple[Any, ...] = (chat_id,)
    if account_id is not None:
        query += " AND account_id = ?"
        values += (account_id,)
    with sqlite3.connect(STATE_DB) as conn:
        rows = conn.execute(query + " ORDER BY account_id, truck_name", values).fetchall()
    return [{"account_id": row[0], "id": row[1], "name": row[2]} for row in rows]


def save_group_trucks(chat_id: int, account_id: str, trucks: list[dict[str, Any]]) -> None:
    with sqlite3.connect(STATE_DB) as conn:
        conn.execute("DELETE FROM group_trucks WHERE chat_id = ? AND account_id = ?", (chat_id, account_id))
        conn.executemany(
            "INSERT INTO group_trucks(chat_id, account_id, truck_id, truck_name) VALUES(?, ?, ?, ?)",
            [(chat_id, account_id, str(item["id"]), str(item.get("name") or item["id"])) for item in trucks],
        )


def get_checkpoint(account: SamsaraAccount) -> datetime:
    key = "checkpoint" if account.account_id == "1" else f"checkpoint:{account.account_id}"
    with sqlite3.connect(STATE_DB) as conn:
        row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    if row:
        return datetime.fromisoformat(row[0].replace("Z", "+00:00"))
    return datetime.now(timezone.utc) - timedelta(minutes=INITIAL_LOOKBACK_MINUTES)


def save_checkpoint(account: SamsaraAccount, value: datetime) -> None:
    key = "checkpoint" if account.account_id == "1" else f"checkpoint:{account.account_id}"
    with sqlite3.connect(STATE_DB) as conn:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, utc_iso(value)),
        )


def was_sent(event_id: str) -> bool:
    with sqlite3.connect(STATE_DB) as conn:
        return conn.execute("SELECT 1 FROM sent_events WHERE event_id = ?", (event_id,)).fetchone() is not None


def mark_sent(event_id: str) -> None:
    with sqlite3.connect(STATE_DB) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO sent_events(event_id, sent_at) VALUES(?, ?)",
            (event_id, utc_iso(datetime.now(timezone.utc))),
        )


_SAMSARA_SESSION: aiohttp.ClientSession | None = None


def samsara_session() -> aiohttp.ClientSession:
    """Shared HTTP session so Samsara calls reuse keep-alive TLS connections."""
    global _SAMSARA_SESSION
    if _SAMSARA_SESSION is None or _SAMSARA_SESSION.closed:
        _SAMSARA_SESSION = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=20, ttl_dns_cache=300, keepalive_timeout=60)
        )
    return _SAMSARA_SESSION


async def close_samsara_session() -> None:
    global _SAMSARA_SESSION
    if _SAMSARA_SESSION is not None and not _SAMSARA_SESSION.closed:
        await _SAMSARA_SESSION.close()
    _SAMSARA_SESSION = None


async def get_paginated(
    url: str,
    account: SamsaraAccount,
    params: dict[str, str],
    timeout: aiohttp.ClientTimeout,
    session: aiohttp.ClientSession | None = None,
) -> list[dict[str, Any]]:
    """Collect every page of a Samsara list endpoint."""
    session = session or samsara_session()
    headers = {"Authorization": f"Bearer {account.api_key}", "Accept": "application/json"}
    params = dict(params)
    results: list[dict[str, Any]] = []
    while True:
        async with session.get(url, headers=headers, params=params, timeout=timeout) as response:
            body = await response.json(content_type=None)
            if response.status >= 400:
                raise RuntimeError(f"Samsara API returned HTTP {response.status}: {body}")
        results.extend(item for item in body.get("data", []) if isinstance(item, dict))
        pagination = body.get("pagination", {})
        if not pagination.get("hasNextPage") or not pagination.get("endCursor"):
            break
        params["after"] = pagination["endCursor"]
    return results


async def get_events(session: aiohttp.ClientSession, account: SamsaraAccount, start: datetime, end: datetime) -> list[dict[str, Any]]:
    params: dict[str, str] = {
        "startTime": utc_iso(start),
        "endTime": utc_iso(end),
        "queryByTimeField": "updatedAtTime",
        "behaviorLabels": "Braking",
        "includeAsset": "true",
        "includeDriver": "true",
    }
    return await get_paginated(API_URL, account, params, aiohttp.ClientTimeout(total=45), session)


async def get_braking_events(
    account: SamsaraAccount,
    start: datetime,
    end: datetime,
    vehicle_id: str | None = None,
) -> list[dict[str, Any]]:
    """Fetch braking events detected during a caller-selected time range."""
    params: dict[str, str] = {
        "startTime": utc_iso(start),
        "endTime": utc_iso(end),
        "queryByTimeField": "createdAtTime",
        "behaviorLabels": "Braking",
        "includeAsset": "true",
        "includeDriver": "true",
    }
    if vehicle_id:
        params["assetIds"] = str(vehicle_id)
    events = await get_paginated(API_URL, account, params, aiohttp.ClientTimeout(total=90))
    events.sort(key=lambda item: event_timestamp(item) or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return events


async def get_braking_history(
    vehicle_id: str,
    account: SamsaraAccount,
    start: datetime | None = None,
    end: datetime | None = None,
) -> list[dict[str, Any]]:
    """Fetch one vehicle's braking events; defaults to the configured recent-history window."""
    end = end or datetime.now(timezone.utc)
    start = start or end - timedelta(days=HISTORY_LOOKBACK_DAYS)
    return await get_braking_events(account, start, end, vehicle_id)


async def get_truck_locations(account: SamsaraAccount) -> list[dict[str, Any]]:
    """Return the latest GPS snapshot for each vehicle in the organization."""
    return await get_paginated(VEHICLE_STATS_URL, account, {"types": "gps"}, aiohttp.ClientTimeout(total=45))


def truck_line(truck: dict[str, Any]) -> str:
    name = html.escape(str(truck.get("name") or f"Truck {truck.get('id', 'unnamed')}"))
    gps = truck.get("gps") or {}
    if not isinstance(gps, dict) or gps.get("latitude") is None or gps.get("longitude") is None:
        return f"🚚 <b>{name}</b> — location is not currently available"

    latitude = gps["latitude"]
    longitude = gps["longitude"]
    address = (gps.get("reverseGeo") or {}).get("formattedLocation")
    location_text = html.escape(str(address)) if address else f"{latitude}, {longitude}"
    map_url = f"https://maps.google.com/?q={latitude},{longitude}"
    line = f'🚚 <b>{name}</b>\n📍 <a href="{map_url}">{location_text}</a>'
    gps_time = gps.get("time")
    if gps_time:
        line += f"\n🕒 Updated: {html.escape(str(gps_time))}"
    speed_mph = gps.get("speedMilesPerHour")
    if isinstance(speed_mph, (int, float)):
        line += f"\n🚦 Speed: {round(speed_mph)} mph"
    return line


async def send_truck_list(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.answer("⛔ This command is available to bot administrators only.")
        return
    selected = configured_trucks(message.chat.id)
    if not selected:
        await message.answer("No trucks have been selected for this group yet. A bot administrator can run /setup.")
        return
    status = await message.answer("⏳ Loading the truck list and latest GPS locations from Samsara…")
    try:
        trucks = []
        for account in ACCOUNTS:
            chosen_ids = {item["id"] for item in selected if item["account_id"] == account.account_id}
            if chosen_ids:
                account_trucks = [truck for truck in await get_truck_locations(account) if str(truck.get("id")) in chosen_ids]
                for truck in account_trucks:
                    truck["_samsaraAccountId"] = account.account_id
                trucks.extend(account_trucks)
    except RuntimeError as error:
        error_text = str(error)
        if "HTTP 403" in error_text:
            await status.edit_text(
                "⛔ Samsara denied access to GPS data. Add the "
                "<b>Read Vehicle Statistics</b> permission in the Vehicles category, then restart the bot."
            )
        else:
            logger.exception("Could not load truck list")
            await status.edit_text("Could not load trucks from Samsara. Please try again later.")
        return
    except Exception:
        logger.exception("Could not load truck list")
        await status.edit_text("Could not load trucks from Samsara. Please try again later.")
        return

    if not trucks:
        await status.edit_text("No trucks were found in Samsara.")
        return

    intro = f"🚛 <b>Trucks and Latest Locations</b> ({len(trucks)}):\n\n"
    chunks: list[str] = []
    current = intro
    for truck in trucks:
        org_label = html.escape(account_label(str(truck.get("_samsaraAccountId", "1"))))
        line = truck_line(truck) + f"\n🏢 {org_label}\n\n"
        if len(current) + len(line) > 3800 and current != intro:
            chunks.append(current.rstrip())
            current = ""
        current += line
    if current.strip():
        chunks.append(current.rstrip())

    await status.edit_text(chunks[0], disable_web_page_preview=True)
    for chunk in chunks[1:]:
        await message.answer(chunk, disable_web_page_preview=True)


def event_id(event: dict[str, Any]) -> str:
    return str(event.get("id") or event.get("eventId") or event.get("safetyEventId") or "")


def first_value(event: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = event.get(key)
        if value is not None and not isinstance(value, (dict, list)):
            return str(value)
    return None


def find_video_url(value: Any, parent_key: str = "") -> str | None:
    """Find a video link in the different media shapes returned by Samsara."""
    if isinstance(value, dict):
        for key, child in value.items():
            url = find_video_url(child, key.lower())
            if url:
                return url
    elif isinstance(value, list):
        for child in value:
            url = find_video_url(child, parent_key)
            if url:
                return url
    elif isinstance(value, str) and value.startswith(("https://", "http://")):
        key = parent_key.lower()
        if any(word in key for word in ("video", "media", "url", "download", "clip")):
            return value
    return None


def find_video_url_for_type(value: Any, media_type: str) -> str | None:
    """Prefer the URL belonging to the requested Samsara media type."""
    typed_entries: list[dict[str, Any]] = []

    def collect(node: Any) -> None:
        if isinstance(node, dict):
            node_type = node.get("mediaType") or node.get("media_type")
            if node_type:
                typed_entries.append(node)
            for child in node.values():
                collect(child)
        elif isinstance(node, list):
            for child in node:
                collect(child)

    collect(value)
    if typed_entries:
        for entry in typed_entries:
            entry_type = str(entry.get("mediaType") or entry.get("media_type") or "")
            if entry_type == media_type:
                url = find_video_url(entry)
                if url:
                    return url
        # The retrieval ID is created for one requested mediaType, and the GET
        # endpoint returns that retrieval. Some Samsara response shapes omit
        # mediaType on the URL object, so do not reject that valid URL.
    return find_video_url(value)


def find_safety_event_hd_url(event: dict[str, Any]) -> str | None:
    """Prefer Samsara's explicit forward-facing safety-event download URL."""
    for key in ("downloadForwardVideoUrl", "download_forward_video_url", "forwardVideoUrl"):
        value = event.get(key)
        if isinstance(value, str) and value.startswith(("https://", "http://")):
            return value
    for key in ("cameraMedia", "media", "videos", "video"):
        value = event.get(key)
        if isinstance(value, (dict, list)):
            result = find_video_url(value)
            if result:
                return result
    return None


def _media_timestamp(value: Any) -> datetime | None:
    if isinstance(value, (int, float)) or (isinstance(value, str) and value.isdigit()):
        number = float(value)
        return datetime.fromtimestamp(number / 1000 if number > 10_000_000_000 else number, timezone.utc)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed
        except ValueError:
            return None
    return None


async def find_uploaded_highres_video(
    session: aiohttp.ClientSession, event: dict[str, Any], account: SamsaraAccount
) -> str | None:
    """Find the already-uploaded road-facing HD clip nearest the safety event."""
    asset = event.get("asset") or event.get("vehicle") or {}
    vehicle_id = event.get("vehicleId") or event.get("assetId")
    if not vehicle_id and isinstance(asset, dict):
        vehicle_id = asset.get("id")
    event_time = event_timestamp(event)
    if not vehicle_id or not event_time:
        return None

    headers = {"Authorization": f"Bearer {account.api_key}", "Accept": "application/json"}
    params = {
        "vehicleIds": str(vehicle_id),
        "inputs": "dashcamRoadFacing",
        "mediaTypes": "videoHighRes",
        "startTime": utc_iso(event_time - timedelta(minutes=2)),
        "endTime": utc_iso(event_time + timedelta(minutes=2)),
    }
    candidates: list[tuple[float, str]] = []
    while True:
        async with session.get("https://api.samsara.com/cameras/media", headers=headers, params=params) as response:
            result = await response.json(content_type=None)
            if response.status >= 400:
                logger.warning("Could not list uploaded Samsara media (HTTP %s)", response.status)
                return None

        for item in result.get("data", []):
            if not isinstance(item, dict):
                continue
            item_type = item.get("mediaType") or item.get("mediaTypeEnum")
            if item_type and str(item_type) != "videoHighRes":
                continue
            camera_input = item.get("input") or item.get("cameraInput") or item.get("cameraRole")
            if camera_input and str(camera_input) not in {"dashcamRoadFacing", "roadFacing"}:
                continue
            media_url = find_video_url(item)
            if not media_url:
                continue
            media_time = next(
                (_media_timestamp(item.get(key)) for key in ("startTime", "capturedAtTime", "timestamp", "eventTime") if item.get(key) is not None),
                None,
            )
            if media_time is None:
                logger.debug("Skipping Samsara media item without a timestamp")
                continue
            distance = abs((media_time - event_time).total_seconds())
            if distance <= 180:
                candidates.append((distance, media_url))

        pagination = result.get("pagination") or {}
        cursor = pagination.get("endCursor")
        if not pagination.get("hasNextPage") or not cursor:
            break
        params["after"] = cursor

    if candidates:
        candidates.sort(key=lambda item: item[0])
        logger.info("Found uploaded road-facing Samsara HD clip %.1f seconds from event", candidates[0][0])
        return candidates[0][1]
    return None


def event_timestamp(event: dict[str, Any]) -> datetime | None:
    raw_time = event.get("startMs") or event.get("startTime") or event.get("updatedAtTime")
    if isinstance(raw_time, (int, float)) or (isinstance(raw_time, str) and raw_time.isdigit()):
        return datetime.fromtimestamp(float(raw_time) / 1000, timezone.utc)
    if raw_time:
        try:
            value = datetime.fromisoformat(str(raw_time).replace("Z", "+00:00"))
            return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
        except ValueError:
            return None
    return None


def format_event_time(event: dict[str, Any], *, short: bool = False) -> str:
    value = event_timestamp(event)
    if not value:
        return "Time unavailable"
    display_timezone = event_timezone()
    local_time = value.astimezone(display_timezone)
    if short:
        return local_time.strftime("%b %d, %Y %I:%M %p")
    zone_name = getattr(display_timezone, "key", "America/New_York")
    return f"{local_time:%b %d, %Y %I:%M:%S %p} ({zone_name}, UTC{local_time:%z})"


@lru_cache(maxsize=1)
def event_timezone() -> timezone | ZoneInfo:
    try:
        return ZoneInfo(EVENT_TIMEZONE)
    except ZoneInfoNotFoundError:
        logger.error(
            "Time zone database is missing or EVENT_TIMEZONE %r is invalid; falling back to fixed America/New_York UTC-05:00. "
            "Install dependencies with 'python -m pip install -r requirements.txt' to enable regional time zones.",
            EVENT_TIMEZONE,
        )
        return timezone(timedelta(hours=-5), name="America/New_York")


def event_location_text(event: dict[str, Any]) -> str | None:
    location = event.get("location") or {}
    if not isinstance(location, dict):
        return None
    reverse_geo = location.get("reverseGeo") or {}
    address = (
        location.get("formattedLocation")
        or location.get("address")
        or (reverse_geo.get("formattedLocation") if isinstance(reverse_geo, dict) else None)
    )
    if isinstance(address, dict):
        address_parts = [
            address.get(key)
            for key in ("streetNumber", "street", "city", "state", "postalCode", "country")
            if address.get(key)
        ]
        address = ", ".join(str(part) for part in address_parts) or None
    if address:
        return str(address)
    latitude, longitude = location.get("latitude"), location.get("longitude")
    if latitude is not None and longitude is not None:
        return f"{latitude}, {longitude}"
    return None


def event_caption(event: dict[str, Any]) -> str:
    asset = event.get("asset") or event.get("vehicle") or {}
    driver = event.get("driver") or {}
    vehicle_name = (asset.get("name") if isinstance(asset, dict) else None) or first_value(event, "vehicleName", "assetName") or "Unknown truck"
    if isinstance(driver, dict):
        driver_name = driver.get("name")
        if not driver_name:
            driver_id = driver.get("id")
            if driver_id is not None and str(driver_id).strip() not in {"", "0"}:
                driver_name = driver_id
    else:
        driver_name = driver
        if str(driver_name).strip() in {"", "0"}:
            driver_name = None
    driver_name = driver_name or first_value(event, "driverName") or "Not assigned in Samsara"
    driver_username = driver.get("username") if isinstance(driver, dict) else None
    if driver_username and driver_name != "Not assigned in Samsara":
        driver_name = f"{driver_name} (@{driver_username})"
    behavior = event.get("behaviorLabels")
    if isinstance(behavior, list):
        label = next((item.get("name") or item.get("label") for item in behavior if isinstance(item, dict)), None)
    elif isinstance(behavior, dict):
        label = behavior.get("name") or behavior.get("label")
    else:
        label = None
    label = label or first_value(event, "behaviorLabel", "type") or "Harsh braking"
    timestamp = format_event_time(event)

    lines = ["🚨 <b>Safety Alert: Harsh Braking</b>", f"🚚 Truck: {html.escape(str(vehicle_name))}"]
    lines.append(f"👤 Driver: {html.escape(str(driver_name))}")
    lines.append(f"🕒 Date & Time: {html.escape(timestamp)}")
    lines.append(f"📌 Event: {html.escape(str(label))}")

    location = event.get("location") or {}
    if not isinstance(location, dict):
        location = {}
    latitude = location.get("latitude")
    longitude = location.get("longitude")
    address = event_location_text(event)
    if latitude is not None and longitude is not None:
        map_url = f"https://maps.google.com/?q={latitude},{longitude}"
        place = html.escape(address) if address and address != f"{latitude}, {longitude}" else f"{latitude}, {longitude}"
        lines.append(f'📍 Location: <a href="{map_url}">{place}</a>')
    elif address:
        lines.append(f"📍 Location: {html.escape(str(address))}")
    else:
        lines.append("📍 Location: coordinates are not available in the event data")

    incident_url = event.get("incidentLink")
    if incident_url:
        lines.append(f'<a href="{html.escape(str(incident_url), quote=True)}">Open event in Samsara</a>')
    return "\n".join(lines)


def _point_time(point: dict[str, Any]) -> datetime | None:
    value = point.get("time") or point.get("timestamp") or point.get("updatedAtTime") or point.get("startTime")
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed
    except ValueError:
        return None


def _vehicle_id(event: dict[str, Any]) -> str | None:
    asset = event.get("asset") or event.get("vehicle") or {}
    value = event.get("vehicleId") or event.get("assetId")
    if not value and isinstance(asset, dict):
        value = asset.get("id")
    return str(value) if value else None


async def enrich_event_for_video(event: dict[str, Any], account: SamsaraAccount) -> dict[str, Any]:
    """Fetch optional driver and historical GPS/speed data for burned-in overlays."""
    enriched = dict(event)
    event_time = event_timestamp(event)
    vehicle_id = _vehicle_id(event)
    if not event_time or not vehicle_id:
        return enriched

    headers = {"Authorization": f"Bearer {account.api_key}", "Accept": "application/json"}
    timeout = aiohttp.ClientTimeout(total=25, connect=10)
    driver = event.get("driver") if isinstance(event.get("driver"), dict) else {}
    driver = dict(driver)
    driver_id = driver.get("id") or event.get("driverId")
    if str(driver_id or "").strip() in {"", "0"}:
        driver_id = None

    async with aiohttp.ClientSession(timeout=timeout) as session:
        if not driver_id:
            params = {
                "filterBy": "vehicles",
                "vehicleIds": vehicle_id,
                "startTime": utc_iso(event_time - timedelta(minutes=2)),
                "endTime": utc_iso(event_time + timedelta(minutes=2)),
            }
            try:
                async with session.get(DRIVER_ASSIGNMENTS_URL, headers=headers, params=params) as response:
                    if response.status < 400:
                        body = await response.json(content_type=None)
                        assignments = body.get("data", [])
                        for assignment in assignments if isinstance(assignments, list) else []:
                            if not isinstance(assignment, dict):
                                continue
                            assigned_from = _point_time({"time": assignment.get("startTime") or assignment.get("assignedAtTime")})
                            assigned_until = _point_time({"time": assignment.get("endTime")})
                            if assigned_from and event_time < assigned_from:
                                continue
                            if assigned_until and event_time > assigned_until:
                                continue
                            ref = assignment.get("driver")
                            if isinstance(ref, dict):
                                driver.update({key: value for key, value in ref.items() if value is not None})
                                driver_id = ref.get("id") or assignment.get("driverId")
                            else:
                                driver_id = assignment.get("driverId") or ref
                                if str(driver_id or "").strip() in {"", "0"}:
                                    driver_id = None
                                name = assignment.get("driverName")
                                if name:
                                    driver["name"] = name
                            if str(driver_id or "").strip() == "0":
                                driver_id = None
                            if driver_id:
                                break
                    else:
                        logger.info("Driver assignment lookup unavailable (HTTP %s)", response.status)
            except (aiohttp.ClientError, asyncio.TimeoutError):
                logger.info("Could not load driver assignment for event %s", event_id(event), exc_info=True)

        if driver_id and not driver.get("username"):
            try:
                async with session.get(f"{DRIVER_URL}/{quote(str(driver_id), safe='')}", headers=headers) as response:
                    if response.status < 400:
                        body = await response.json(content_type=None)
                        profile = body.get("data", body)
                        if isinstance(profile, dict):
                            driver.update({key: value for key, value in profile.items() if value is not None})
                    else:
                        logger.info("Driver profile lookup unavailable (HTTP %s)", response.status)
            except (aiohttp.ClientError, asyncio.TimeoutError):
                logger.info("Could not load driver profile %s", driver_id, exc_info=True)

        stats: dict[str, Any] = {}
        try:
            params = {
                "vehicleIds": vehicle_id,
                "types": "gps,ecuSpeedMph",
                "startTime": utc_iso(event_time - timedelta(seconds=45)),
                "endTime": utc_iso(event_time + timedelta(seconds=45)),
            }
            async with session.get(VEHICLE_STATS_HISTORY_URL, headers=headers, params=params) as response:
                if response.status < 400:
                    body = await response.json(content_type=None)
                    rows = body.get("data", [])
                    if rows and isinstance(rows[0], dict):
                        stats = rows[0]
                else:
                    logger.info("Historical speed lookup unavailable (HTTP %s)", response.status)
        except (aiohttp.ClientError, asyncio.TimeoutError):
            logger.info("Could not load telematics for event %s", event_id(event), exc_info=True)

    speed_points: list[dict[str, Any]] = []
    speed_raw = stats.get("ecuSpeedMph", []) if isinstance(stats, dict) else []
    if isinstance(speed_raw, dict):
        speed_raw = [speed_raw]
    for raw in speed_raw if isinstance(speed_raw, list) else []:
        if isinstance(raw, dict):
            try:
                speed_points.append({"time": _point_time(raw), "mph": float(raw["value"])})
            except (TypeError, ValueError):
                pass
    gps_points = stats.get("gps", []) if isinstance(stats, dict) else []
    if isinstance(gps_points, dict):
        gps_points = [gps_points]
    for point in gps_points if isinstance(gps_points, list) else []:
        if not isinstance(point, dict):
            continue
        speed = point.get("speedMilesPerHour")
        if speed is not None:
            try:
                speed_points.append({"time": _point_time(point), "mph": float(speed)})
            except (TypeError, ValueError):
                pass

    if speed_points:
        speed_points.sort(key=lambda item: item["time"] or event_time)
    event_location = event.get("location")
    if not isinstance(event_location, dict) or event_location.get("latitude") is None:
        geo_candidates = [point for point in gps_points if isinstance(point, dict) and point.get("latitude") is not None]
        if geo_candidates:
            nearest_geo = min(
                geo_candidates,
                key=lambda point: abs((_point_time(point) or event_time) - event_time),
            )
            enriched["location"] = nearest_geo

    if driver:
        enriched["driver"] = driver
    asset = enriched.get("asset") or enriched.get("vehicle") or {}
    vehicle_name = asset.get("name") if isinstance(asset, dict) else None
    overlay_location = event_location_text(enriched) or "Location unavailable"
    nearest_speed = min(
        speed_points,
        key=lambda point: abs((point.get("time") or event_time) - event_time),
        default=None,
    )
    enriched["_videoOverlay"] = {
        "driver": str(driver.get("name") or "Driver not assigned"),
        "username": str(driver.get("username") or ""),
        "vehicle": str(vehicle_name or first_value(enriched, "vehicleName", "assetName") or "Truck"),
        "date": format_event_time(enriched, short=True),
        "location": overlay_location,
        "speed_points": speed_points,
        "event_speed_mph": nearest_speed["mph"] if nearest_speed else None,
        "event_epoch": event_time.timestamp(),
    }
    logger.info(
        "Telematics for event %s: %s speed point(s), event speed %s mph",
        event_id(event),
        len(speed_points),
        round(nearest_speed["mph"], 1) if nearest_speed else "unavailable",
    )
    return enriched


async def retrieve_video(
    session: aiohttp.ClientSession,
    event: dict[str, Any],
    account: SamsaraAccount,
    *,
    duration_seconds: int = 30,
    media_type: str = "videoHighRes",
) -> str | None:
    """Request a clip centered on the event from Samsara's camera archive."""
    asset = event.get("asset") or event.get("vehicle") or {}
    vehicle_id = event.get("vehicleId") or event.get("assetId")
    if not vehicle_id and isinstance(asset, dict):
        vehicle_id = asset.get("id")
    event_time = event_timestamp(event)
    if not vehicle_id or not event_time:
        return None

    headers = {"Authorization": f"Bearer {account.api_key}", "Accept": "application/json", "Content-Type": "application/json"}
    half_duration = duration_seconds / 2
    body = {
        "vehicleId": str(vehicle_id),
        "startTime": utc_iso(event_time - timedelta(seconds=half_duration)),
        "endTime": utc_iso(event_time + timedelta(seconds=half_duration)),
        "inputs": ["dashcamRoadFacing"],
        "mediaType": media_type,
    }
    async with session.post("https://api.samsara.com/cameras/media/retrieval", headers=headers, json=body) as response:
        result = await response.json(content_type=None)
        if response.status >= 400:
            logger.warning("Media retrieval request failed (HTTP %s): %s", response.status, result)
            return None

    retrieval_id = result.get("retrievalId") or (result.get("data") or {}).get("retrievalId")
    if not retrieval_id:
        return find_video_url(result)

    # Retrieval is asynchronous. Poll briefly; the device may also be offline, in which
    # case Samsara will complete the request when it reconnects.
    for _ in range(12):
        await asyncio.sleep(5)
        async with session.get(
            "https://api.samsara.com/cameras/media/retrieval",
            headers=headers,
            params={"retrievalId": retrieval_id},
        ) as response:
            result = await response.json(content_type=None)
            if response.status >= 400:
                logger.warning("Could not read media retrieval (HTTP %s): %s", response.status, result)
                return None
        video_url = find_video_url(result)
        if video_url:
            return video_url
        status = str(result.get("status") or (result.get("data") or {}).get("status") or "").lower()
        if status in {"failed", "error", "expired"}:
            return None
    return None


class VideoTooLargeError(Exception):
    pass


def _render_telematics_overlay(
    path: Path, overlay: dict[str, Any], width: int, height: int,
    current_speed_mph: float | None = None, current_epoch: float | None = None,
) -> None:
    """Draw the dashboard-style data layer with Pillow, independent of FFmpeg drawtext."""
    from PIL import Image, ImageDraw, ImageFont

    scale = min(width / 1920, height / 1080)
    pad = max(10, round(20 * scale))
    header_band_h = max(42, round(62 * scale))
    graph_band_h = max(100, round(190 * scale))
    image = Image.new("RGBA", (width, header_band_h + height + graph_band_h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(image, "RGBA")
    footage_top = header_band_h
    graph_band_top = header_band_h + height
    font_candidates = [
        Path(os.environ.get("WINDIR", r"C:\Windows")) / "Fonts" / "arial.ttf",
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
        Path("/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf"),
    ]
    font_path = next((candidate for candidate in font_candidates if candidate.is_file()), None)

    def font(size: int):
        return ImageFont.truetype(str(font_path), size) if font_path else ImageFont.load_default()

    def clean(value: Any, limit: int) -> str:
        return " ".join(str(value or "").replace("\r", " ").replace("\n", " ").split())[:limit]

    header_font_size = max(13, round(21 * scale))
    font_main = font(header_font_size)
    draw.rectangle((0, 0, width, header_band_h), fill=(10, 13, 17, 255))
    header_y = max(2, (header_band_h - round(25 * scale)) // 2)
    vehicle_text = clean(overlay.get("vehicle"), 70)
    date_text = clean(overlay.get("date"), 32)
    location_text = clean(overlay.get("location"), 140)
    side_gap = max(12, round(24 * scale))
    max_side_width = int(width * 0.31)

    def fit_text(value: str, max_width: int) -> str:
        if draw.textbbox((0, 0), value, font=font_main)[2] <= max_width:
            return value
        shortened = value
        while shortened and draw.textbbox((0, 0), shortened + "…", font=font_main)[2] > max_width:
            shortened = shortened[:-1]
        return shortened + "…" if shortened else "…"

    vehicle_text = fit_text(vehicle_text, max_side_width)
    draw.text((pad, header_y), vehicle_text, font=font_main, fill=(255, 255, 255, 255))
    vehicle_box = draw.textbbox((0, 0), vehicle_text, font=font_main)
    date_box = draw.textbbox((0, 0), date_text, font=font_main)
    date_x = pad + (vehicle_box[2] - vehicle_box[0]) + side_gap
    draw.text((date_x, header_y), date_text, font=font_main, fill=(230, 235, 240, 255))
    location_font = font_main
    location_left = date_x + (date_box[2] - date_box[0]) + side_gap
    location_available_width = max(0, width - pad - location_left)
    for size in range(header_font_size, 9, -1):
        location_font = font(size)
        if draw.textbbox((0, 0), location_text, font=location_font)[2] <= location_available_width:
            break
    if draw.textbbox((0, 0), location_text, font=location_font)[2] > location_available_width:
        location_text = fit_text(location_text, location_available_width)
    location_box = draw.textbbox((0, 0), location_text, font=location_font)
    draw.text((width - pad - (location_box[2] - location_box[0]), header_y),
              location_text, font=location_font, fill=(190, 205, 216, 255))
    draw.rectangle((0, graph_band_top, width, graph_band_top + graph_band_h), fill=(10, 13, 17, 255))

    event_speed = current_speed_mph if current_speed_mph is not None else overlay.get("event_speed_mph")
    if isinstance(event_speed, (int, float)):
        speed_font = font(max(16, round(27 * scale)))
        speed_text = f"SPEED: {event_speed:.1f} mph"
        text_box = draw.textbbox((0, 0), speed_text, font=speed_font)
        horizontal_pad = max(8, round(12 * scale))
        vertical_pad = max(5, round(8 * scale))
        badge_w = (text_box[2] - text_box[0]) + horizontal_pad * 2
        badge_h = (text_box[3] - text_box[1]) + vertical_pad * 2
        badge_x = pad
        badge_y = footage_top + height - pad - badge_h
        draw.rounded_rectangle(
            (badge_x, badge_y, badge_x + badge_w, badge_y + badge_h),
            radius=max(6, round(9 * scale)), fill=(0, 0, 0, 190),
        )
        draw.text((badge_x + horizontal_pad, badge_y + vertical_pad - text_box[1]),
                  speed_text, font=speed_font, fill=(255, 255, 255, 255))

    samples = [
        item for item in overlay.get("speed_points", [])
        if isinstance(item, dict) and isinstance(item.get("mph"), (int, float))
    ]
    if samples:
        panel_w = width - pad * 2
        panel_h = graph_band_h
        left, top = pad, graph_band_top
        label_font = font(max(13, round(18 * scale)))
        draw.text((left + pad, top + round(8 * scale)), "TELEMATICS · SPEED MPH", font=label_font, fill=(255, 255, 255, 255))
        graph_left, graph_top = left + pad, top + round(38 * scale)
        graph_w, graph_h = panel_w - pad * 2, panel_h - round(52 * scale)
        for fraction in (0.25, 0.5, 0.75, 1.0):
            y = graph_top + round(graph_h * fraction)
            draw.line((graph_left, y, graph_left + graph_w, y), fill=(170, 190, 205, 90), width=max(1, round(scale)))
        max_speed = max(60.0, max(float(item["mph"]) for item in samples))
        timed_samples = [item for item in samples if item.get("time") is not None]
        first_epoch = min((item["time"].timestamp() for item in timed_samples), default=0.0)
        last_epoch = max((item["time"].timestamp() for item in timed_samples), default=0.0)
        points = []
        for index, sample in enumerate(samples):
            if sample.get("time") is not None and last_epoch > first_epoch:
                fraction = (sample["time"].timestamp() - first_epoch) / (last_epoch - first_epoch)
            else:
                fraction = index / max(1, len(samples) - 1)
            x = graph_left + round(graph_w * min(1.0, max(0.0, fraction)))
            y = graph_top + graph_h - round(graph_h * min(max_speed, max(0.0, float(sample["mph"])))/ max_speed)
            points.append((x, y))
        if len(points) == 1:
            draw.ellipse((points[0][0] - 3, points[0][1] - 3, points[0][0] + 3, points[0][1] + 3), fill=(25, 167, 206, 255))
        else:
            draw.line(points, fill=(25, 167, 206, 255), width=max(2, round(4 * scale)))
            for x, y in points:
                radius = max(2, round(4 * scale))
                draw.ellipse((x-radius, y-radius, x+radius, y+radius), fill=(25, 167, 206, 255))
            if current_epoch is not None and len(timed_samples) > 1:
                fraction = (current_epoch - first_epoch) / max(0.001, last_epoch - first_epoch)
                cursor_x = graph_left + round(graph_w * min(1.0, max(0.0, fraction)))
                cursor_y = graph_top + graph_h - round(graph_h * min(max_speed, max(0.0, float(current_speed_mph or 0))) / max_speed)
                draw.line((cursor_x, graph_top, cursor_x, graph_top + graph_h), fill=(255, 220, 40, 210), width=max(2, round(3 * scale)))
                dot_radius = max(4, round(7 * scale))
                draw.ellipse((cursor_x-dot_radius, cursor_y-dot_radius, cursor_x+dot_radius, cursor_y+dot_radius), fill=(255, 220, 40, 255))
    image.save(path, format="PNG")


async def _run_ffmpeg_encode(
    ffmpeg: str,
    source_path: Path,
    output_path: Path,
    filters: list[str],
    crf: int,
    overlay_path: Path | None = None,
    overlay_size: tuple[int, int] | None = None,
) -> tuple[int, str]:
    output_path.unlink(missing_ok=True)
    command = [ffmpeg, "-y", "-i", str(source_path)]
    if overlay_path and overlay_size:
        if overlay_path.is_dir():
            command.extend(["-framerate", "5", "-i", str(overlay_path / "frame-%04d.png")])
        else:
            command.extend(["-loop", "1", "-framerate", "30", "-i", str(overlay_path)])
        base = ",".join([*filters, "format=yuv420p"])
        command.extend([
            "-filter_complex",
            f"[0:v]{base}[base];[1:v]format=rgba[ov];[base][ov]overlay=0:0:shortest=1,format=yuv420p[v]",
            "-map", "[v]", "-shortest",
        ])
    else:
        command.extend(["-map", "0:v:0", "-vf", ",".join(filters)])
    command.extend([
        "-an", "-sn", "-dn",
        "-c:v", "libx264", "-preset", "slow", "-crf", str(crf),
        "-pix_fmt", "yuv420p", "-movflags", "+faststart", "-f", "mp4", str(output_path),
    ])
    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        _, stderr = await asyncio.wait_for(process.communicate(), timeout=180)
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()
        return -1, "FFmpeg timed out while converting the camera video"
    lines = (stderr or b"").decode("utf-8", errors="replace").splitlines()
    return process.returncode, "\n".join(lines[-30:])


async def convert_video_for_telegram(
    source_path: Path, overlay: dict[str, Any], *, include_overlay: bool = True
) -> tuple[BufferedInputFile, bool]:
    """Render selected Samsara-style data overlays while keeping high quality."""
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        raise RuntimeError("FFmpeg was not found in PATH; install FFmpeg to send Telegram videos.")

    ffprobe = shutil.which("ffprobe")
    source_width, source_height = 1920, 1080
    video_duration = 12.0
    if ffprobe:
        probe = await asyncio.create_subprocess_exec(
            ffprobe, "-v", "error", "-select_streams", "v:0", "-show_entries",
            "stream=width,height,bit_rate,codec_name:format=duration", "-of", "default=noprint_wrappers=1", str(source_path),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await probe.communicate()
        if probe.returncode == 0:
            probe_text = stdout.decode("utf-8", errors="replace")
            source_info = " ".join(probe_text.split())
            logger.info("Samsara camera source: %s, %s bytes", source_info, source_path.stat().st_size)
            for line in probe_text.splitlines():
                key, separator, value = line.partition("=")
                if separator and key == "width":
                    source_width = int(value)
                elif separator and key == "height":
                    source_height = int(value)
                elif separator and key == "duration":
                    try:
                        video_duration = max(1.0, float(value))
                    except ValueError:
                        pass

    output_path = source_path.with_name("telegram-video.mp4")
    # Keep native resolution and high quality for several attempts before resizing.
    attempts = [
        ((3840, 2160), 16), ((3840, 2160), 18), ((3840, 2160), 20), ((3840, 2160), 22),
        ((1920, 1080), 20), ((1920, 1080), 22), ((1920, 1080), 24),
        ((1280, 720), 23), ((1280, 720), 26), ((960, 540), 27),
    ]
    last_diagnostic = ""
    overlay_encode_failed = False
    for (width, height), crf in (attempts if include_overlay else []):
        scale = min(1.0, width / source_width, height / source_height)
        target_width = max(2, int(source_width * scale) // 2 * 2)
        target_height = max(2, int(source_height * scale) // 2 * 2)
        header_band_h = max(42, round(62 * min(target_width / 1920, target_height / 1080)))
        graph_band_h = max(100, round(190 * min(target_width / 1920, target_height / 1080)))
        canvas_height = target_height + header_band_h + graph_band_h
        overlay_path = source_path.with_name(f"telematics-overlay-{target_width}x{target_height}")
        try:
            if not overlay_path.is_dir() or not any(overlay_path.glob("frame-*.png")):
                overlay_path.mkdir(parents=True, exist_ok=True)
                samples = [
                    point for point in overlay.get("speed_points", [])
                    if isinstance(point, dict) and point.get("time") is not None and isinstance(point.get("mph"), (int, float))
                ]
                event_epoch = float(overlay.get("event_epoch") or 0)
                frames_per_second = 5
                frame_count = max(1, min(600, int(video_duration * frames_per_second + 0.999)))
                timed = sorted(samples, key=lambda point: point["time"].timestamp())
                for frame_index in range(frame_count):
                    # Safety-event clips are requested around the event timestamp.
                    video_second = frame_index / frames_per_second
                    sample_epoch = event_epoch + video_second - video_duration / 2
                    current_speed = None
                    if timed:
                        before = [point for point in timed if point["time"].timestamp() <= sample_epoch]
                        after = [point for point in timed if point["time"].timestamp() >= sample_epoch]
                        left = before[-1] if before else None
                        right = after[0] if after else None
                        if left and right:
                            left_time, right_time = left["time"].timestamp(), right["time"].timestamp()
                            if sample_epoch - left_time <= 15 and right_time - sample_epoch <= 15:
                                if right_time <= left_time:
                                    current_speed = float(left["mph"])
                                else:
                                    ratio = (sample_epoch - left_time) / (right_time - left_time)
                                    current_speed = float(left["mph"]) + (float(right["mph"]) - float(left["mph"])) * ratio
                        else:
                            nearest = min(timed, key=lambda point: abs(point["time"].timestamp() - sample_epoch))
                            if abs(nearest["time"].timestamp() - sample_epoch) <= 3:
                                current_speed = float(nearest["mph"])
                    _render_telematics_overlay(
                        overlay_path / f"frame-{frame_index:04d}.png", overlay, target_width, target_height,
                        current_speed, sample_epoch,
                    )
        except Exception as error:
            last_diagnostic = f"Could not render telemetry overlay image: {error}"
            logger.warning("Could not render telemetry overlay image: %s", error, exc_info=True)
            overlay_encode_failed = True
            break
        return_code, last_diagnostic = await _run_ffmpeg_encode(
            ffmpeg, source_path, output_path,
            [f"scale={target_width}:{target_height}", f"pad={target_width}:{canvas_height}:0:{header_band_h}:black"],
            crf, overlay_path, (target_width, canvas_height)
        )
        if return_code != 0:
            overlay_encode_failed = True
            logger.warning("FFmpeg could not render the telematics overlay: %s", last_diagnostic)
            break
        if return_code == 0 and output_path.is_file():
            output_size = output_path.stat().st_size
            if output_size <= MAX_TELEGRAM_VIDEO_BYTES:
                logger.info("Prepared video with Samsara-style data overlay (max %sx%s, CRF %s, preset slow, %s bytes)", width, height, crf, output_size)
                return BufferedInputFile(output_path.read_bytes(), filename="samsara-safety-event.mp4"), True
            logger.info("Compressed video is still too large at CRF %s (%s bytes); lowering size/quality", crf, output_size)

    # The graph is optional. Preserve the HD picture if FFmpeg cannot draw it
    # or the overlaid result remains too large, then only resize/compress as needed.
    logger.warning(
        "Trying clean high-quality video after overlay %s",
        "encoding failed" if overlay_encode_failed else "exceeded the upload limit",
    )
    for (width, height), crf in attempts:
        scale_filter = (
            f"scale=w='min(iw,{width})':h='min(ih,{height})':"
            "force_original_aspect_ratio=decrease:force_divisible_by=2"
        )
        return_code, clean_diagnostic = await _run_ffmpeg_encode(
            ffmpeg, source_path, output_path, [scale_filter, "format=yuv420p"], crf
        )
        if return_code != 0:
            last_diagnostic = clean_diagnostic
            break
        if output_path.is_file():
            output_size = output_path.stat().st_size
            if output_size <= MAX_TELEGRAM_VIDEO_BYTES:
                logger.info("Prepared clean high-quality video (max %sx%s, CRF %s, %s bytes)", width, height, crf, output_size)
                return BufferedInputFile(output_path.read_bytes(), filename="samsara-safety-event.mp4"), False
            logger.info("Clean video remains over limit at CRF %s (%s bytes)", crf, output_size)

    logger.warning("Could not prepare a Telegram-compatible video: %s", last_diagnostic)
    raise VideoTooLargeError("Could not encode a Telegram video under the upload limit")


async def download_video_source(url: str, source_path: Path) -> int:
    """Download and validate the original Samsara file without changing its bytes."""
    timeout = aiohttp.ClientTimeout(total=120, connect=20, sock_read=60)
    try:
        size = 0
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as response:
                if response.status >= 400:
                    logger.warning("Samsara media URL returned HTTP %s", response.status)
                    raise RuntimeError(f"Samsara media URL returned HTTP {response.status}")
                content_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
                if content_type and not (
                    content_type.startswith("video/")
                    or content_type in {"application/octet-stream", "application/mp4", "binary/octet-stream"}
                ):
                    logger.warning("Samsara media URL returned non-video content type %s", content_type)
                    raise RuntimeError("Samsara media URL did not return a video file")
                if response.content_length and response.content_length > MAX_SOURCE_VIDEO_BYTES:
                    raise VideoTooLargeError
                with source_path.open("wb") as source_file:
                    async for chunk in response.content.iter_chunked(256 * 1024):
                        size += len(chunk)
                        if size > MAX_SOURCE_VIDEO_BYTES:
                            raise VideoTooLargeError
                        source_file.write(chunk)
        if size == 0:
            raise RuntimeError("Samsara returned an empty camera video")
        return size
    except (aiohttp.ClientError, asyncio.TimeoutError):
        logger.exception("Could not download the Samsara video URL")
        raise RuntimeError("Could not download the Samsara camera video")


async def validate_video_source(source_path: Path) -> bool:
    """Reject partial/HTML media responses before uploading or transcoding them."""
    ffprobe = shutil.which("ffprobe")
    if not ffprobe:
        return True
    process = await asyncio.create_subprocess_exec(
        ffprobe, "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=codec_type",
        "-of", "csv=p=0", str(source_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=25)
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()
        logger.warning("FFprobe timed out validating a Samsara video source")
        return False
    if process.returncode != 0:
        diagnostic = (stderr or b"").decode("utf-8", errors="replace").splitlines()
        logger.warning("Rejected invalid Samsara video source: %s", " ".join(diagnostic[-3:]))
        return False
    if "video" not in stdout.decode("utf-8", errors="replace").lower().splitlines():
        logger.warning("Rejected Samsara media without a video stream")
        return False
    return True


async def try_send_video_url(
    bot: Bot, chat_id: int | str, url: str, caption: str, overlay: dict[str, Any], source_label: str
) -> tuple[bool, bool]:
    try:
        with tempfile.TemporaryDirectory(prefix="samsara-video-") as temp_dir:
            source_path = Path(temp_dir) / "camera-source.mp4"
            source_size = await download_video_source(url, source_path)
            if not await validate_video_source(source_path):
                logger.warning("Skipping invalid video candidate from %s", source_label)
                return False, False
            logger.info("Sending original Samsara video unchanged from %s (%s bytes)", source_label, source_size)

            if VIDEO_TELEMATICS_OVERLAY:
                try:
                    processed_video, overlay_applied = await convert_video_for_telegram(source_path, overlay)
                    if overlay_applied:
                        await telegram_call_with_retry(bot.send_video,
                            chat_id=chat_id,
                            video=processed_video,
                            caption=caption,
                            supports_streaming=True,
                        )
                        return True, False
                    if source_size > MAX_TELEGRAM_VIDEO_BYTES:
                        await telegram_call_with_retry(bot.send_video,
                            chat_id=chat_id,
                            video=processed_video,
                            caption=caption,
                            supports_streaming=True,
                        )
                        return True, False
                    logger.info("Overlay rendering failed; keeping the under-limit original unchanged")
                except (VideoTooLargeError, TelegramBadRequest, RuntimeError):
                    logger.warning(
                        "Could not send the Samsara-style data overlay; falling back to the original file",
                        exc_info=True,
                    )

            try:
                # Best quality: upload exactly what Samsara returned, without
                # transcoding, overlays, resizing, or audio removal.
                await telegram_call_with_retry(bot.send_video,
                    chat_id=chat_id,
                    video=BufferedInputFile(source_path.read_bytes(), filename="samsara-safety-event.mp4"),
                    caption=caption,
                    supports_streaming=True,
                )
                return True, False
            except TelegramBadRequest:
                if source_size <= MAX_TELEGRAM_VIDEO_BYTES:
                    logger.warning(
                        "Telegram rejected an original video below the size limit (%s bytes); preserving it and trying as a document",
                        source_size,
                        exc_info=True,
                    )
                    await telegram_call_with_retry(bot.send_document,
                        chat_id=chat_id,
                        document=BufferedInputFile(source_path.read_bytes(), filename="samsara-safety-event.mp4"),
                        caption=caption,
                    )
                    return True, False
                logger.warning("Original video is over Telegram's size limit (%s bytes); trying compressed variants", source_size)

            # Only modify the clip after Telegram rejects the original file.
            video, _ = await convert_video_for_telegram(
                source_path, overlay, include_overlay=VIDEO_TELEMATICS_OVERLAY
            )
            await telegram_call_with_retry(bot.send_video, chat_id=chat_id, video=video, caption=caption, supports_streaming=True)
            return True, False
    except TelegramRetryAfter:
        # Let the polling worker retry this event instead of silently advancing
        # the Samsara checkpoint and losing the notification.
        raise
    except (TelegramNetworkError, TelegramServerError):
        raise
    except VideoTooLargeError:
        return False, True
    except TelegramBadRequest:
        logger.exception("Telegram rejected both original and compressed video")
        return False, False
    except Exception as error:
        logger.warning("Could not prepare/send Samsara video: %s", error)
        return False, False


async def send_video_with_fallback(
    bot: Bot,
    chat_id: int | str,
    url: str | None,
    caption: str,
    event: dict[str, Any],
    account: SamsaraAccount,
) -> str:
    overlay = event.get("_videoOverlay", {})
    # Samsara's explicit forward-facing safety-event download is its closest
    # documented equivalent to the dashboard's road-camera export.
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=90)) as session:
        event_hd_url = find_safety_event_hd_url(event)
        if event_hd_url:
            delivered, _ = await try_send_video_url(
                bot, chat_id, event_hd_url, caption, overlay, "safety-event downloadForwardVideoUrl"
            )
            if delivered:
                return "video"

        # Then use uploaded HD road-facing media or request an HD retrieval.
        # The Samsara dashboard download commonly corresponds to an already
        # uploaded safety-event clip. Retrieve that HD file before requesting a
        # new clip, which may be a different rendition or may not be available.
        uploaded_url = await find_uploaded_highres_video(session, event, account)
        if uploaded_url:
            delivered, _ = await try_send_video_url(
                bot, chat_id, uploaded_url, caption, overlay, "uploaded videoHighRes"
            )
            if delivered:
                return "video"

        high_res_url = await retrieve_video(
            session, event, account, duration_seconds=12, media_type="videoHighRes"
        )
        if high_res_url:
            delivered, too_large = await try_send_video_url(
                bot, chat_id, high_res_url, caption, overlay, "retrieved videoHighRes"
            )
            if delivered:
                return "video"
            logger.warning("The high-resolution clip %s; trying low-resolution clips", "exceeded the video limit" if too_large else "could not be converted")

        # Use the event's own media only after explicitly checking for HD footage.
        if url:
            delivered, _ = await try_send_video_url(
                bot, chat_id, url, caption, overlay, "event media URL"
            )
            if delivered:
                return "video"

        # Restore the previous short low-resolution fallback. The HD request and
        # event URL can both be unavailable for older events or offline devices.
        for duration in (12, 8, 5):
            short_url = await retrieve_video(
                session,
                event,
                account,
                duration_seconds=duration,
                media_type="videoLowRes",
            )
            if not short_url:
                logger.info("Samsara did not return a %s-second low-resolution clip; trying a shorter duration", duration)
                continue
            delivered, too_large = await try_send_video_url(
                bot, chat_id, short_url, caption, overlay, f"retrieved videoLowRes ({duration}s)"
            )
            if delivered:
                logger.warning("HD media was unavailable; sent low-resolution fallback (%s seconds)", duration)
                return "video"
            reason = "exceeded the video limit" if too_large else "could not be downloaded or sent"
            logger.warning("The %s-second low-resolution clip %s; trying a shorter clip", duration, reason)

    await telegram_call_with_retry(bot.send_message, chat_id=chat_id, text=caption + "\n📹 Video is unavailable in Telegram.")
    return "unavailable"


async def send_event_media(
    bot: Bot,
    chat_id: int | str,
    event: dict[str, Any],
    account: SamsaraAccount,
) -> str:
    enriched_event = await enrich_event_for_video(event, account)
    video_url = find_video_url(enriched_event)
    caption = event_caption(enriched_event)
    return await send_video_with_fallback(bot, chat_id, video_url, caption, enriched_event, account)


async def notify_event(bot: Bot, event: dict[str, Any], account: SamsaraAccount) -> None:
    identifier = event_id(event)
    if not identifier:
        logger.warning("Skipping event without an ID: %s", event)
        return
    asset = event.get("asset") or event.get("vehicle") or {}
    vehicle_id = event.get("vehicleId") or event.get("assetId")
    if not vehicle_id and isinstance(asset, dict):
        vehicle_id = asset.get("id")
    if not vehicle_id:
        logger.warning("Skipping event without a vehicle ID: %s", identifier)
        return
    with sqlite3.connect(STATE_DB) as conn:
        rows = conn.execute(
            "SELECT DISTINCT chat_id FROM group_trucks WHERE account_id = ? AND truck_id = ?",
            (account.account_id, str(vehicle_id)),
        ).fetchall()
    for (chat_id,) in rows:
        scoped_identifier = f"{account.account_id}:{chat_id}:{identifier}"
        if was_sent(scoped_identifier):
            continue
        try:
            await send_event_media(bot, chat_id, event, account)
            mark_sent(scoped_identifier)
            logger.info("Sent safety event %s for account %s to chat %s", identifier, account.account_id, chat_id)
        except Exception as error:
            logger.exception("Could not send safety event %s to chat %s", identifier, chat_id)
            # Transient Telegram failures and unexpected bugs stop checkpoint
            # advancement; permanent chat errors are isolated to this group.
            if isinstance(error, (TelegramRetryAfter, TelegramNetworkError, TelegramServerError)):
                raise
            if not isinstance(error, (TelegramBadRequest, TelegramForbiddenError)):
                raise


async def admin_command(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.answer("⛔ This command is available to bot administrators only.")
        return
    await message.answer(
        "🛠 <b>Bot administration</b>\n"
        "• In each Telegram group, run /setup and select the Samsara account and trucks.\n"
        "• /add_admin ID — grant administrator access.\n"
        "• /remove_admin ID — revoke administrator access.\n"
        "• /admins — list bot administrators."
    )


async def add_admin_command(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.answer("⛔ This command is available to bot administrators only.")
        return
    parts = (message.text or "").split()
    try:
        user_id = int(parts[1])
    except (IndexError, ValueError):
        await message.answer("Usage: /add_admin Telegram_ID")
        return
    with sqlite3.connect(STATE_DB) as conn:
        conn.execute("INSERT OR IGNORE INTO admins(user_id, added_at) VALUES(?, ?)", (user_id, utc_iso(datetime.now(timezone.utc))))
    reset_admin_cache()
    await message.answer(f"✅ User <code>{user_id}</code> is now a bot administrator.")


async def remove_admin_command(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.answer("⛔ This command is available to bot administrators only.")
        return
    parts = (message.text or "").split()
    try:
        user_id = int(parts[1])
    except (IndexError, ValueError):
        await message.answer("Usage: /remove_admin Telegram_ID")
        return
    if user_id in bootstrap_admin_ids():
        await message.answer("The primary administrator configured in BOT_ADMIN_IDS cannot be removed with this command.")
        return
    with sqlite3.connect(STATE_DB) as conn:
        conn.execute("DELETE FROM admins WHERE user_id = ?", (user_id,))
    reset_admin_cache()
    await message.answer(f"✅ Administrator access was removed from user <code>{user_id}</code>.")


async def list_admins_command(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.answer("⛔ This command is available to bot administrators only.")
        return
    with sqlite3.connect(STATE_DB) as conn:
        rows = conn.execute("SELECT user_id FROM admins ORDER BY user_id").fetchall()
    await message.answer("👮 Bot administrators:\n" + "\n".join(f"• <code>{row[0]}</code>" for row in rows))


async def my_id_command(message: Message) -> None:
    if message.from_user:
        await message.answer(f"Your Telegram ID: <code>{message.from_user.id}</code>")


async def app_command(message: Message) -> None:
    user_id = message.from_user.id if message.from_user else None
    admin = is_admin(user_id)
    driver = is_linked_driver(user_id)
    if not admin and not driver:
        await message.answer("Your Telegram account is not linked to a truck. Ask your fleet administrator to add your Telegram ID to your truck.")
        return
    if message.chat.type != "private":
        await message.answer("Open the bot in a private chat and run /app there.")
        return
    if not WEBAPP_URL.startswith("https://"):
        await message.answer("Web App is not configured yet. Set WEBAPP_URL to the public HTTPS address, then restart the bot.")
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Open FleetBot", web_app=WebAppInfo(url=WEBAPP_URL))
    ]])
    await message.answer(
        "Open your FleetBot fleet dashboard:" if admin else "Open your FleetBot driver PTI app:",
        reply_markup=keyboard,
    )


async def start_command(message: Message) -> None:
    await app_command(message)


def get_overdue_pti_trucks(account_id: str) -> set[str]:
    """Return linked trucks whose PTI is overdue or has been returned."""
    with sqlite3.connect(STATE_DB) as conn:
        interval_row = conn.execute(
            "SELECT value FROM pti_settings WHERE key='interval_hours'"
        ).fetchone()
        rows = conn.execute(
            "SELECT d.truck_id,d.linked_at,"
            "(SELECT s.submitted_at FROM pti_submissions s WHERE s.telegram_user_id=d.telegram_user_id "
            "AND s.account_id=d.account_id AND s.truck_id=d.truck_id ORDER BY s.submitted_at DESC LIMIT 1),"
            "(SELECT s.invalidated_at FROM pti_submissions s WHERE s.telegram_user_id=d.telegram_user_id "
            "AND s.account_id=d.account_id AND s.truck_id=d.truck_id ORDER BY s.submitted_at DESC LIMIT 1) "
            "FROM driver_accounts d WHERE d.account_id=?",
            (account_id,),
        ).fetchall()
    interval_hours = int(interval_row[0]) if interval_row else 24
    now = datetime.now(timezone.utc)
    overdue = set()
    for truck_id, linked_at, submitted_at, invalidated_at in rows:
        if invalidated_at:
            overdue.add(str(truck_id))
            continue
        try:
            anchor = datetime.fromisoformat((submitted_at or linked_at).replace("Z", "+00:00"))
            if anchor.tzinfo is None:
                anchor = anchor.replace(tzinfo=timezone.utc)
            if now >= anchor + timedelta(hours=interval_hours):
                overdue.add(str(truck_id))
        except (AttributeError, TypeError, ValueError):
            logger.warning("Invalid PTI timestamp for truck %s", truck_id)
    return overdue


def setup_keyboard(chat_id: int, offset: int = 0) -> InlineKeyboardMarkup:
    session = SETUP_SESSIONS.get(chat_id, {})
    trucks = session.get("trucks", [])
    selected = session.get("selected", set())
    overdue = session.get("pti_overdue", set())
    page_size = 10
    rows = []
    for truck in trucks[offset:offset + page_size]:
        truck_id = str(truck.get("id", ""))
        prefix = "✅" if truck_id in selected else "▫️"
        name = str(truck.get("name") or truck_id)
        if truck_id in overdue:
            label = f"{prefix} {name[:38]} · 🔴 PTI OVERDUE"
        else:
            label = f"{prefix} {name}"[:60]
        rows.append([InlineKeyboardButton(text=label, callback_data=f"setup:toggle:{truck_id}")])
    navigation = []
    if offset > 0:
        navigation.append(InlineKeyboardButton(text="← Back", callback_data=f"setup:page:{offset - page_size}"))
    if offset + page_size < len(trucks):
        navigation.append(InlineKeyboardButton(text="Next →", callback_data=f"setup:page:{offset + page_size}"))
    if navigation:
        rows.append(navigation)
    rows.append([InlineKeyboardButton(text="💾 Save selection", callback_data="setup:save")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def setup_command(message: Message) -> None:
    if message.chat.type == "private":
        await message.answer("Add the bot to the desired group, then run /setup there.")
        return
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.answer("⛔ Only bot administrators can configure groups.")
        return
    buttons = [[InlineKeyboardButton(text=account_label(account.account_id), callback_data=f"setup:account:{account.account_id}")] for account in ACCOUNTS]
    await message.answer("Choose the Samsara organization whose trucks you want to add to this group:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


async def setup_callback(callback: CallbackQuery) -> None:
    if not callback.data or callback.message is None:
        await callback.answer()
        return
    if not is_admin(callback.from_user.id):
        await callback.answer("Insufficient permissions", show_alert=True)
        return
    await callback.answer()
    chat_id = callback.message.chat.id
    parts = callback.data.split(":", 2)
    if parts[1] == "account":
        account = next((item for item in ACCOUNTS if item.account_id == parts[2]), None)
        if account is None:
            await edit_text_with_retry(callback.message, "Samsara account not found.")
            return
        await edit_text_with_retry(callback.message, "⏳ Loading trucks from the selected organization…")
        try:
            trucks = await get_truck_locations(account)
        except Exception:
            logger.exception("Could not load trucks during group setup")
            await edit_text_with_retry(callback.message, "Could not load trucks. Check the API key and the Read Vehicle Statistics permission.")
            return
        if not trucks:
            await edit_text_with_retry(callback.message, "No trucks were found in the selected organization.")
            return
        selected = {item["id"] for item in configured_trucks(chat_id, account.account_id)}
        SETUP_SESSIONS[chat_id] = {
            "account_id": account.account_id, "trucks": trucks, "selected": selected,
            "pti_overdue": get_overdue_pti_trucks(account.account_id),
        }
        await edit_text_with_retry(callback.message,
            f"Select the trucks that should send alerts to the group <b>{html.escape(callback.message.chat.title or str(chat_id))}</b>.\n"
            "Tap a truck to select or deselect it, then save your selection.",
            reply_markup=setup_keyboard(chat_id),
        )
        return
    session = SETUP_SESSIONS.get(chat_id)
    if not session:
        await callback.message.answer("This setup session has expired. Run /setup again.")
        return
    if parts[1] == "page":
        await edit_reply_markup_with_retry(callback.message, reply_markup=setup_keyboard(chat_id, int(parts[2])))
    elif parts[1] == "toggle":
        selected = session["selected"]
        if parts[2] in selected:
            selected.remove(parts[2])
        else:
            selected.add(parts[2])
        await edit_reply_markup_with_retry(callback.message, reply_markup=setup_keyboard(chat_id))
    elif parts[1] == "save":
        selected = session["selected"]
        chosen = [truck for truck in session["trucks"] if str(truck.get("id")) in selected]
        save_group_trucks(chat_id, session["account_id"], chosen)
        del SETUP_SESSIONS[chat_id]
        await edit_text_with_retry(callback.message, f"✅ Trucks saved for this group: <b>{len(chosen)}</b>.")


def history_truck_keyboard(chat_id: int, offset: int = 0) -> InlineKeyboardMarkup:
    trucks = HISTORY_TRUCKS.get(chat_id, [])
    page_size = 12
    buttons = []
    for truck in trucks[offset:offset + page_size]:
        truck_id = str(truck.get("id", ""))
        name = str(truck.get("name") or f"Truck {truck_id}")
        buttons.append([InlineKeyboardButton(text=name[:60], callback_data=f"hist:truck:{truck_id}")])
    navigation = []
    if offset > 0:
        navigation.append(InlineKeyboardButton(text="← Back", callback_data=f"hist:page:{max(0, offset - page_size)}"))
    if offset + page_size < len(trucks):
        navigation.append(InlineKeyboardButton(text="Next →", callback_data=f"hist:page:{offset + page_size}"))
    if navigation:
        buttons.append(navigation)
    return InlineKeyboardMarkup(inline_keyboard=buttons)


async def history_command(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.answer("⛔ This command is available to bot administrators only.")
        return
    selected = configured_trucks(message.chat.id)
    if not selected:
        await message.answer("No trucks have been selected for this group yet. A bot administrator can run /setup.")
        return
    trucks = [{"id": f"{item['account_id']}:{item['id']}", "name": f"{account_label(item['account_id'])} · {item['name']}"} for item in selected]
    chat_id = message.chat.id
    HISTORY_TRUCKS[chat_id] = trucks
    HISTORY_TRUCK_PAGE[chat_id] = 0
    await message.answer(
        f"🚛 <b>Select a truck</b> to view harsh-braking events from the last {HISTORY_LOOKBACK_DAYS} days:",
        reply_markup=history_truck_keyboard(chat_id),
    )


def history_event_keyboard(chat_id: int, events: list[dict[str, Any]], offset: int = 0) -> InlineKeyboardMarkup:
    page_size = 10
    buttons = []
    for event in events[offset:offset + page_size]:
        identifier = event_id(event)
        if not identifier:
            continue
        account_id = str(event.get("_samsaraAccountId", "1"))
        HISTORY_EVENTS[(chat_id, account_id, identifier)] = event
        place = event_location_text(event) or "Location unavailable"
        title = f"{format_event_time(event, short=True)} · {place}"
        buttons.append([InlineKeyboardButton(text=title[:60], callback_data=f"hist:event:{account_id}:{identifier}")])
    navigation = []
    if offset > 0:
        navigation.append(InlineKeyboardButton(text="← Older", callback_data=f"hist:eventpage:{max(0, offset - page_size)}"))
    if offset + page_size < len(events):
        navigation.append(InlineKeyboardButton(text="Newer →", callback_data=f"hist:eventpage:{offset + page_size}"))
    if navigation:
        buttons.append(navigation)
    buttons.append([InlineKeyboardButton(text="← Choose another truck", callback_data="hist:back")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def history_event_page_text(chat_id: int, offset: int = 0) -> str:
    events = HISTORY_EVENT_LISTS.get(chat_id, [])
    total = len(events)
    first = offset + 1
    last = min(offset + 10, total)
    truck_name = HISTORY_SELECTED_TRUCK.get(chat_id, "Truck")
    return (
        f"📋 <b>{truck_name}</b> — harsh-braking events from the last {HISTORY_LOOKBACK_DAYS} days "
        f"({first}–{last} of {total}); select an event:"
    )


async def history_callback(callback: CallbackQuery, bot: Bot) -> None:
    if not callback.data or callback.message is None:
        await callback.answer()
        return
    if not is_admin(callback.from_user.id):
        await callback.answer("This command is available to bot administrators only.", show_alert=True)
        return
    await callback.answer()
    chat_id = callback.message.chat.id
    parts = callback.data.split(":", 3)

    if parts[1] == "page":
        offset = int(parts[2])
        HISTORY_TRUCK_PAGE[chat_id] = offset
        await edit_text_with_retry(callback.message,
            f"🚛 <b>Select a truck</b> to view harsh-braking events from the last {HISTORY_LOOKBACK_DAYS} days:",
            reply_markup=history_truck_keyboard(chat_id, offset),
        )
        return

    if parts[1] == "eventpage":
        offset = int(parts[2])
        await edit_text_with_retry(callback.message,
            history_event_page_text(chat_id, offset),
            reply_markup=history_event_keyboard(chat_id, HISTORY_EVENT_LISTS.get(chat_id, []), offset),
        )
        return

    if parts[1] == "back":
        offset = HISTORY_TRUCK_PAGE.get(chat_id, 0)
        await edit_text_with_retry(callback.message,
            f"🚛 <b>Select a truck</b> to view harsh-braking events from the last {HISTORY_LOOKBACK_DAYS} days:",
            reply_markup=history_truck_keyboard(chat_id, offset),
        )
        return

    if parts[1] == "truck":
        if len(parts) < 4:
            await callback.message.answer("This truck selection has expired. Run /history again.")
            return
        account_id, vehicle_id = parts[2], parts[3]
        account = next((item for item in ACCOUNTS if item.account_id == account_id), None)
        if account is None:
            await callback.message.answer("Samsara organization not found.")
            return
        trucks = HISTORY_TRUCKS.get(chat_id, [])
        truck = next((item for item in trucks if str(item.get("id")) == f"{account_id}:{vehicle_id}"), {})
        truck_name = html.escape(str(truck.get("name") or f"Truck {vehicle_id}"))
        await edit_text_with_retry(callback.message, f"⏳ Searching braking history for <b>{truck_name}</b>…")
        try:
            events = await get_braking_history(vehicle_id, account)
        except Exception:
            logger.exception("Could not load braking history for vehicle %s", vehicle_id)
            await edit_text_with_retry(callback.message,
                "Could not retrieve history from Samsara. Check the "
                "<b>Read Safety Events &amp; Scores</b> permission and try again."
            )
            return
        if not events:
            offset = HISTORY_TRUCK_PAGE.get(chat_id, 0)
            await edit_text_with_retry(callback.message,
                f"No harsh-braking events were found for <b>{truck_name}</b> in the last {HISTORY_LOOKBACK_DAYS} days.",
                reply_markup=history_truck_keyboard(chat_id, offset),
            )
            return
        for key in [key for key in HISTORY_EVENTS if key[0] == chat_id]:
            del HISTORY_EVENTS[key]
        for event in events:
            event["_samsaraAccountId"] = account_id
        HISTORY_EVENT_LISTS[chat_id] = events
        HISTORY_SELECTED_TRUCK[chat_id] = truck_name
        await edit_text_with_retry(callback.message, history_event_page_text(chat_id), reply_markup=history_event_keyboard(chat_id, events))
        return

    if parts[1] == "event":
        if len(parts) < 4:
            await callback.message.answer("This event selection has expired. Run /history again.")
            return
        account_id, identifier = parts[2], parts[3]
        event = HISTORY_EVENTS.get((chat_id, account_id, identifier))
        if not event:
            await callback.message.answer("This history selection has expired. Run /history again.")
            return
        status = await callback.message.answer("⏳ Loading video and event details from Samsara…")
        account = next((item for item in ACCOUNTS if item.account_id == account_id), None)
        if account is None:
            await status.delete()
            await callback.message.answer("Samsara organization not found.")
            return
        try:
            await send_event_media(bot, chat_id, event, account)
        except Exception:
            logger.exception("Could not send selected safety event")
            await bot.send_message(
                chat_id=chat_id,
                text=event_caption(event) + "\n📹 Video is unavailable in Telegram.",
            )
        finally:
            try:
                await status.delete()
            except Exception:
                logger.debug("Could not remove history loading message", exc_info=True)


async def samsara_worker(bot: Bot, account: SamsaraAccount) -> None:
    timeout = aiohttp.ClientTimeout(total=45)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            cycle_end = datetime.now(timezone.utc)
            cycle_start = get_checkpoint(account)
            try:
                events = await get_events(session, account, cycle_start, cycle_end)
                for event in events:
                    await notify_event(bot, event, account)
                save_checkpoint(account, cycle_end)
                logger.info("Account %s: checked %s safety event(s)", account.account_id, len(events))
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Samsara polling failed; will retry")
            await asyncio.sleep(POLL_INTERVAL_SECONDS)


async def pti_reminder_worker(bot: Bot) -> None:
    """Remind drivers every few hours (PTI settings) once their PTI is overdue, until they submit."""
    while True:
        try:
            now = datetime.now(timezone.utc)
            with sqlite3.connect(STATE_DB) as conn:
                interval_row = conn.execute(
                    "SELECT value FROM pti_settings WHERE key='interval_hours'"
                ).fetchone()
                interval_hours = int(interval_row[0]) if interval_row else 24
                reminder_row = conn.execute(
                    "SELECT value FROM pti_settings WHERE key='reminder_hours'"
                ).fetchone()
                reminder_hours = max(1, int(reminder_row[0])) if reminder_row else 2
                drivers = conn.execute(
                    "SELECT d.telegram_user_id,d.truck_name,d.linked_at,"
                    "(SELECT s.id FROM pti_submissions s WHERE s.telegram_user_id=d.telegram_user_id "
                    "AND s.account_id=d.account_id AND s.truck_id=d.truck_id ORDER BY s.submitted_at DESC LIMIT 1),"
                    "(SELECT s.submitted_at FROM pti_submissions s WHERE s.telegram_user_id=d.telegram_user_id "
                    "AND s.account_id=d.account_id AND s.truck_id=d.truck_id ORDER BY s.submitted_at DESC LIMIT 1),"
                    "(SELECT s.invalidated_at FROM pti_submissions s WHERE s.telegram_user_id=d.telegram_user_id "
                    "AND s.account_id=d.account_id AND s.truck_id=d.truck_id ORDER BY s.submitted_at DESC LIMIT 1) "
                    "FROM driver_accounts d"
                ).fetchall()
            for user_id, truck_name, linked_at, submission_id, submitted_at, invalidated_at in drivers:
                anchor_text = invalidated_at or submitted_at or linked_at
                try:
                    anchor = datetime.fromisoformat(anchor_text.replace("Z", "+00:00"))
                    if anchor.tzinfo is None:
                        anchor = anchor.replace(tzinfo=timezone.utc)
                    # A returned PTI is due immediately, like pti_timing in the Mini App API.
                    deadline = anchor if invalidated_at else anchor + timedelta(hours=interval_hours)
                except (AttributeError, TypeError, ValueError):
                    logger.warning("Invalid PTI schedule timestamp for Telegram user %s", user_id)
                    continue
                # The first reminder goes out reminder_hours after the deadline, then again
                # every reminder_hours. Only the latest one is sent after a bot restart.
                reminder_number = int((now - deadline) / timedelta(hours=reminder_hours))
                if reminder_number < 1:
                    continue
                kind = f"overdue:{reminder_number}"
                schedule_anchor = anchor.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
                sent_at = utc_iso(now)
                with sqlite3.connect(STATE_DB) as conn:
                    cursor = conn.execute(
                        "INSERT OR IGNORE INTO pti_notifications(telegram_user_id,schedule_anchor,kind,sent_at) "
                        "VALUES(?,?,?,?)", (int(user_id), schedule_anchor, kind, sent_at)
                    )
                if cursor.rowcount == 0:
                    continue
                keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="Open PTI", web_app=WebAppInfo(url=WEBAPP_URL))
                ]]) if WEBAPP_URL.startswith("https://") else None
                message = (
                    f"⏰ PTI is overdue for <b>{html.escape(str(truck_name))}</b> "
                    f"(by {int((now - deadline) / timedelta(hours=1))} h). "
                    + ("Please complete your inspection in the PTI app." if keyboard else "Please open the bot and send /app to complete your inspection.")
                )
                try:
                    await bot.send_message(int(user_id), message, reply_markup=keyboard)
                    logger.info("Sent overdue PTI reminder %s to Telegram user %s", reminder_number, user_id)
                except Exception:
                    with sqlite3.connect(STATE_DB) as conn:
                        conn.execute(
                            "DELETE FROM pti_notifications WHERE telegram_user_id=? AND schedule_anchor=? AND kind=?",
                            (int(user_id), schedule_anchor, kind),
                        )
                    logger.warning("Could not send overdue PTI reminder to Telegram user %s", user_id, exc_info=True)
        except Exception:
            logger.exception("PTI reminder worker failed; will retry")
        await asyncio.sleep(300)


async def main() -> None:
    global ACCOUNTS
    ACCOUNTS = configured_accounts()
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("Set TELEGRAM_BOT_TOKEN in .env")
    if not shutil.which("ffmpeg"):
        raise RuntimeError(
            "Install FFmpeg and add ffmpeg.exe to PATH. It is required to format and compress videos for Telegram."
        )

    init_state()
    with sqlite3.connect(STATE_DB) as conn:
        admin_count = conn.execute("SELECT COUNT(*) FROM admins").fetchone()[0]
    if not admin_count:
        logger.warning("No bot admins configured; use /myid, add that ID to BOT_ADMIN_IDS, and restart the bot")
    bot = Bot(TELEGRAM_BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dispatcher = Dispatcher()
    dispatcher.message.outer_middleware(track_group_messages)
    dispatcher.my_chat_member.register(bot_membership_changed)
    dispatcher.message.register(app_command, Command("app"))
    dispatcher.message.register(start_command, Command("start"))
    dispatcher.message.register(my_id_command, Command("myid"))
    dispatcher.message.register(admin_command, Command("admin"))
    dispatcher.message.register(add_admin_command, Command("add_admin"))
    dispatcher.message.register(remove_admin_command, Command("remove_admin"))
    dispatcher.message.register(list_admins_command, Command("admins"))
    dispatcher.message.register(setup_command, Command("setup"))
    dispatcher.message.register(send_truck_list, Command("trucks"))
    dispatcher.message.register(send_truck_list, Command("locations"))
    dispatcher.message.register(history_command, Command("history"))
    dispatcher.callback_query.register(setup_callback, F.data.startswith("setup:"))
    dispatcher.callback_query.register(history_callback, F.data.startswith("hist:"))
    from aiohttp import web as aiohttp_web
    from miniapp_server import create_app
    web_runner = aiohttp_web.AppRunner(create_app(bot, sys.modules[__name__]))
    try:
        await web_runner.setup()
        web_site = aiohttp_web.TCPSite(web_runner, WEBAPP_HOST, WEBAPP_PORT)
        await web_site.start()
        logger.info("Telegram Mini App API listening on %s:%s", WEBAPP_HOST, WEBAPP_PORT)
        await bot.set_my_commands([
            BotCommand(command="app", description="Open FleetBot Web App"),
            BotCommand(command="myid", description="Show my Telegram ID"),
            BotCommand(command="admin", description="Bot administrator help"),
            BotCommand(command="setup", description="Select trucks for this group"),
            BotCommand(command="trucks", description="List trucks and their locations"),
            BotCommand(command="locations", description="Latest truck locations"),
            BotCommand(command="history", description="Recent events for a selected truck"),
        ])
        workers = [samsara_worker(bot, account) for account in ACCOUNTS]
        workers.append(pti_reminder_worker(bot))
        await asyncio.gather(dispatcher.start_polling(bot), *workers)
    finally:
        await web_runner.cleanup()
        await close_samsara_session()
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped")
