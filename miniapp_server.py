"""Role-protected Telegram Mini App API for fleet admins and assigned drivers."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import html
import json
import logging
import sqlite3
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qsl

from aiohttp import web
from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo


logger = logging.getLogger("samsara_webapp")
WEB_ROOT = Path(__file__).parent / "webapp"
INIT_DATA_MAX_AGE_SECONDS = 24 * 60 * 60
ACTIVE_EVENT_SENDS: set[tuple[int, str, str, str]] = set()
PTI_PHOTO_SLOTS = ("front", "rear", "driver_side", "passenger_side", "cabin", "dashboard")
PTI_PHOTO_RETENTION_DAYS = 15


def cleanup_expired_pti_photos(core) -> int:
    """Delete PTI image files and photo references older than the retention window."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=PTI_PHOTO_RETENTION_DAYS)).isoformat(
        timespec="seconds"
    ).replace("+00:00", "Z")
    photo_root = (Path(core_state_path(core)) / "pti_uploads").resolve()
    if not photo_root.exists():
        return 0

    with sqlite3.connect(core.STATE_DB) as conn:
        expired = conn.execute(
            "SELECT p.submission_id,p.slot,p.file_path FROM pti_photos p "
            "JOIN pti_submissions s ON s.id=p.submission_id "
            "WHERE julianday(s.submitted_at) <= julianday(?)",
            (cutoff,),
        ).fetchall()
        deleted = 0
        for submission_id, slot, filename in expired:
            path = (photo_root / filename).resolve()
            if path.parent != photo_root:
                logger.error("Skipping PTI cleanup for unsafe photo path %r", filename)
                continue
            try:
                path.unlink(missing_ok=True)
            except OSError:
                logger.exception("Could not delete expired PTI photo %s", filename)
                continue
            conn.execute(
                "DELETE FROM pti_photos WHERE submission_id=? AND slot=?",
                (submission_id, slot),
            )
            deleted += 1
        if deleted:
            conn.commit()
    if deleted:
        logger.info("Removed %s expired PTI photo(s) past %s days", deleted, PTI_PHOTO_RETENTION_DAYS)
    return deleted


async def pti_photo_retention_worker(app: web.Application) -> None:
    while True:
        try:
            await asyncio.to_thread(cleanup_expired_pti_photos, app["core"])
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("PTI photo retention cleanup failed")
        await asyncio.sleep(60 * 60)


async def start_pti_photo_retention(app: web.Application) -> None:
    app["pti_photo_retention_task"] = asyncio.create_task(pti_photo_retention_worker(app))


async def stop_pti_photo_retention(app: web.Application) -> None:
    task = app.get("pti_photo_retention_task")
    if task is not None:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


def ensure_pti_schema(core) -> None:
    with sqlite3.connect(core.STATE_DB) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS driver_accounts ("
            "telegram_user_id INTEGER PRIMARY KEY, telegram_name TEXT NOT NULL, account_id TEXT NOT NULL, "
            "truck_id TEXT NOT NULL, truck_name TEXT NOT NULL, linked_at TEXT NOT NULL, "
            "UNIQUE(account_id, truck_id))"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS pti_settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        conn.execute(
            "INSERT OR IGNORE INTO pti_settings(key, value) VALUES('interval_hours', '24')"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS pti_submissions ("
            "id TEXT PRIMARY KEY, telegram_user_id INTEGER NOT NULL, account_id TEXT NOT NULL, "
            "truck_id TEXT NOT NULL, submitted_at TEXT NOT NULL, invalidated_at TEXT, invalidation_reason TEXT)"
        )
        submission_columns = {row[1] for row in conn.execute("PRAGMA table_info(pti_submissions)").fetchall()}
        if "invalidated_at" not in submission_columns:
            conn.execute("ALTER TABLE pti_submissions ADD COLUMN invalidated_at TEXT")
        if "invalidation_reason" not in submission_columns:
            conn.execute("ALTER TABLE pti_submissions ADD COLUMN invalidation_reason TEXT")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pti_submissions_driver "
            "ON pti_submissions(telegram_user_id, submitted_at DESC)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS pti_photos ("
            "submission_id TEXT NOT NULL, slot TEXT NOT NULL, file_path TEXT NOT NULL, "
            "PRIMARY KEY(submission_id, slot), "
            "FOREIGN KEY(submission_id) REFERENCES pti_submissions(id) ON DELETE CASCADE)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS pti_notifications ("
            "telegram_user_id INTEGER NOT NULL, schedule_anchor TEXT NOT NULL, kind TEXT NOT NULL, "
            "sent_at TEXT NOT NULL, PRIMARY KEY(telegram_user_id,schedule_anchor,kind))"
        )


def validate_init_data(raw: str, bot_token: str) -> dict | None:
    """Validate Telegram WebApp initData using Telegram's documented HMAC scheme."""
    try:
        fields = dict(parse_qsl(raw, keep_blank_values=True, strict_parsing=True))
        received_hash = fields.pop("hash")
        auth_date = int(fields["auth_date"])
        now = int(time.time())
        if auth_date > now + 60 or now - auth_date > INIT_DATA_MAX_AGE_SECONDS:
            return None
        check_string = "\n".join(f"{key}={value}" for key, value in sorted(fields.items()))
        secret_key = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
        expected_hash = hmac.new(secret_key, check_string.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(received_hash, expected_hash):
            return None
        user = json.loads(fields["user"])
        return user if isinstance(user, dict) else None
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None


async def require_admin(request: web.Request) -> dict:
    bot: Bot = request.app["bot"]
    raw = request.headers.get("X-Telegram-Init-Data", "")
    user = validate_init_data(raw, bot.token)
    core = request.app["core"]
    if user is None or not core.is_admin(int(user.get("id", 0))):
        raise web.HTTPForbidden(text="Telegram admin authentication is required")
    return user


def get_driver_link(core, user_id: int) -> dict | None:
    with sqlite3.connect(core.STATE_DB) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT telegram_user_id, telegram_name, account_id, truck_id, truck_name, linked_at "
            "FROM driver_accounts WHERE telegram_user_id = ?", (user_id,)
        ).fetchone()
    return dict(row) if row else None


async def require_user(request: web.Request) -> tuple[dict, str, dict | None]:
    bot: Bot = request.app["bot"]
    user = validate_init_data(request.headers.get("X-Telegram-Init-Data", ""), bot.token)
    core = request.app["core"]
    if user is None:
        raise web.HTTPForbidden(text="Valid Telegram authentication is required")
    user_id = int(user.get("id", 0))
    if core.is_admin(user_id):
        return user, "admin", None
    link = get_driver_link(core, user_id)
    if link is None:
        raise web.HTTPForbidden(text="Your Telegram account is not linked to a truck. Contact your fleet administrator.")
    return user, "driver", link


async def index(_: web.Request) -> web.FileResponse:
    return web.FileResponse(WEB_ROOT / "index.html")


async def api_me(request: web.Request) -> web.Response:
    user, role, link = await require_user(request)
    core = request.app["core"]
    with sqlite3.connect(core.STATE_DB) as conn:
        row = conn.execute("SELECT value FROM pti_settings WHERE key='interval_hours'").fetchone()
    interval_hours = int(row[0]) if row else 24
    if link:
        link["telegram_name"] = user.get("first_name") or link["telegram_name"]
    return web.json_response({
        "id": user.get("id"), "name": user.get("first_name", "Admin" if role == "admin" else "Driver"),
        "role": role, "truck": link, "ptiIntervalHours": interval_hours,
    })


def assigned_groups(core, account_id: str, vehicle_id: str) -> list[tuple[int, str]]:
    with sqlite3.connect(core.STATE_DB) as conn:
        rows = conn.execute(
            "SELECT DISTINCT chat_id FROM group_trucks WHERE account_id = ? AND truck_id = ? ORDER BY chat_id",
            (account_id, vehicle_id),
        ).fetchall()
    return [(int(row[0]), str(row[0])) for row in rows]


async def chat_title(bot: Bot, chat_id: int, fallback: str) -> str:
    try:
        chat = await bot.get_chat(chat_id)
        return chat.title or chat.full_name or chat.username or fallback
    except Exception:
        logger.info("Could not resolve Telegram group title for %s", chat_id, exc_info=True)
        return fallback


def pti_status_by_truck(core) -> dict[tuple[str, str], dict]:
    """Return current PTI deadline status for trucks with a linked driver."""
    with sqlite3.connect(core.STATE_DB) as conn:
        interval_row = conn.execute(
            "SELECT value FROM pti_settings WHERE key='interval_hours'"
        ).fetchone()
        rows = conn.execute(
            "SELECT d.account_id,d.truck_id,d.linked_at,"
            "(SELECT s.submitted_at FROM pti_submissions s WHERE s.telegram_user_id=d.telegram_user_id "
            "AND s.account_id=d.account_id AND s.truck_id=d.truck_id ORDER BY s.submitted_at DESC LIMIT 1),"
            "(SELECT s.invalidated_at FROM pti_submissions s WHERE s.telegram_user_id=d.telegram_user_id "
            "AND s.account_id=d.account_id AND s.truck_id=d.truck_id ORDER BY s.submitted_at DESC LIMIT 1) "
            "FROM driver_accounts d"
        ).fetchall()
    interval_hours = int(interval_row[0]) if interval_row else 24
    now = datetime.now(timezone.utc)
    statuses = {}
    for account_id, truck_id, linked_at, submitted_at, invalidated_at in rows:
        timing = pti_timing(
            submitted_at or linked_at, interval_hours, now,
            invalidated_at, first_submission=not submitted_at,
        )
        statuses[(str(account_id), str(truck_id))] = {
            "overdue": timing["isDue"],
            "invalidated": timing["isInvalidated"],
        }
    return statuses


async def api_trucks(request: web.Request) -> web.Response:
    await require_admin(request)
    company_filter = request.query.get("company", "all")
    if company_filter not in {"all", "1", "2"}:
        raise web.HTTPBadRequest(text="Invalid company filter")

    bot: Bot = request.app["bot"]
    core = request.app["core"]
    with sqlite3.connect(core.STATE_DB) as conn:
        assignment_rows = conn.execute(
            "SELECT account_id, truck_id, chat_id FROM group_trucks ORDER BY chat_id"
        ).fetchall()
        driver_rows = conn.execute(
            "SELECT telegram_user_id, telegram_name, account_id, truck_id FROM driver_accounts"
        ).fetchall()
    drivers = {(str(row[2]), str(row[3])): {"telegramUserId": str(row[0]), "name": row[1]} for row in driver_rows}
    pti_statuses = pti_status_by_truck(core)
    assignments: dict[tuple[str, str], list[tuple[int, str]]] = {}
    all_groups: dict[int, str] = {}
    for account_id, truck_id, chat_id in assignment_rows:
        group_id = int(chat_id)
        assignments.setdefault((str(account_id), str(truck_id)), []).append((group_id, str(group_id)))
        all_groups[group_id] = str(group_id)
    resolved_names = await asyncio.gather(
        *(chat_title(bot, group_id, fallback) for group_id, fallback in all_groups.items())
    )
    group_names = dict(zip(all_groups, resolved_names))
    result: list[dict] = []
    for account in core.ACCOUNTS:
        if company_filter != "all" and account.account_id != company_filter:
            continue
        try:
            vehicles = await core.get_truck_locations(account)
        except Exception:
            logger.exception("Could not fetch vehicles for Samsara account %s", account.account_id)
            continue
        for vehicle in vehicles:
            vehicle_id = str(vehicle.get("id", ""))
            if not vehicle_id:
                continue
            raw_groups = assignments.get((account.account_id, vehicle_id), [])
            gps = vehicle.get("gps") if isinstance(vehicle.get("gps"), dict) else {}
            location = core.event_location_text({"location": gps}) if gps else None
            driver = vehicle.get("driver") if isinstance(vehicle.get("driver"), dict) else {}
            result.append({
                "id": vehicle_id,
                "name": str(vehicle.get("name") or f"Truck {vehicle_id}"),
                "accountId": account.account_id,
                "company": ("U.Z. Freight" if account.account_id == "1" else "Safe & SN"),
                "driver": str(driver.get("name") or vehicle.get("driverName") or ""),
                "location": location or "Location unavailable",
                "latitude": gps.get("latitude"),
                "longitude": gps.get("longitude"),
                "speedMph": gps.get("speedMilesPerHour"),
                "gpsTime": gps.get("time"),
                "driverAccount": drivers.get((account.account_id, vehicle_id)),
                "ptiOverdue": pti_statuses.get((account.account_id, vehicle_id), {}).get("overdue", False),
                "ptiInvalidated": pti_statuses.get((account.account_id, vehicle_id), {}).get("invalidated", False),
                "groups": [
                    {"id": str(group_id), "name": group_names.get(group_id, fallback)}
                    for group_id, fallback in raw_groups
                ],
            })
    result.sort(key=lambda item: (item["company"], item["name"].casefold()))
    return web.json_response({"trucks": result})


async def api_attach_driver(request: web.Request) -> web.Response:
    await require_admin(request)
    core = request.app["core"]
    account_id, vehicle_id = request.match_info["account_id"], request.match_info["vehicle_id"]
    if not any(item.account_id == account_id for item in core.ACCOUNTS):
        raise web.HTTPNotFound(text="Company not found")
    try:
        payload = await request.json()
        telegram_user_id = int(str(payload.get("telegramUserId", "")).strip())
    except (ValueError, AttributeError, json.JSONDecodeError, web.HTTPBadRequest) as error:
        raise web.HTTPBadRequest(text="Enter a valid Telegram user ID") from error
    if telegram_user_id <= 0:
        raise web.HTTPBadRequest(text="Enter a valid Telegram user ID")
    if core.is_admin(telegram_user_id):
        raise web.HTTPBadRequest(text="A bot administrator account cannot be assigned as a driver")
    telegram_name = str(payload.get("name") or "Driver").strip()[:100] or "Driver"
    truck_name = str(payload.get("truckName") or vehicle_id).strip()[:160]
    linked_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    with sqlite3.connect(core.STATE_DB) as conn:
        existing_user = conn.execute(
            "SELECT account_id, truck_id FROM driver_accounts WHERE telegram_user_id=?", (telegram_user_id,)
        ).fetchone()
        existing_truck = conn.execute(
            "SELECT telegram_user_id FROM driver_accounts WHERE account_id=? AND truck_id=?", (account_id, vehicle_id)
        ).fetchone()
        if existing_user and (existing_user[0], existing_user[1]) != (account_id, vehicle_id):
            raise web.HTTPConflict(text="This Telegram user is already linked to another truck. Unlink it first.")
        if existing_truck and int(existing_truck[0]) != telegram_user_id:
            raise web.HTTPConflict(text="This truck is already linked to another Telegram user. Unlink it first.")
        conn.execute(
            "INSERT INTO driver_accounts(telegram_user_id,telegram_name,account_id,truck_id,truck_name,linked_at) "
            "VALUES(?,?,?,?,?,?) ON CONFLICT(telegram_user_id) DO UPDATE SET "
            "telegram_name=excluded.telegram_name,account_id=excluded.account_id,truck_id=excluded.truck_id,"
            "truck_name=excluded.truck_name,linked_at=excluded.linked_at",
            (telegram_user_id, telegram_name, account_id, vehicle_id, truck_name, linked_at),
        )
    return web.json_response({"driver": {"telegramUserId": str(telegram_user_id), "name": telegram_name}})


async def api_remove_driver(request: web.Request) -> web.Response:
    await require_admin(request)
    core = request.app["core"]
    account_id, vehicle_id = request.match_info["account_id"], request.match_info["vehicle_id"]
    with sqlite3.connect(core.STATE_DB) as conn:
        cursor = conn.execute("DELETE FROM driver_accounts WHERE account_id=? AND truck_id=?", (account_id, vehicle_id))
    if cursor.rowcount == 0:
        raise web.HTTPNotFound(text="No Telegram driver is linked to this truck")
    return web.json_response({"removed": True})


def parse_utc_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def pti_timing(
    anchor_value: str, interval_hours: int, now: datetime,
    invalidated_at: str | None = None, first_submission: bool = False,
) -> dict:
    anchor = parse_utc_timestamp(invalidated_at or anchor_value)
    is_invalidated = invalidated_at is not None
    due_at = anchor if is_invalidated else anchor + timedelta(hours=interval_hours)
    can_submit_at = anchor if is_invalidated or first_submission else anchor + timedelta(hours=max(0, interval_hours - 4))
    return {
        "dueAt": due_at.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "canSubmitAt": can_submit_at.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "isDue": is_invalidated or now >= due_at,
        "canSubmit": is_invalidated or now >= can_submit_at,
        "isInvalidated": is_invalidated,
    }


async def api_pti_settings(request: web.Request) -> web.Response:
    await require_admin(request)
    core = request.app["core"]
    if request.method == "POST":
        try:
            payload = await request.json()
            hours = int(payload.get("intervalHours", 24))
        except (ValueError, AttributeError, json.JSONDecodeError, web.HTTPBadRequest) as error:
            raise web.HTTPBadRequest(text="Enter an interval from 1 to 168 hours") from error
        if not 1 <= hours <= 168:
            raise web.HTTPBadRequest(text="PTI interval must be between 1 and 168 hours")
        with sqlite3.connect(core.STATE_DB) as conn:
            conn.execute(
                "INSERT INTO pti_settings(key,value) VALUES('interval_hours',?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(hours),)
            )
    with sqlite3.connect(core.STATE_DB) as conn:
        row = conn.execute("SELECT value FROM pti_settings WHERE key='interval_hours'").fetchone()
        linked = conn.execute("SELECT COUNT(*) FROM driver_accounts").fetchone()[0]
        drivers = conn.execute(
            "SELECT d.telegram_user_id,d.telegram_name,d.account_id,d.truck_id,d.truck_name,d.linked_at,"
            "(SELECT s.id FROM pti_submissions s WHERE s.telegram_user_id=d.telegram_user_id "
            "AND s.account_id=d.account_id AND s.truck_id=d.truck_id ORDER BY s.submitted_at DESC LIMIT 1),"
            "(SELECT s.submitted_at FROM pti_submissions s WHERE s.telegram_user_id=d.telegram_user_id "
            "AND s.account_id=d.account_id AND s.truck_id=d.truck_id ORDER BY s.submitted_at DESC LIMIT 1),"
            "(SELECT s.invalidated_at FROM pti_submissions s WHERE s.telegram_user_id=d.telegram_user_id "
            "AND s.account_id=d.account_id AND s.truck_id=d.truck_id ORDER BY s.submitted_at DESC LIMIT 1),"
            "(SELECT s.invalidation_reason FROM pti_submissions s WHERE s.telegram_user_id=d.telegram_user_id "
            "AND s.account_id=d.account_id AND s.truck_id=d.truck_id ORDER BY s.submitted_at DESC LIMIT 1) "
            "FROM driver_accounts d ORDER BY d.truck_name COLLATE NOCASE"
        ).fetchall()
    interval_hours = int(row[0]) if row else 24
    now = datetime.now(timezone.utc)
    driver_items = []
    for user_id, name, account_id, truck_id, truck_name, linked_at, last_id, submitted_at, invalidated_at, invalidation_reason in drivers:
        timing = pti_timing(submitted_at or linked_at, interval_hours, now, invalidated_at, first_submission=not submitted_at)
        driver_items.append({
            "telegramUserId": str(user_id), "name": name, "accountId": account_id,
            "truckId": truck_id, "truckName": truck_name,
            "company": "U.Z. Freight" if account_id == "1" else "Safe & SN",
            "submissionId": last_id, "lastSubmittedAt": submitted_at,
            "invalidationReason": invalidation_reason, **timing,
        })
    return web.json_response({"intervalHours": interval_hours, "linkedDrivers": linked, "drivers": driver_items})


def pti_status_payload(core, user_id: int, link: dict) -> dict:
    with sqlite3.connect(core.STATE_DB) as conn:
        conn.row_factory = sqlite3.Row
        interval_row = conn.execute("SELECT value FROM pti_settings WHERE key='interval_hours'").fetchone()
        latest = conn.execute(
            "SELECT id,submitted_at,invalidated_at,invalidation_reason FROM pti_submissions "
            "WHERE telegram_user_id=? AND account_id=? AND truck_id=? "
            "ORDER BY submitted_at DESC LIMIT 1",
            (user_id, link["account_id"], link["truck_id"]),
        ).fetchone()
        history = conn.execute(
            "SELECT id,submitted_at,invalidated_at,invalidation_reason FROM pti_submissions "
            "WHERE telegram_user_id=? AND account_id=? AND truck_id=? "
            "ORDER BY submitted_at DESC LIMIT 20",
            (user_id, link["account_id"], link["truck_id"]),
        ).fetchall()
    interval_hours = int(interval_row[0]) if interval_row else 24
    now = datetime.now(timezone.utc)
    anchor = latest["submitted_at"] if latest else link["linked_at"]
    timing = pti_timing(
        anchor, interval_hours, now,
        latest["invalidated_at"] if latest else None,
        first_submission=latest is None,
    )
    return {
        "truck": {
            "accountId": link["account_id"], "truckId": link["truck_id"], "name": link["truck_name"],
            "company": "U.Z. Freight" if link["account_id"] == "1" else "Safe & SN",
        },
        "intervalHours": interval_hours,
        "lastSubmittedAt": latest["submitted_at"] if latest else None,
        "isInvalidated": bool(latest and latest["invalidated_at"]),
        "invalidationReason": latest["invalidation_reason"] if latest else None,
        **timing,
        "history": [{
            "id": row["id"], "submittedAt": row["submitted_at"],
            "invalidatedAt": row["invalidated_at"], "invalidationReason": row["invalidation_reason"],
            "photos": list(PTI_PHOTO_SLOTS),
        } for row in history],
    }


async def api_pti_status(request: web.Request) -> web.Response:
    user, role, link = await require_user(request)
    if role != "driver" or link is None:
        raise web.HTTPForbidden(text="This PTI page is for linked drivers")
    return web.json_response(pti_status_payload(request.app["core"], int(user["id"]), link))


async def api_driver_activity(request: web.Request) -> web.Response:
    user, role, link = await require_user(request)
    if role != "driver" or link is None:
        raise web.HTTPForbidden(text="This activity feed is for linked drivers")
    core = request.app["core"]
    account = next((item for item in core.ACCOUNTS if item.account_id == link["account_id"]), None)
    if account is None:
        raise web.HTTPBadGateway(text="The Samsara account for this truck is unavailable")
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=7)
    try:
        try:
            events = await core.get_braking_history(link["truck_id"], account, start, end)
        except Exception:
            logger.info("Vehicle-filtered activity query failed for driver %s; retrying account-wide", user.get("id"), exc_info=True)
            events = []
        matching = [event for event in events if event_vehicle_id(core, event) == link["truck_id"]]
        if not matching:
            events = await core.get_braking_events(account, start, end)
            matching = [event for event in events if event_vehicle_id(core, event) == link["truck_id"]]
        formatted = []
        for event in matching:
            stamp = core.event_timestamp(event)
            if stamp and start <= stamp < end:
                formatted.append((stamp, format_alert_event(core, event, account.account_id, link["truck_id"])))
        formatted.sort(key=lambda pair: pair[0], reverse=True)
        return web.json_response({"events": [item for _, item in formatted[:5]]})
    except Exception as error:
        logger.exception("Could not load driver safety activity for Telegram user %s", user.get("id"))
        raise web.HTTPBadGateway(text="Could not load recent safety activity") from error


async def api_pti_submit(request: web.Request) -> web.Response:
    user, role, link = await require_user(request)
    if role != "driver" or link is None:
        raise web.HTTPForbidden(text="This PTI action is for linked drivers")
    schedule = pti_status_payload(request.app["core"], int(user["id"]), link)
    if not schedule["canSubmit"]:
        core = request.app["core"]
        available_at = parse_utc_timestamp(schedule["canSubmitAt"]).astimezone(
            core.ZoneInfo(core.EVENT_TIMEZONE)
        ).strftime("%b %d, %Y at %I:%M %p %Z")
        raise web.HTTPConflict(text=f"You can start the next PTI at {available_at}.")
    reader = await request.multipart()
    fields: dict[str, tuple[bytes, str]] = {}
    total_size = 0
    while part := await reader.next():
        if part.name not in PTI_PHOTO_SLOTS or part.name in fields or not part.filename:
            raise web.HTTPBadRequest(text="Submit one camera photo for each required PTI view")
        content_type = part.headers.get("Content-Type", "")
        if content_type not in {"image/jpeg", "image/png", "image/webp"}:
            raise web.HTTPBadRequest(text="PTI photos must be JPEG, PNG, or WebP images")
        content = await part.read(decode=False)
        total_size += len(content)
        if len(content) > 8 * 1024 * 1024 or total_size > 30 * 1024 * 1024:
            raise web.HTTPRequestEntityTooLarge(max_size=30 * 1024 * 1024, actual_size=total_size)
        if not content:
            raise web.HTTPBadRequest(text="A PTI photo is empty")
        signatures = {
            "image/jpeg": content.startswith(b"\xff\xd8\xff"),
            "image/png": content.startswith(b"\x89PNG\r\n\x1a\n"),
            "image/webp": len(content) >= 12 and content.startswith(b"RIFF") and content[8:12] == b"WEBP",
        }
        if not signatures.get(content_type, False):
            raise web.HTTPBadRequest(text="The captured photo data is invalid. Please retake it.")
        fields[part.name] = (content, content_type)
    if set(fields) != set(PTI_PHOTO_SLOTS):
        raise web.HTTPBadRequest(text="Take all six required PTI photos before submitting")

    core = request.app["core"]
    photo_root = Path(core_state_path(core)) / "pti_uploads"
    photo_root.mkdir(parents=True, exist_ok=True)
    submission_id = uuid.uuid4().hex
    saved: list[tuple[str, str]] = []
    try:
        for slot, (content, content_type) in fields.items():
            suffix = {"image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp"}[content_type]
            filename = f"{submission_id}_{slot}{suffix}"
            (photo_root / filename).write_bytes(content)
            saved.append((slot, filename))
        submitted_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        with sqlite3.connect(core.STATE_DB) as conn:
            conn.execute(
                "INSERT INTO pti_submissions(id,telegram_user_id,account_id,truck_id,submitted_at) VALUES(?,?,?,?,?)",
                (submission_id, int(user["id"]), link["account_id"], link["truck_id"], submitted_at),
            )
            conn.executemany(
                "INSERT INTO pti_photos(submission_id,slot,file_path) VALUES(?,?,?)",
                [(submission_id, slot, filename) for slot, filename in saved],
            )
    except Exception:
        for _, filename in saved:
            (photo_root / filename).unlink(missing_ok=True)
        raise
    return web.json_response({"id": submission_id, "submittedAt": submitted_at}, status=201)


def pti_app_keyboard(core) -> InlineKeyboardMarkup | None:
    url = str(getattr(core, "WEBAPP_URL", "") or "").strip()
    if not url.startswith("https://"):
        return None
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Open PTI", web_app=WebAppInfo(url=url))
    ]])


async def api_notify_truck_driver(request: web.Request) -> web.Response:
    await require_admin(request)
    core = request.app["core"]
    account_id, truck_id = request.match_info["account_id"], request.match_info["vehicle_id"]
    with sqlite3.connect(core.STATE_DB) as conn:
        row = conn.execute(
            "SELECT telegram_user_id,telegram_name,truck_name FROM driver_accounts WHERE account_id=? AND truck_id=?",
            (account_id, truck_id),
        ).fetchone()
    if not row:
        raise web.HTTPNotFound(text="No driver is linked to this truck")
    bot: Bot = request.app["bot"]
    keyboard = pti_app_keyboard(core)
    next_step = (
        "Please open the PTI app and complete the required six photos."
        if keyboard else "Open the bot and send /app to complete the required six photos."
    )
    text = f"📋 PTI reminder for <b>{html.escape(row[2])}</b>. {next_step}"
    try:
        await bot.send_message(int(row[0]), text, reply_markup=keyboard)
    except Exception as error:
        logger.warning("Could not send admin PTI reminder to Telegram user %s", row[0], exc_info=True)
        raise web.HTTPBadGateway(
            text="Could not message this driver. Ask them to open the bot once, then try again."
        ) from error
    return web.json_response({"sent": True, "telegramUserId": str(row[0])})


async def api_invalidate_pti(request: web.Request) -> web.Response:
    await require_admin(request)
    core = request.app["core"]
    submission_id = request.match_info["submission_id"]
    try:
        payload = await request.json()
    except (json.JSONDecodeError, web.HTTPBadRequest) as error:
        raise web.HTTPBadRequest(text="Enter a reason for invalidating this PTI") from error
    reason = str(payload.get("reason", "")).strip() if isinstance(payload, dict) else ""
    if not 3 <= len(reason) <= 500:
        raise web.HTTPBadRequest(text="The reason must contain between 3 and 500 characters")
    invalidated_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    try:
        with sqlite3.connect(core.STATE_DB, timeout=30) as conn:
            row = conn.execute(
                "SELECT s.telegram_user_id,COALESCE(d.truck_name,s.truck_id),"
                "s.invalidated_at,s.invalidation_reason "
                "FROM pti_submissions s LEFT JOIN driver_accounts d "
                "ON d.telegram_user_id=s.telegram_user_id AND d.account_id=s.account_id "
                "AND d.truck_id=s.truck_id WHERE s.id=?",
                (submission_id,),
            ).fetchone()
            if not row:
                raise web.HTTPNotFound(text="PTI submission not found")
            # Retrying the same return action is safe and can recover a notice
            # if an earlier request stopped after saving the invalidation.
            if row[2] and row[3] != reason:
                raise web.HTTPConflict(text="This PTI submission was already returned with a different reason")
            if not row[2]:
                conn.execute(
                    "UPDATE pti_submissions SET invalidated_at=?,invalidation_reason=? "
                    "WHERE id=? AND invalidated_at IS NULL",
                    (invalidated_at, reason, submission_id),
                )
    except web.HTTPException:
        raise
    except sqlite3.Error as error:
        database_path = str(Path(core.STATE_DB).resolve())
        logger.exception(
            "Could not save return state for PTI submission %s in database %s",
            submission_id, database_path,
        )
        raise web.HTTPInternalServerError(
            text=f"SQLite error for {database_path}: {error}"
        ) from error

    notification_sent = False
    notification_error = None
    try:
        keyboard = pti_app_keyboard(core)
    except Exception:
        keyboard = None
        logger.exception("Could not build the PTI WebApp button; will notify driver without it")
    next_step = "Open the PTI app to redo the inspection." if keyboard else "Open the bot and send /app to redo the inspection."
    message = (
        f"⚠️ Your PTI for <b>{html.escape(row[1])}</b> was returned and needs to be completed again.\n\n"
        f"Reason: {html.escape(reason)}\n\n{next_step}"
    )
    try:
        await request.app["bot"].send_message(
            int(row[0]), message, reply_markup=keyboard
        )
        notification_sent = True
    except Exception as error:
        notification_error = str(error)[:300] or error.__class__.__name__
        logger.exception("PTI submission %s was invalidated; driver notice failed", submission_id)
    return web.json_response({
        "invalidated": True, "notificationSent": notification_sent,
        "warning": notification_error,
    })


def core_state_path(core) -> str:
    return str(Path(core.STATE_DB).resolve().parent)


async def api_pti_photo(request: web.Request) -> web.StreamResponse:
    user, role, link = await require_user(request)
    core = request.app["core"]
    submission_id, slot = request.match_info["submission_id"], request.match_info["slot"]
    with sqlite3.connect(core.STATE_DB) as conn:
        row = conn.execute(
            "SELECT p.file_path,s.telegram_user_id,s.account_id,s.truck_id FROM pti_photos p JOIN pti_submissions s "
            "ON s.id=p.submission_id WHERE p.submission_id=? AND p.slot=?", (submission_id, slot)
        ).fetchone()
    if row is None:
        with sqlite3.connect(core.STATE_DB) as conn:
            submission = conn.execute(
                "SELECT submitted_at FROM pti_submissions WHERE id=?", (submission_id,)
            ).fetchone()
        if submission:
            submitted_at = datetime.fromisoformat(submission[0].replace("Z", "+00:00"))
            if submitted_at.tzinfo is None:
                submitted_at = submitted_at.replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) - submitted_at >= timedelta(days=PTI_PHOTO_RETENTION_DAYS):
                raise web.HTTPGone(text="PTI photos are kept for 15 days and have expired")
        raise web.HTTPNotFound(text="PTI photo not found")
    if role != "admin" and int(row[1]) != int(user["id"]):
        raise web.HTTPForbidden(text="You cannot view another driver's PTI photo")
    if role != "admin" and (row[2] != link["account_id"] or row[3] != link["truck_id"]):
        raise web.HTTPForbidden(text="This PTI photo is not part of your current truck assignment")
    photo_dir = (Path(core_state_path(core)) / "pti_uploads").resolve()
    path = (photo_dir / row[0]).resolve()
    if path.parent != photo_dir or not path.is_file():
        if path.parent == photo_dir and not path.exists():
            raise web.HTTPGone(text="PTI photo is no longer available; photos are kept for 15 days")
        raise web.HTTPNotFound(text="PTI photo file is unavailable")
    return web.FileResponse(path, headers={"Cache-Control": "private, no-store"})


def normalize_telegram_group_id(value) -> int:
    raw = str(value or "").strip().replace(" ", "")
    digits = raw.lstrip("-")
    if not digits.isdigit():
        raise web.HTTPBadRequest(text="Enter a valid Telegram group ID")
    normalized = digits if digits.startswith("100") else "100" + digits
    if len(normalized) > 18:
        raise web.HTTPBadRequest(text="Telegram group ID is too long")
    return -int(normalized)


async def api_add_truck_group(request: web.Request) -> web.Response:
    await require_admin(request)
    core = request.app["core"]
    account_id = request.match_info["account_id"]
    vehicle_id = request.match_info["vehicle_id"]
    if not any(item.account_id == account_id for item in core.ACCOUNTS):
        raise web.HTTPNotFound(text="Company not found")
    try:
        payload = await request.json()
    except (json.JSONDecodeError, web.HTTPBadRequest) as error:
        raise web.HTTPBadRequest(text="Invalid request body") from error
    if not isinstance(payload, dict):
        raise web.HTTPBadRequest(text="Invalid request body")
    chat_id = normalize_telegram_group_id(payload.get("groupId"))
    bot: Bot = request.app["bot"]
    try:
        chat = await bot.get_chat(chat_id)
    except Exception as error:
        raise web.HTTPBadRequest(
            text="The bot cannot access this group. Add the bot to the group and try again."
        ) from error
    if getattr(chat, "type", None) not in {"group", "supergroup"}:
        raise web.HTTPBadRequest(text="This Telegram chat is not a group")

    truck_name = str(payload.get("truckName") or vehicle_id).strip()[:160]
    with sqlite3.connect(core.STATE_DB) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO group_trucks(chat_id, account_id, truck_id, truck_name) VALUES(?, ?, ?, ?)",
            (chat_id, account_id, vehicle_id, truck_name),
        )
    title = chat.title or chat.full_name or str(chat_id)
    return web.json_response({"group": {"id": str(chat_id), "name": title}})


async def api_remove_truck_group(request: web.Request) -> web.Response:
    await require_admin(request)
    account_id = request.match_info["account_id"]
    vehicle_id = request.match_info["vehicle_id"]
    try:
        chat_id = int(request.match_info["chat_id"])
    except ValueError as error:
        raise web.HTTPBadRequest(text="Invalid Telegram group ID") from error
    core = request.app["core"]
    with sqlite3.connect(core.STATE_DB) as conn:
        cursor = conn.execute(
            "DELETE FROM group_trucks WHERE chat_id = ? AND account_id = ? AND truck_id = ?",
            (chat_id, account_id, vehicle_id),
        )
        if cursor.rowcount == 0:
            raise web.HTTPNotFound(text="Telegram group assignment not found")
    return web.json_response({"removed": True, "groupId": str(chat_id)})


async def api_truck_events(request: web.Request) -> web.Response:
    await require_admin(request)
    account_id = request.match_info["account_id"]
    vehicle_id = request.match_info["vehicle_id"]
    core = request.app["core"]
    account = next((item for item in core.ACCOUNTS if item.account_id == account_id), None)
    if account is None:
        raise web.HTTPNotFound(text="Company not found")
    if request.query.get("from") or request.query.get("to"):
        start, end, from_date, to_date = parse_local_date_range(
            request.query.get("from", ""), request.query.get("to", ""), core
        )
    else:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=core.HISTORY_LOOKBACK_DAYS)
        from_date = to_date = None
    try:
        events = await core.get_braking_history(vehicle_id, account, start, end)
    except Exception:
        logger.warning(
            "Vehicle-filtered safety-event query failed for account %s, vehicle %s; retrying account-wide",
            account_id, vehicle_id, exc_info=True,
        )
        events = []
    recent: list[dict] = []

    def append_matching_event(event: dict) -> None:
        if event_vehicle_id(core, event) != vehicle_id:
            return
        stamp = core.event_timestamp(event)
        if not stamp or not (start <= stamp < end):
            return
        location = core.event_location_text(event)
        coords = event.get("location") if isinstance(event.get("location"), dict) else {}
        driver = event.get("driver") if isinstance(event.get("driver"), dict) else {}
        recent.append({
            "id": core.event_id(event),
            "accountId": account_id,
            "truckId": vehicle_id,
            "truckName": str(((event.get("asset") or event.get("vehicle") or {}).get("name") if isinstance(event.get("asset") or event.get("vehicle") or {}, dict) else "") or f"Truck {vehicle_id}"),
            "eventDate": stamp.astimezone(core.ZoneInfo(core.EVENT_TIMEZONE)).date().isoformat(),
            "time": core.format_event_time(event),
            "location": location or "Location unavailable",
            "mapUrl": (
                f"https://maps.google.com/?q={coords['latitude']},{coords['longitude']}"
                if coords.get("latitude") is not None and coords.get("longitude") is not None else None
            ),
            "driver": str(driver.get("name") or event.get("driverName") or "Driver not assigned"),
            "type": "Harsh braking",
        })

    for event in events:
        append_matching_event(event)

    # Samsara's assetIds filter can occasionally omit an event; match against
    # the organization's date-filtered stream before telling the admin there are none.
    if not recent:
        try:
            org_events = await core.get_braking_events(account, start, end)
        except Exception as error:
            logger.exception(
                "Could not load organization-wide safety events for account %s",
                account_id,
            )
            raise web.HTTPBadGateway(text="Could not load braking events from Samsara") from error
        for event in org_events:
            append_matching_event(event)
        if recent:
            logger.info(
                "Truck history fallback found %s event(s) for account %s, vehicle %s",
                len(recent), account_id, vehicle_id,
            )

    return web.json_response({
        "events": recent[:100],
        "from": from_date.isoformat() if from_date else None,
        "to": to_date.isoformat() if to_date else None,
    })


def parse_local_date_range(from_value: str, to_value: str, core):
    try:
        from_date = date.fromisoformat(from_value)
        to_date = date.fromisoformat(to_value)
    except ValueError as error:
        raise web.HTTPBadRequest(text="Enter valid From and To dates") from error
    if from_date > to_date:
        raise web.HTTPBadRequest(text="The From date must not be after the To date")
    if (to_date - from_date).days > 365:
        raise web.HTTPBadRequest(text="Choose a date range of no more than 366 days")
    local_zone = core.ZoneInfo(core.EVENT_TIMEZONE)
    start_local = datetime.combine(from_date, datetime.min.time(), tzinfo=local_zone)
    end_local = datetime.combine(to_date + timedelta(days=1), datetime.min.time(), tzinfo=local_zone)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc), from_date, to_date


def event_vehicle_id(core, event: dict) -> str | None:
    value = core._vehicle_id(event)
    return str(value) if value else None


def format_alert_event(core, event: dict, account_id: str, vehicle_id: str) -> dict:
    stamp = core.event_timestamp(event)
    location = core.event_location_text(event)
    coords = event.get("location") if isinstance(event.get("location"), dict) else {}
    driver = event.get("driver") if isinstance(event.get("driver"), dict) else {}
    asset = event.get("asset") or event.get("vehicle") or {}
    truck_name = asset.get("name") if isinstance(asset, dict) else None
    return {
        "id": core.event_id(event),
        "accountId": account_id,
        "truckId": vehicle_id,
        "truckName": str(truck_name or f"Truck {vehicle_id}"),
        "eventDate": stamp.astimezone(core.ZoneInfo(core.EVENT_TIMEZONE)).date().isoformat() if stamp else "",
        "time": core.format_event_time(event),
        "location": location or "Location unavailable",
        "mapUrl": (
            f"https://maps.google.com/?q={coords['latitude']},{coords['longitude']}"
            if coords.get("latitude") is not None and coords.get("longitude") is not None else None
        ),
        "driver": str(driver.get("name") or event.get("driverName") or "Driver not assigned"),
        "type": "Harsh braking",
    }


async def api_alerts(request: web.Request) -> web.Response:
    await require_admin(request)
    core = request.app["core"]
    # Mark the cursor before querying Samsara. Live polling uses an overlap, so
    # events indexed while this historical search is in flight are not skipped.
    checked_at = core.utc_iso(datetime.now(timezone.utc))
    start, end, from_date, to_date = parse_local_date_range(
        request.query.get("from", ""), request.query.get("to", ""), core
    )
    selected_truck = request.query.get("truck", "all")
    selected_account_id = None
    selected_vehicle_id = None
    if selected_truck != "all":
        parts = selected_truck.split("::", 1)
        if len(parts) != 2:
            raise web.HTTPBadRequest(text="Invalid truck selection")
        selected_account_id, selected_vehicle_id = parts
        if not any(item.account_id == selected_account_id for item in core.ACCOUNTS):
            raise web.HTTPBadRequest(text="Invalid company selection")

    results: list[dict] = []
    for account in core.ACCOUNTS:
        if selected_account_id and account.account_id != selected_account_id:
            continue
        try:
            events = await core.get_braking_events(
                account, start, end,
                selected_vehicle_id if account.account_id == selected_account_id else None,
            )
        except Exception as error:
            logger.exception("Could not search Samsara alerts for account %s", account.account_id)
            raise web.HTTPBadGateway(text=f"Could not load alerts for {core.account_label(account.account_id)}: {error}") from error
        for event in events:
            vehicle_id = event_vehicle_id(core, event)
            stamp = core.event_timestamp(event)
            if not vehicle_id or not stamp or not (start <= stamp < end):
                continue
            if selected_vehicle_id and vehicle_id != selected_vehicle_id:
                continue
            formatted = format_alert_event(core, event, account.account_id, vehicle_id)
            if formatted["id"]:
                formatted["_sortTimestamp"] = stamp.timestamp()
                results.append(formatted)

    results.sort(key=lambda item: item["_sortTimestamp"], reverse=True)
    for item in results:
        item.pop("_sortTimestamp", None)
    return web.json_response({
        "events": results[:500],
        "count": len(results),
        "from": from_date.isoformat(),
        "to": to_date.isoformat(),
        "checkedAt": checked_at,
    })


async def api_live_alerts(request: web.Request) -> web.Response:
    await require_admin(request)
    core = request.app["core"]
    now = datetime.now(timezone.utc)
    since_value = request.query.get("since", "")
    try:
        since = datetime.fromisoformat(since_value.replace("Z", "+00:00")) if since_value else now - timedelta(minutes=2)
    except ValueError as error:
        raise web.HTTPBadRequest(text="Invalid alert polling cursor") from error
    if since.tzinfo is None:
        since = since.replace(tzinfo=timezone.utc)
    # Re-query a short overlap so delayed Samsara indexing does not lose events;
    # the Mini App removes duplicates by event ID.
    start = max(since.astimezone(timezone.utc) - timedelta(minutes=10), now - timedelta(days=1))
    results: list[dict] = []
    for account in core.ACCOUNTS:
        try:
            events = await core.get_braking_events(account, start, now)
        except Exception as error:
            logger.exception("Could not poll live safety events for account %s", account.account_id)
            raise web.HTTPBadGateway(text=f"Could not refresh alerts for {core.account_label(account.account_id)}") from error
        for event in events:
            vehicle_id = event_vehicle_id(core, event)
            stamp = core.event_timestamp(event)
            if not vehicle_id or not stamp or not (start <= stamp < now):
                continue
            item = format_alert_event(core, event, account.account_id, vehicle_id)
            if item["id"]:
                item["_sortTimestamp"] = stamp.timestamp()
                results.append(item)
    results.sort(key=lambda item: item["_sortTimestamp"], reverse=True)
    for item in results:
        item.pop("_sortTimestamp", None)
    return web.json_response({"events": results, "checkedAt": core.utc_iso(now)})


async def get_event_for_date(core, request: web.Request, account, vehicle_id: str, selected_event_id: str):
    event_date = request.query.get("date", "")
    if not event_date:
        events = await core.get_braking_history(vehicle_id, account)
    else:
        start, end, _, _ = parse_local_date_range(event_date, event_date, core)
        events = await core.get_braking_history(vehicle_id, account, start, end)
    return next((item for item in events if core.event_id(item) == selected_event_id), None)


async def send_event_in_background(bot: Bot, core, user_id: int, account, event: dict, task_key: tuple[int, str, str, str]) -> None:
    try:
        await core.send_event_media(bot, user_id, event, account)
    except Exception:
        logger.exception("Could not send selected event %s to Telegram admin %s", task_key[3], user_id)
        try:
            await bot.send_message(user_id, "Could not prepare this safety-event video. Please try again later.")
        except Exception:
            logger.exception("Could not notify Telegram admin %s about video failure", user_id)
    finally:
        ACTIVE_EVENT_SENDS.discard(task_key)


async def api_send_event(request: web.Request) -> web.Response:
    user = await require_admin(request)
    account_id = request.match_info["account_id"]
    vehicle_id = request.match_info["vehicle_id"]
    selected_event_id = request.match_info["event_id"]
    core = request.app["core"]
    bot: Bot = request.app["bot"]
    account = next((item for item in core.ACCOUNTS if item.account_id == account_id), None)
    if account is None:
        raise web.HTTPNotFound(text="Company not found")
    event = await get_event_for_date(core, request, account, vehicle_id, selected_event_id)
    if event is None:
        raise web.HTTPNotFound(text="Safety event not found in the selected date range")
    user_id = int(user["id"])
    task_key = (user_id, account_id, vehicle_id, selected_event_id)
    if task_key in ACTIVE_EVENT_SENDS:
        return web.json_response({"queued": True, "duplicate": True})
    ACTIVE_EVENT_SENDS.add(task_key)
    asyncio.create_task(send_event_in_background(bot, core, user_id, account, event, task_key))
    return web.json_response({"queued": True})


async def prepare_event_video(core, event: dict, account) -> bytes:
    """Prepare the same Samsara clip and visual treatment used for Telegram delivery."""
    enriched = await core.enrich_event_for_video(event, account)
    overlay = enriched.get("_videoOverlay", {})
    event_url = core.find_video_url(enriched)

    async def try_candidate(url: str | None, source_label: str) -> bytes | None:
        if not url:
            return None
        try:
            with core.tempfile.TemporaryDirectory(prefix="samsara-webapp-video-") as temp_dir:
                source_path = core.Path(temp_dir) / "camera-source.mp4"
                source_size = await core.download_video_source(url, source_path)
                if not await core.validate_video_source(source_path):
                    return None

                if core.VIDEO_TELEMATICS_OVERLAY:
                    try:
                        processed, overlay_applied = await core.convert_video_for_telegram(source_path, overlay)
                        if overlay_applied or source_size > core.MAX_TELEGRAM_VIDEO_BYTES:
                            logger.info("Prepared Mini App video from %s with the Telegram video settings", source_label)
                            return processed.data
                    except (core.VideoTooLargeError, RuntimeError):
                        logger.warning("Could not process Mini App video from %s; trying the source file", source_label, exc_info=True)

                if source_size <= core.MAX_TELEGRAM_VIDEO_BYTES:
                    logger.info("Using original Mini App video from %s (%s bytes)", source_label, source_size)
                    return source_path.read_bytes()

                processed, _ = await core.convert_video_for_telegram(
                    source_path, overlay, include_overlay=core.VIDEO_TELEMATICS_OVERLAY
                )
                return processed.data
        except Exception:
            logger.warning("Could not prepare Mini App video candidate %s", source_label, exc_info=True)
            return None

    direct_hd = core.find_safety_event_hd_url(enriched)
    video = await try_candidate(direct_hd, "safety-event HD")
    if video is not None:
        return video

    timeout = core.aiohttp.ClientTimeout(total=90)
    async with core.aiohttp.ClientSession(timeout=timeout) as session:
        uploaded_hd = await core.find_uploaded_highres_video(session, enriched, account)
        video = await try_candidate(uploaded_hd, "uploaded HD")
        if video is not None:
            return video

        retrieved_hd = await core.retrieve_video(
            session, enriched, account, duration_seconds=12, media_type="videoHighRes"
        )
        video = await try_candidate(retrieved_hd, "retrieved HD")
        if video is not None:
            return video

        video = await try_candidate(event_url, "event media")
        if video is not None:
            return video

        for duration in (12, 8, 5):
            low_res = await core.retrieve_video(
                session, enriched, account, duration_seconds=duration, media_type="videoLowRes"
            )
            video = await try_candidate(low_res, f"retrieved low-res {duration}s")
            if video is not None:
                return video

    raise web.HTTPNotFound(text="Video is unavailable for this safety event")


async def api_event_video(request: web.Request) -> web.Response:
    await require_admin(request)
    account_id = request.match_info["account_id"]
    vehicle_id = request.match_info["vehicle_id"]
    selected_event_id = request.match_info["event_id"]
    core = request.app["core"]
    account = next((item for item in core.ACCOUNTS if item.account_id == account_id), None)
    if account is None:
        raise web.HTTPNotFound(text="Company not found")
    event = await get_event_for_date(core, request, account, vehicle_id, selected_event_id)
    if event is None:
        raise web.HTTPNotFound(text="Safety event not found in the selected date range")
    video = await prepare_event_video(core, event, account)
    return web.Response(
        body=video,
        content_type="video/mp4",
        headers={
            "Cache-Control": "private, no-store",
            "Content-Disposition": 'inline; filename="safety-event.mp4"',
            "X-Content-Type-Options": "nosniff",
        },
    )


def create_app(bot: Bot, core) -> web.Application:
    ensure_pti_schema(core)
    app = web.Application(client_max_size=32 * 1024 * 1024)
    app["bot"] = bot
    app["core"] = core
    app.on_startup.append(start_pti_photo_retention)
    app.on_cleanup.append(stop_pti_photo_retention)
    app.router.add_get("/", index)
    app.router.add_get("/api/me", api_me)
    app.router.add_get("/api/trucks", api_trucks)
    app.router.add_post("/api/trucks/{account_id}/{vehicle_id}/driver", api_attach_driver)
    app.router.add_delete("/api/trucks/{account_id}/{vehicle_id}/driver", api_remove_driver)
    app.router.add_post("/api/trucks/{account_id}/{vehicle_id}/pti/notify", api_notify_truck_driver)
    app.router.add_get("/api/pti/settings", api_pti_settings)
    app.router.add_post("/api/pti/settings", api_pti_settings)
    app.router.add_get("/api/pti/status", api_pti_status)
    app.router.add_get("/api/pti/activity", api_driver_activity)
    app.router.add_post("/api/pti/submit", api_pti_submit)
    app.router.add_post("/api/pti/submissions/{submission_id}/invalidate", api_invalidate_pti)
    app.router.add_get("/api/pti/submissions/{submission_id}/photos/{slot}", api_pti_photo)
    app.router.add_get("/api/alerts", api_alerts)
    app.router.add_get("/api/alerts/live", api_live_alerts)
    app.router.add_post("/api/trucks/{account_id}/{vehicle_id}/groups", api_add_truck_group)
    app.router.add_delete("/api/trucks/{account_id}/{vehicle_id}/groups/{chat_id}", api_remove_truck_group)
    app.router.add_get("/api/trucks/{account_id}/{vehicle_id}/events", api_truck_events)
    app.router.add_get("/api/trucks/{account_id}/{vehicle_id}/events/{event_id}/video", api_event_video)
    app.router.add_post("/api/trucks/{account_id}/{vehicle_id}/events/{event_id}/send", api_send_event)
    return app
