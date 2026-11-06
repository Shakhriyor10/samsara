"""PTI steps: stored in the database so admins can add, edit, remove and reorder them in the Mini App.

Each step is a photo or a video. Every truck's PTI follows all steps, in the admin's order.

``icon`` is "diagram:<view>" for one of the Mini App's built-in camera-position drawings
(front, rear, driver_side, passenger_side, cabin, dashboard), "lucide:<Name>" for a Lucide icon,
or "image:<file>" for a square picture an admin uploaded (see miniapp_server.api_upload_step_image).
"""

from __future__ import annotations

import re
import secrets
import sqlite3
from typing import Any


KINDS = {"photo", "video"}
SECTION_TITLES = {"photo": "Photos", "video": "Videos"}
MAX_STEPS = 60
DEFAULT_VIDEO_SECONDS = 90
VIDEO_SECONDS_RANGE = (5, 300)
ICON_PATTERN = re.compile(r"^(diagram:[a-z_]{2,20}|lucide:[A-Za-z0-9]{2,60}|image:[0-9a-f]{32}\.(?:png|jpg))$")

# The PTI before steps became editable; it seeds the table on first start.
DEFAULT_STEPS: list[dict[str, Any]] = [
    {"key": "front", "kind": "photo", "title": "Front", "icon": "diagram:front",
     "hint": "Stand in front of the truck: hood latched, grille, bumper, headlights and windshield in frame."},
    {"key": "rear", "kind": "photo", "title": "Rear", "icon": "diagram:rear",
     "hint": "Stand behind the trailer: doors, lights, plate, reflective tape and mud flaps in frame."},
    {"key": "driver_side", "kind": "photo", "title": "Driver side", "icon": "diagram:driver_side",
     "hint": "Full driver side from front to back: tires, fuel tank, steps and trailer side."},
    {"key": "passenger_side", "kind": "photo", "title": "Passenger side", "icon": "diagram:passenger_side",
     "hint": "Full passenger side from front to back: tires, tanks and trailer side."},
    {"key": "cabin", "kind": "photo", "title": "Driver cabin", "icon": "diagram:cabin",
     "hint": "Seat, steering wheel, seat belt and safety kit (extinguisher and triangles)."},
    {"key": "dashboard", "kind": "photo", "title": "Dashboard / odometer", "icon": "diagram:dashboard",
     "hint": "Engine on: odometer, fuel, DEF and air gauges readable, no warning lamps."},
    {"key": "walkaround", "kind": "video", "title": "Walk-around and cab video", "icon": "lucide:Video",
     "seconds": DEFAULT_VIDEO_SECONDS,
     "hint": "Walk around the truck and trailer, then film the cab. Tap stop whenever you are done."},
]


def ensure_steps_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS pti_steps (key TEXT PRIMARY KEY, position INTEGER NOT NULL, "
        "kind TEXT NOT NULL, title TEXT NOT NULL, hint TEXT NOT NULL DEFAULT '', icon TEXT NOT NULL, "
        "seconds INTEGER)"
    )
    if conn.execute("SELECT 1 FROM pti_steps LIMIT 1").fetchone() is None:
        _write_steps(conn, DEFAULT_STEPS)


def _decorate(step: dict[str, Any]) -> dict[str, Any]:
    """Add the fields the Mini App and reports group steps by."""
    return {**step, "section": f"{step['kind']}s", "sectionTitle": SECTION_TITLES[step["kind"]]}


def load_steps(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT key, kind, title, hint, icon, seconds FROM pti_steps ORDER BY position, key"
    ).fetchall()
    steps = []
    for key, kind, title, hint, icon, seconds in rows:
        step = {"key": key, "kind": kind, "title": title, "hint": hint, "icon": icon}
        if kind == "video":
            step["seconds"] = int(seconds or DEFAULT_VIDEO_SECONDS)
        steps.append(_decorate(step))
    return steps


def mode_name(steps: list[dict[str, Any]]) -> str:
    """What a PTI contains, for the report caption: photos, videos or both."""
    kinds = {step["kind"] for step in steps}
    return "both" if kinds == KINDS else "video" if kinds == {"video"} else "photo"


def clean_steps(items: Any) -> list[dict[str, Any]]:
    """Validate steps sent by the admin editor; raise ValueError with a message the admin can act on."""
    if not isinstance(items, list) or not items:
        raise ValueError("Add at least one PTI step")
    if len(items) > MAX_STEPS:
        raise ValueError(f"A PTI can have at most {MAX_STEPS} steps")
    cleaned: list[dict[str, Any]] = []
    seen: set[str] = set()
    for number, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Step {number} is invalid")
        kind = str(item.get("kind") or "")
        title = str(item.get("title") or "").strip()
        hint = str(item.get("hint") or "").strip()
        icon = str(item.get("icon") or "").strip()
        if kind not in KINDS:
            raise ValueError(f"Step {number} must be a photo or a video")
        if not 1 <= len(title) <= 80:
            raise ValueError(f"Step {number} needs a name of 1 to 80 characters")
        if len(hint) > 300:
            raise ValueError(f"The instruction for “{title}” is longer than 300 characters")
        if not ICON_PATTERN.match(icon):
            icon = "lucide:Video" if kind == "video" else "lucide:Camera"
        # Existing keys are kept so earlier submissions still match; new steps get a fresh key.
        key = str(item.get("key") or "").strip()
        if not re.fullmatch(r"[a-z0-9_]{2,40}", key) or key in seen:
            key = f"step_{secrets.token_hex(4)}"
        seen.add(key)
        step = {"key": key, "kind": kind, "title": title, "hint": hint, "icon": icon}
        if kind == "video":
            try:
                seconds = int(item.get("seconds") or DEFAULT_VIDEO_SECONDS)
            except (TypeError, ValueError) as error:
                raise ValueError(f"Choose a valid video length for “{title}”") from error
            low, high = VIDEO_SECONDS_RANGE
            if not low <= seconds <= high:
                raise ValueError(f"The video for “{title}” must be {low} to {high} seconds")
            step["seconds"] = seconds
        cleaned.append(step)
    return cleaned


def _write_steps(conn: sqlite3.Connection, steps: list[dict[str, Any]]) -> None:
    conn.execute("DELETE FROM pti_steps")
    conn.executemany(
        "INSERT INTO pti_steps(key, position, kind, title, hint, icon, seconds) VALUES(?,?,?,?,?,?,?)",
        [
            (step["key"], position, step["kind"], step["title"], step.get("hint", ""), step["icon"],
             step.get("seconds") if step["kind"] == "video" else None)
            for position, step in enumerate(steps)
        ],
    )


def save_steps(conn: sqlite3.Connection, items: Any) -> list[dict[str, Any]]:
    _write_steps(conn, clean_steps(items))
    return load_steps(conn)


def snapshot(steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """What a submission remembers about its steps, so renamed or deleted steps keep their labels."""
    return [{"key": step["key"], "kind": step["kind"], "title": step["title"]} for step in steps]


def step_label(key: str) -> str:
    # Steps no longer in the PTI still get a readable label in older archives.
    return key.replace("_", " ").capitalize()
