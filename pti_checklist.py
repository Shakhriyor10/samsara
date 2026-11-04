"""PTI inspection checklist: one definition shared by the Mini App, upload validation and reports.

A truck's PTI is six photos, the 1:30 walk-around video, or both (photos first).

Step kinds the Mini App and upload validation understand:
- photo: one camera photo. ``input == "psi"`` also asks for a tire pressure reading.
- clip: a short video recorded in the app, auto-stopped after ``seconds``.
- guided: one continuous video with timed phases shown on screen.
- check: a Pass/Fail answer without media.
- walkaround: the 1.5-minute walk-around and cab video (the per-truck "video" option).

``optional`` steps may be marked "Not applicable".
``art`` picks one of the camera-position drawings the Mini App already has.
"""

from __future__ import annotations

from typing import Any


SECTIONS: list[tuple[str, str, list[dict[str, Any]]]] = [
    ("photos", "Truck photos", [
        {"key": "front", "title": "Front", "hint": "Stand in front of the truck: hood latched, grille, bumper, headlights and windshield in frame.", "art": "front"},
        {"key": "rear", "title": "Rear", "hint": "Stand behind the trailer: doors, lights, plate, reflective tape and mud flaps in frame.", "art": "rear"},
        {"key": "driver_side", "title": "Driver side", "hint": "Full driver side from front to back: tires, fuel tank, steps and trailer side.", "art": "driver_side"},
        {"key": "passenger_side", "title": "Passenger side", "hint": "Full passenger side from front to back: tires, tanks and trailer side.", "art": "passenger_side"},
        {"key": "cabin", "title": "Driver cabin", "hint": "Seat, steering wheel, seat belt and safety kit (extinguisher and triangles).", "art": "cabin"},
        {"key": "dashboard", "title": "Dashboard / odometer", "hint": "Engine on: odometer, fuel, DEF and air gauges readable, no warning lamps.", "art": "dashboard"},
    ]),
]

WALKAROUND_STEP: dict[str, Any] = {
    "key": "walkaround", "kind": "walkaround", "seconds": 90, "section": "video",
    "sectionTitle": "Walk-around video", "title": "Walk-around and cab video", "art": "front",
    "hint": "Walk around the truck and trailer, then film the cab. Tap stop whenever you are done; recording ends at 1:30.",
    "phases": [
        {"title": "Walk around the truck", "seconds": 70, "hint": "Start at the front and walk a full circle around the truck and trailer."},
        {"title": "Cab", "seconds": 20, "hint": "Climb in and film the cab: dashboard, seat, safety equipment."},
    ],
}

MEDIA_KINDS = {"photo", "clip", "guided", "walkaround"}


def _checklist_steps() -> list[dict[str, Any]]:
    steps = []
    for section_key, section_title, items in SECTIONS:
        for item in items:
            step = {"kind": "photo", **item, "section": section_key, "sectionTitle": section_title}
            steps.append(step)
    return steps


CHECKLIST_STEPS = _checklist_steps()
STEP_BY_KEY = {step["key"]: step for step in [*CHECKLIST_STEPS, WALKAROUND_STEP]}


def steps_for_mode(photos: bool, video: bool) -> list[dict[str, Any]]:
    """Steps a driver completes for a truck's PTI options, in order."""
    steps = list(CHECKLIST_STEPS) if photos else []
    if video:
        steps.append(WALKAROUND_STEP)
    return steps


def mode_name(photos: bool, video: bool) -> str:
    return "both" if photos and video else "video" if video else "photo"


def step_label(key: str) -> str:
    step = STEP_BY_KEY.get(key)
    if step:
        return step["title"]
    # Steps no longer in the checklist still get a readable label in older archives.
    return key.replace("_", " ").capitalize()
