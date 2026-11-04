"""PTI inspection checklist: one definition shared by the Mini App, upload validation and reports.

Step kinds:
- photo: one camera photo. ``input == "psi"`` also asks for a tire pressure reading.
- clip: a short video recorded in the app, auto-stopped after ``seconds``.
- guided: one continuous video with timed phases shown on screen (air brake test).
- check: a Pass/Fail answer without media.
- walkaround: the 1.5-minute walk-around and cab video (the per-truck "video" option).

``optional`` steps may be marked "Not applicable" (reefer, hazmat).
``art`` picks one of the camera-position drawings the Mini App already has.
"""

from __future__ import annotations

from typing import Any


SECTIONS: list[tuple[str, str, list[dict[str, Any]]]] = [
    ("engine", "Engine compartment (engine off)", [
        {"key": "engine_bay_driver", "title": "Engine bay, driver side", "hint": "One wide shot: hoses, wiring, steering gear, visible leaks.", "art": "front"},
        {"key": "engine_bay_passenger", "title": "Engine bay, passenger side", "hint": "One wide shot: coolant tank, alternator, belts.", "art": "front"},
        {"key": "oil_level", "title": "Engine oil level", "hint": "Dipstick between MIN and MAX. The level must be readable.", "art": "front"},
        {"key": "coolant_level", "title": "Coolant level", "hint": "Reservoir level between MIN and MAX. Never open the cap on a hot engine.", "art": "front"},
        {"key": "belts_hoses", "title": "Belts, hoses, alternator", "hint": "No cracks, fraying or looseness.", "art": "front"},
        {"key": "power_steering", "title": "Power-steering fluid and hoses", "hint": "Level OK, no leaks.", "art": "front"},
        {"key": "steering_linkage", "title": "Steering gearbox and linkage", "hint": "No leaks, no loose connections.", "art": "front"},
    ]),
    ("cab", "Cab (engine on)", [
        {"key": "dashboard", "title": "Dashboard", "hint": "Fuel, DEF, both air gauges, no warning lamps after start-up. ABS lamp goes out.", "art": "dashboard"},
        {"key": "safety_kit", "title": "Safety kit", "hint": "Extinguisher gauge in the green and 3 triangles in one photo.", "art": "cabin"},
        {"key": "wipers_horn", "kind": "clip", "seconds": 15, "title": "Wipers, washer, horn", "hint": "Run the wipers and washer, then sound the horn.", "art": "dashboard"},
        {"key": "mirrors_belt_steering", "title": "Mirrors, seat belt, steering play", "hint": "Mirrors adjusted and intact, belt latches, no excessive steering-wheel play.", "art": "cabin"},
    ]),
    ("front", "Front of truck", [
        {"key": "truck_front", "title": "Truck front", "hint": "Hood latched, grille and bumper undamaged.", "art": "front"},
        {"key": "front_lights", "kind": "clip", "seconds": 10, "title": "Front lights", "hint": "Low beam, high beam, 4-way flashers, left and right signals.", "art": "front"},
        {"key": "windshield_wipers", "title": "Windshield and wipers", "hint": "No cracks in the driver's view, blades in good condition.", "art": "front"},
    ]),
    ("left_steer", "Left steer wheel", [
        {"key": "left_steer_tire", "title": "Tire and wheel", "hint": "Tire, rim, lug nuts, hub. No cuts, bulges, loose or missing nuts, rust trails or leaks.", "art": "driver_side"},
        {"key": "left_steer_tread", "title": "Tread depth", "hint": "Gauge photo. Steer tires minimum 4/32 in.", "art": "driver_side"},
        {"key": "left_steer_pressure", "input": "psi", "title": "Tire pressure", "hint": "Gauge or TPMS vs. spec. Enter the PSI reading.", "art": "driver_side"},
        {"key": "left_steer_brake", "title": "Brake and suspension", "hint": "Springs, shock, brake parts, steering parts, mounts. Nothing cracked, leaking or missing.", "art": "driver_side"},
    ]),
    ("left_tractor", "Left side of tractor", [
        {"key": "left_tractor_side", "title": "Tractor left side", "hint": "Full side: body damage, fuel tank in frame.", "art": "driver_side"},
        {"key": "left_fuel_battery", "title": "Fuel tank, cap, straps, battery box", "hint": "Cap on, straps tight, no leaks.", "art": "driver_side"},
        {"key": "left_door_mirror", "title": "Door, mirror, steps, handles", "hint": "Secure and undamaged.", "art": "driver_side"},
        {"key": "left_drive_axle_1", "title": "Drive axle 1: tires, brakes, suspension", "hint": "One photo showing both duals, brake chamber, hoses, springs.", "art": "driver_side"},
        {"key": "left_drive_axle_2", "title": "Drive axle 2: tires, brakes, suspension", "hint": "One photo showing both duals, brake chamber, hoses, springs.", "art": "driver_side"},
        {"key": "left_drive_tires", "title": "Left drive tires and brakes", "hint": "Tread min 2/32 in, pressure, damage, chambers, air hoses.", "art": "driver_side"},
    ]),
    ("coupling", "Coupling (behind the cab)", [
        {"key": "fifth_wheel", "title": "Fifth wheel seating", "hint": "Trailer apron sits flat on the plate. No gap.", "art": "rear"},
        {"key": "kingpin_jaws", "title": "Kingpin jaws and release handle", "hint": "Jaws closed around kingpin, handle fully in, safety latch engaged.", "art": "rear"},
        {"key": "air_lines", "title": "Air lines and electrical cable", "hint": "Red and blue lines connected, seals good, cable plugged in, no chafing or kinks.", "art": "rear"},
        {"key": "air_leak", "kind": "clip", "seconds": 20, "title": "Air leak / hose damage", "hint": "Show the lines and connections. No audible leak.", "art": "rear"},
    ]),
    ("trailer_left", "Trailer front and left side", [
        {"key": "trailer_front_landing", "title": "Trailer front and landing gear", "hint": "Front face and both landing-gear legs raised in one photo.", "art": "rear"},
        {"key": "landing_gear_handle", "title": "Landing-gear handle", "hint": "Secured in its bracket.", "art": "driver_side"},
        {"key": "reefer", "optional": True, "title": "Reefer display and fuel", "hint": "If reefer: set-point, alarms and fuel level in one photo.", "art": "driver_side"},
        {"key": "hazmat_placards", "optional": True, "title": "Hazmat placards", "hint": "If hazmat: placard condition on the front and left side of the trailer.", "art": "driver_side"},
        {"key": "trailer_left_side", "title": "Trailer left side", "hint": "Full side: panels, reflective tape, damage.", "art": "driver_side"},
        {"key": "trailer_left_pins", "title": "Tandem lock pins", "hint": "All pins fully engaged.", "art": "driver_side"},
        {"key": "trailer_left_axle_1", "title": "Left trailer axle 1", "hint": "Tires, brakes, hoses, suspension in one shot.", "art": "driver_side"},
        {"key": "trailer_left_axle_2", "title": "Left trailer axle 2", "hint": "Tires, brakes, hoses, suspension in one shot.", "art": "driver_side"},
        {"key": "trailer_left_tires", "title": "Left trailer tires and brakes", "hint": "Tread, pressure, damage, chambers, hoses.", "art": "driver_side"},
    ]),
    ("trailer_rear", "Trailer rear", [
        {"key": "trailer_rear", "title": "Trailer rear", "hint": "Whole back: plate, reflective tape, mud flaps.", "art": "rear"},
        {"key": "trailer_doors", "title": "Doors, locks, hinges", "hint": "Closed, latched, no damage.", "art": "rear"},
        {"key": "rear_bumper", "title": "Rear bumper and mounts", "hint": "Present, straight, mounts tight.", "art": "rear"},
        {"key": "rear_lights", "kind": "clip", "seconds": 10, "title": "Rear lights", "hint": "Hazards and markers; brake lights with a helper or pedal tool.", "art": "rear"},
    ]),
    ("trailer_right", "Trailer right side", [
        {"key": "trailer_right_side", "title": "Trailer right side", "hint": "Full side: panels, reflective tape, damage.", "art": "passenger_side"},
        {"key": "trailer_right_pins", "title": "Tandem lock pins", "hint": "All pins fully engaged.", "art": "passenger_side"},
        {"key": "trailer_right_axle_1", "title": "Right trailer axle 1", "hint": "Tires, brakes, hoses, suspension in one shot.", "art": "passenger_side"},
        {"key": "trailer_right_axle_2", "title": "Right trailer axle 2", "hint": "Tires, brakes, hoses, suspension in one shot.", "art": "passenger_side"},
        {"key": "trailer_right_tires", "title": "Right trailer tires and brakes", "hint": "Tread, pressure, damage, chambers, hoses.", "art": "passenger_side"},
    ]),
    ("right_tractor", "Right side of tractor", [
        {"key": "right_drive_axle_1", "title": "Drive axle 1: tires, brakes, suspension", "hint": "One photo showing both duals, chamber, hoses, springs.", "art": "passenger_side"},
        {"key": "right_drive_axle_2", "title": "Drive axle 2: tires, brakes, suspension", "hint": "One photo showing both duals, chamber, hoses, springs.", "art": "passenger_side"},
        {"key": "right_drive_tires", "title": "Right drive tires and brakes", "hint": "Tread min 2/32 in, pressure, damage, chambers, air hoses.", "art": "passenger_side"},
        {"key": "right_tractor_side", "title": "Tractor right side", "hint": "Full side: body damage, tanks in frame.", "art": "passenger_side"},
        {"key": "right_fuel_def", "title": "Fuel and DEF tanks, caps, straps", "hint": "Caps on, straps tight, no leaks.", "art": "passenger_side"},
        {"key": "right_exhaust_steps", "title": "Exhaust, steps, handles", "hint": "Secure, no leaks or cracks.", "art": "passenger_side"},
    ]),
    ("right_steer", "Right steer wheel", [
        {"key": "right_steer_tire", "title": "Tire and wheel", "hint": "Tire, rim, lug nuts, hub. No cuts, bulges, loose or missing nuts, rust trails or leaks.", "art": "passenger_side"},
        {"key": "right_steer_tread", "title": "Tread depth", "hint": "Gauge photo. Steer tires minimum 4/32 in.", "art": "passenger_side"},
        {"key": "right_steer_pressure", "input": "psi", "title": "Tire pressure", "hint": "Gauge or TPMS vs. spec. Enter the PSI reading.", "art": "passenger_side"},
        {"key": "right_steer_brake", "title": "Brake and suspension", "hint": "Springs, shock, brake parts, steering parts, mounts. Nothing cracked, leaking or missing.", "art": "passenger_side"},
    ]),
    ("brakes", "Brake and coupling tests", [
        {"key": "tug_test", "kind": "check", "title": "Tug test", "hint": "Trailer brakes applied, pull gently forward in low gear. The combination must hold.", "art": "dashboard"},
        {
            "key": "air_brake_test", "kind": "guided", "title": "Air brake test", "art": "dashboard",
            "hint": "One continuous take with both air gauges and the buzzer in frame. The timer guides each step.",
            "phases": [
                {"title": "Static air leak", "seconds": 60, "hint": "Engine off, brakes released. Loss no more than 3 psi in 1 minute."},
                {"title": "Applied air leak", "seconds": 60, "hint": "Hold the service brake. Loss no more than 4 psi in 1 minute."},
                {"title": "Low-air warning", "seconds": 0, "hint": "Fan the brake pedal. Light and buzzer must come on before 60 psi. Tap Next when done."},
                {"title": "Spring brakes", "seconds": 0, "hint": "Keep fanning. Parking valves must pop out between 20 and 45 psi. Tap Finish when done."},
            ],
        },
        {"key": "parking_service_hold", "kind": "check", "title": "Parking and service brake hold", "hint": "Parking brake holds against a light pull; service brake stops smoothly in a short roll.", "art": "dashboard"},
        {"key": "trailer_abs_lamp", "kind": "check", "title": "Trailer ABS lamp", "hint": "Comes on at key-on and goes out.", "art": "dashboard"},
    ]),
]

WALKAROUND_STEP: dict[str, Any] = {
    "key": "walkaround", "kind": "walkaround", "seconds": 90, "section": "video",
    "sectionTitle": "Walk-around video", "title": "Walk-around and cab video", "art": "front",
    "hint": "Walk a full circle around the truck and trailer, then film the cab. Recording stops at 1:30.",
    "phases": [
        {"title": "Walk around the truck", "seconds": 70, "hint": "Start at the front and walk a full circle around the truck and trailer."},
        {"title": "Cab", "seconds": 20, "hint": "Climb in and film the cab: dashboard, seat, safety equipment."},
    ],
}

# Photos from the original six-view PTI, kept so older archives still have labels.
LEGACY_LABELS = {
    "front": "Front", "rear": "Rear", "driver_side": "Driver side",
    "passenger_side": "Passenger side", "cabin": "Driver cabin", "dashboard": "Dashboard / odometer",
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
    return LEGACY_LABELS.get(key, key.replace("_", " ").capitalize())
