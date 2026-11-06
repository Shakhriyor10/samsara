"""Build a ZIP of each submitted PTI and post it to the configured Telegram report group."""

from __future__ import annotations

import asyncio
import html
import json
import logging
import re
import sqlite3
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from aiogram import Bot
from aiogram.exceptions import TelegramNetworkError, TelegramRetryAfter, TelegramServerError
from aiogram.types import FSInputFile

import pti_checklist


logger = logging.getLogger("samsara_pti_reports")
# Telegram bots may upload files up to 50 MB; leave room for ZIP headers.
ZIP_PART_LIMIT = 48 * 1024 * 1024
VIDEO_SUFFIXES = {".mp4", ".mov", ".3gp", ".webm"}
TRANSCODE_TIMEOUT_SECONDS = 600
VIDEO_LONG_SIDE = 1920
# Keep 60 fps when the phone really recorded it; otherwise 30 fps.
HIGH_FPS = 60
BASE_FPS = 30
NORMALIZED_SUFFIX = "_cfr.mp4"
# HDR (HLG/PQ) to standard-range BT.709 so iPhone videos keep normal colors after re-encoding.
HDR_TO_SDR = (
    "zscale=t=linear:npl=100,format=gbrpf32le,zscale=p=bt709,"
    "tonemap=tonemap=hable:desat=0,zscale=t=bt709:m=bt709:r=tv,format=yuv420p"
)
REMUXED_STEM = "_indexed"
# Re-encoded, remuxed, and files from the earlier 30 fps-only version are all already playable.
NORMALIZED_SUFFIXES = (NORMALIZED_SUFFIX, "_30fps.mp4", f"{REMUXED_STEM}.mp4", f"{REMUXED_STEM}.webm")
# One video must fit a ZIP part on its own, with room for the report.
VIDEO_SIZE_LIMIT = ZIP_PART_LIMIT - 2 * 1024 * 1024
_BUILD_LOCK: asyncio.Lock | None = None


def submission_items(slots: set[str], answers: dict) -> list[dict]:
    """Captured steps of one PTI in checklist order, plus any older six-view photos."""
    items: list[dict] = []
    known = set()
    for step in [*pti_checklist.CHECKLIST_STEPS, pti_checklist.WALKAROUND_STEP]:
        key = step["key"]
        known.add(key)
        answer = answers.get(key) or {}
        if key not in slots and not answer:
            continue
        items.append({
            "key": key, "title": step["title"], "kind": step["kind"],
            "section": step["section"], "sectionTitle": step["sectionTitle"],
            "hasMedia": key in slots, "answer": answer,
        })
    for key in sorted(slots - known):
        items.append({
            "key": key, "title": pti_checklist.step_label(key), "kind": "photo",
            "section": "legacy", "sectionTitle": "Photos", "hasMedia": True, "answer": {},
        })
    return items


def schedule_report(app, submission_id: str) -> None:
    tasks: set[asyncio.Task] = app.setdefault("pti_report_tasks", set())
    task = asyncio.create_task(send_pti_report(app["bot"], app["core"], submission_id))
    tasks.add(task)
    task.add_done_callback(tasks.discard)


def _safe_name(value: str, limit: int = 60) -> str:
    cleaned = re.sub(r"[^\w.\- ]+", "", value).strip().replace(" ", "_")
    return (cleaned or "item")[:limit]


async def _run_ffmpeg(args: list[str], name: str) -> bool:
    process = await asyncio.create_subprocess_exec(
        "ffmpeg", "-y", "-v", "error", *args,
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
    )
    try:
        _, stderr = await asyncio.wait_for(process.communicate(), TRANSCODE_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()
        logger.warning("Timed out compressing PTI video %s", name)
        return False
    if process.returncode != 0:
        logger.warning("Could not compress PTI video %s: %s", name, stderr.decode(errors="replace")[-500:])
        return False
    return True


async def _video_seconds(path: Path) -> float | None:
    process = await asyncio.create_subprocess_exec(
        "ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", str(path),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
    )
    stdout, _ = await process.communicate()
    try:
        return float(stdout.decode().strip())
    except ValueError:
        return None


async def _source_fps(path: Path) -> float | None:
    """Measure the real frame rate from packet timestamps; phone recordings rarely state it correctly."""
    process = await asyncio.create_subprocess_exec(
        "ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "packet=pts_time",
        "-of", "csv=p=0", str(path),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
    )
    stdout, _ = await process.communicate()
    times = sorted(float(value) for value in stdout.decode().split() if value.replace(".", "", 1).isdigit())
    if len(times) < 10 or times[-1] <= times[0]:
        return None
    return (len(times) - 1) / (times[-1] - times[0])


async def _target_fps(source: Path) -> int:
    fps = await _source_fps(source)
    return HIGH_FPS if fps and fps >= 45 else BASE_FPS


async def _is_hdr(path: Path) -> bool:
    """iPhones record HDR (HLG or Dolby Vision/PQ) by default; it looks washed out unless tone-mapped."""
    process = await asyncio.create_subprocess_exec(
        "ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=color_transfer",
        "-of", "csv=p=0", str(path),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
    )
    stdout, _ = await process.communicate()
    # Rotated videos add an empty side-data column ("arib-std-b67,"), so match the values, not the whole line.
    return bool({"arib-std-b67", "smpte2084"} & set(stdout.decode().replace(",", " ").split()))


async def _encode(source: Path, target: Path, fps: int, rate_args: list[str], tonemap: bool = False) -> bool:
    """H.264 at up to 1080p and a constant frame rate.

    Phone browsers record variable frame rate video (WebM without an index on Android); many
    players show that as stutter, so every frame gets an even 1/fps slot.
    """
    scale = f"scale='if(gt(iw,ih),min({VIDEO_LONG_SIDE},iw),-2)':'if(gt(iw,ih),-2,min({VIDEO_LONG_SIDE},ih))'"
    filters = f"{scale},fps={fps}"
    color: list[str] = []
    if tonemap:
        filters = f"{HDR_TO_SDR},{filters}"
        color = ["-colorspace", "bt709", "-color_primaries", "bt709", "-color_trc", "bt709"]
    return await _run_ffmpeg(
        # The fps filter plus -r gives a constant rate on every FFmpeg release; -fps_mode needs 5.1+,
        # newer than the ffmpeg package on Ubuntu 22.04.
        ["-i", str(source), "-vf", filters, "-r", str(fps),
         "-c:v", "libx264", "-preset", "veryfast", *rate_args, "-g", str(fps * 2), "-pix_fmt", "yuv420p", *color,
         "-c:a", "aac", "-b:a", "128k", "-ar", "48000", "-movflags", "+faststart", str(target)],
        source.name,
    ) and target.is_file()


async def normalize_video(source: Path, target: Path) -> bool:
    fps = await _target_fps(source)
    maxrate = 9000 if fps == HIGH_FPS else 6000
    rate_args = ["-crf", "21", "-maxrate", f"{maxrate}k", "-bufsize", f"{maxrate * 2}k"]
    # Tone mapping needs FFmpeg built with zimg; without it, fall back to a plain encode.
    if await _is_hdr(source) and await _encode(source, target, fps, rate_args, tonemap=True):
        return True
    return await _encode(source, target, fps, rate_args)


async def _fit_video(source: Path, target: Path) -> Path:
    """Return a version of the video that fits one Telegram upload, re-encoding only when needed."""
    if source.stat().st_size <= VIDEO_SIZE_LIMIT:
        return source
    seconds = await _video_seconds(source)
    if seconds:
        video_kbps = int(VIDEO_SIZE_LIMIT * 8 / seconds / 1000 * 0.95) - 128
        if video_kbps >= 300 and await _encode(
            source, target, await _target_fps(source),
            ["-b:v", f"{video_kbps}k", "-maxrate", f"{video_kbps}k", "-bufsize", f"{video_kbps * 2}k"],
        ):
            return target
    return source


def _remux_suffix(source: Path) -> str:
    return ".webm" if source.suffix.lower() == ".webm" else ".mp4"


async def _remux(source: Path, target: Path) -> bool:
    """Copy the streams into a container with a proper index (MP4 with moov first, or WebM with cues)."""
    args = ["-i", str(source), "-map", "0:v:0", "-map", "0:a?", "-c", "copy"]
    if target.suffix == ".mp4":
        args += ["-movflags", "+faststart"]
    return await _run_ffmpeg([*args, str(target)], source.name) and await _is_seekable(target)


async def _is_seekable(path: Path) -> bool:
    """A file players can scrub through reports its duration up front."""
    return path.is_file() and bool(await _video_seconds(path))


async def _normalize_submission_videos(core, submission_id: str, upload_root: Path) -> None:
    """Replace each uploaded recording with a constant frame rate MP4 so the app archive and the ZIP play smoothly."""
    with sqlite3.connect(core.STATE_DB) as conn:
        rows = conn.execute("SELECT slot,file_path FROM pti_photos WHERE submission_id=?", (submission_id,)).fetchall()
    for slot, filename in rows:
        source = upload_root / filename
        if Path(filename).suffix.lower() not in VIDEO_SUFFIXES or filename.endswith(NORMALIZED_SUFFIXES) or not source.is_file():
            continue
        target_name = f"{Path(filename).stem}{NORMALIZED_SUFFIX}"
        target = upload_root / target_name
        if not await normalize_video(source, target) or not await _is_seekable(target):
            target.unlink(missing_ok=True)
            # Never keep the raw browser recording: it has no seek index.
            target_name = f"{Path(filename).stem}{REMUXED_STEM}{_remux_suffix(source)}"
            target = upload_root / target_name
            if not await _remux(source, target):
                target.unlink(missing_ok=True)
                logger.error("PTI video %s could not be re-encoded or remuxed; keeping the original", filename)
                continue
            logger.warning("PTI video %s could not be re-encoded; stored a remuxed copy instead", filename)
        with sqlite3.connect(core.STATE_DB) as conn:
            conn.execute(
                "UPDATE pti_photos SET file_path=? WHERE submission_id=? AND slot=?", (target_name, submission_id, slot)
            )
        source.unlink(missing_ok=True)


def _report_text(meta: dict, items: list[dict]) -> str:
    lines = [
        "PTI INSPECTION REPORT",
        f"Truck: {meta['truck']}",
        f"Company: {meta['company']}",
        f"Driver: {meta['driver']} (Telegram {meta['driver_id']})",
        f"Submitted: {meta['submitted']}",
        f"Includes: {meta['mode_label']}",
        "",
    ]
    section = None
    for item in items:
        if item["sectionTitle"] != section:
            section = item["sectionTitle"]
            lines += ["", section.upper()]
        answer = item["answer"]
        if answer.get("na"):
            result = "Not applicable"
        elif "result" in answer:
            result = "PASS" if answer["result"] == "pass" else "FAIL"
        elif item["kind"] == "photo":
            result = "Photo" + (f" - {answer['psi']:g} PSI" if "psi" in answer else "")
        else:
            result = "Video"
        if item["hasMedia"] and item.get("file"):
            result += f" ({item['file']})"
        lines.append(f"- {item['title']}: {result}")
    return "\n".join(lines) + "\n"


def _pack_parts(files: list[tuple[Path, str]], report: str, base: Path) -> list[Path]:
    """Pack files into as few ZIPs as fit Telegram's upload limit; each part opens on its own."""
    groups: list[list[tuple[Path, str]]] = [[]]
    sizes = [len(report.encode())]
    for path, arcname in files:
        size = path.stat().st_size
        if sizes[-1] + size > ZIP_PART_LIMIT and groups[-1]:
            groups.append([])
            sizes.append(0)
        groups[-1].append((path, arcname))
        sizes[-1] += size
    parts = []
    for index, group in enumerate(groups, start=1):
        suffix = f"_part{index}of{len(groups)}" if len(groups) > 1 else ""
        target = base.with_name(f"{base.name}{suffix}.zip")
        # Photos and videos are already compressed; storing them keeps packing fast.
        with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_STORED) as archive:
            archive.writestr("report.txt", report, compress_type=zipfile.ZIP_DEFLATED)
            for path, arcname in group:
                archive.write(path, arcname)
        parts.append(target)
    return parts


def _load_submission(core, submission_id: str) -> tuple[dict, list[dict], list[tuple[str, str]], int | None] | None:
    with sqlite3.connect(core.STATE_DB) as conn:
        row = conn.execute(
            "SELECT s.telegram_user_id,s.account_id,s.truck_id,s.submitted_at,s.mode,s.answers,"
            "COALESCE(d.truck_name,s.truck_id),COALESCE(d.telegram_name,'') "
            "FROM pti_submissions s LEFT JOIN driver_accounts d ON d.account_id=s.account_id AND d.truck_id=s.truck_id "
            "WHERE s.id=?", (submission_id,),
        ).fetchone()
        if row is None:
            return None
        media = conn.execute("SELECT slot,file_path FROM pti_photos WHERE submission_id=?", (submission_id,)).fetchall()
        chat_row = conn.execute("SELECT value FROM pti_settings WHERE key='report_chat_id'").fetchone()
    chat_value = (chat_row[0] if chat_row else "") or ""
    chat_id = int(chat_value) if chat_value.lstrip("-").isdigit() else None
    submitted = datetime.fromisoformat(row[3].replace("Z", "+00:00"))
    if submitted.tzinfo is None:
        submitted = submitted.replace(tzinfo=timezone.utc)
    local = submitted.astimezone(core.ZoneInfo(core.EVENT_TIMEZONE))
    mode = row[4] or "photo"
    meta = {
        "truck": row[6], "company": core.account_label(row[1]), "driver": row[7] or "Driver",
        "driver_id": row[0], "submitted": local.strftime("%b %d, %Y %I:%M %p %Z"),
        "stamp": local.strftime("%Y-%m-%d_%H%M"), "mode": mode,
        "mode_label": {"photo": "Photos", "video": "Walk-around video", "both": "Photos + walk-around video"}[mode],
    }
    items = submission_items({slot for slot, _ in media}, json.loads(row[5] or "{}"))
    return meta, items, media, chat_id


def _record(core, submission_id: str, chat_id: int | None, error: str | None) -> None:
    sent_at = None if error else datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    with sqlite3.connect(core.STATE_DB) as conn:
        conn.execute(
            "INSERT INTO pti_reports(submission_id,chat_id,sent_at,error) VALUES(?,?,?,?) "
            "ON CONFLICT(submission_id) DO UPDATE SET chat_id=excluded.chat_id,sent_at=excluded.sent_at,error=excluded.error",
            (submission_id, chat_id, sent_at, error),
        )


async def normalize_pending_videos(core) -> None:
    """Fix recordings stored before re-encoding worked on this server (e.g. older FFmpeg without -fps_mode)."""
    global _BUILD_LOCK
    _BUILD_LOCK = _BUILD_LOCK or asyncio.Lock()
    upload_root = Path(core.STATE_DB).resolve().parent / "pti_uploads"
    try:
        with sqlite3.connect(core.STATE_DB) as conn:
            rows = conn.execute("SELECT DISTINCT submission_id,file_path FROM pti_photos").fetchall()
    except sqlite3.Error:
        logger.exception("Could not look for PTI videos to re-encode")
        return
    pending = sorted({
        submission_id for submission_id, filename in rows
        if Path(filename).suffix.lower() in VIDEO_SUFFIXES and not filename.endswith(NORMALIZED_SUFFIXES)
    })
    for submission_id in pending:
        try:
            async with _BUILD_LOCK:
                await _normalize_submission_videos(core, submission_id, upload_root)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Could not re-encode stored videos for PTI %s", submission_id)
    if pending:
        logger.info("Re-encoded stored videos for %s PTI submission(s)", len(pending))


async def send_pti_report(bot: Bot, core, submission_id: str) -> None:
    global _BUILD_LOCK
    _BUILD_LOCK = _BUILD_LOCK or asyncio.Lock()
    try:
        upload_root = Path(core.STATE_DB).resolve().parent / "pti_uploads"
        # One submission at a time: video encoding is CPU-heavy.
        async with _BUILD_LOCK:
            await _normalize_submission_videos(core, submission_id, upload_root)
            loaded = await asyncio.to_thread(_load_submission, core, submission_id)
            if loaded is None:
                return
            meta, items, media, chat_id = loaded
            if chat_id is None:
                await asyncio.to_thread(_record, core, submission_id, None, "No PTI report group is configured")
                return
            files_by_slot = dict(media)
            with tempfile.TemporaryDirectory(prefix="pti_report_") as workdir:
                work = Path(workdir)
                packed: list[tuple[Path, str]] = []
                section_numbers: dict[str, int] = {}
                for number, item in enumerate(items, start=1):
                    section_numbers.setdefault(item["sectionTitle"], len(section_numbers) + 1)
                    if not item["hasMedia"]:
                        continue
                    source = upload_root / files_by_slot[item["key"]]
                    if not source.is_file():
                        continue
                    suffix = source.suffix.lower()
                    if suffix in VIDEO_SUFFIXES:
                        source = await _fit_video(source, work / f"{item['key']}.mp4")
                    folder = f"{section_numbers[item['sectionTitle']]:02d}_{_safe_name(item['sectionTitle'])}"
                    arcname = f"{folder}/{number:02d}_{_safe_name(item['title'])}{suffix}"
                    item["file"] = arcname
                    packed.append((source, arcname))
                report = _report_text(meta, items)
                base = work / f"PTI_{_safe_name(meta['truck'], 40)}_{meta['stamp']}"
                parts = await asyncio.to_thread(_pack_parts, packed, report, base)
                failed = [item["title"] for item in items if item["answer"].get("result") == "fail"]
                caption = (
                    f"📋 <b>PTI · {html.escape(meta['truck'])}</b>\n"
                    f"{html.escape(meta['company'])}\n"
                    f"Driver: {html.escape(meta['driver'])}\n"
                    f"Submitted: {html.escape(meta['submitted'])}\n"
                    f"Includes: {html.escape(meta['mode_label'])}"
                )
                if failed:
                    caption += "\n⚠️ Failed: " + html.escape(", ".join(failed))
                for index, part in enumerate(parts, start=1):
                    part_caption = caption + (f"\nPart {index} of {len(parts)}" if len(parts) > 1 else "")
                    await _send_with_retry(bot, chat_id, part, part_caption[:1024])
        await asyncio.to_thread(_record, core, submission_id, chat_id, None)
        logger.info("Sent PTI report %s to chat %s", submission_id, chat_id)
    except asyncio.CancelledError:
        raise
    except Exception as error:
        logger.exception("Could not send PTI report %s", submission_id)
        try:
            await asyncio.to_thread(_record, core, submission_id, None, str(error)[:500])
        except Exception:
            logger.exception("Could not record PTI report failure %s", submission_id)


async def _send_with_retry(bot: Bot, chat_id: int, path: Path, caption: str) -> None:
    for attempt in range(3):
        try:
            await bot.send_document(
                chat_id, FSInputFile(path, filename=path.name), caption=caption,
                parse_mode="HTML", request_timeout=900,
            )
            return
        except TelegramRetryAfter as error:
            await asyncio.sleep(error.retry_after + 1)
        except (TelegramNetworkError, TelegramServerError):
            if attempt == 2:
                raise
            await asyncio.sleep(10 * (attempt + 1))
    raise RuntimeError("Telegram kept rate-limiting the PTI report upload")
