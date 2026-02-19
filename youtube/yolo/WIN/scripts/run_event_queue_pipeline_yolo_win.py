#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

from common_win import append_jsonl, load_config, read_jsonl, write_jsonl_atomic

DEFAULT_API2_PROMPT_FILE = r"D:\OBS\REC\prompts\api2_system_yolo.txt"


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def run_cmd(cmd: List[str], timeout: int = 1800, env: Dict[str, str] | None = None) -> bool:
    run_env = os.environ.copy()
    if env:
        run_env.update(env)

    res = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        env=run_env,
    )
    if res.returncode != 0:
        log(f"CMD_ERROR: {' '.join(cmd)}")
        if res.stderr:
            log(res.stderr.strip())
        return False
    return True


def escape_drawtext(s: str) -> str:
    return s.replace("\\", "\\\\").replace("\n", r"\n").replace(":", r"\:").replace("'", r"\'")


def get_median_cx(raw_yolo_path: Path, start_abs: int, end_abs: int) -> float:
    cxs: List[float] = []
    if not raw_yolo_path.exists():
        return 320.0

    with raw_yolo_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            sec = int(obj.get("sec", -1))
            if not (start_abs <= sec <= end_abs):
                continue
            bb = obj.get("bbox_xyxy")
            if not (isinstance(bb, list) and len(bb) == 4):
                continue
            cx = (float(bb[0]) + float(bb[2])) / 2.0
            cxs.append(cx)

    return float(statistics.median(cxs)) if cxs else 320.0


def calculate_crop_x(cx: float) -> int:
    target_w_in_360 = 202
    crop_x = int(cx - (target_w_in_360 / 2))
    return max(0, min(crop_x, 640 - target_w_in_360))


def export_cropped_previews(conf: Dict[str, Any], raw_path: Path, out_dir: Path, start_abs: int, dur: int, crop_x_360: int) -> bool:
    ffmpeg = conf.get("ffmpeg", "ffmpeg")
    cmd = [
        ffmpeg,
        "-y",
        "-ss",
        str(start_abs),
        "-t",
        str(dur),
        "-i",
        str(raw_path),
        "-vf",
        f"fps=1,crop=ih*9/16:ih:{crop_x_360 * 3}:0,scale=225:400",
        str(out_dir / "frame_%03d.jpg"),
    ]

    for attempt in range(1, 4):
        try:
            if out_dir.exists():
                shutil.rmtree(out_dir)
            out_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            log(f"preview dir reset failed (attempt {attempt}/3): {e}")
            if attempt < 3:
                time.sleep(0.5)
                continue
            return False

        if run_cmd(cmd, timeout=1200):
            return True

        if attempt < 3:
            log(f"retry export_cropped_previews ({attempt}/3)")
            time.sleep(0.8)
    return False


def prepare_review_samples(frames_dir: Path, review_dir_name: str = "images_review") -> List[Path]:
    """
    Pick 4 preview JPEGs at roughly 0/5/10/15 sec from 1fps cropped frames.
    """
    if not frames_dir.exists():
        return []

    src = sorted(frames_dir.glob("frame_*.jpg"))
    if not src:
        return []

    sec_points = [0, 5, 10, 15]
    review_dir = frames_dir.parent / review_dir_name
    review_dir.mkdir(parents=True, exist_ok=True)

    out: List[Path] = []
    for sec in sec_points:
        idx = min(max(sec, 0), len(src) - 1)
        s = src[idx]
        if not s.exists():
            # Another process may be rotating images_cropped; refresh once.
            src = sorted(frames_dir.glob("frame_*.jpg"))
            if not src:
                continue
            idx = min(max(sec, 0), len(src) - 1)
            s = src[idx]
        d = review_dir / f"review_{sec:02d}s.jpg"
        try:
            d.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(s, d)
            out.append(d)
        except FileNotFoundError:
            continue
    return out

def read_env_file(env_file: Path) -> Dict[str, str]:
    vals: Dict[str, str] = {}
    if not env_file.exists():
        return vals

    try:
        with env_file.open("r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                k, v = line.split("=", 1)
                key = k.strip()
                if not key:
                    continue
                vals[key] = v.strip().strip("\"").strip("'")
    except OSError as e:
        log(f"[WARN] env file read failed: {env_file} ({e})")
    return vals


def build_api_env(conf: Dict[str, Any]) -> Dict[str, str]:
    env_file = Path(conf.get("api_env_file", str(Path(__file__).resolve().parents[1] / ".env.win")))
    env_vals = read_env_file(env_file)

    # Backward compatibility: allow direct key in config if user already uses it.
    env_name = str(conf.get("api_key_env_name", "OPENAI_API_KEY"))
    direct_key = str(conf.get("api_key", "")).strip()
    if direct_key and env_name not in env_vals:
        env_vals[env_name] = direct_key

    return env_vals


def call_api_for_content(conf: Dict[str, Any], event_dir: Path, frames_dir: Path) -> bool:
    python_cmd = conf.get("python", "python")
    api_script = conf.get("api_script")
    prompt_file = conf.get("api2_prompt_file", DEFAULT_API2_PROMPT_FILE)
    cmd = [python_cmd, api_script, "--event-dir", str(event_dir), "--frames-dir", str(frames_dir), "--step", "2"]
    if prompt_file:
        cmd.extend(["--api2-prompt-file", prompt_file])
    log(f"Calling API with {frames_dir}")
    return run_cmd(cmd, timeout=1800, env=build_api_env(conf))


def build_vf(conf: Dict[str, Any], crop_x: int) -> str:
    tel = conf.get("telop", {})
    fontfile = conf.get("fontfile", r"C:\Windows\Fonts\meiryo.ttc")
    ff_fontfile = fontfile.replace("\\", "/").replace(":", r"\:")
    out_w = int(conf.get("out_w", 720))
    out_h = int(conf.get("out_h", 1280))
    t1 = escape_drawtext(tel.get("top1", "AI自動切り抜きショート"))
    t2 = escape_drawtext(tel.get("top2", "詳しくは説明欄へ"))
    bottom_raw = str(
        tel.get("bottom3", "チャンネル登録してね！\\n見たいと思った時はライブで\\nリアルなまさおが見れるかも")
    )
    bottom_raw = bottom_raw.replace("\\n", "\n")
    bottom_lines = [escape_drawtext(x.strip()) for x in bottom_raw.splitlines() if x.strip()]
    if not bottom_lines:
        bottom_lines = [escape_drawtext("チャンネル登録してね")]

    filters = [
        f"crop=ih*9/16:ih:{crop_x}:0",
        f"scale={out_w}:{out_h}",
        f"drawtext=text='{t1}':fontsize=54:fontcolor=white@0.45:x=(w-text_w)/2:y=180:fontfile='{ff_fontfile}'",
        f"drawtext=text='{t2}':fontsize=36:fontcolor=white@0.38:x=(w-text_w)/2:y=260:fontfile='{ff_fontfile}'",
    ]

    for idx, line in enumerate(bottom_lines[:3]):
        y_expr = f"h-{430 - (idx * 54)}"
        filters.append(
            "drawtext="
            f"text='{line}':fontsize=38:fontcolor=white@0.9:"
            "borderw=3:bordercolor=black@0.8:shadowx=2:shadowy=2:shadowcolor=black@0.7:"
            "box=1:boxcolor=black@0.16:boxborderw=10:"
            f"x=(w-text_w)/2:y={y_expr}:fontfile='{ff_fontfile}':"
            "alpha='if(lt(t,16),0,min(0.78,(t-16)*0.39))'"
        )

    return ",".join(filters)


def resolve_logo_png(conf: Dict[str, Any], config_dir: Path) -> str | None:
    logo_raw = str(conf.get("logo_png", "")).strip()
    if not logo_raw:
        return None

    logo_path = Path(logo_raw)
    if not logo_path.is_absolute():
        logo_path = (config_dir / logo_path).resolve()

    if not logo_path.is_file():
        log(f"[WARN] logo file not found: {logo_path} (skip logo overlay)")
        return None
    return str(logo_path)


def build_logo_overlay_filter_complex(vf_main: str) -> str:
    # 0-1s: visible, 1-2s: fade out, 2s+: hidden.
    return (
        f"[0:v]{vf_main}[vmain];"
        "[1:v]format=rgba,loop=loop=-1:size=1:start=0,trim=duration=2,setpts=N/(30*TB),fade=t=out:st=1:d=1:alpha=1[logo_faded];"
        "[logo_faded][vmain]scale2ref=w=main_w*0.70:h=ow/mdar[logo][vref];"
        "[vref][logo]overlay=x=(main_w-overlay_w)/2:y=(main_h-overlay_h)/2:format=auto:enable='lt(t,2)'[vout]"
    )


def render_video(conf: Dict[str, Any], raw_path: Path, out_path: Path, start_abs: int, dur: int, crop_x_360: int, config_dir: Path) -> bool:
    ffmpeg = conf.get("ffmpeg", "ffmpeg")
    bgm_path = conf.get("bgm_path")
    crop_x = crop_x_360 * 3
    logo_path = resolve_logo_png(conf, config_dir)
    vf_main = build_vf(conf, crop_x)
    cmd = [
        ffmpeg,
        "-y",
        "-hide_banner",
        "-ss",
        str(start_abs),
        "-t",
        str(dur),
        "-i",
        str(raw_path),
    ]

    if logo_path:
        cmd += ["-i", logo_path]

    cmd += [
        "-stream_loop",
        "-1",
        "-i",
        str(bgm_path),
    ]

    if logo_path:
        cmd += [
            "-filter_complex",
            build_logo_overlay_filter_complex(vf_main),
            "-map",
            "[vout]",
        ]
    else:
        cmd += [
            "-vf",
            vf_main,
            "-map",
            "0:v:0",
        ]

    if logo_path:
        cmd += ["-map", "2:a:0"]
    else:
        cmd += ["-map", "1:a:0"]
    cmd += [
        "-c:v",
        "libx264",
        "-crf",
        "20",
        "-preset",
        "veryfast",
        "-pix_fmt",
        "yuv420p",
        "-af",
        "afade=t=in:st=1:d=1,volume=0.16",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-shortest",
        str(out_path),
    ]
    ok = run_cmd(cmd, timeout=2400)
    if not ok:
        fail_flag = out_path.parent / ".render_fail_logo"
        try:
            fail_flag.touch()
        except Exception as e:
            log(f"[FAIL FLAG ERROR] {fail_flag}: {e}")
    return ok


def parse_action(args_action: str) -> str:
    val = args_action.strip().lower()
    if val not in {"approve", "defer", "reject", "prompt"}:
        raise ValueError("review action must be approve|defer|reject|prompt")
    return val


def review_action(action: str, event_name: str, frames_dir: Path) -> str:
    samples = prepare_review_samples(frames_dir)
    if samples:
        print("-" * 60)
        print("Review samples (0/5/10/15 sec):")
        for p in samples:
            print(f"  {p}")

    if action != "prompt":
        return action

    print("-" * 60)
    print(f"Review required: {event_name}")
    print(f"JPEG dir: {frames_dir}")
    print("Choose action [a]pprove / [d]efer / [r]eject")
    while True:
        try:
            s = input("> ").strip().lower()
        except EOFError:
            # Non-interactive run (UI/background): fail safe to defer.
            return "defer"
        if s in ("a", "approve"):
            return "approve"
        if s in ("d", "defer"):
            return "defer"
        if s in ("r", "reject"):
            return "reject"


def main() -> None:
    ap = argparse.ArgumentParser(description="Windows event queue pipeline with JPEG review gate")
    ap.add_argument("--config", default=str(Path(__file__).resolve().parents[1] / "config.json"))
    ap.add_argument("--max", type=int, default=5)
    ap.add_argument("--event-dir", default="", help="manual single event dir")
    ap.add_argument("--no-api", action="store_true")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--review-before-api", action="store_true")
    ap.add_argument("--review-action", default="prompt", help="approve|defer|reject|prompt")
    args = ap.parse_args()

    conf = load_config(args.config)
    event_queue_path = Path(conf["event_queue"])
    upload_queue_path = Path(conf["upload_queue"])
    deferred_queue_path = Path(conf["deferred_queue"])
    rejected_queue_path = Path(conf["rejected_queue"])
    raw_video_name = conf.get("raw_video_name", "raw.mkv")
    raw_yolo_name = conf.get("raw_yolo_name", "raw_yolo.jsonl")
    duration = int(conf.get("render_duration_sec", 20))

    action_mode = parse_action(args.review_action)

    lock_path = event_queue_path.with_suffix(event_queue_path.suffix + ".lock")
    lock_fd: int | None = None
    try:
        lock_fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(lock_fd, str(os.getpid()).encode("utf-8"))
    except FileExistsError:
        log(f"another pipeline process is running (lock exists): {lock_path}")
        return

    if args.event_dir:
        ev_dir = Path(args.event_dir)
        if not ev_dir.exists():
            log(f"event dir not found: {ev_dir}")
            return
        sess_dir = ev_dir.parent.parent
        ev_name = ev_dir.name
        work_items = [
            {
                "event_dir": str(ev_dir),
                "session_dir": str(sess_dir),
                "event_name": ev_name,
                "publishAt": (datetime.now() + timedelta(hours=1)).isoformat(),
                "route": conf.get("route", "yolo_win"),
            }
        ]
        rem_lines: List[Dict[str, Any]] = []
        queue_mode = False
    else:
        lines = read_jsonl(event_queue_path)
        if not lines:
            log("event queue empty")
            return
        work_items = lines[: args.max]
        rem_lines = lines[args.max :]
        queue_mode = True

    retry_items: List[Dict[str, Any]] = []
    try:
        for item in work_items:
            ev_dir = Path(item["event_dir"])
            sess_dir = Path(item["session_dir"])
            ev_name = str(item["event_name"])
            publish_at = item.get("publishAt")
            route = item.get("route", conf.get("route", "yolo_win"))

            log(f"Processing {ev_name}")
            start_abs = int(ev_name.split("_")[0])
            raw_yolo = sess_dir / raw_yolo_name
            raw_video = sess_dir / raw_video_name

            med_cx = get_median_cx(raw_yolo, start_abs, start_abs + duration)
            crop_x_360 = calculate_crop_x(med_cx)
            log(f"median_cx={med_cx:.1f} crop_x_360={crop_x_360}")

            frames_dir = ev_dir / "images_cropped"
            if not export_cropped_previews(conf, raw_video, frames_dir, start_abs, duration, crop_x_360):
                log("[ERROR] failed to create cropped JPEG")
                retry_items.append(item)
                continue

            if args.review_before_api:
                act = review_action(action_mode, ev_name, frames_dir)
                if act == "defer":
                    append_jsonl(
                        deferred_queue_path,
                        {
                            "event_dir": str(ev_dir),
                            "session_dir": str(sess_dir),
                            "event_name": ev_name,
                            "publishAt": publish_at,
                            "route": route,
                            "reason": "manual_defer",
                            "at": datetime.now().isoformat(),
                        },
                    )
                    log("deferred")
                    continue
                if act == "reject":
                    append_jsonl(
                        rejected_queue_path,
                        {
                            "event_dir": str(ev_dir),
                            "session_dir": str(sess_dir),
                            "event_name": ev_name,
                            "publishAt": publish_at,
                            "route": route,
                            "reason": "manual_reject",
                            "at": datetime.now().isoformat(),
                        },
                    )
                    log("rejected")
                    continue

            if not args.no_api:
                if not call_api_for_content(conf, ev_dir, frames_dir):
                    log("[ERROR] api failed")
                    retry_items.append(item)
                    continue
            else:
                log("skip api (--no-api)")

            decision_json = ev_dir / "api" / "v1" / "decision.json"
            if not decision_json.exists():
                log(f"[ERROR] decision not found: {decision_json}")
                retry_items.append(item)
                continue

            out_mp4 = ev_dir / "shorts" / f"{ev_name}_v1_bgm_V1.mp4"
            if out_mp4.exists() and not args.force:
                log(f"[SKIP] already exists: {out_mp4}")
                continue

            out_mp4.parent.mkdir(parents=True, exist_ok=True)
            if not render_video(conf, raw_video, out_mp4, start_abs, duration, crop_x_360, Path(args.config).resolve().parent):
                log("[ERROR] render failed")
                retry_items.append(item)
                continue

            append_jsonl(
                upload_queue_path,
                {
                    "video_path": str(out_mp4),
                    "decision_path": str(decision_json),
                    "published_flag_path": str(ev_dir / "api" / "v1" / ".published"),
                    "publishAt": publish_at,
                    "route": route,
                },
            )
            log(f"[OK] rendered and enqueued upload: {out_mp4}")

        if queue_mode:
            new_queue = retry_items + rem_lines
            write_jsonl_atomic(event_queue_path, new_queue)
            log(f"queue updated rem={len(new_queue)}")
    finally:
        if lock_fd is not None:
            os.close(lock_fd)
        try:
            lock_path.unlink(missing_ok=True)
        except OSError:
            pass


if __name__ == "__main__":
    main()
