#!/usr/bin/env python3
import argparse
import json
import os
import re
import subprocess
import time
from pathlib import Path

DEFAULT_BGM = "/media/sf_REC/bgm/bgm_V1.mp3"

def find_latest_decision(event_dir: Path) -> Path | None:
    api_dir = event_dir / "api"
    if not api_dir.exists():
        return None
    versions = []
    for p in api_dir.iterdir():
        m = re.match(r"v(\d+)", p.name)
        if m:
            versions.append((int(m.group(1)), p))
    if not versions:
        return None
    versions.sort(key=lambda x: x[0])
    return versions[-1][1] / "decision.json"

def parse_event_start_sec(event_dir: Path) -> int:
    m = re.match(r"^(\d+)_\d+$", event_dir.name)
    if not m:
        raise ValueError(f"Event folder name must be like SSSSS_EEEEE, got: {event_dir.name}")
    return int(m.group(1))

# ------------------------------
# ★ ここが唯一の追加ロジック
#   ・通常ffmpegを2回トライ
#   ・2回失敗したら False を返す（例外で落ちない）
# ------------------------------
def run_ffmpeg_with_retry(cmd: list[str], tries: int = 2, timeout_sec: int = 900) -> bool:
    for i in range(1, tries + 1):
        print(f"[FFMPEG] try {i}/{tries}")
        full = ["timeout", str(timeout_sec)] + cmd
        p = subprocess.run(full, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        if p.returncode == 0:
            return True
        print(f"[FFMPEG_NG] rc={p.returncode}")
        if p.stderr:
            for l in p.stderr.splitlines()[-10:]:
                print(l)
        time.sleep(2)
    return False

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--event-dir", required=True)
    ap.add_argument("--decision", default="")
    ap.add_argument("--raw", default="")
    ap.add_argument("--overwrite", action="store_true")
    args = ap.parse_args()

    event_dir = Path(args.event_dir)
    if args.decision:
        decision_path = Path(args.decision)
    else:
        decision_path = find_latest_decision(event_dir)
    if not decision_path or not decision_path.exists():
        print("decision.json not found")
        return 0

    if args.raw:
        raw_path = Path(args.raw)
    else:
        raw_path = event_dir.parent.parent / "raw.mkv"
    if not raw_path.exists():
        print("raw.mkv not found")
        return 0

    with open(decision_path, "r", encoding="utf-8") as f:
        d = json.load(f)

    start_rel = int(d["start_sec_rel"])
    end_rel = int(d["end_sec_rel"])
    dur = end_rel - start_rel
    crop_x = int(d["crop_x"] * 3)

    ev_start = parse_event_start_sec(event_dir)
    abs_start = ev_start + start_rel

    shorts_dir = event_dir / "shorts"
    shorts_dir.mkdir(exist_ok=True)

    fail_flag = shorts_dir / ".render_failed"
    if fail_flag.exists():
        print("[SKIP] .render_failed exists")
        return 0

    vdir = decision_path.parent
    vnum = re.search(r"v(\d+)", vdir.name).group(1)
    out_path = shorts_dir / f"short_v{vnum}_bgm_V1.mp4"

    if out_path.exists() and not args.overwrite:
        print("[SKIP] already exists:", out_path)
        return 0

    # ===== ここから下は「元のffmpegそのまま」 =====
    cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-ss", str(abs_start),
        "-t", str(dur),
        "-i", str(raw_path),
        "-stream_loop", "-1",
        "-i", DEFAULT_BGM,
        "-vf",
        "crop=trunc(trunc(ih*9/16)/2)*2:ih:{}:0,scale=720:1280,"
        "drawtext=text='AI自動切り抜きショート':fontsize=68:fontcolor=white@0.45:x=(w-text_w)/2:y=40,"
        "drawtext=text='詳しくは説明欄へ':fontsize=40:fontcolor=white@0.38:x=(w-text_w)/2:y=120".format(crop_x),
        "-map", "0:v:0",
        "-map", "1:a:0",
        "-c:v", "libx264",
        "-crf", "20",
        "-preset", "veryfast",
        "-pix_fmt", "yuv420p",
        "-filter:a", "volume=0.14",
        "-c:a", "aac",
        "-b:a", "128k",
        "-ar", "48000",
        "-shortest",
        str(out_path),
    ]

    ok = run_ffmpeg_with_retry(cmd, tries=2, timeout_sec=900)
    if not ok:
        fail_flag.write_text("render_failed\n", encoding="utf-8")
        print("[SKIP] render failed twice")
        return 0

    print("OK: rendered ->", out_path)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
