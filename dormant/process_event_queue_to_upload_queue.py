#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import re
import subprocess
from pathlib import Path
from typing import Optional, Tuple

EVENT_QUEUE = Path("/media/sf_REC/posting/event_queue.jsonl")
UPLOAD_QUEUE = Path("/media/sf_REC/posting/queue.jsonl")

API_PIPELINE = Path("/media/sf_REC/scripts/api_decision_pipeline.py")
RENDER = Path("/media/sf_REC/scripts/render_short_from_decision.py")

V_RE = re.compile(r"^v(\d+)$")

def run(cmd: list[str], dry_run: bool):
    print("\n$ " + " ".join(cmd))
    if dry_run:
        return
    subprocess.run(cmd, check=True)

def find_latest_v(api_dir: Path) -> Optional[Tuple[int, Path]]:
    if not api_dir.exists():
        return None
    best = None
    for p in api_dir.iterdir():
        m = V_RE.match(p.name)
        if not m:
            continue
        n = int(m.group(1))
        if (p / "decision.json").exists():
            if best is None or n > best[0]:
                best = (n, p)
    return best

def find_latest_short(event_dir: Path) -> Optional[Path]:
    shorts = event_dir / "shorts"
    if not shorts.exists():
        return None
    best = None
    for mp4 in shorts.glob("short_v*_bgm_V1.mp4"):
        m = re.search(r"short_(v\d+)_bgm_V1\.mp4$", mp4.name)
        if not m:
            continue
        vn = int(m.group(1)[1:])
        if best is None or vn > best[0]:
            best = (vn, mp4)
    return best[1] if best else None

def load_lines(path: Path):
    if not path.exists():
        return []
    lines = []
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if ln:
                lines.append(ln)
    return lines

def write_lines(path: Path, lines: list[str], dry_run: bool):
    print(f"[INFO] write upload queue -> {path} lines={len(lines)}")
    if dry_run:
        print("[DRY_RUN] not writing upload queue.")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for ln in lines:
            f.write(ln + "\n")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max", type=int, default=2, help="process first N lines (default 2)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    ev_lines = load_lines(EVENT_QUEUE)
    if not ev_lines:
        print("[INFO] event_queue empty:", EVENT_QUEUE)
        return

    ev_lines = ev_lines[: args.max]
    out_jobs = []

    for idx, ln in enumerate(ev_lines, start=1):
        job = json.loads(ln)
        frames_dir = Path(job["frames_dir"])
        event_dir = Path(job["event_dir"])
        publishAt = job["publishAt"]
        session_dir = Path(job["session_dir"])
        raw = session_dir / "raw.mkv"
        if not raw.exists():
            raise FileNotFoundError(f"raw.mkv not found: {raw}")

        print("\n" + "=" * 60)
        print(f"[{idx}] session={session_dir.name} event={job['event_name']} publishAt={publishAt}")

        # ---- DRY RUN: 生成物を要求しない（予定パスを仮組み立て）
        if args.dry_run:
            vnum = 1
            vdir = event_dir / "api" / f"v{vnum}"
            decision_path = vdir / "decision.json"
            published_flag_path = vdir / ".published"
            video_path = event_dir / "shorts" / f"short_v{vnum}_bgm_V1.mp4"

            print("[DRY_RUN] would run API + render and then enqueue upload job:")
            print("  decision :", decision_path)
            print("  video    :", video_path)
            print("  flag     :", published_flag_path)

            out_job = {
                "video_path": str(video_path),
                "decision_path": str(decision_path),
                "published_flag_path": str(published_flag_path),
                "publishAt": publishAt,
                "route": job.get("route", "A"),
            }
            out_jobs.append(json.dumps(out_job, ensure_ascii=False))
            continue

        # 1) API(2段) 実行 → events/<EVENT>/api/vN/decision.json を作る
        event_dir.mkdir(parents=True, exist_ok=True)
        run(["python3", str(API_PIPELINE), "--frames-dir", str(frames_dir), "--step", "2"], dry_run=False)

        # 2) 最新 v を特定
        latest = find_latest_v(event_dir / "api")
        if latest is None:
            raise RuntimeError(f"decision.json not found under: {event_dir}/api")
        vnum, vfolder = latest
        decision_path = vfolder / "decision.json"
        published_flag_path = vfolder / ".published"

        # 3) short生成（RAW→縦動画＋BGM）
        run(["python3", str(RENDER), "--event-dir", str(event_dir), "--overwrite"], dry_run=False)

        # 4) 出来た mp4 を特定
        video_path = find_latest_short(event_dir)
        if video_path is None:
            raise RuntimeError(f"render output not found under: {event_dir}/shorts")

        out_job = {
            "video_path": str(video_path),
            "decision_path": str(decision_path),
            "published_flag_path": str(published_flag_path),
            "publishAt": publishAt,
            "route": job.get("route", "A"),
        }
        out_jobs.append(json.dumps(out_job, ensure_ascii=False))

        print("[OK] prepared upload job:")
        print(out_jobs[-1])

    write_lines(UPLOAD_QUEUE, out_jobs, dry_run=args.dry_run)

if __name__ == "__main__":
    main()
