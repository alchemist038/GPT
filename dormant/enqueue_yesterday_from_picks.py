#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
enqueue_yesterday_from_picks.py

picked_yesterday.jsonl を読み、
各イベントの「最新 vN」の
- decision.json
- .published
- shorts/short_vN_*.mp4
を揃えて、publishAt を割り当てて queue.jsonl に書く。

B運用（private→publishAt）前提で、ここでは publishAt を決めるだけ。
"""

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path


JST = timezone(timedelta(hours=9))


def find_latest_v(api_dir: Path) -> tuple[int, Path] | None:
    if not api_dir.exists():
        return None
    best = None
    for p in api_dir.iterdir():
        if p.is_dir() and p.name.lower().startswith("v"):
            try:
                n = int(p.name[1:])
            except:
                continue
            if best is None or n > best[0]:
                best = (n, p)
    return best


def pick_video(shorts_dir: Path, vnum: int) -> Path | None:
    if not shorts_dir.exists():
        return None
    # 優先：short_v{n}_bgm_*.mp4 → 次点：short_v{n}.mp4 → 最後：short_*.mp4
    prefs = [
        list(shorts_dir.glob(f"short_v{vnum}_bgm_*.mp4")),
        list(shorts_dir.glob(f"short_v{vnum}.mp4")),
        list(shorts_dir.glob("short_*.mp4")),
    ]
    for arr in prefs:
        if arr:
            arr.sort()
            return arr[-1]
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--picked", default="/media/sf_REC/posting/picked_yesterday.jsonl")
    ap.add_argument("--queue", default="/media/sf_REC/posting/queue.jsonl")
    ap.add_argument("--x", type=int, default=6)
    ap.add_argument("--start", default="07:00", help="HH:MM (JST)")
    ap.add_argument("--date", default="", help="YYYY-MM-DD (JST). empty=오늘")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    picked_path = Path(args.picked).resolve()
    queue_path = Path(args.queue).resolve()

    if not picked_path.exists():
        raise SystemExit(f"picked not found: {picked_path}")

    lines = [ln for ln in picked_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    if not lines:
        print("[OK] no picks. nothing to enqueue.")
        return

    # X本に制限（素材が少なければあるだけ）
    lines = lines[: max(0, args.x)]

    # 起点日時（JST）
    if args.date:
        base_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        base_date = datetime.now(JST).date()

    hh, mm = [int(x) for x in args.start.split(":")]
    start_dt = datetime(base_date.year, base_date.month, base_date.day, hh, mm, tzinfo=JST)

    pitch_hours = 24.0 / args.x if args.x > 0 else 24.0
    pitch = timedelta(seconds=int(pitch_hours * 3600))

    out_lines = []
    skipped_published = 0
    skipped_missing = 0

    for i, line in enumerate(lines):
        it = json.loads(line)
        session_dir = Path(it["session_dir"]).resolve()
        event = it["event"]

        event_dir = session_dir / "events" / event
        api_dir = event_dir / "api"
        latest = find_latest_v(api_dir)
        if latest is None:
            print("[SKIP] no api/vN:", event_dir)
            skipped_missing += 1
            continue

        vnum, vdir = latest
        decision_path = vdir / "decision.json"
        published_flag = vdir / ".published"
        shorts_dir = event_dir / "shorts"
        video_path = pick_video(shorts_dir, vnum)

        if published_flag.exists():
            print("[SKIP] already published:", published_flag)
            skipped_published += 1
            continue

        if not decision_path.exists() or video_path is None or not video_path.exists():
            print("[SKIP] missing decision/video:", event_dir)
            skipped_missing += 1
            continue

        publish_at = (start_dt + pitch * i).isoformat()
        # publishAt は RFC3339 (ISO8601) +09:00 でOK
        obj = {
            "video_path": str(video_path),
            "decision_path": str(decision_path),
            "published_flag_path": str(published_flag),
            "publishAt": publish_at,
            "route": "yesterday",
        }
        out_lines.append(json.dumps(obj, ensure_ascii=False))

    print(f"[INFO] x={args.x} pitch_hours={pitch_hours} start={start_dt.isoformat()}")
    print(f"[INFO] will_enqueue={len(out_lines)} skipped_published={skipped_published} skipped_missing={skipped_missing}")

    if args.dry_run:
        print("[DRY-RUN] not writing queue.")
        if out_lines:
            print("[SAMPLE]")
            for ln in out_lines[:3]:
                print(ln)
        return

    queue_path.parent.mkdir(parents=True, exist_ok=True)
    with queue_path.open("a", encoding="utf-8") as f:
        for ln in out_lines:
            f.write(ln + "\n")

    print(f"[OK] appended: {queue_path}")
    if out_lines:
        print("[SAMPLE]")
        for ln in out_lines[:3]:
            print(ln)


if __name__ == "__main__":
    main()
