#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
pick_yesterday_events.py

前日付けのセッションフォルダから、
frames_360/<EVENT> があり、events/<EVENT> が無い（未着手）イベントを集め、
ランダムにX本ピックして JSONL に出力する。

出力JSONL 1行例:
{"session_dir":".../2025-12-19_06-40-12","event":"35834_35849","frames_dir":".../frames_360/35834_35849","route":"yesterday"}
"""

import argparse
import json
import random
from pathlib import Path
from datetime import datetime, timedelta, timezone


JST = timezone(timedelta(hours=9))


def list_sessions_for_date(root: Path, ymd: str) -> list[Path]:
    # e.g. 2025-12-19_06-40-12
    return sorted([p for p in root.glob(f"{ymd}_*") if p.is_dir()])


def list_unprocessed_events(session_dir: Path) -> list[dict]:
    frames_root = session_dir / "frames_360"
    events_root = session_dir / "events"

    if not frames_root.exists():
        return []

    frame_events = sorted([p.name for p in frames_root.iterdir() if p.is_dir()])
    done_events = set()
    if events_root.exists():
        done_events = set([p.name for p in events_root.iterdir() if p.is_dir()])

    picks = []
    for ev in frame_events:
        if ev in done_events:
            continue
        frames_dir = frames_root / ev
        if not frames_dir.exists():
            continue
        picks.append({
            "session_dir": str(session_dir),
            "event": ev,
            "frames_dir": str(frames_dir),
            "route": "yesterday",
        })
    return picks


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="/media/sf_masaos_mov", help="masao_mov root (HDD)")
    ap.add_argument("--x", type=int, required=True, help="pick X events (random)")
    ap.add_argument("--date", default="", help="YYYY-MM-DD (default: yesterday in JST)")
    ap.add_argument("--out", default="/media/sf_REC/posting/picked_yesterday.jsonl", help="output JSONL path")
    ap.add_argument("--seed", type=int, default=0, help="random seed (0=none)")
    args = ap.parse_args()

    root = Path(args.root).expanduser().resolve()
    if not root.exists():
        raise SystemExit(f"root not found: {root}")

    if args.date.strip():
        ymd = args.date.strip()
    else:
        ymd = (datetime.now(JST) - timedelta(days=1)).strftime("%Y-%m-%d")

    sessions = list_sessions_for_date(root, ymd)

    candidates: list[dict] = []
    for s in sessions:
        candidates.extend(list_unprocessed_events(s))

    # uniq by (session_dir,event)
    uniq = {}
    for c in candidates:
        key = (c["session_dir"], c["event"])
        uniq[key] = c
    candidates = list(uniq.values())

    if args.seed:
        random.seed(args.seed)

    if len(candidates) <= args.x:
        picked = candidates
    else:
        picked = random.sample(candidates, args.x)

    out = Path(args.out).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)

    with out.open("w", encoding="utf-8") as f:
        for item in picked:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

    print(f"[OK] date={ymd} sessions={len(sessions)} candidates={len(candidates)} picked={len(picked)}")
    print(f"[OK] wrote: {out}")
    if picked:
        print("[PICKED] sample:")
        for item in picked[:5]:
            print(" ", item["session_dir"], item["event"])


if __name__ == "__main__":
    main()
