#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import datetime as dt
import json
import random
from pathlib import Path
from typing import Dict, List, Set, Tuple

JST = dt.timezone(dt.timedelta(hours=9))


def parse_hhmm(s: str) -> dt.time:
    hh, mm = s.split(":")
    return dt.time(int(hh), int(mm), 0, tzinfo=JST)


def parse_date_yyyy_mm_dd(s: str) -> dt.date:
    y, m, d = s.split("-")
    return dt.date(int(y), int(m), int(d))


def session_date_from_name(session_dir: Path) -> dt.date:
    # session folder name: YYYY-MM-DD_HH-MM-SS
    name = session_dir.name
    return parse_date_yyyy_mm_dd(name[:10])


def list_frames_events(session_dir: Path) -> List[Tuple[str, Path]]:
    frames_root = session_dir / "frames_360"
    if not frames_root.exists():
        return []
    out: List[Tuple[str, Path]] = []
    for p in sorted(frames_root.iterdir()):
        if p.is_dir():
            out.append((p.name, p))
    return out


def load_existing_event_keys(event_queue_path: Path) -> Set[str]:
    keys: Set[str] = set()
    if not event_queue_path.exists():
        return keys
    try:
        with event_queue_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                sd = obj.get("session_dir")
                en = obj.get("event_name")
                if sd and en:
                    keys.add(f"{sd}::{en}")
    except Exception:
        pass
    return keys


def build_pool(
    root: Path,
    *,
    date_filter_fn,
    existing_keys: Set[str],
) -> List[Dict]:
    pool: List[Dict] = []

    # sessions: /media/sf_masaos_mov/YYYY-MM-DD_HH-MM-SS
    for s in sorted(root.glob("2025-*-*_*-*-*")) + sorted(root.glob("2026-*-*_*-*-*")):
        if not s.is_dir():
            continue

        try:
            S = session_date_from_name(s)
        except Exception:
            continue

        if not date_filter_fn(S):
            continue

        # require analyze_done
        if not (s / "logs" / ".analyze_done").exists():
            continue

        for event_name, frames_dir in list_frames_events(s):
            event_dir = s / "events" / event_name

            # 未着手在庫：frames_360 あり & events/<EVENT> なし
            if event_dir.exists():
                continue

            key = f"{str(s)}::{event_name}"
            if key in existing_keys:
                continue

            pool.append(
                {
                    "session_dir": str(s),
                    "event_name": event_name,
                    "frames_dir": str(frames_dir),
                    "event_dir": str(event_dir),
                }
            )

    return pool


def assign_times(D: dt.date, start_hhmm: str, pitch_h: float, n: int) -> List[str]:
    t0 = parse_hhmm(start_hhmm)
    base = dt.datetime(D.year, D.month, D.day, t0.hour, t0.minute, 0, tzinfo=JST)
    out = []
    for i in range(n):
        ts = base + dt.timedelta(hours=pitch_h * i)
        out.append(ts.isoformat())
    return out


def append_jsonl(path: Path, items: List[Dict], dry_run: bool) -> None:
    if dry_run:
        print("[DRY_RUN] not writing", str(path))
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for obj in items:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="/media/sf_masaos_mov")
    ap.add_argument("--event_queue", default="/media/sf_REC/posting/event_queue.jsonl")
    ap.add_argument("--date", default=None, help="YYYY-MM-DD (JST). 未指定なら今日。")
    ap.add_argument("--y", type=int, default=3, help="Y本数（昨日在庫のみ）")
    ap.add_argument("--a", type=int, default=3, help="A本数（直近days_back在庫）")
    ap.add_argument("--start_y", default="07:00")
    ap.add_argument("--start_a", default="19:00")
    ap.add_argument("--pitch_y", type=float, default=4.0)
    ap.add_argument("--pitch_a", type=float, default=4.0)
    ap.add_argument("--days_back", type=int, default=14, help="Aが見る過去日数（0〜days_back日）")
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    root = Path(args.root)
    event_queue = Path(args.event_queue)

    if args.seed is not None:
        random.seed(args.seed)

    today = dt.datetime.now(JST).date()
    D = parse_date_yyyy_mm_dd(args.date) if args.date else today
    YDAY = D - dt.timedelta(days=1)

    existing_keys = load_existing_event_keys(event_queue)
    print(f"[INFO] event_queue={event_queue} existing_keys={len(existing_keys)}")
    print(f"[INFO] 기준일(D)={D}  Yは昨日のみ={YDAY}  Aは過去{args.days_back}日")

    # --- Y: 昨日のセッションのみ
    def y_filter(sdate: dt.date) -> bool:
        return sdate == YDAY

    # --- A: 直近 days_back 日（今日含む）
    def a_filter(sdate: dt.date) -> bool:
        d = (D - sdate).days
        return (0 <= d <= args.days_back)

    pool_y = build_pool(root, date_filter_fn=y_filter, existing_keys=existing_keys)
    pool_a = build_pool(root, date_filter_fn=a_filter, existing_keys=existing_keys)

    # シャッフル
    random.shuffle(pool_y)
    random.shuffle(pool_a)

    y_sel = pool_y[: args.y]
    y_keys = {f'{it["session_dir"]}::{it["event_name"]}' for it in y_sel}

    # A側は、Yで選ばれたものは除外（重複防止）
    a_filtered = []
    for it in pool_a:
        k = f'{it["session_dir"]}::{it["event_name"]}'
        if k in y_keys:
            continue
        a_filtered.append(it)
    a_sel = a_filtered[: args.a]

    out_items: List[Dict] = []

    # Y: 昨日在庫が無いなら 0 本（＝何も出さない）
    y_times = assign_times(D, args.start_y, args.pitch_y, len(y_sel))
    for it, t in zip(y_sel, y_times):
        key = f'{it["session_dir"]}::{it["event_name"]}'
        if key in existing_keys:
            continue
        obj = dict(it)
        obj["publishAt"] = t
        obj["route"] = "Y"
        out_items.append(obj)
        existing_keys.add(key)

    # A: 通常通り
    a_times = assign_times(D, args.start_a, args.pitch_a, len(a_sel))
    for it, t in zip(a_sel, a_times):
        key = f'{it["session_dir"]}::{it["event_name"]}'
        if key in existing_keys:
            continue
        obj = dict(it)
        obj["publishAt"] = t
        obj["route"] = "A"
        out_items.append(obj)
        existing_keys.add(key)

    print("\n[EVENT_QUEUE_PREVIEW]")
    if not out_items:
        print("(empty)")
    else:
        for i, obj in enumerate(out_items, 1):
            print(f"{i:02d} {obj['route']} {obj['publishAt']}  {obj['session_dir']}/events/{obj['event_name']}")

    append_jsonl(event_queue, out_items, args.dry_run)
    if not args.dry_run:
        print(f"[OK] appended {len(out_items)} lines -> {event_queue}")


if __name__ == "__main__":
    main()
