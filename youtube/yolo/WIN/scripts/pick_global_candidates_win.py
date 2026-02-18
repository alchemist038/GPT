#!/usr/bin/env python3
from __future__ import annotations

import argparse
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from common_win import append_jsonl, load_config, read_jsonl, write_jsonl_atomic

JST = timezone(timedelta(hours=9))


@dataclass
class RangeSpec:
    lo: float
    hi: float
    n: int
    label: str


def parse_ranges(range_args: List[str]) -> List[RangeSpec]:
    out: List[RangeSpec] = []
    for s in range_args:
        parts = [p.strip() for p in s.split(",")]
        if len(parts) not in (3, 4):
            raise ValueError(f"invalid --range: {s}")
        lo = float(parts[0])
        hi = float(parts[1])
        n = int(parts[2])
        label = parts[3] if len(parts) == 4 else f"{lo}_{hi}"
        out.append(RangeSpec(lo=lo, hi=hi, n=n, label=label))
    return out


def overlaps(a: Tuple[int, int], b: Tuple[int, int]) -> bool:
    return not (a[1] <= b[0] or b[1] <= a[0])


def in_range(motion: float, r: RangeSpec) -> bool:
    return r.lo <= motion <= r.hi


def collect_candidates(base_dir: Path, candidates_name: str, skip_uploaded: bool) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for cand_path in base_dir.rglob(candidates_name):
        session_dir = cand_path.parent
        source_rows = read_jsonl(cand_path)
        for i, row in enumerate(source_rows):
            if row.get("picked_at"):
                continue
            if skip_uploaded and row.get("video_id"):
                continue
            if "start_abs" not in row or "end_abs" not in row:
                continue
            row2 = dict(row)
            row2["_source_path"] = str(cand_path)
            row2["_source_index"] = i
            row2["_session_dir"] = str(session_dir)
            rows.append(row2)
    return rows


def mark_picked(chosen: List[Dict[str, Any]], picked_at: str) -> None:
    by_file: Dict[str, List[int]] = {}
    for row in chosen:
        by_file.setdefault(row["_source_path"], []).append(int(row["_source_index"]))

    for src, idxs in by_file.items():
        source_rows = read_jsonl(src)
        for idx in idxs:
            source_rows[idx]["picked_at"] = picked_at
            source_rows[idx]["pick_id"] = picked_at.replace(":", "").replace("-", "")
        write_jsonl_atomic(src, source_rows)


def choose_random(
    pool: List[Dict[str, Any]],
    total: int,
    rng: random.Random,
    no_overlap: bool,
    max_per_session: int,
) -> List[Dict[str, Any]]:
    candidates = list(pool)
    rng.shuffle(candidates)
    out: List[Dict[str, Any]] = []
    windows_by_session: Dict[str, List[Tuple[int, int]]] = {}
    cnt_by_session: Dict[str, int] = {}

    for row in candidates:
        if len(out) >= total:
            break

        sess = row["_session_dir"]
        cnt = cnt_by_session.get(sess, 0)
        if max_per_session > 0 and cnt >= max_per_session:
            continue

        s = int(row["start_abs"])
        e = int(row["end_abs"])
        w = (s, e)

        if no_overlap:
            existing = windows_by_session.get(sess, [])
            if any(overlaps(w, x) for x in existing):
                continue

        out.append(row)
        cnt_by_session[sess] = cnt + 1
        windows_by_session.setdefault(sess, []).append(w)

    return out


def choose_motion(
    pool: List[Dict[str, Any]],
    total: int,
    no_overlap: bool,
    max_per_session: int,
) -> List[Dict[str, Any]]:
    ordered = sorted(pool, key=lambda r: float(r.get("motion", -1.0)), reverse=True)
    return choose_random(ordered, total, random.Random(0), no_overlap, max_per_session)


def choose_band(
    pool: List[Dict[str, Any]],
    ranges: List[RangeSpec],
    rng: random.Random,
    no_overlap: bool,
    max_per_session: int,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    windows_by_session: Dict[str, List[Tuple[int, int]]] = {}
    cnt_by_session: Dict[str, int] = {}

    for r in ranges:
        items = [x for x in pool if in_range(float(x.get("motion", -1)), r)]
        rng.shuffle(items)
        got = 0
        for row in items:
            if got >= r.n:
                break
            sess = row["_session_dir"]
            cnt = cnt_by_session.get(sess, 0)
            if max_per_session > 0 and cnt >= max_per_session:
                continue

            s = int(row["start_abs"])
            e = int(row["end_abs"])
            w = (s, e)
            if no_overlap and any(overlaps(w, x) for x in windows_by_session.get(sess, [])):
                continue

            row2 = dict(row)
            row2["pick_reason"] = f"band:{r.label}"
            out.append(row2)
            got += 1
            cnt_by_session[sess] = cnt + 1
            windows_by_session.setdefault(sess, []).append(w)

    return out


def choose_hybrid(
    pool: List[Dict[str, Any]],
    ranges: List[RangeSpec],
    rng: random.Random,
    no_overlap: bool,
    max_per_session: int,
) -> List[Dict[str, Any]]:
    # Within each band, keep top 50% by motion then random pick.
    out: List[Dict[str, Any]] = []
    windows_by_session: Dict[str, List[Tuple[int, int]]] = {}
    cnt_by_session: Dict[str, int] = {}

    for r in ranges:
        items = [x for x in pool if in_range(float(x.get("motion", -1)), r)]
        if not items:
            continue
        items = sorted(items, key=lambda x: float(x.get("motion", -1)), reverse=True)
        keep_n = max(1, len(items) // 2)
        items = items[:keep_n]
        rng.shuffle(items)

        got = 0
        for row in items:
            if got >= r.n:
                break
            sess = row["_session_dir"]
            cnt = cnt_by_session.get(sess, 0)
            if max_per_session > 0 and cnt >= max_per_session:
                continue

            s = int(row["start_abs"])
            e = int(row["end_abs"])
            w = (s, e)
            if no_overlap and any(overlaps(w, x) for x in windows_by_session.get(sess, [])):
                continue

            row2 = dict(row)
            row2["pick_reason"] = f"hybrid:{r.label}"
            out.append(row2)
            got += 1
            cnt_by_session[sess] = cnt + 1
            windows_by_session.setdefault(sess, []).append(w)

    return out


def to_event_row(row: Dict[str, Any], publish_at: str, route: str) -> Dict[str, Any]:
    session_dir = Path(row["_session_dir"])
    start_abs = int(row["start_abs"])
    end_abs = int(row["end_abs"])
    event_name = f"{start_abs:05d}_{end_abs:05d}"
    event_dir = session_dir / "events" / event_name
    return {
        "session_dir": str(session_dir),
        "event_name": event_name,
        "event_dir": str(event_dir),
        "frames_dir": str(event_dir / "images_cropped"),
        "publishAt": publish_at,
        "route": route,
        "motion": row.get("motion"),
        "pick_reason": row.get("pick_reason", ""),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Global picker across all candidates_20s.jsonl")
    ap.add_argument("--config", default=str(Path(__file__).resolve().parents[1] / "config.json"))
    ap.add_argument("--base-dir", default="")
    ap.add_argument("--mode", choices=["random", "motion", "band", "hybrid"], default="band")
    ap.add_argument("--total", type=int, default=8)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--min-motion", type=float, default=-1e9)
    ap.add_argument("--max-motion", type=float, default=1e9)
    ap.add_argument("--max-per-session", type=int, default=2)
    ap.add_argument("--no-overlap", action="store_true")
    ap.add_argument("--skip-uploaded", action="store_true", default=True)
    ap.add_argument(
        "--range",
        action="append",
        default=["0,10,2,le10", "10,20,3,10_20", "30,1e9,3,ge30"],
        help="band spec lo,hi,n,label",
    )
    ap.add_argument("--start", default="")
    ap.add_argument("--pitch-hours", type=float, default=-1)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conf = load_config(args.config)
    base_dir = Path(args.base_dir or conf["base_dir"])
    event_queue = conf["event_queue"]
    route = conf.get("route", "yolo_win")
    candidates_name = conf.get("candidates_name", "candidates_20s.jsonl")
    pitch_hours = conf.get("publish_pitch_hours", 3.0) if args.pitch_hours < 0 else args.pitch_hours

    pool = collect_candidates(base_dir, candidates_name, args.skip_uploaded)
    pool = [
        x
        for x in pool
        if args.min_motion <= float(x.get("motion", -1)) <= args.max_motion
    ]

    if not pool:
        print("[WARN] no eligible candidates")
        return

    rng = random.Random(args.seed)
    ranges = parse_ranges(args.range)

    if args.mode == "random":
        chosen = choose_random(pool, args.total, rng, args.no_overlap, args.max_per_session)
    elif args.mode == "motion":
        chosen = choose_motion(pool, args.total, args.no_overlap, args.max_per_session)
    elif args.mode == "band":
        chosen = choose_band(pool, ranges, rng, args.no_overlap, args.max_per_session)
    else:
        chosen = choose_hybrid(pool, ranges, rng, args.no_overlap, args.max_per_session)

    if args.total > 0:
        chosen = chosen[: args.total]

    if not chosen:
        print("[WARN] nothing picked")
        return

    now = datetime.now(JST).replace(microsecond=0)
    if args.start:
        try:
            start_dt = datetime.fromisoformat(args.start)
            if start_dt.tzinfo is None:
                start_dt = start_dt.replace(tzinfo=JST)
            else:
                start_dt = start_dt.astimezone(JST)
        except ValueError:
            hh, mm = args.start.split(":")
            start_dt = now.replace(hour=int(hh), minute=int(mm), second=0)
    else:
        start_dt = now + timedelta(minutes=5)

    rows_out: List[Dict[str, Any]] = []
    for idx, row in enumerate(chosen):
        publish_at = (start_dt + timedelta(hours=idx * float(pitch_hours))).isoformat()
        rows_out.append(to_event_row(row, publish_at, route))

    print(f"[OK] picked={len(chosen)} mode={args.mode}")
    for row in rows_out:
        print(f"  {row['event_name']} motion={row['motion']} session={Path(row['session_dir']).name}")

    if args.dry_run:
        print("[DRY] no writes")
        return

    picked_at = now.isoformat()
    mark_picked(chosen, picked_at)
    for row in rows_out:
        append_jsonl(event_queue, row)

    print(f"[OK] event_queue appended: {event_queue}")


if __name__ == "__main__":
    main()


