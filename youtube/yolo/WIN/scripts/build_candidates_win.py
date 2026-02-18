#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional

from common_win import load_config, write_jsonl_atomic

CONF_MIN = 0.40
BBOX_W_MIN = 50.0
BBOX_W_MAX = 210.0
WIN_SEC = 20
STRIDE_SEC = 20
HITS_MIN = 15
CUT_HEAD_SEC = 300
CUT_TAIL_SEC = 300


def pctl(vals: List[float], q: float) -> Optional[float]:
    if not vals:
        return None
    s = sorted(vals)
    if len(s) == 1:
        return s[0]
    pos = q * (len(s) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return s[lo]
    frac = pos - lo
    return s[lo] * (1 - frac) + s[hi] * frac


def motion_p90_p10(cx_list: List[float]) -> Optional[float]:
    p10 = pctl(cx_list, 0.10)
    p90 = pctl(cx_list, 0.90)
    if p10 is None or p90 is None:
        return None
    return float(p90 - p10)


def reject_fullscreen(bb: List[float]) -> bool:
    x1, y1, x2, y2 = bb
    w = x2 - x1
    h = y2 - y1
    if y2 >= 358:
        return True
    if y1 <= 2:
        return True
    if h >= 320:
        return True
    if w >= 320:
        return True
    return False


def is_valid_det(obj: Dict[str, Any]) -> bool:
    conf = float(obj.get("conf", 0.0))
    bb = obj.get("bbox_xyxy")
    if conf < CONF_MIN:
        return False
    if not (isinstance(bb, list) and len(bb) == 4):
        return False

    x1, y1, x2, y2 = map(float, bb)
    bb = [x1, y1, x2, y2]

    if reject_fullscreen(bb):
        return False

    w = x2 - x1
    if w < BBOX_W_MIN or w > BBOX_W_MAX:
        return False

    return True


def cx_of(bb: List[float]) -> float:
    x1, _, x2, _ = bb
    return (x1 + x2) / 2.0


def load_per_sec(raw_yolo_jsonl: Path) -> Dict[int, Dict[str, Any]]:
    per: Dict[int, Dict[str, Any]] = {}
    with raw_yolo_jsonl.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if "sec" not in obj:
                continue
            per[int(obj["sec"])] = obj
    return per


def infer_duration_sec(per: Dict[int, Dict[str, Any]]) -> int:
    if not per:
        return 0
    return max(per.keys()) + 1


def build_candidates(per: Dict[int, Dict[str, Any]], dur_sec: int) -> List[Dict[str, Any]]:
    if dur_sec <= 0:
        return []

    start_min = CUT_HEAD_SEC
    start_max = max(0, dur_sec - CUT_TAIL_SEC - WIN_SEC)
    if start_max < start_min:
        return []

    out: List[Dict[str, Any]] = []
    for start in range(start_min, start_max + 1, STRIDE_SEC):
        cx_list: List[float] = []
        hits = 0
        for t in range(start, start + WIN_SEC):
            obj = per.get(t)
            if not obj:
                continue
            if not is_valid_det(obj):
                continue
            bb = list(map(float, obj["bbox_xyxy"]))
            hits += 1
            cx_list.append(cx_of(bb))

        if hits < HITS_MIN:
            continue

        m = motion_p90_p10(cx_list)
        if m is None:
            continue

        out.append(
            {
                "start_abs": start,
                "end_abs": start + WIN_SEC,
                "motion": round(float(m), 3),
                "hits": hits,
            }
        )
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Build candidates_20s.jsonl across sessions")
    ap.add_argument("--config", default=str(Path(__file__).resolve().parents[1] / "config.json"))
    ap.add_argument("--base-dir", default="", help="Override base_dir in config")
    ap.add_argument("--pattern", default="*")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conf = load_config(args.config)
    base_dir = Path(args.base_dir or conf["base_dir"])
    raw_name = conf.get("raw_yolo_name", "raw_yolo.jsonl")
    out_name = conf.get("candidates_name", "candidates_20s.jsonl")

    if not base_dir.exists():
        raise SystemExit(f"base_dir not found: {base_dir}")

    sessions = sorted(p for p in base_dir.glob(args.pattern) if p.is_dir() and (p / raw_name).exists())
    if not sessions:
        print("[WARN] no sessions found")
        return

    written = 0
    skipped = 0

    for sess in sessions:
        raw = sess / raw_name
        out = sess / out_name

        if out.exists() and not args.force:
            skipped += 1
            continue

        per = load_per_sec(raw)
        dur = infer_duration_sec(per)
        cands = build_candidates(per, dur)

        print(f"[SESSION] {sess.name} dur={dur}s candidates={len(cands)}")
        if args.dry_run:
            continue

        write_jsonl_atomic(out, cands)
        written += 1

    print(f"[DONE] written={written} skipped={skipped} dry_run={args.dry_run}")


if __name__ == "__main__":
    main()
