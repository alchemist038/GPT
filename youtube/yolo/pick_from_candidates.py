#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
pick_from_candidates.py
- セッション直下の candidates_20s.jsonl を素材台帳として扱う
- picked_at が無い行から、motion帯ごとに指定本数だけランダムにピック
- ピックした行へ picked_at を追記（追記のみ＝ファイルが真実）
- 足りない帯は warn 空ファイルを作る（例: .warn_no_motion30）
- 監査用に yolo/<timestamp>/picked.jsonl を任意で出す

前提（このスレで確定）：
- candidates_20s.jsonl は 1行1素材、必須キー start_abs/end_abs/motion
- 追記キー picked_at/uploaded_at/video_id を使う
- 通常ラインでは一度 picked した素材は二度と使わない
"""

import argparse
import json
import os
import random
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

JST = timezone(timedelta(hours=9))


@dataclass
class RangeSpec:
    lo: float
    hi: float
    n: int
    label: str  # warn file name suffix, log label


def now_jst_iso() -> str:
    return datetime.now(JST).replace(microsecond=0).isoformat()


def parse_ranges(range_args: List[str]) -> List[RangeSpec]:
    """
    --range "0,10,2,le10"
    --range "10,20,3,10_20"
    --range "30,1e9,3,ge30"
    """
    out: List[RangeSpec] = []
    for s in range_args:
        parts = [p.strip() for p in s.split(",")]
        if len(parts) not in (3, 4):
            raise ValueError(f"Invalid --range: {s} (need 3 or 4 fields: lo,hi,n[,label])")
        lo = float(parts[0])
        hi = float(parts[1])
        n = int(parts[2])
        label = parts[3] if len(parts) == 4 else f"{lo}_{hi}"
        out.append(RangeSpec(lo=lo, hi=hi, n=n, label=label))
    return out


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def write_jsonl_atomic(path: Path, rows: List[Dict[str, Any]]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    tmp.replace(path)


def in_range(motion: float, spec: RangeSpec) -> bool:
    # 仕様：lo < = motion <= hi ではなく、「lo < motion <= hi」か「motion <= hi」など揺れやすい。
    # ここは実装を明確に固定する：
    #   motion が lo 以上 かつ hi 以下
    # 例：0-10 は 0<=m<=10
    return (motion >= spec.lo) and (motion <= spec.hi)


def overlaps(a: Tuple[int, int], b: Tuple[int, int]) -> bool:
    # 20秒窓が重なり過ぎるのを避けたい場合に使う（デフォルトON）
    (s1, e1) = a
    (s2, e2) = b
    return not (e1 <= s2 or e2 <= s1)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--session-dir", required=True, help="session dir (candidates_20s.jsonl がある場所)")
    ap.add_argument("--candidates", default="candidates_20s.jsonl", help="candidates file name (default: candidates_20s.jsonl)")
    ap.add_argument("--seed", type=int, default=42, help="random seed (default: 42)")
    ap.add_argument("--no-overlap", action="store_true", help="allow overlapping windows (default: overlap禁止)")
    ap.add_argument("--write-log", action="store_true", help="write yolo/<timestamp>/picked.jsonl audit log")
    ap.add_argument("--warn-empty-file", action="store_true", help="create .warn_no_xxx empty file when shortage (recommended)")
    ap.add_argument("--picked-at-key", default="picked_at", help="picked flag key name (default: picked_at)")
    ap.add_argument("--skip-uploaded", action="store_true", help="skip rows that already have video_id (safety)")
    # enqueue to event_queue_yolo.jsonl（必要なときだけ）
    ap.add_argument("--enqueue", action="store_true", help="also append picked items to event_queue_yolo.jsonl")
    ap.add_argument("--event-queue", default="/media/sf_REC/posting/event_queue_yolo.jsonl", help="event queue path")
    ap.add_argument("--start", default="", help='publishAt start. RFC3339(+09:00) or "HH:MM" (JST today). e.g. 2026-02-12T18:00:00+09:00 or 18:00')
    ap.add_argument("--pitch-hours", type=float, default=3.0, help="publishAt pitch hours (default: 3.0)")
    ap.add_argument("--route", default="yolo", help="route tag stored in queue (default: yolo)")

    # レンジ指定：このスレの例をデフォルトとして内蔵
    ap.add_argument(
        "--range",
        action="append",
        default=[
            "0,10,2,le10",
            "10,20,3,10_20",
            "30,1e9,3,ge30",
        ],
        help='range spec "lo,hi,n[,label]" (repeatable). default: 0-10:2, 10-20:3, 30+:3',
    )

    args = ap.parse_args()

    # --- publishAt start parse ---
    publish_start = None
    if args.start:
        s = args.start.strip()
        try:
            # RFC3339 を優先（例: 2026-02-12T18:00:00+09:00）
            publish_start = datetime.fromisoformat(s)
        except Exception:
            # "HH:MM" を許可（JSTの今日）
            try:
                hh, mm = s.split(":")
                hh = int(hh); mm = int(mm)
                nowj = datetime.now(JST)
                publish_start = nowj.replace(hour=hh, minute=mm, second=0, microsecond=0)
            except Exception:
                raise SystemExit(f"Invalid --start: {args.start} (use RFC3339 or HH:MM)")


    session_dir = Path(args.session_dir)
    cand_path = session_dir / args.candidates
    if not cand_path.exists():
        raise SystemExit(f"candidates not found: {cand_path}")

    ranges = parse_ranges(args.range)
    rng = random.Random(args.seed)

    rows = load_jsonl(cand_path)

    picked_key = args.picked_at_key
    now_iso = now_jst_iso()
    pick_id = datetime.now(JST).strftime("%Y%m%d_%H%M%S")

    # eligible をレンジ別に集める（ただし picked 済みは除外）
    # ※候補の全件保持は candidates の段階で軽いので問題なし
    chosen_indices: List[int] = []
    chosen_windows: List[Tuple[int, int]] = []

    # 監査用ログ
    picked_log: List[Dict[str, Any]] = []

    for spec in ranges:
        # 未picked かつ（必要なら）未uploaded
        eligible: List[int] = []
        for i, r in enumerate(rows):
            if picked_key in r and r[picked_key]:
                continue
            if args.skip_uploaded and r.get("video_id"):
                continue
            m = float(r.get("motion", -1))
            if not in_range(m, spec):
                continue
            # 必須項目
            if "start_abs" not in r or "end_abs" not in r:
                continue
            eligible.append(i)

        # ランダムに混ぜる
        rng.shuffle(eligible)

        need = spec.n
        got = 0
        for idx in eligible:
            if got >= need:
                break
            s = int(rows[idx]["start_abs"])
            e = int(rows[idx]["end_abs"])
            w = (s, e)

            if not args.no_overlap:
                # 既選択と重なればスキップ
                if any(overlaps(w, w2) for w2 in chosen_windows):
                    continue

            # 採用
            chosen_indices.append(idx)
            chosen_windows.append(w)
            got += 1

            picked_log.append(
                {
                    "start_abs": s,
                    "end_abs": e,
                    "motion": float(rows[idx]["motion"]),
                    "range": spec.label,
                }
            )

        # 足りなければ warn
        if got < need and args.warn_empty_file:
            warn_name = f".warn_no_motion_{spec.label}"
            (session_dir / warn_name).write_text("", encoding="utf-8")

    # candidates に picked_at を追記（追記のみ）
    for idx in chosen_indices:
        rows[idx][picked_key] = now_iso
        rows[idx]["pick_id"] = pick_id  # 追跡用（不要なら後で外しても良い）

    write_jsonl_atomic(cand_path, rows)

    # -----------------------------
    # event_queue_yolo.jsonl へ追記（任意）
    # -----------------------------
    if args.enqueue:
        if publish_start is None:
            # start未指定なら「今から5分後」を開始時刻にする
            publish_start = datetime.now(JST).replace(microsecond=0) + timedelta(minutes=5)

        event_queue_path = Path(args.event_queue)
        event_queue_path.parent.mkdir(parents=True, exist_ok=True)

        # chosen_indices の順に publishAt を振る（一定ピッチ）
        for k, idx in enumerate(chosen_indices):
            r = rows[idx]
            start_abs = int(r["start_abs"])
            end_abs = int(r["end_abs"])

            event_name = f"{start_abs:05d}_{end_abs:05d}"
            publish_at = (publish_start + timedelta(hours=k * float(args.pitch_hours))).isoformat()

            line = {
                "session_dir": str(session_dir),
                "event_name": event_name,
                "frames_dir": str(session_dir / "frames_360" / event_name),
                "event_dir": str(session_dir / "events" / event_name),
                "publishAt": publish_at,
                "route": args.route,
            }

            with event_queue_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(line, ensure_ascii=False) + "\n")

        print(f"[OK] enqueued_to_event_queue={len(chosen_indices)} -> {event_queue_path}")

    # 監査ログ（任意）
    if args.write_log:
        yolo_dir = session_dir / "yolo" / pick_id
        yolo_dir.mkdir(parents=True, exist_ok=True)
        log_path = yolo_dir / "picked.jsonl"
        with log_path.open("w", encoding="utf-8") as f:
            for r in picked_log:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"[OK] candidates={cand_path}")
    print(f"[OK] picked={len(chosen_indices)}  pick_id={pick_id}  at={now_iso}")
    if args.write_log:
        print(f"[OK] log_written=yolo/{pick_id}/picked.jsonl")


if __name__ == "__main__":
    main()
