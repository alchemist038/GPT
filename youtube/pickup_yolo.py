#!/usr/bin/env python3
"""
publish_yolo pickUP
- random pick : 正規在庫のみ（.yolo_done/.yolo_reject/.yolo_published が無い）
- manual pick : 制限あり / --force で全解除
- 保険：/media/sf_REC/<SESSION> が来ても、存在すれば /media/sf_masaos_mov/<SESSION> に置換

出力：
/media/sf_REC/posting/yolo_event_queue.jsonl に1行JSONを追記
"""

import json
import argparse
from pathlib import Path
from datetime import datetime

CTRL_BASE = Path("/media/sf_REC")   # 司令塔（固定）
POSTING = CTRL_BASE / "posting"
QUEUE = POSTING / "yolo_event_queue.jsonl"


def yolo_flags_exist(event_dir: Path) -> list[str]:
    flags = []
    yolo = event_dir / "yolo"
    if not yolo.exists():
        return flags
    for p in yolo.rglob(".yolo_done"):
        flags.append(str(p))
    for p in yolo.rglob(".yolo_reject"):
        flags.append(str(p))
    for p in yolo.rglob(".yolo_published"):
        flags.append(str(p))
    return flags


def enqueue(row: dict):
    QUEUE.parent.mkdir(parents=True, exist_ok=True)
    with QUEUE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def normalize_session_dir(session_dir: Path, mov_base: Path) -> Path:
    """
    保険：
    - session_dir が存在しない場合、session名だけ取り出して mov_base/<session名> を試す
    - /media/sf_REC/<session名> -> /media/sf_masaos_mov/<session名> 置換を自動で行う
    """
    if session_dir.exists():
        return session_dir

    name = session_dir.name
    cand = mov_base / name
    if cand.exists():
        return cand

    # もし session_dir が "2026-..." のような相対名だけなら mov_base を付ける
    if str(session_dir) == name:
        if cand.exists():
            return cand

    return session_dir  # 最後まで無ければそのまま返す


def manual_pick(session_dir: Path, event: str, force: bool, mov_base: Path):
    session_dir = normalize_session_dir(session_dir, mov_base)

    event_dir = session_dir / "events" / event
    frames_dir = session_dir / "frames_360" / event

    if not frames_dir.exists():
        print(f"[REJECT] frames not found: {frames_dir}")
        return

    flags = yolo_flags_exist(event_dir)
    if flags and not force:
        print("[REJECT] yolo flags exist:")
        for f in flags:
            print(" ", f)
        print("=> use --force to override")
        return

    enqueue({
        "session_dir": str(session_dir),
        "event": event,
        "frames_dir": str(frames_dir),
        "event_dir": str(event_dir),
        "route": "yolo_manual_force" if force else "yolo_manual",
        "picked_at": datetime.now().isoformat()
    })
    print("[PUBLISH] enqueued")


def random_pick(n: int, mov_base: Path):
    """
    真のランダム抽出（reservoir sampling）
    - 出力フォーマットは既存のまま（frames_dir/event_dir/route/picked_at）
    - 全件リスト化しない（重くしない）
    """
    import random
    from datetime import datetime

    # reservoir: 最大 n 件
    picked = []
    seen = 0

    for session_dir in mov_base.glob("20??-??-??_*"):
        frames_root = session_dir / "frames_360"
        events_root = session_dir / "events"
        if not frames_root.exists():
            continue

        for frames_dir in frames_root.iterdir():
            if not frames_dir.is_dir():
                continue

            event = frames_dir.name
            event_dir = events_root / event

            # 正規在庫条件（既存と同じ）
            if yolo_flags_exist(event_dir):
                continue

            # reservoir sampling
            seen += 1
            if len(picked) < n:
                picked.append((session_dir, frames_dir, event_dir, event))
            else:
                j = random.randint(1, seen)
                if j <= n:
                    picked[j - 1] = (session_dir, frames_dir, event_dir, event)

    # enqueue（既存フォーマットそのまま）
    if not picked:
        print("[EMPTY] no stock found")
        return

    for session_dir, frames_dir, event_dir, event in picked:
        enqueue({
            "session_dir": str(session_dir),
            "event": event,
            "frames_dir": str(frames_dir),
            "event_dir": str(event_dir),
            "route": "yolo_random",
            "picked_at": datetime.now().isoformat()
        })
        print(f"[PUBLISH] random enqueue: {event}")

    print(f"[SCAN] stock_seen={seen} picked={len(picked)}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--session-dir", help="manual pick session dir")
    ap.add_argument("--event", help="manual pick event")
    ap.add_argument("--force", action="store_true", help="override all yolo flags")
    ap.add_argument("--random", type=int, help="random pick N events")

    # ★追加：素材ルート（正）
    ap.add_argument("--mov-base", default="/media/sf_masaos_mov",
                    help="sessions base dir for materials (default: /media/sf_masaos_mov)")

    args = ap.parse_args()
    mov_base = Path(args.mov_base)

    if args.random:
        random_pick(args.random, mov_base)
        return

    if args.session_dir and args.event:
        manual_pick(Path(args.session_dir), args.event, args.force, mov_base)
        return

    ap.print_help()


if __name__ == "__main__":
    main()
