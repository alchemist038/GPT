#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
holiday_enqueue_from_stock.py

休日ランダム入口：
- 在庫（frames_360にあり / eventsに無い）を数えて表示
- 本数指定ランダム or イベント名指定で抽出
- event_queue.jsonl に追記（新規作成）

起動時ガード：
- event_queue.jsonl が存在したら「警告＋選択」
  1) 消して続行
  2) 終了

DBなし / ファイルが真実 前提
"""

import argparse
import datetime as dt
import json
import os
import random
import sys
from typing import List, Dict, Tuple


DEFAULT_ROOT = "/media/sf_masaos_mov"
DEFAULT_EVENT_QUEUE = "/media/sf_REC/posting/event_queue.jsonl"


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def list_session_dirs(root: str) -> List[str]:
    if not os.path.isdir(root):
        return []
    items = []
    for name in sorted(os.listdir(root)):
        p = os.path.join(root, name)
        if os.path.isdir(p) and not name.startswith("."):
            # セッションっぽいフォルダだけ（厳密判定は不要）
            items.append(p)
    return items


def find_stock_events(root: str) -> List[Dict[str, str]]:
    """
    在庫定義（事実）：
      frames_360/<EVENT>/ が存在し、 events/<EVENT>/ が存在しないもの
    返却：[{session_dir, event_name, frames_dir, event_dir}]
    """
    out = []
    for session_dir in list_session_dirs(root):
        frames_root = os.path.join(session_dir, "frames_360")
        if not os.path.isdir(frames_root):
            continue
        events_root = os.path.join(session_dir, "events")  # 無くてもOK（=全在庫）
        try:
            event_names = sorted(
                [d for d in os.listdir(frames_root) if os.path.isdir(os.path.join(frames_root, d)) and not d.startswith(".")]
            )
        except Exception:
            continue

        for ev in event_names:
            frames_dir = os.path.join(frames_root, ev)
            event_dir = os.path.join(events_root, ev)
            if os.path.isdir(frames_dir) and (not os.path.isdir(event_dir)):
                out.append({
                    "session_dir": session_dir,
                    "event_name": ev,
                    "frames_dir": frames_dir,
                    "event_dir": event_dir,
                })
    return out


def parse_hhmm(s: str) -> Tuple[int, int]:
    parts = s.strip().split(":")
    if len(parts) != 2:
        raise ValueError("HH:MM format required")
    h = int(parts[0])
    m = int(parts[1])
    if not (0 <= h <= 23 and 0 <= m <= 59):
        raise ValueError("Invalid HH:MM")
    return h, m


def rfc3339_jst(t: dt.datetime) -> str:
    # JST固定 +09:00
    tz = dt.timezone(dt.timedelta(hours=9))
    if t.tzinfo is None:
        t = t.replace(tzinfo=tz)
    else:
        t = t.astimezone(tz)
    return t.isoformat(timespec="seconds")


def build_publish_times(date_str: str, start_hhmm: str, count: int, pitch_hours: float) -> List[str]:
    if count <= 0:
        return []
    y, mo, d = [int(x) for x in date_str.split("-")]
    h, m = parse_hhmm(start_hhmm)
    tz = dt.timezone(dt.timedelta(hours=9))
    start = dt.datetime(y, mo, d, h, m, 0, tzinfo=tz)

    times = []
    for i in range(count):
        t = start + dt.timedelta(seconds=int(round(pitch_hours * 3600 * i)))
        times.append(rfc3339_jst(t))
    return times


def prompt_existing_queue(queue_path: str) -> str:
    """
    返り値：'delete' or 'exit'
    """
    # 件数ざっくり
    try:
        with open(queue_path, "r", encoding="utf-8") as f:
            lines = [ln for ln in f.read().splitlines() if ln.strip()]
        n = len(lines)
    except Exception:
        n = -1

    print("")
    print("[WARN] event_queue.jsonl already exists:")
    print(f"      {queue_path}")
    if n >= 0:
        print(f"[INFO] current entries: {n}")
    print("")
    print("Choose action:")
    print("  [1] Delete event_queue.jsonl and continue")
    print("  [2] Exit without changes")
    print("")

    while True:
        choice = input("Select 1 or 2 > ").strip()
        if choice == "1":
            return "delete"
        if choice == "2":
            return "exit"
        print("Please type 1 or 2.")


def main():
    ap = argparse.ArgumentParser(
        description="休日：在庫（frames_360あり / eventsなし）からイベントを選んで event_queue.jsonl に投入"
    )
    ap.add_argument("--root", default=DEFAULT_ROOT, help=f"セッションルート（既定: {DEFAULT_ROOT}）")
    ap.add_argument("--queue", default=DEFAULT_EVENT_QUEUE, help=f"出力 event_queue.jsonl（既定: {DEFAULT_EVENT_QUEUE}）")

    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--pick", type=int, help="在庫からランダムに投入する本数")
    g.add_argument("--event", action="append", help="特定イベント名を投入（複数指定可）例: --event 00260_00303")

    ap.add_argument("--date", default=None, help="JST日付 YYYY-MM-DD（既定: 今日）")
    ap.add_argument("--start", default="07:00", help="最初の publishAt JST HH:MM（既定: 07:00）")
    ap.add_argument("--pitch-hours", type=float, default=None, help="投稿間隔（時間）。未指定なら 24/本数")
    ap.add_argument("--seed", type=int, default=None, help="ランダムseed（再現用）")
    ap.add_argument("--dry-run", action="store_true", help="書き込まず表示のみ")
    ap.add_argument("--show", type=int, default=20, help="在庫の表示件数（既定: 20）")

    args = ap.parse_args()

    root = args.root
    queue_path = args.queue

    # 日付
    tz = dt.timezone(dt.timedelta(hours=9))
    today = dt.datetime.now(tz).date()
    date_str = args.date if args.date else today.strftime("%Y-%m-%d")

    # 在庫抽出
    stock = find_stock_events(root)
    print(f"[STOCK] root={root}")
    print(f"[STOCK] date={date_str} JST")
    print(f"[STOCK] count={len(stock)}")

    if len(stock) == 0:
        print("[STOCK] empty. nothing to do.")
        sys.exit(0)

    # サンプル表示
    show_n = max(0, args.show)
    if show_n > 0:
        print("")
        print(f"[STOCK] sample (up to {show_n}):")
        for i, it in enumerate(stock[:show_n], start=1):
            print(f"  {i:02d}  {os.path.basename(it['session_dir'])}  {it['event_name']}")
        if len(stock) > show_n:
            print(f"  ... and {len(stock) - show_n} more")
        print("")

    # 起動時ガード：event_queue が存在したら選択
    if os.path.exists(queue_path):
        action = prompt_existing_queue(queue_path)
        if action == "exit":
            print("[EXIT] event_queue exists. no changes made.")
            sys.exit(0)
        elif action == "delete":
            if args.dry_run:
                print("[DRY] would delete existing event_queue.jsonl, but dry-run is on.")
            else:
                os.remove(queue_path)
                print(f"[OK] deleted: {queue_path}")
        else:
            print("[ERROR] invalid choice state")
            sys.exit(2)

    # 選択（pick or event）
    chosen: List[Dict[str, str]] = []
    if args.seed is not None:
        random.seed(args.seed)

    if args.pick is not None:
        n = args.pick
        if n <= 0:
            print("[ERROR] --pick must be >= 1")
            sys.exit(2)
        if n > len(stock):
            print(f"[WARN] requested pick={n} but stock={len(stock)}. will pick all stock.")
            n = len(stock)
        chosen = random.sample(stock, n)
        chosen.sort(key=lambda x: (os.path.basename(x["session_dir"]), x["event_name"]))
    else:
        # イベント名指定：在庫にあるものだけ拾う
        req = args.event or []
        req = [x.strip() for x in req if x and x.strip()]
        if not req:
            print("[ERROR] --event requires a value")
            sys.exit(2)

        # 在庫を event_name で索引（同名が複数セッションにある可能性があるので全件）
        by_name: Dict[str, List[Dict[str, str]]] = {}
        for it in stock:
            by_name.setdefault(it["event_name"], []).append(it)

        missing = []
        for name in req:
            if name not in by_name:
                missing.append(name)
                continue
            # 同名が複数セッションにある場合は全部入れると危険なので、最新っぽい（ディレクトリ名が大きい）を1つだけ選ぶ
            # ※これは「誤投入防止」のための安全側。必要なら後で拡張可能。
            candidates = sorted(by_name[name], key=lambda x: os.path.basename(x["session_dir"]), reverse=True)
            chosen.append(candidates[0])

        if missing:
            print("[WARN] not in stock (skipped):")
            for m in missing:
                print(f"  - {m}")

        # 重複除去（念のため）
        uniq = {}
        for it in chosen:
            key = (it["session_dir"], it["event_name"])
            uniq[key] = it
        chosen = list(uniq.values())
        chosen.sort(key=lambda x: (os.path.basename(x["session_dir"]), x["event_name"]))

        if len(chosen) == 0:
            print("[DONE] nothing selected from stock.")
            sys.exit(0)

    # publishAt 割り当て
    count = len(chosen)
    pitch = args.pitch_hours if args.pitch_hours is not None else (24.0 / float(count))
    publish_times = build_publish_times(date_str, args.start, count, pitch)

    # event_queue 1行JSONを構築
    route = "holiday_random"
    lines = []
    for it, publishAt in zip(chosen, publish_times):
        obj = {
            "session_dir": it["session_dir"],
            "event_name": it["event_name"],
            "frames_dir": it["frames_dir"],
            "event_dir": it["event_dir"],
            "publishAt": publishAt,
            "route": route,
        }
        lines.append(json.dumps(obj, ensure_ascii=False))

    # 予定表示
    print("[PLAN] selected:")
    for i, (it, t) in enumerate(zip(chosen, publish_times), start=1):
        print(f"  {i:02d}  {t}  {os.path.basename(it['session_dir'])}  {it['event_name']}")

    if args.dry_run:
        print("[DRY] no write. done.")
        sys.exit(0)

    # 書き込み（新規作成/追記ではなく、このスクリプトは「新規作成」運用）
    os.makedirs(os.path.dirname(queue_path), exist_ok=True)
    with open(queue_path, "a", encoding="utf-8") as f:
        for ln in lines:
            f.write(ln + "\n")

    print(f"[OK] wrote {len(lines)} lines to {queue_path}")
    sys.exit(0)


if __name__ == "__main__":
    main()
