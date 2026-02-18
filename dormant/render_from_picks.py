#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
render_from_picks.py

picked_yesterday.jsonl を読み、
各イベントの最新vN/decision.json を元に render_short_from_decision.py を実行して
short mp4 を生成する。

※投稿やqueueには触れない
"""

import argparse
import json
import subprocess
from pathlib import Path


def find_latest_v(api_dir: Path) -> Path | None:
    if not api_dir.exists():
        return None
    vs = []
    for p in api_dir.iterdir():
        if p.is_dir() and p.name.lower().startswith("v"):
            try:
                n = int(p.name[1:])
                vs.append((n, p))
            except:
                pass
    if not vs:
        return None
    vs.sort(key=lambda x: x[0])
    return vs[-1][1]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--picked", default="/media/sf_REC/posting/picked_yesterday.jsonl")
    ap.add_argument("--render-script", default="/media/sf_REC/scripts/render_short_from_decision.py")
    ap.add_argument("--max", type=int, default=9999, help="max items to render")
    args = ap.parse_args()

    picked_path = Path(args.picked).expanduser().resolve()
    render_script = Path(args.render_script).expanduser().resolve()

    if not picked_path.exists():
        raise SystemExit(f"picked not found: {picked_path}")
    if not render_script.exists():
        raise SystemExit(f"render script not found: {render_script}")

    lines = picked_path.read_text(encoding="utf-8").splitlines()
    if not lines:
        print("[OK] no picks. nothing to render.")
        return

    ok = 0
    ng = 0

    for i, line in enumerate(lines, 1):
        if i > args.max:
            break

        item = json.loads(line)
        session_dir = Path(item["session_dir"]).expanduser().resolve()
        event = item["event"]

        event_dir = session_dir / "events" / event
        api_dir = event_dir / "api"
        latest_v_dir = find_latest_v(api_dir)

        print(f"\n==== [{i}/{len(lines)}] {event} ====")
        print(f"session_dir: {session_dir}")

        if latest_v_dir is None:
            print("[SKIP] no api/vN (run 2API first)")
            ng += 1
            continue

        decision_path = latest_v_dir / "decision.json"
        if not decision_path.exists():
            print(f"[SKIP] decision.json missing: {decision_path}")
            ng += 1
            continue

        # render_short_from_decision.py は event-dir と overwrite を受ける実装
        cmd = [
            "python3",
            str(render_script),
            "--event-dir",
            str(event_dir),
            "--overwrite",
        ]

        rc = subprocess.call(cmd)
        if rc == 0:
            ok += 1
            print("[OK] rendered")
        else:
            ng += 1
            print(f"[NG] render failed rc={rc}")

    print(f"\n[DONE] ok={ok} ng={ng} total={min(len(lines), args.max)}")


if __name__ == "__main__":
    main()
