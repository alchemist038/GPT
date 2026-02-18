#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
run_api2_from_picks.py

picked_yesterday.jsonl を読み、
各 frames_dir を api_decision_pipeline.py に渡して step=2 を実行する。

ここでは投稿やqueueには触れない。
成功すると events/<EVENT>/api/vN/decision.json が増える。
"""

import argparse
import json
import subprocess
from pathlib import Path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--picked", default="/media/sf_REC/posting/picked_yesterday.jsonl", help="picked JSONL")
    ap.add_argument("--api-script", default="/media/sf_REC/scripts/api_decision_pipeline.py", help="api_decision_pipeline.py path")
    ap.add_argument("--step", type=int, default=2, choices=[1, 2], help="1=API1 only, 2=API1+API2")
    args = ap.parse_args()

    picked_path = Path(args.picked).expanduser().resolve()
    api_script = Path(args.api_script).expanduser().resolve()

    if not picked_path.exists():
        raise SystemExit(f"picked not found: {picked_path}")
    if not api_script.exists():
        raise SystemExit(f"api script not found: {api_script}")

    lines = picked_path.read_text(encoding="utf-8").splitlines()
    if not lines:
        print("[OK] no picks. nothing to do.")
        return

    ok = 0
    ng = 0

    for i, line in enumerate(lines, 1):
        item = json.loads(line)
        frames_dir = Path(item["frames_dir"]).expanduser().resolve()
        session_dir = item.get("session_dir", "")
        event = item.get("event", "")

        print(f"\n==== [{i}/{len(lines)}] {event} ====")
        print(f"session_dir: {session_dir}")
        print(f"frames_dir : {frames_dir}")

        if not frames_dir.exists():
            print("[SKIP] frames_dir missing")
            ng += 1
            continue

        cmd = [
            "python3",
            str(api_script),
            "--frames-dir",
            str(frames_dir),
            "--step",
            str(args.step),
        ]

        rc = subprocess.call(cmd)
        if rc == 0:
            ok += 1
            print("[OK] api_decision_pipeline done")
        else:
            ng += 1
            print(f"[NG] api_decision_pipeline failed rc={rc}")

    print(f"\n[DONE] ok={ok} ng={ng} picked={len(lines)}")


if __name__ == "__main__":
    main()
