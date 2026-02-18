#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from pathlib import Path
import re
from datetime import datetime, timezone, timedelta

JST = timezone(timedelta(hours=9))

SESSION_RE = re.compile(r"^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}$")
EVENT_RE = re.compile(r"^\d{5}_\d{5}$")
V_RE = re.compile(r"^v(\d+)$")


def latest_v_dir(api_dir: Path):
    best_v = None
    best_dir = None
    if not api_dir.is_dir():
        return None
    for p in api_dir.iterdir():
        if not p.is_dir():
            continue
        m = V_RE.match(p.name)
        if not m:
            continue
        v = int(m.group(1))
        if best_v is None or v > best_v:
            best_v = v
            best_dir = p
    if best_v is None:
        return None
    return best_v, best_dir


def has_any_short_mp4(shorts_dir: Path) -> bool:
    if not shorts_dir.is_dir():
        return False
    for p in shorts_dir.iterdir():
        if p.is_file() and p.suffix.lower() == ".mp4" and p.name.startswith("short_"):
            return True
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="/media/sf_masaos_mov", help="セッションが並ぶルート")
    ap.add_argument("--dry-run", action="store_true", help="書き込みなしで一覧表示のみ")
    args = ap.parse_args()

    root = Path(args.root)
    if not root.is_dir():
        raise SystemExit(f"[ERROR] root not found: {root}")

    now = datetime.now(JST).strftime("%Y-%m-%dT%H:%M:%S%z")
    touched = 0
    skipped = 0
    errors = 0

    print(f"[INFO] root={root}")
    print(f"[INFO] mode={'DRY_RUN' if args.dry_run else 'APPLY'}  now={now}")

    for session_dir in sorted(root.iterdir()):
        if not session_dir.is_dir():
            continue
        if not SESSION_RE.match(session_dir.name):
            continue

        events_root = session_dir / "events"
        if not events_root.is_dir():
            continue

        for ev_dir in sorted(events_root.iterdir()):
            if not ev_dir.is_dir():
                continue
            if not EVENT_RE.match(ev_dir.name):
                continue

            api_dir = ev_dir / "api"
            latest = latest_v_dir(api_dir)
            if latest is None:
                continue
            vnum, vdir = latest

            decision = vdir / "decision.json"
            pub = vdir / ".published"
            shorts_dir = ev_dir / "shorts"

            # “解析投稿済み扱い” の条件：
            # - decision.json がある
            # - shorts/ に short_*.mp4 が1つでもある
            if not decision.is_file():
                continue
            if not has_any_short_mp4(shorts_dir):
                continue

            if pub.exists():
                skipped += 1
                continue

            rel = pub.relative_to(root)
            if args.dry_run:
                print(f"[TOUCH] {rel}")
                touched += 1
                continue

            try:
                pub.write_text(
                    f"bulk_mark_published\t{now}\n"
                    f"session={session_dir.name}\n"
                    f"event={ev_dir.name}\n"
                    f"v=v{vnum}\n",
                    encoding="utf-8",
                )
                touched += 1
                print(f"[TOUCHED] {rel}")
            except Exception as e:
                errors += 1
                print(f"[ERROR] failed: {pub}  {e}")

    print("\n[SUMMARY]")
    print(f"touched={touched}  skipped(already)={skipped}  errors={errors}")


if __name__ == "__main__":
    main()
