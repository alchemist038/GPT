#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, json, random, re
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path("/media/sf_masaos_mov")
OUT_QUEUE = Path("/media/sf_REC/posting/event_queue.jsonl")

SESSION_RE = re.compile(r"^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}$")
EVENT_RE = re.compile(r"^\d+_\d+$")

def parse_session_date(name: str):
    return datetime.strptime(name[:10], "%Y-%m-%d").date()

def is_published(session_dir: Path, event_name: str) -> bool:
    # events/<EVENT>/api/v*/.published がどこかにあれば投稿済み扱い
    ev_dir = session_dir / "events" / event_name / "api"
    if not ev_dir.exists():
        return False
    for v in ev_dir.iterdir():
        if re.match(r"^v\d+$", v.name) and (v / ".published").exists():
            return True
    return False

def frames_exist(session_dir: Path, event_name: str) -> bool:
    d = session_dir / "frames_360" / event_name
    return d.exists() and any(d.glob("*.jpg"))

def collect_A_pool(D_date, A_max_date):
    pool = []  # (session_dir, event_name)
    for sdir in sorted([p for p in ROOT.iterdir() if p.is_dir() and SESSION_RE.match(p.name)]):
        s_date = parse_session_date(sdir.name)
        if s_date > A_max_date:
            continue
        froot = sdir / "frames_360"
        if not froot.exists():
            continue

        for ev_frames in sorted([p for p in froot.iterdir() if p.is_dir() and EVENT_RE.match(p.name)]):
            ev = ev_frames.name
            if not frames_exist(sdir, ev):
                continue
            if is_published(sdir, ev):
                continue
            pool.append((sdir, ev))
    return pool

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="D date (YYYY-MM-DD). default=today(JST)")
    ap.add_argument("--publish-date", required=True, help="publish date (YYYY-MM-DD)")
    ap.add_argument("--times", default="08:00,12:00", help="HH:MM,HH:MM ... default 08:00,12:00")
    ap.add_argument("--seed", type=int, default=None, help="random seed (optional)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    # D
    if args.date:
        D = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        D = datetime.now().date()

    # A targets S <= D-2
    A_max = D - timedelta(days=2)

    if args.seed is not None:
        random.seed(args.seed)

    pool = collect_A_pool(D, A_max)
    print(f"[INFO] D={D}  A targets S<= {A_max}")
    print(f"[INFO] A pool={len(pool)} (stock from frames_360)")

    if len(pool) == 0:
        print("[INFO] No stock for A.")
        return

    times = [t.strip() for t in args.times.split(",") if t.strip()]
    if len(times) < 2:
        raise SystemExit("need 2 times, e.g. --times 08:00,12:00")

    picks = random.sample(pool, k=min(2, len(pool)))

    lines = []
    for i, (sdir, ev) in enumerate(picks):
        hhmm = times[i]
        publishAt = f"{args.publish_date}T{hhmm}:00+09:00"
        job = {
            "session_dir": str(sdir),
            "event_name": ev,
            "frames_dir": str(sdir / "frames_360" / ev),
            "event_dir": str(sdir / "events" / ev),  # ここは「無ければ後で作る」前提
            "publishAt": publishAt,
            "route": "A_test",
        }
        lines.append(json.dumps(job, ensure_ascii=False))

    print("\n[PREVIEW]")
    for ln in lines:
        print(ln)

    if args.dry_run:
        print("\n[DRY_RUN] not writing", OUT_QUEUE)
        return

    OUT_QUEUE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_QUEUE, "w", encoding="utf-8") as f:
        for ln in lines:
            f.write(ln + "\n")

    print(f"\n[WROTE] {OUT_QUEUE} lines={len(lines)}")

if __name__ == "__main__":
    main()
