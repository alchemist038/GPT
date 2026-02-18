#!/usr/bin/env python3
import argparse
import csv
import os
import re
import subprocess
from pathlib import Path

# ----------------------------
# Helpers
# ----------------------------
def run_cmd(cmd, check=True):
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if check and p.returncode != 0:
        raise RuntimeError(
            "Command failed:\n"
            f"  cmd: {' '.join(cmd)}\n"
            f"  rc : {p.returncode}\n"
            f"  out: {p.stdout[-2000:]}\n"
            f"  err: {p.stderr[-4000:]}\n"
        )
    return p

def ffprobe_duration_sec(video_path: Path) -> float:
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        str(video_path)
    ]
    p = run_cmd(cmd)
    s = (p.stdout or "").strip()
    try:
        return float(s)
    except Exception:
        raise RuntimeError(f"ffprobe duration parse failed: '{s}' for {video_path}")

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def write_text(p: Path, text: str):
    ensure_dir(p.parent)
    p.write_text(text, encoding="utf-8")

# ----------------------------
# Parsing showinfo
# ----------------------------
# Example showinfo contains: "pts_time:12.000 ... mean:[123  ...]"
re_pts = re.compile(r"pts_time:(\d+(\.\d+)?)")
re_mean = re.compile(r"mean:\[([0-9]+)")

def parse_showinfo_meanY_per_sec(showinfo_log: Path):
    """
    Returns list of (sec_int, meanY_int) sorted by sec.
    Assumes fps=1, but still reads pts_time to be safe.
    """
    rows = []
    if not showinfo_log.exists():
        raise RuntimeError(f"showinfo log not found: {showinfo_log}")

    with showinfo_log.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if "showinfo" not in line:
                continue
            m_pts = re_pts.search(line)
            m_mean = re_mean.search(line)
            if not (m_pts and m_mean):
                continue
            pts = float(m_pts.group(1))
            sec = int(round(pts))  # fps=1なので基本一致するが保険
            meanY = int(m_mean.group(1))
            rows.append((sec, meanY))

    # dedup by sec (keep last)
    by = {}
    for sec, y in rows:
        by[sec] = y
    out = sorted(by.items(), key=lambda x: x[0])
    return out

def compute_delta(rows):
    """
    rows: [(sec, meanY)]
    returns: [(sec, meanY, deltaY)] where deltaY = meanY - prev_meanY (sec順)
    """
    out = []
    prev = None
    for sec, y in rows:
        if prev is None:
            out.append((sec, y, 0.0))
        else:
            out.append((sec, y, float(y - prev)))
        prev = y
    return out

# ----------------------------
# Hit detection & segment/event logic
# ----------------------------
def detect_hits_4sec(delta_rows, dy_threshold, min_len_sec):
    """
    delta_rows: [(sec, meanY, deltaY)]
    A "moving" sec is abs(deltaY) >= dy_threshold.
    Detect runs of consecutive moving seconds with length >= min_len_sec.
    Return list of (run_start_sec, run_end_sec_exclusive).
    """
    moving = []
    for sec, y, dy in delta_rows:
        moving.append((sec, abs(dy) >= dy_threshold))

    hits = []
    run_start = None
    last_sec = None
    for sec, is_move in moving:
        if is_move:
            if run_start is None:
                run_start = sec
            last_sec = sec
        else:
            if run_start is not None:
                run_end_excl = last_sec + 1
                if (run_end_excl - run_start) >= min_len_sec:
                    hits.append((run_start, run_end_excl))
                run_start = None
                last_sec = None

    if run_start is not None:
        run_end_excl = (last_sec + 1) if last_sec is not None else (run_start + 1)
        if (run_end_excl - run_start) >= min_len_sec:
            hits.append((run_start, run_end_excl))

    return hits

def hits_to_segments_15s(hits, pre_sec, seg_len_sec, duration_sec):
    """
    Convert each hit to fixed-length segment:
      start = max(hit_start - pre_sec, 0)
      end = min(start + seg_len_sec, duration)
    """
    segs = []
    for hs, he in hits:
        start = max(hs - pre_sec, 0)
        end = min(start + seg_len_sec, int(duration_sec))
        if end > start:
            segs.append((start, end))
    return segs

def merge_overlaps(segs):
    """
    Merge overlapping/touching segments.
    segs: [(start,end)]
    """
    if not segs:
        return []
    segs = sorted(segs)
    merged = [list(segs[0])]
    for s, e in segs[1:]:
        last = merged[-1]
        if s <= last[1]:  # overlap or touch
            last[1] = max(last[1], e)
        else:
            merged.append([s, e])
    return [(s, e) for s, e in merged]

def cap_and_interval(events, max_len, min_gap):
    """
    Apply:
    - cap each event length to max_len (truncate end)
    - ensure min_gap between events: keep earliest, skip those starting within gap window
    """
    out = []
    last_end = None
    for s, e in sorted(events):
        if e - s > max_len:
            e = s + max_len
        if last_end is None:
            out.append((s, e))
            last_end = e
        else:
            if s < last_end + min_gap:
                # skip (interval rule)
                continue
            out.append((s, e))
            last_end = e
    return out

def apply_op_ed_exclusion(events, duration_sec, op_sec, ed_sec):
    """
    Exclude events that overlap OP [0, op_sec) or ED (duration-ed_sec, duration]
    Strategy: drop events that start < op_sec or end > duration - ed_sec
    """
    dur = int(duration_sec)
    ed_start = max(dur - ed_sec, 0)
    out = []
    for s, e in events:
        if s < op_sec:
            continue
        if e > ed_start:
            continue
        out.append((s, e))
    return out

# ----------------------------
# Frame extraction
# ----------------------------
def event_name(start, end):
    return f"{start:05d}_{end:05d}"

def extract_frames(proxy_path: Path, out_dir: Path, start: int, end: int, jpg_q=2):
    """
    Extract 1fps JPEGs for [start,end) from proxy_360.mp4
    Names: 001.jpg ...
    """
    ensure_dir(out_dir)
    dur = max(end - start, 1)
    # -an (audioなし) / fps=1 / 001.jpg形式
    cmd = [
        "ffmpeg", "-y",
        "-ss", str(start),
        "-t", str(dur),
        "-i", str(proxy_path),
        "-an",
        "-vf", "fps=1",
        "-q:v", str(jpg_q),
        str(out_dir / "%03d.jpg"),
    ]
    run_cmd(cmd)

# ----------------------------
# Main
# ----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--session-dir", required=True, help="e.g. /media/sf_masaos_mov/2025-12-17_06-44-27")
    ap.add_argument("--dy-th", type=float, default=3.0, help="abs(deltaY) threshold (default: 3.0)")
    ap.add_argument("--min-len", type=int, default=4, help="min consecutive seconds (default: 4)")
    ap.add_argument("--pre-sec", type=int, default=3, help="pre-roll seconds for segment start (default: 3)")
    ap.add_argument("--seg-len", type=int, default=15, help="segment length seconds (default: 15)")
    ap.add_argument("--max-event", type=int, default=60, help="max event length seconds (default: 60)")
    ap.add_argument("--gap", type=int, default=60, help="min gap between events seconds (default: 60)")
    ap.add_argument("--op", type=int, default=180, help="OP exclusion seconds (default: 180)")
    ap.add_argument("--ed", type=int, default=180, help="ED exclusion seconds (default: 180)")
    ap.add_argument("--jpg-q", type=int, default=2, help="JPEG quality (lower is better, default: 2)")
    args = ap.parse_args()

    session_dir = Path(args.session_dir)
    proxy = session_dir / "proxy_360.mp4"
    if not proxy.exists():
        raise RuntimeError(f"proxy_360.mp4 not found: {proxy}")

    logs_dir = session_dir / "logs"
    frames_root = session_dir / "frames_360"
    ensure_dir(logs_dir)
    ensure_dir(frames_root)

    duration = ffprobe_duration_sec(proxy)

    # 1) showinfo fps=1 on left-bottom view (y2)
    showinfo_log = logs_dir / "showinfo_fps1.log"
    # Crop left-bottom: w=iw/2 h=ih/2 x=0 y=ih/2
    vf = "crop=iw/2:ih/2:0:ih/2,fps=1,showinfo"
    cmd = [
        "ffmpeg", "-y",
        "-i", str(proxy),
        "-an",
        "-vf", vf,
        "-f", "null",
        "-"
    ]
    p = run_cmd(cmd, check=False)
    # showinfo is on stderr usually
    showinfo_log.write_text(p.stderr, encoding="utf-8", errors="ignore")

    # 2) parse meanY per sec
    mean_rows = parse_showinfo_meanY_per_sec(showinfo_log)
    mean_csv = logs_dir / "meanY_sec.csv"
    with mean_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["sec", "meanY"])
        for sec, y in mean_rows:
            w.writerow([sec, y])

    # 3) deltaY
    delta_rows = compute_delta(mean_rows)
    delta_csv = logs_dir / "deltaY_sec.csv"
    with delta_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["sec", "meanY", "deltaY"])
        for sec, y, dy in delta_rows:
            w.writerow([sec, y, f"{dy:.1f}"])

    # 4) hits_4sec
    hits = detect_hits_4sec(delta_rows, args.dy_th, args.min_len)
    hits_txt = logs_dir / "hits_4sec.txt"
    with hits_txt.open("w", encoding="utf-8") as f:
        for s, e in hits:
            f.write(f"{s} {e}\n")

    # 5) segments_15s
    segs = hits_to_segments_15s(hits, args.pre_sec, args.seg_len, duration)
    segs_txt = logs_dir / "segments_15s.txt"
    with segs_txt.open("w", encoding="utf-8") as f:
        for s, e in segs:
            f.write(f"{s} {e}\n")

    # 6) merge overlaps -> events_merged
    merged = merge_overlaps(segs)
    merged_txt = logs_dir / "events_merged.txt"
    with merged_txt.open("w", encoding="utf-8") as f:
        for s, e in merged:
            f.write(f"{s} {e}\n")

    # 7) OP/ED exclusion -> events_no_oped
    no_oped = apply_op_ed_exclusion(merged, duration, args.op, args.ed)

    # 8) cap 60s & 60s interval
    final_events = cap_and_interval(no_oped, args.max_event, args.gap)

    no_oped_txt = logs_dir / "events_no_oped.txt"
    with no_oped_txt.open("w", encoding="utf-8") as f:
        for s, e in final_events:
            f.write(f"{s} {e}\n")

    # 9) extract frames for each event (skip if already exists)
    frame_log = logs_dir / "frames_extract.nohup.log"
    with frame_log.open("a", encoding="utf-8") as lf:
        for s, e in final_events:
            name = event_name(s, e)
            out_dir = frames_root / name
            if out_dir.exists() and any(out_dir.glob("*.jpg")):
                lf.write(f"[SKIP] frames exist: {name}\n")
                continue
            lf.write(f"[EXTRACT] {name}\n")
            extract_frames(proxy, out_dir, s, e, jpg_q=args.jpg_q)
            lf.write(f"[OK] {name}\n")

    (logs_dir / ".analyze_done").write_text("ok\n", encoding="utf-8")

    print(f"[OK] analyzed: {session_dir}")
    print(
        f"     duration={duration:.1f}s "
        f"events={len(final_events)} "
        f"dy_th={args.dy_th} min_len={args.min_len}"
    )


if __name__ == "__main__":
    main()

