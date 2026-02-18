#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
publish_yolo: yolo_queue.jsonl -> render(mp4 w/ telop) -> add BGM -> enqueue to main posting/queue.jsonl

重要：
- decision_post.json の start_sec_rel/end_sec_rel は「イベント内の相対秒」
- event 名 "SSSSS_EEEEE" の SSSSS が「セッション開始からの絶対秒（イベント先頭）」
- raw.mkv への -ss/-to は「絶対秒」に変換してから渡す
"""

import argparse
import json
import re
import subprocess
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional

CTRL_BASE = Path("/media/sf_REC")
POSTING = CTRL_BASE / "posting"
YOLO_Q = POSTING / "yolo_queue.jsonl"
MAIN_Q = POSTING / "queue.jsonl"

DEFAULT_BGM = CTRL_BASE / "bgm" / "bgm_V1.mp3"

# =========================
# テロップ（既存 publish と完全同一）
# =========================
TEL1 = "AI自動切り抜きショート"
TEL2 = "詳しくは説明欄へ"
FS1, FS2 = 58, 30
Y1, Y2 = 100, 160
AL1, AL2 = 0.45, 0.38
FONTFILE = "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"
OUT_W, OUT_H = 720, 1280

JST = timezone(timedelta(hours=9))


def safe_mkdir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def read_first_jsonl_and_dequeue(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines:
        return None
    first = lines[0].strip()
    rest = lines[1:]
    tmp = path.with_suffix(".tmp")
    tmp.write_text("\n".join(rest) + ("\n" if rest else ""), encoding="utf-8")
    tmp.replace(path)
    if not first:
        return None
    return json.loads(first)


def append_jsonl(path: Path, obj: dict):
    safe_mkdir(path.parent)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def rfc3339_jst(dt: datetime) -> str:
    return dt.astimezone(JST).isoformat(timespec="seconds")


def ffprobe_wh(path: Path) -> tuple[int, int]:
    cmd = [
        "ffprobe", "-v", "error", "-select_streams", "v:0",
        "-show_entries", "stream=width,height", "-of", "json", str(path)
    ]
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    obj = json.loads(p.stdout)
    st = obj["streams"][0]
    return int(st["width"]), int(st["height"])


def run(cmd: list[str]) -> int:
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    print(p.stdout.rstrip())
    return p.returncode


def escape_drawtext(s: str) -> str:
    return s.replace("\\", "\\\\").replace(":", "\\:").replace("'", "\\'")


def parse_event_start_abs(event: str) -> int:
    """
    event="12007_12022" の左側 12007 をイベント開始の絶対秒として扱う
    """
    m = re.match(r"^(\d+)_\d+$", event)
    if not m:
        raise ValueError(f"invalid event format: {event}")
    return int(m.group(1))


def build_vf(crop_w: int, raw_h: int, crop_x: int, no_telop: bool,
             arrow_on: bool, arrow_text: str, arrow_x: str, arrow_y: str, dur: float) -> str:
    parts = []
    parts.append(f"crop={crop_w}:{raw_h}:{crop_x}:0")
    parts.append(f"scale={OUT_W}:{OUT_H}")

    if not no_telop:
        t1 = escape_drawtext(TEL1)
        t2 = escape_drawtext(TEL2)
        parts.append(
            "drawtext=text='{t}':fontcolor=white@{a}:fontsize={fs}:x=(w-text_w)/2:y={y}:fontfile='{ff}':enable='gte(t,1.5)'".format(
                t=t1, a=AL1, fs=FS1, y=Y1, ff=FONTFILE
            )
        )
        parts.append(
            "drawtext=text='{t}':fontcolor=white@{a}:fontsize={fs}:x=(w-text_w)/2:y={y}:fontfile='{ff}':enable='gte(t,1.5)'".format(
                t=t2, a=AL2, fs=FS2, y=Y2, ff=FONTFILE
            )
        )

    # ↓（最終2秒）※仕込み。デフォルトOFF
    if arrow_on:
        start_t = max(0.0, dur - 2.0)
        at = escape_drawtext(arrow_text)
        parts.append(
            "drawtext=text='{t}':fontcolor=white@{a}:fontsize={fs}:x={x}:y={y}:fontfile='{ff}':enable='gte(t,{st})'".format(
                t=at,
                a=0.38,
                fs=80,
                x=arrow_x,
                y=arrow_y,
                ff=FONTFILE,
                st=f"{start_t:.3f}",
            )
        )

    return ",".join(parts)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max", type=int, default=1, help="process N items per run (default 1)")
    ap.add_argument("--mov-base", default="/media/sf_masaos_mov", help="materials base (safety)")

    # BGM
    ap.add_argument("--bgm", default=str(DEFAULT_BGM), help="bgm mp3 path")
    ap.add_argument("--bgm-delay", type=float, default=0.3, help="bgm delay seconds")
    ap.add_argument("--bgm-fade", type=float, default=0.6, help="bgm fade-in seconds")
    ap.add_argument("--bgm-vol", type=float, default=0.16, help="bgm volume")

    # publishAt
    ap.add_argument("--start", default=None, help="publishAt base (JST) e.g. 2026-02-03T20:00:00+09:00")
    ap.add_argument("--pitch-min", type=int, default=240, help="minutes between uploads")

    # テロップ/矢印
    ap.add_argument("--no-telop", action="store_true", help="disable 2-line telop (default: ON)")
    ap.add_argument("--arrow", action="store_true", help="enable arrow in last 2 sec (default: OFF)")
    ap.add_argument("--arrow-text", default="↓", help="arrow text")
    ap.add_argument("--arrow-x", default="(w-text_w)/2", help="arrow x (ffmpeg expr or number)")
    ap.add_argument("--arrow-y", default="h*0.78", help="arrow y (ffmpeg expr or number)")

    ap.add_argument("--dry-run", action="store_true", help="do not write main queue / do not render")
    args = ap.parse_args()

    mov_base = Path(args.mov_base)
    bgm = Path(args.bgm)
    if not bgm.exists():
        print(f"[WARN] bgm not found: {bgm}")

    if args.start:
        publish_at = datetime.fromisoformat(args.start)
    else:
        publish_at = datetime.now(JST) + timedelta(minutes=10)

    for _ in range(args.max):
        item = read_first_jsonl_and_dequeue(YOLO_Q)
        if item is None:
            print("[EMPTY] yolo_queue is empty")
            return

        session_dir = Path(item["session_dir"])
        event = item["event"]
        event_dir = Path(item["event_dir"])
        decision_post = Path(item["decision_post_path"])

        # 保険：/media/sf_REC/<SESSION> 混入 → mov_base へ寄せる
        if not session_dir.exists():
            cand = mov_base / session_dir.name
            if cand.exists():
                session_dir = cand
        if not event_dir.exists():
            cand = session_dir / "events" / event
            if cand.exists():
                event_dir = cand

        yolo_v1 = event_dir / "yolo" / "v1"
        published_flag = yolo_v1 / ".yolo_published"
        shorts_dir = yolo_v1 / "shorts"
        safe_mkdir(shorts_dir)

        if not decision_post.exists():
            print(f"[SKIP] decision_post missing: {decision_post}")
            continue

        dp = json.loads(decision_post.read_text(encoding="utf-8"))
        start_rel = float(dp["start_sec_rel"])
        end_rel = float(dp["end_sec_rel"])
        dur = max(0.01, end_rel - start_rel)

        # ★ここが修正点：相対→絶対
        event_start_abs = parse_event_start_abs(event)
        start_abs = event_start_abs + start_rel
        end_abs = event_start_abs + end_rel

        crop_x_360 = float(dp["crop_x"])
        crop_w_360 = float(dp.get("crop_w", 202.5))

        raw = session_dir / "raw.mkv"
        if not raw.exists():
            print(f"[SKIP] raw not found: {raw}")
            continue

        raw_w, raw_h = ffprobe_wh(raw)
        scale = raw_w / 640.0
        crop_x = int(round(crop_x_360 * scale))
        crop_w = int(round(crop_w_360 * scale))

        if crop_x < 0:
            crop_x = 0
        if crop_x + crop_w > raw_w:
            crop_x = max(0, raw_w - crop_w)

        out_video = shorts_dir / "short_yolo_v1.mp4"

        vf = build_vf(
            crop_w=crop_w,
            raw_h=raw_h,
            crop_x=crop_x,
            no_telop=args.no_telop,
            arrow_on=args.arrow,
            arrow_text=args.arrow_text,
            arrow_x=args.arrow_x,
            arrow_y=args.arrow_y,
            dur=dur,
        )

        # ★-ss/-to は絶対秒で入れる
        cmd = [
            "ffmpeg", "-y",
            "-ss", str(start_abs), "-to", str(end_abs), "-i", str(raw),
            "-i", str(bgm),
            "-vf", vf,
            "-an",
            "-filter_complex",
            (
                f"[1:a]adelay={int(args.bgm_delay*1000)}|{int(args.bgm_delay*1000)},"
                f"afade=t=in:st={args.bgm_delay}:d={args.bgm_fade},"
                f"volume={args.bgm_vol}[a]"
            ),
            "-map", "0:v:0",
            "-map", "[a]",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
            "-c:a", "aac", "-b:a", "128k",
            "-shortest",
            str(out_video)
        ]

        print("=" * 60)
        print(f"[ITEM] {session_dir.name} / {event}")
        print(f"[RAW] {raw}")
        print(f"[TIME] event_start_abs={event_start_abs} start_rel={start_rel} end_rel={end_rel} -> start_abs={start_abs} end_abs={end_abs}")
        print(f"[CROP] raw_w={raw_w} scale={scale:.3f} crop_x={crop_x} crop_w={crop_w}")
        print(f"[VF] {vf}")
        print(f"[OUT] {out_video}")

        if args.dry_run:
            print("[DRY] skip render + enqueue")
            publish_at = publish_at + timedelta(minutes=args.pitch_min)
            continue

        rc = run(cmd)
        if rc != 0 or not out_video.exists():
            print("[WARN] render failed -> not enqueued")
            continue

        append_jsonl(MAIN_Q, {
            "video_path": str(out_video),
            "decision_path": str(decision_post),
            "published_flag_path": str(published_flag),
            "publishAt": rfc3339_jst(publish_at),
            "route": "yolo"
        })
        print(f"[PUBLISH] enqueued main queue: publishAt={rfc3339_jst(publish_at)}")

        publish_at = publish_at + timedelta(minutes=args.pitch_min)


if __name__ == "__main__":
    main()
