#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
render_latest_from_decision.pyovs

目的:
- events/<EVENT>/api/vN/decision.json の "最新V" を読む
- start/end は「相対秒」(event内) を「絶対秒」(録画開始から) に変換して ffmpeg に渡す
- crop_x は 解析基準 → RAW基準へ倍率補正（デフォルト x3）
- shorts/short_vN.mp4 を生成
- YouTube説明欄をスクショ形式（太郎→日時1行→五郎→タグ）で組み立てて shorts/desc_vN.txt に保存
- タイトルは shorts/title_vN.txt に保存

前提（あなたの確定ルール）:
- 読むのは "V最新" の decision.json
- 絶対秒 = (イベントフォルダ名先頭) + (decisionの相対秒)
- crop_x_raw = crop_x * 3
- 親フォルダ名（YYYY-MM-DD_HH-MM-SS）は「日時表示」のみで使用
"""

import argparse
import json
import os
import re
import subprocess
from pathlib import Path
from datetime import datetime, timedelta

# --- 固定テロップ（恒常仕様） ---
TEL1 = "AI自動切り抜きショート"
TEL2 = "詳しくは説明欄へ"
FS1 = 68
FS2 = 40
Y1  = 40
Y2  = 120
AL1 = 0.45
AL2 = 0.38

# --- 五郎固定テンプレ（恒常仕様） ---
GORO_TEXT = (
    "プロジェクトメンバーのGPT五郎です。\n"
    "まさおのライブ配信を低解像度の映像で見守りつつ、フレーム単位の動きの変化から9〜15秒の区間を自動で拾ってショートにしています。\n"
    "まだ実験段階なので、「ここ好き」や「見やすさ」などコメントで教えてもらえると助かります。"
)

HASHTAGS = "#まさお #AI切り抜き #ショート動画 #自動編集 #n8n #FFmpeg #shorts"


def die(msg: str, code: int = 1):
    print(f"[ERROR] {msg}")
    raise SystemExit(code)


def find_latest_v(api_dir: Path) -> tuple[int, Path]:
    """
    api/vN/decision.json の N 最大を選ぶ
    """
    if not api_dir.is_dir():
        die(f"api dir not found: {api_dir}")

    best_n = None
    best_path = None
    for p in api_dir.glob("v*/decision.json"):
        m = re.match(r"^v(\d+)$", p.parent.name)
        if not m:
            continue
        n = int(m.group(1))
        if best_n is None or n > best_n:
            best_n = n
            best_path = p

    if best_n is None or best_path is None:
        die(f"no decision.json under: {api_dir}")

    return best_n, best_path


def parse_event_start(event_name: str) -> int:
    """
    EVENT名: 02371_02386 -> 2371
    """
    m = re.match(r"^(\d+)_\d+$", event_name)
    if not m:
        die(f"invalid event_name: {event_name} (expected SSSSS_EEEEE)")
    return int(m.group(1))


def parse_session_start(session_dir: Path) -> datetime:
    """
    親フォルダ名: YYYY-MM-DD_HH-MM-SS
    """
    name = session_dir.name
    try:
        return datetime.strptime(name, "%Y-%m-%d_%H-%M-%S")
    except Exception:
        die(f"invalid session dir name: {name} (expected YYYY-MM-DD_HH-MM-SS)")


def ffprobe_size(video_path: Path) -> tuple[int, int]:
    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=width,height",
        "-of", "json",
        str(video_path),
    ]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        die(f"ffprobe failed: {r.stderr.strip()}")
    j = json.loads(r.stdout)
    st = j.get("streams", [])
    if not st:
        die("ffprobe: no video stream")
    w = int(st[0]["width"])
    h = int(st[0]["height"])
    return w, h


def escape_drawtext(s: str) -> str:
    # drawtext の文字エスケープ（最小限）
    # ':' はフィルタグラフ区切りなので \: にする
    # "'" は \' にする
    return s.replace("\\", "\\\\").replace(":", "\\:").replace("'", "\\'")


def build_desc(api2_desc: str, when_line: str) -> str:
    return (
        f"{api2_desc}\n\n"
        f"{when_line}\n\n"
        f"{GORO_TEXT}\n\n"
        f"{HASHTAGS}\n"
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--event-dir", required=True, help=".../events/<EVENT> directory")
    ap.add_argument("--crop-mult", type=float, default=3.0, help="crop_x multiplier (default 3.0)")
    ap.add_argument("--overwrite", action="store_true", help="overwrite output mp4")
    ap.add_argument("--no-telop", action="store_true", help="disable drawtext overlays")
    args = ap.parse_args()

    event_dir = Path(args.event_dir).resolve()
    if not event_dir.is_dir():
        die(f"event_dir not found: {event_dir}")

    event_name = event_dir.name
    session_dir = event_dir.parent.parent  # .../<SESSION>/events/<EVENT>
    raw_path = session_dir / "raw.mkv"
    if not raw_path.exists():
        die(f"raw.mkv not found: {raw_path}")

    api_dir = event_dir / "api"
    v, decision_path = find_latest_v(api_dir)

    decision = json.loads(decision_path.read_text(encoding="utf-8"))

    # decision.json 最小仕様（あなたの恒常ルール）
    # start_sec_rel / end_sec_rel / crop_x / title / description
    for k in ("start_sec_rel", "end_sec_rel", "crop_x", "title", "description"):
        if k not in decision:
            die(f"missing key in decision.json: {k} ({decision_path})")

    start_rel = int(decision["start_sec_rel"])
    end_rel   = int(decision["end_sec_rel"])
    crop_x    = int(decision["crop_x"])
    title     = str(decision["title"])
    api2_desc = str(decision["description"])

    dur = end_rel - start_rel
    if not (9 <= dur <= 15):
        die(f"duration invalid: {dur} (must be 9..15) decision={decision_path}")

    event_start = parse_event_start(event_name)

    # --- 絶対秒（録画開始から）---
    abs_start = event_start + start_rel

    # --- crop_x RAW補正（×3 デフォルト）---
    crop_x_raw = int(round(crop_x * args.crop_mult))

    # --- RAWサイズから縦9:16クロップ幅を決める（高さは維持）---
    w, h = ffprobe_size(raw_path)
    crop_w = int(round(h * 9 / 16))
    if crop_w <= 0 or crop_w > w:
        die(f"invalid crop_w: {crop_w} (w={w}, h={h})")

    max_x = max(0, w - crop_w)
    if crop_x_raw < 0:
        crop_x_raw = 0
    if crop_x_raw > max_x:
        crop_x_raw = max_x

    shorts_dir = event_dir / "shorts"
    shorts_dir.mkdir(parents=True, exist_ok=True)

    out_mp4 = shorts_dir / f"short_v{v}.mp4"
    out_title = shorts_dir / f"title_v{v}.txt"
    out_desc  = shorts_dir / f"desc_v{v}.txt"

    if out_mp4.exists() and not args.overwrite:
        print(f"[SKIP] output exists: {out_mp4} (use --overwrite)")
        return

    # --- 日時1行（スクショ形式）---
    # 親フォルダ開始時刻 + (event_start + 中央秒) を分単位に丸め
    session_start = parse_session_start(session_dir)
    center_rel = (start_rel + end_rel) / 2.0
    center_abs = event_start + center_rel
    when_dt = session_start + timedelta(seconds=center_abs)
    # 分に丸め（秒切り捨て）
    when_dt = when_dt.replace(second=0, microsecond=0)
    when_line = f"この動画は、{when_dt.year}年{when_dt.month:02d}月{when_dt.day:02d}日 {when_dt.hour:02d}:{when_dt.minute:02d}頃のライブ配信中の一場面です。"

    # --- 説明欄を保存（太郎文は改変禁止：そのまま）---
    out_title.write_text(title + "\n", encoding="utf-8")
    out_desc.write_text(build_desc(api2_desc, when_line), encoding="utf-8")

    # --- フィルタ作成 ---
    vf_parts = []
    vf_parts.append(f"crop={crop_w}:{h}:{crop_x_raw}:0")
    vf_parts.append("scale=720:1280")

    if not args.no_telop:
        fontfile = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        fontopt = f":fontfile={fontfile}" if Path(fontfile).exists() else ""
        t1 = escape_drawtext(TEL1)
        t2 = escape_drawtext(TEL2)
        vf_parts.append(
            "drawtext=text='{t}':fontcolor=white@{a}:fontsize={fs}:x=(w-text_w)/2:y={y}{fo}".format(
                t=t1, a=AL1, fs=FS1, y=Y1, fo=fontopt
            )
        )
        vf_parts.append(
            "drawtext=text='{t}':fontcolor=white@{a}:fontsize={fs}:x=(w-text_w)/2:y={y}{fo}".format(
                t=t2, a=AL2, fs=FS2, y=Y2, fo=fontopt
            )
        )

    vf = ",".join(vf_parts)

    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "error",
        "-stats",
        "-y" if args.overwrite else "-n",
        "-ss", str(abs_start),
        "-t", str(dur),
        "-i", str(raw_path),
        "-vf", vf,
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        "-movflags", "+faststart",
        str(out_mp4),
    ]

    print("[INFO] event_dir:", event_dir)
    print("[INFO] decision:", decision_path)
    print("[INFO] raw:", raw_path)
    print("[INFO] abs_start:", abs_start, "dur:", dur)
    print("[INFO] crop:", f"w={crop_w} h={h} x={crop_x_raw} (max_x={max_x})")
    print("[INFO] out:", out_mp4)
    print("[INFO] title_saved:", out_title)
    print("[INFO] desc_saved :", out_desc)

    r = subprocess.run(cmd)
    if r.returncode != 0:
        die(f"ffmpeg failed (rc={r.returncode})")

    print("[DONE] rendered:", out_mp4)


if __name__ == "__main__":
    main()
