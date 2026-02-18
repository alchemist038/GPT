#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
run_event_queue_pipeline.py (bridge)

目的：
- event_queue.jsonl を読み、各イベントについて
  (A) API判断（必要なら）→ (B) ffmpeg生成 → (C) upload queue.jsonl 追記
- --no-api で API を完全停止し、既存の最新 decision.json から生成だけ回せる
- decision は必ず「V最新」を使用
- ffmpeg 失敗時は 2トライで .render_skip を付けて次へ（ラインを止めない）
- テロップは固定2行（C）で入れる（日本語フォントを明示）

【追加（BGM構想）】
- 最終成果物（完成品）は「BGM付きのみ」：
  short_vN_bgm_V1.mp4 が存在する → 成功
  short_vN.mp4 だけ存在 → BGM工程が未完了（失敗 or 未実行）
- BGM成功後は short_vN.mp4（中間）を削除（再生成できるため保持しない）
- BGMは固定パス、音量固定（今は定数のまま）
- BGM工程の失敗も render(生成) の失敗として 2トライ→.render_skip に統合（別フラグは作らない）

入力（event_queue 1行JSON）：
- session_dir, event_name, frames_dir, event_dir, publishAt, route

出力（upload queue 1行JSON）：
- video_path, decision_path, published_flag_path, publishAt, route
  ※ video_path は常に BGM版（short_vN_bgm_V1.mp4）

【重要：event_queue の扱い（運用方針）】
- event_queue は「使い捨て」。
- 今回触った行（最大 --max 行）は、成功/失敗/skip に関係なく必ず削除する。
"""

import argparse
import json
import re
import subprocess
import time
from pathlib import Path
from typing import Optional, Tuple

# 固定テロップ（恒常）
TEL1 = "AI自動切り抜きショート"
TEL2 = "詳しくは説明欄へ"
FS1 = 68
FS2 = 40
Y1 = 40
Y2 = 120
AL1 = 0.45
AL2 = 0.38
# ラスト差し替え（YOLOライン用・最終確定）
TEL3 = "チャンネル登録してね\\n見たいと思った時に\\nライブでリアルなまさおが見れるかも"

FS3 = 38
Y3  = 980   # 下部中央（720x1280想定）
AL3 = 0.48  # 下部は少しだけ濃く
SWITCH_LAST_SEC = 2.0
FADE_SEC = 0.3
TEL_START_SEC = 0.0  # ほぼ全編（必要なら 1.0 に）
# 日本語フォント（存在確認済み）
FONTFILE = "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"

# 生成出力の基準サイズ
OUT_W, OUT_H = 720, 1280

# =========================
# BGM（固定）
# =========================
BGM_PATH = "/media/sf_REC/bgm/bgm_V1.mp3"  # 固定
BGM_TAG = "V1"                             # 出力名に使う
BGM_GAIN = 0.16                            # 固定（今は変数化しない）


def log(msg: str):
    print(msg, flush=True)


def safe_mkdir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def touch(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("", encoding="utf-8")


def parse_event_start_abs(event_name: str) -> int:
    # "02371_02386" -> 2371
    m = re.match(r"^(\d+)_\d+$", event_name)
    if not m:
        raise ValueError(f"invalid event_name: {event_name}")
    return int(m.group(1))


def find_latest_decision(api_dir: Path) -> Optional[Tuple[int, Path]]:
    # api/vN/decision.json の N 最大を返す
    best_v = None
    best_p = None
    if not api_dir.is_dir():
        return None
    for p in api_dir.glob("v*/decision.json"):
        m = re.match(r"^v(\d+)$", p.parent.name)
        if not m:
            continue
        v = int(m.group(1))
        if best_v is None or v > best_v:
            best_v, best_p = v, p
    if best_v is None or best_p is None:
        return None
    return best_v, best_p


def ffprobe_size(video_path: Path) -> Tuple[int, int]:
    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=width,height",
        "-of", "json",
        str(video_path),
    ]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"ffprobe failed: {r.stderr.strip()}")
    j = json.loads(r.stdout)
    st = j.get("streams", [])
    if not st:
        raise RuntimeError("ffprobe: no video stream")
    w = int(st[0]["width"])
    h = int(st[0]["height"])
    return w, h


def escape_drawtext(s: str) -> str:
    # drawtext 用エスケープ
    # 改行は \n、: と ' と \ を逃がす
    return (
        s
        .replace("\\", "\\\\")
        .replace("\n", r"\n")
        .replace(":", r"\:")
        .replace("'", r"\'")
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--event-queue", default="/media/sf_REC/posting/event_queue_yolo.jsonl")
    ap.add_argument("--upload-queue", default="/media/sf_REC/posting/queue_yolo.jsonl")
    ap.add_argument("--api-script", default="/media/sf_REC/scripts/core/api_decision_pipeline.py")
    ap.add_argument("--max", type=int, default=10)
    ap.add_argument("--sleep", type=float, default=0.0)
    ap.add_argument("--no-api", action="store_true", help="do not call API; use existing latest decision.json only")
    ap.add_argument("--crop-mult", type=float, default=3.0)
    ap.add_argument("--no-telop", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    event_queue = Path(args.event_queue)
    upload_queue = Path(args.upload_queue)
    api_script = Path(args.api_script)

    if not event_queue.exists():
        log(f"[ERROR] event_queue not found: {event_queue}")
        raise SystemExit(2)

    if not Path(FONTFILE).exists() and not args.no_telop:
        log(f"[ERROR] fontfile missing: {FONTFILE}")
        raise SystemExit(2)

    processed = 0
    enqueued = 0
    skipped = 0

    with event_queue.open("r", encoding="utf-8") as f:
        all_lines = [ln.strip() for ln in f if ln.strip()]

    # 「触ったら削除」：今回触る行（最大 --max 行）を先に確定し、残りは最後に書き戻す
    work_lines = all_lines[: args.max]
    remaining_lines = all_lines[len(work_lines):]

    for ln in work_lines:
        processed += 1
        try:
            item = json.loads(ln)
        except Exception:
            log("[SKIP] invalid json line")
            skipped += 1
            continue

        for k in ("session_dir", "event_name", "frames_dir", "event_dir", "publishAt"):
            if k not in item:
                log(f"[SKIP] missing {k} in event_queue line")
                skipped += 1
                continue

        session_dir = Path(item["session_dir"])
        event_name = str(item["event_name"])
        frames_dir = Path(item["frames_dir"])
        event_dir  = Path(item["event_dir"])
        publishAt  = str(item["publishAt"])
        route      = str(item.get("route", ""))

        log("============================================================")
        log(f"[EVENT] {event_name}")
        log(f"[INFO] session_dir: {session_dir}")
        log(f"[INFO] frames_dir : {frames_dir}")
        log(f"[INFO] event_dir  : {event_dir}")
        log(f"[INFO] publishAt  : {publishAt} route={route}")

        ok = run_api_if_needed(frames_dir, event_dir, args.no_api, api_script)
        if not ok:
            skipped += 1
            continue

        latest = find_latest_decision(event_dir / "api")
        if latest is None:
            log("[SKIP] no decision.json even after API/lookup")
            skipped += 1
            continue

        v, decision_path = latest
        published_flag_path = event_dir / "api" / f"v{v}" / ".published"

        if published_flag_path.exists():
            log("[SKIP] already published (.published exists)")
            skipped += 1
            continue

        if already_enqueued(upload_queue, str(published_flag_path)):
            log("[SKIP] already enqueued (same published_flag_path)")
            skipped += 1
            continue

        out_mp4 = render_with_retry(
            event_name=event_name,
            session_dir=session_dir,
            event_dir=event_dir,
            decision_path=decision_path,
            crop_mult=args.crop_mult,
            no_telop=args.no_telop,
            dry_run=args.dry_run,
        )

        if out_mp4 is None:
            skipped += 1
            continue

        row = {
            "video_path": str(out_mp4),
            "decision_path": str(decision_path),
            "published_flag_path": str(published_flag_path),
            "publishAt": publishAt,
            "route": route,
        }
        append_upload_queue(upload_queue, row, dry_run=args.dry_run)
        enqueued += 1

        if args.sleep > 0:
            time.sleep(args.sleep)

    log("============================================================")
    log(f"[SUMMARY] processed={processed} enqueued={enqueued} skipped={skipped} no_api={args.no_api} dry_run={args.dry_run}")

    # event_queue は使い捨て：今回触った分（work_lines）は削除し、残りだけを書き戻す
    if args.dry_run:
        log(f"[DRY] event_queue unchanged (would remove {len(work_lines)} lines)")
    else:
        tmp = event_queue.with_suffix(event_queue.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as wf:
            for l in remaining_lines:
                wf.write(l + "\n")
        tmp.replace(event_queue)
        log(f"[OK] event_queue dequeued: removed={len(work_lines)} remaining={len(remaining_lines)} -> {event_queue}")


if __name__ == "__main__":
    main()
