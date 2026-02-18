#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
run_event_queue_pipeline_yolo_v2.py

【完全版パイプライン】
1. event_queue_yolo.jsonl からイベントを読み込む
2. raw_yolo.jsonl を解析し、まさおの中心座標(cx)から最適なクロップ位置を決定
3. 指定位置で ffmpeg を使い、API用のクロップ済み JPEG（1fps）を書き出し
4. クロップ済み画像を GPT API に渡し、タイトルと説明文を生成
5. 動画本編をレンダリング（テロップ・BGM合成）
6. queue_yolo.jsonl に追記して完了
"""

import argparse
import json
import os
import re
import subprocess
import time
import statistics
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple

# ====== 設定項目 ======
BGM_PATH = "/media/sf_REC/bgm/bgm_V1.mp3"
FONTFILE = "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"
OUT_W, OUT_H = 720, 1280
API_SCRIPT = "/media/sf_REC/scripts/core/api_decision_pipeline.py"

# テロップ設定
TEL1, TEL2 = "AI自動切り抜きショート", "詳しくは説明欄へ"
TEL3 = "チャンネル登録してね！\n見たいと思った時はライブで\nリアルなまさおが見れるかも"

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def run_cmd(cmd: List[str], timeout: int = 900) -> bool:
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if res.returncode != 0:
            log(f"CMD_ERROR: {' '.join(cmd)}\n{res.stderr}")
            return False
        return True
    except Exception as e:
        log(f"EXCEPTION: {e}")
        return False

def get_median_cx(raw_yolo_path: Path, start_abs: int, end_abs: int) -> float:
    """YOLOログから指定区間の cx 中央値を算出"""
    cxs = []
    if not raw_yolo_path.exists():
        return 320.0 # fallback
    with raw_yolo_path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
                sec = int(obj.get("sec", -1))
                if start_abs <= sec <= end_abs:
                    bb = obj.get("bbox_xyxy")
                    if bb and len(bb) == 4:
                        cx = (bb[0] + bb[2]) / 2.0
                        cxs.append(cx)
            except: continue
    return statistics.median(cxs) if cxs else 320.0

def calculate_crop_x(cx: float, source_w: int = 640) -> int:
    """cx を中心に据える crop_x (x1) を計算"""
    # ターゲット幅は計算上 360 (640*9/16は360)
    target_w_in_360 = 202 # 360*9/16=202.5
    crop_x = int(cx - (target_w_in_360 / 2))
    # クランプ (640x360想定)
    return max(0, min(crop_x, 640 - int(target_w_in_360)))

def export_cropped_previews(raw_path: Path, out_dir: Path, start_abs: int, dur: int, crop_x_360: int):
    """API用に1fpsでクロップ済み画像を書き出し"""
    if out_dir.exists():
        import shutil
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    # 360p素材(=crop_x_360) を 720p相当にスケールしてクロップ
    # ih*9/16 がスマホ幅。360*9/16 = 202.5
    # crop_x_360 * 3 = 1080p の時の位置
    cmd = [
        "ffmpeg", "-y", "-ss", str(start_abs), "-t", str(dur),
        "-i", str(raw_path),
        "-vf", f"fps=1,crop=ih*9/16:ih:{crop_x_360*3}:0,scale=225:400",
        str(out_dir / "frame_%03d.jpg")
    ]
    run_cmd(cmd)

def call_api_for_content(event_dir: Path, frames_dir: Path):
    """APIスクリプトを呼び出して内容を確定させる"""
    cmd = [
        "python3", API_SCRIPT,
        "--event-dir", str(event_dir),
        "--frames-dir", str(frames_dir),
        "--api2-prompt-file", "/media/sf_REC/prompts/api2_system_yolo.txt",
        "--step", "2"
    ]
    log(f"  Calling API with frames from {frames_dir.name}...")
    run_cmd(cmd)

def render_video(raw_path: Path, out_path: Path, start_abs: int, dur: int, crop_x_360: int):
    """BGM・テロップ入りの最終動画生成"""
    crop_x = crop_x_360 * 3 # 1080p相当
    tel1 = TEL1.replace("\\", "\\\\").replace("'", "\\'")
    tel2 = TEL2.replace("\\", "\\\\").replace("'", "\\'")
    tel3_file = out_path.with_suffix(".tel3.txt")
    tel3_file.parent.mkdir(parents=True, exist_ok=True)
    tel3_file.write_text(TEL3, encoding="utf-8")
    tel3_file_ff = str(tel3_file).replace("\\", "/").replace(":", "\\:").replace("'", "\\'")
    cmd = [
        "ffmpeg", "-y", "-hide_banner",
        "-ss", str(start_abs), "-t", str(dur), "-i", str(raw_path),
        "-stream_loop", "-1", "-i", BGM_PATH,
        "-vf",
        f"crop=ih*9/16:ih:{crop_x}:0,scale={OUT_W}:{OUT_H},"
        f"drawtext=text='{tel1}':fontsize=54:fontcolor=white@0.45:x=(w-text_w)/2:y=180:fontfile='{FONTFILE}',"
        f"drawtext=text='{tel2}':fontsize=36:fontcolor=white@0.38:x=(w-text_w)/2:y=260:fontfile='{FONTFILE}',"
        f"drawtext=textfile='{tel3_file_ff}':fontsize=42:fontcolor=white@1.0:borderw=4:bordercolor=black@0.9:shadowx=2:shadowy=2:shadowcolor=black@0.8:x=(w-text_w)/2:y=h-380:fontfile='{FONTFILE}':alpha='if(lt(t,16),0,min(0.85,(t-16)*0.42))'",
        "-map", "0:v:0", "-map", "1:a:0",
        "-c:v", "libx264", "-crf", "20", "-preset", "veryfast", "-pix_fmt", "yuv420p",
        "-af", "afade=t=in:st=1:d=1,volume=0.16", "-c:a", "aac", "-b:a", "128k", "-shortest",
        str(out_path)
    ]
    ok = run_cmd(cmd, timeout=1200)
    try:
        tel3_file.unlink(missing_ok=True)
    except Exception:
        pass
    return ok

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--event-queue", default="/media/sf_REC/posting/event_queue_yolo.jsonl")
    ap.add_argument("--upload-queue", default="/media/sf_REC/posting/queue_yolo.jsonl")
    ap.add_argument("--event-dir", type=str, help="Manually process a single event directory")
    ap.add_argument("--max", type=int, default=5)
    ap.add_argument("--no-api", action="store_true", help="Skip API call and use existing decision.json")
    ap.add_argument("--force", action="store_true", help="Overwrite existing mp4")
    args = ap.parse_args()

    # 個別指定モードの判定
    if args.event_dir:
        ev_dir = Path(args.event_dir)
        if not ev_dir.exists():
            log(f"Event dir not found: {ev_dir}")
            return
        # 親パスからセッションディレクトリを推測（.../session/events/event_name）
        sess_dir = ev_dir.parent.parent
        ev_name = ev_dir.name
        work_items = [{
            "event_dir": str(ev_dir),
            "session_dir": str(sess_dir),
            "event_name": ev_name,
            "publishAt": (datetime.now() + timedelta(hours=1)).isoformat()
        }]
        queue_mode = False
    else:
        q_path = Path(args.event_queue)
        if not q_path.exists():
            log(f"Queue not found: {q_path}")
            return
        with q_path.open("r", encoding="utf-8") as f:
            lines = [l.strip() for l in f if l.strip()]
        if not lines:
            log("Queue is empty.")
            return
        work_items = [json.loads(l) for l in lines[:args.max]]
        rem_lines = lines[args.max:]
        queue_mode = True

    for item in work_items:
        try:
            ev_dir = Path(item["event_dir"])
            sess_dir = Path(item["session_dir"])
            ev_name = item["event_name"]
            
            log(f"Processing {ev_name}...")
            
            # 1. クロップ位置決定
            start_abs = int(ev_name.split("_")[0])
            raw_yolo = sess_dir / "raw_yolo.jsonl"
            med_cx = get_median_cx(raw_yolo, start_abs, start_abs + 20)
            crop_x_360 = calculate_crop_x(med_cx)
            log(f"  median_cx={med_cx:.1f} -> crop_x_360={crop_x_360}")
            
            # 2. 画像書き出し & API
            raw_mkv = sess_dir / "raw.mkv"
            api_frames_dir = ev_dir / "images_cropped" 
            
            if not args.no_api:
                export_cropped_previews(raw_mkv, api_frames_dir, start_abs, 20, crop_x_360)
                # API呼出
                call_api_for_content(ev_dir, api_frames_dir)
            else:
                log("  Skipping API step (--no-api)")

            # 3. 本編生成
            v_dir = ev_dir / "api" / "v1" 
            decision_json = v_dir / "decision.json"
            if not decision_json.exists():
                log(f"  [ERROR] decision.json not found in {v_dir}. Cannot skip API.")
                continue

            out_mp4 = ev_dir / "shorts" / f"{ev_name}_v1_bgm_V1.mp4"
            if out_mp4.exists() and not args.force:
                log(f"  [SKIP] mp4 already exists: {out_mp4}")
                # まだキューに追加されていないなら追加するなどの処理が必要ならここに書く
                continue

            out_mp4.parent.mkdir(exist_ok=True)
            
            if render_video(raw_mkv, out_mp4, start_abs, 20, crop_x_360):
                # 4. アップロードキュー追加
                row = {
                    "video_path": str(out_mp4),
                    "decision_path": str(decision_json),
                    "published_flag_path": str(v_dir / ".published"),
                    "publishAt": item["publishAt"],
                    "route": item.get("route", "yolo")
                }
                with open(args.upload_queue, "a", encoding="utf-8") as f:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
                log(f"  OK: {ev_name} -> {out_mp4}")
            else:
                log(f"  [ERROR] Rendering failed for {ev_name}")

        except Exception as e:
            log(f"  [ERROR] Failed to process line: {e}")

    # キュー更新（個別指定モードでない場合のみ）
    if queue_mode:
        with q_path.open("w", encoding="utf-8") as f:
            for l in rem_lines: f.write(l + "\n")
        log(f"Queue updated. (rem={len(rem_lines)})")
    else:
        log("Manual processing finished.")

if __name__ == "__main__":
    main()



