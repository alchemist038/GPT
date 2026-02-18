#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# YOLOで作った decision.json (start/end/crop_x) を使い:
# 1) 指定区間のフレームを 9:16 で固定クロップしてJPEG出力（crop-only）
# 2) クロップ済み静止画を API2(OpenAI) に投げて文章生成（--api2）
#
# 出力（events/<EVENT>/yolo/v1/ 配下）:
#   crops_9x16/         …クロップ済み画像
#   api2/v1/            …API2 request/response/description.txt
#   .crop_done / .api2_done フラグ
#
# ※ rabbit無し問題はここでは関係なし（decision.jsonは既に決定済み前提）

import os
import json
import base64
from pathlib import Path
from typing import List, Tuple, Optional  # ★必須（List未定義エラー対策）
from PIL import Image

DEFAULT_API2_SYSTEM = "/media/sf_REC/prompts/api2_system.txt"  # 既存運用に合わせる（無ければ --api2-system で指定）


def safe_mkdir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def read_text_if_exists(p: Path) -> str:
    if p.exists():
        return p.read_text(encoding="utf-8")
    return ""


def load_frames(frames_dir: Path, max_frames: int = 60) -> List[Path]:
    imgs = sorted(frames_dir.glob("*.jpg"))
    return imgs[:max_frames]


def crop_frames_9x16(
    imgs: List[Path],
    out_dir: Path,
    start: int,
    end_exclusive: int,
    crop_x: int,
    crop_w: float,
    resize_to: Optional[Tuple[int, int]] = (360, 640)  # 見やすい縦に整形（不要なら None）
) -> List[Path]:
    safe_mkdir(out_dir)
    selected = imgs[start:end_exclusive]
    if not selected:
        raise SystemExit("[ERR] selected frames is empty (start/end wrong?)")

    out_paths = []
    # 元サイズは基本 640x360 を想定（ただし決め打ちせず読み取る）
    for i, p in enumerate(selected, start=1):
        with Image.open(p) as im:
            W, H = im.size
            w = int(round(crop_w))
            x0 = max(0, min(int(crop_x), W - w))
            box = (x0, 0, x0 + w, H)
            crop = im.crop(box)

            if resize_to is not None:
                crop = crop.resize(resize_to, Image.LANCZOS)

            outp = out_dir / f"{i:03d}.jpg"
            crop.save(outp, quality=92)
            out_paths.append(outp)

    return out_paths


def encode_images_base64(paths: List[Path]) -> List[str]:
    b64s = []
    for p in paths:
        data = p.read_bytes()
        b64s.append(base64.b64encode(data).decode("ascii"))
    return b64s


def call_api2_openai(
    system_text: str,
    image_paths: List[Path],
    model: str,
    max_output_tokens: int = 220
) -> str:
    """
    OpenAI Responses API で vision 入力（画像）＋テキスト生成。
    429(TPM) は数秒待って自動リトライする。
    openai ライブラリが無い場合は例外で止める。
    """
    try:
        from openai import OpenAI
        from openai import RateLimitError
    except Exception as e:
        raise SystemExit(
            f"[ERR] openai python package not found. install with: pip3 install --user openai\n{e}"
        )

    import time
    import re

    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("[ERR] OPENAI_API_KEY is not set")

    client = OpenAI(api_key=api_key)

    # 画像を data URL として渡す（jpeg）
    content = []
    content.append({
        "type": "input_text",
        "text": "以下はクロップ後（9:16）の連続静止画です。この世界だけを根拠に文章を書いてください。"
    })
    for p in image_paths:
        b64 = base64.b64encode(p.read_bytes()).decode("ascii")
        content.append({
            "type": "input_image",
            "image_url": f"data:image/jpeg;base64,{b64}"
        })

    last_err = None
    resp = None

    # ★TPM 429 対策（数秒待てば通る系を吸収）
    for attempt in range(1, 6):  # 最大5回
        try:
            resp = client.responses.create(
                model=model,
                input=[
                    {"role": "system", "content": system_text},
                    {"role": "user", "content": content},
                ],
                max_output_tokens=max_output_tokens,
            )
            last_err = None
            break

        except RateLimitError as e:
            last_err = e
            msg = str(e)
            m = re.search(r"try again in ([0-9.]+)s", msg, re.IGNORECASE)
            wait_s = float(m.group(1)) + 0.5 if m else (2.0 + attempt)
            print(f"[RATE_LIMIT] attempt={attempt}/5 sleep={wait_s:.3f}s")
            time.sleep(wait_s)

    if last_err is not None:
        raise last_err

    # textを結合
    out_text = ""
    for item in resp.output:
        if item.type == "message":
            for c in item.content:
                if c.type == "output_text":
                    out_text += c.text

    return out_text.strip()


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--session-dir", required=True, help=".../YYYY-MM-DD_HH-MM-SS")
    ap.add_argument("--event", required=True, help="EVENT name like 32954_32980")
    ap.add_argument("--max-frames", type=int, default=60)

    # YOLO decision source
    ap.add_argument("--yolo-ver", default="v1", help="yolo version dir (default v1)")
    ap.add_argument("--crop-w", type=float, default=202.5, help="analysis crop width for 640x360")
    ap.add_argument("--no-resize", action="store_true", help="do not resize cropped images (keep raw crop size)")

    # mode flags
    ap.add_argument("--crop-only", action="store_true", help="only crop frames, do not call API2")
    ap.add_argument("--api2", action="store_true", help="call API2 after cropping")

    # API2 options
    ap.add_argument("--api2-system", default=DEFAULT_API2_SYSTEM, help="path to api2 system prompt txt")
    ap.add_argument("--api2-model", default="gpt-4o-mini", help="OpenAI model for API2 text generation")
    ap.add_argument("--api2-max-tokens", type=int, default=220)

    args = ap.parse_args()

    session_dir = Path(args.session_dir)
    frames_dir = session_dir / "frames_360" / args.event
    event_dir = session_dir / "events" / args.event

    yolo_dir = event_dir / "yolo" / args.yolo_ver
    yolo_decision = yolo_dir / "decision.json"

    if not frames_dir.exists():
        raise SystemExit(f"[ERR] frames_dir not found: {frames_dir}")
    if not yolo_decision.exists():
        raise SystemExit(f"[ERR] yolo decision.json not found: {yolo_decision}")

    imgs = load_frames(frames_dir, max_frames=args.max_frames)
    if len(imgs) == 0:
        raise SystemExit("[ERR] no jpg frames")

    dec = json.loads(yolo_decision.read_text(encoding="utf-8"))
    start = int(dec["start_sec_rel"])
    end_excl = int(dec["end_sec_rel"])
    crop_x = int(dec["crop_x"])
    crop_w = float(dec.get("crop_w", args.crop_w))

    # outputs
    crops_dir = yolo_dir / "crops_9x16"
    crop_done = yolo_dir / ".crop_done"

    resize_to = None if args.no_resize else (360, 640)

    cropped = crop_frames_9x16(
        imgs, crops_dir, start, end_excl, crop_x, crop_w, resize_to=resize_to
    )
    crop_done.write_text("", encoding="utf-8")
    print("[OK] crop wrote:", crops_dir)

    if args.crop_only and (not args.api2):
        return

    if args.api2:
        api2_dir = yolo_dir / "api2" / "v1"
        safe_mkdir(api2_dir)
        api2_done = api2_dir / ".api2_done"

        system_path = Path(args.api2_system)
        system_text = read_text_if_exists(system_path)
        if not system_text.strip():
            raise SystemExit(f"[ERR] api2 system prompt is empty or missing: {system_path}")

        # request保存（監査用）
        req_obj = {
            "model": args.api2_model,
            "system_prompt_path": str(system_path),
            "num_images": len(cropped),
            "images": [p.name for p in cropped],
            "note": "images are cropped 9:16 and represent the only world API2 may refer to."
        }
        (api2_dir / "request.json").write_text(
            json.dumps(req_obj, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

        out_text = call_api2_openai(
            system_text=system_text,
            image_paths=cropped,
            model=args.api2_model,
            max_output_tokens=args.api2_max_tokens
        )

        # response保存（文章だけ）
        (api2_dir / "description.txt").write_text(out_text + "\n", encoding="utf-8")
        api2_done.write_text("", encoding="utf-8")
        print("[OK] api2 wrote:", api2_dir)


if __name__ == "__main__":
    main()
