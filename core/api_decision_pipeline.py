#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
api_decision_pipeline.py

- 入力：frames_360/<EVENT>/ の JPEG（1秒1枚、最大60枚）
- ローカル動画生成：なし
- API1：crop_x + start_sec + end_sec をJSONで返す（9〜15秒 / 不在なら9秒固定）
- ローカル数値チェック：9〜15秒、end_sec<=枚数 を満たさなければ自動返品（API1へ再要求）
- API2：編集者GPT太郎として title/description を生成
- 保存：events/<EVENT>/api/vN/ に証跡を保存（request/response/decision など）

重要修正（2025-12-xx）：
- cron / 非対話実行でも止まらないように、OPENAI_API_KEY が env にある場合は getpass を絶対に呼ばない
- env に無く、非TTYなら入力待ちせず即エラーで落とす（固まらせない）
"""

import os
import sys
import json
import time
import shutil
import random
import argparse
import subprocess
import datetime
from getpass import getpass

# ---- prompt externalization (safe) ----
from pathlib import Path
PROMPT_DIR = Path("/media/sf_REC/prompts")


def read_text_if_exists(path: Path) -> str | None:
    """Return stripped text if file exists and non-empty, else None.
    Never raises (cron-safe).
    """
    try:
        if path.is_file():
            s = path.read_text(encoding="utf-8").strip()
            return s if s else None
    except Exception:
        pass
    return None

from datetime import datetime
from typing import List, Dict, Any, Tuple


# =========================================================
# CONFIG
# =========================================================
DEFAULT_API1_MODEL = os.environ.get("OPENAI_API1_MODEL", "gpt-4.1-mini")
DEFAULT_API2_MODEL = os.environ.get("OPENAI_API2_MODEL", "gpt-4.1-mini")

API1_SYSTEM = """あなたは動画編集の切り抜き判断を行うAIです。

入力は 640x360 の静止画（1秒1枚、最大60枚）です。
最終的に、この映像は
横 202.5px × 縦 360px（9:16）の領域で切り抜かれます。

あなたの役割は、
この 202.5x360 の領域を置いたときに、
【うさぎ】が最も適切に見える位置を選ぶことです。
【クロップ位置は固定（重要）】
- このイベント（最大60枚）に対して crop_x は 1つだけ決め、全フレームで固定する。
- 1枚だけを見て決めてはいけない。複数フレームを通して総合的に最適な crop_x を選ぶ。

【固定クロップの選び方】
- うさぎが映っているフレームがある場合：
  - できるだけ多くのフレームで「うさぎ中心が枠内に入り、中央に近い」crop_x を選ぶ。
  - 一部フレームで端寄り・切れが起きる crop_x は避ける（安定性優先）。
- 同程度なら「最も多くのフレームでうさぎが大きく見える」crop_x を選ぶ。


- 【うさぎ】 が画面内に見えている場合、「【うさぎ】が 202.5x360 の枠内に入らない crop_x」は禁止。
- 【うさぎ】が入る crop_x の中から、【うさぎ】ができるだけ中央に来る crop_x を選ぶ（最優先）。


- crop_x を決めるときは「枠の中心 = crop_x + 101（202.5/2）」を意識し、【うさぎ】の中心に近づける。
- 迷ったら 0 ではなく、【うさぎ】が最も大きく見える位置を優先する。
【中心位置の定義（数値ルール）】
- うさぎ中心 rabbit_center_x を推定する：
  - 顔（目鼻）が見えるなら顔中心
  - 顔が不明なら胴体の最も大きい塊の中心
  - どちらも不明なら「気配」ルールへ

- 枠の中心は frame_center_x = crop_x + 101 とする。
- 中央合格：abs(frame_center_x - rabbit_center_x) <= 25
- 中央合格が可能な crop_x があるなら、その中で abs(frame_center_x - rabbit_center_x) が最小のものを選ぶ。

【端寄り禁止（うさぎが映っている場合）】
- うさぎ中心が枠の端に近すぎる crop_x は禁止：
  - rabbit_center_x - crop_x < 40 は禁止（左端が近すぎ）
  - (crop_x + 202.5) - rabbit_center_x < 40 は禁止（右端が近すぎ）

【タイブレーク】
- 上記が同点の場合のみ、「うさぎが最も大きく見える位置」を選ぶ。

- うさぎが映っているなら、「うさぎ中心が枠の端から40px未満になるフレームが多い crop_x」は避ける。

出力は JSON のみ。
以下の3つのキーだけを返してください。

- crop_x :
  202.5x360 の切り抜き領域の【左端の X 座標】（整数）
- start_sec :
  切り抜き開始秒（整数）
- end_sec :
  切り抜き終了秒（整数）

ルール：
- 通常は end_sec - start_sec を 9〜15 秒にする
- 万が一、【うさぎ】が画面内に明確に映っていない場合でも discard は禁止
- その場合は、【うさぎ】の「存在・気配・ぬくもり」が感じられる空間を選び、
  切り抜き時間は必ず 9 秒（固定）にする
- 【うさぎ】が映っている場合は、人間より必ず優先する
- 余計な文章、説明、前置きは禁止。JSONのみを出力する
"""

# 旧：API2_SYSTEM を内蔵していたが、外部ファイル優先へ（/media/sf_REC/prompts/api2_system.txt）
API2_SYSTEM_DEFAULT = """あなたは「編集者GPT太郎」です。
入力は API1 により指定された区間の静止画だけです（その外側は存在しないものとして扱う）。
あなたはその静止画だけを見て、ショート動画のタイトルと説明欄を作ります。

出力はJSONのみ。キーは title と description の2つだけ：

description は必ず3文構成：
1文目：名乗り（「編集者GPT太郎です。」）
2文目：戸惑い/愚痴/悩みは1文まで（技術の話は禁止）
3文目：必ず「まさおのぬくもり／気配／存在」に着地
複数案・比較・代替案は禁止。必ず1つだけ。
"""


def get_api2_system_prompt(custom_path: str = None) -> str:
    """Load API2 system prompt.
    1. If custom_path (arg) is provided, use it.
    2. Else, try /media/sf_REC/prompts/api2_system.txt
    Fallback to built-in default. Cron-safe.
    """
    if custom_path:
        p = Path(custom_path)
        external = read_text_if_exists(p)
        if external:
            print(f"[INFO] API2 system prompt: custom file ({p.name})")
            return external
        else:
            print(f"[WARN] Custom prompt not found: {custom_path}. Fallback to default.")

    external = read_text_if_exists(PROMPT_DIR / "api2_system.txt")
    if external:
        print("[INFO] API2 system prompt: external file")
        return external
    print("[INFO] API2 system prompt: default")
    return API2_SYSTEM_DEFAULT

def build_api2_crop_context(crop_x: int) -> str:
    return f"""この映像は、
元の 640x360 映像から
X = {crop_x} を左端として
202.5x360（9:16）の領域で切り抜かれた世界です。

この切り抜き位置は既に確定しています。
このクロップ後の画角だけが、あなたにとっての世界のすべてです。
クロップ外の情報や、元の全体構図を想像してはいけません。
"""

# =========================================================
# UTIL
# =========================================================
def die(msg: str, code: int = 1) -> None:
    print(f"[ERROR] {msg}", file=sys.stderr)
    sys.exit(code)


def safe_mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def is_tty() -> bool:
    try:
        return sys.stdin.isatty()
    except Exception:
        return False


def get_api_key() -> str:
    k = os.environ.get("OPENAI_API_KEY", "").strip()
    if k:
        return k
    # envに無くて非TTYなら固まらせない
    if not is_tty():
        raise RuntimeError("OPENAI_API_KEY not set and stdin is not a TTY (non-interactive).")
    return getpass("OPENAI_API_KEY: ").strip()


def list_jpegs(frames_dir: Path) -> List[Path]:
    if not frames_dir.is_dir():
        return []
    files = sorted(frames_dir.glob("*.jpg")) + sorted(frames_dir.glob("*.jpeg")) + sorted(frames_dir.glob("*.png"))
    return [p for p in files if p.is_file()]


def ensure_duration_ok(start_sec: int, end_sec: int) -> bool:
    dur = end_sec - start_sec
    return 9 <= dur <= 15


def clamp_int(v: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, v))


def find_latest_decision(api_dir: Path) -> Path | None:
    if not api_dir.is_dir():
        return None
    vers = []
    for d in api_dir.glob("v*"):
        if d.is_dir():
            m = d.name[1:]
            if m.isdigit():
                vers.append((int(m), d))
    if not vers:
        return None
    vers.sort(key=lambda x: x[0])
    latest = vers[-1][1]
    dec = latest / "decision.json"
    return dec if dec.is_file() else None


def next_version(api_dir: Path) -> int:
    if not api_dir.exists():
        return 1
    vers = []
    for d in api_dir.glob("v*"):
        if d.is_dir():
            s = d.name[1:]
            if s.isdigit():
                vers.append(int(s))
    return (max(vers) + 1) if vers else 1


def save_json(path: Path, obj: Any) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


# =========================================================
# OPENAI CALL (via curl)
# =========================================================
def openai_chat_images(
    api_key: str,
    model: str,
    system: str,
    user_text: str,
    image_paths: List[Path],
    max_tokens: int = 600,
) -> str:
    # images: base64
    import base64
    content = [{"type": "text", "text": user_text}]
    for p in image_paths:
        b = base64.b64encode(p.read_bytes()).decode("ascii")
        # png/jpg判定は簡易でOK
        mime = "image/jpeg"
        if p.suffix.lower() == ".png":
            mime = "image/png"
        content.append(
            {
                "type": "image_url",
                "image_url": {"url": f"data:{mime};base64,{b}"},
            }
        )

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": content},
        ],
        "max_tokens": max_tokens,
    }

    # curlで叩く（requests無し運用）
    # NOTE: 画像base64でpayloadが巨大になるため、argvに載せずSTDINで渡す（Argument list too long回避）
    payload_json = json.dumps(payload)
    cmd = ["curl", "-sS", "https://api.openai.com/v1/chat/completions",
           "-H", f"Authorization: Bearer {api_key}",
           "-H", "Content-Type: application/json",
           "--data-binary", "@-"]
    p = subprocess.run(
        cmd,
        input=payload_json.encode("utf-8"),
        capture_output=True,
        text=False,
    )

    def _decode_blob(b: bytes | None) -> str:
        if not b:
            return ""
        try:
            return b.decode("utf-8")
        except UnicodeDecodeError:
            return b.decode("cp932", errors="replace")

    stderr_text = _decode_blob(p.stderr)
    if p.returncode != 0:
        raise RuntimeError(f"curl failed: rc={p.returncode} stderr={stderr_text[:500]}")

    raw = _decode_blob(p.stdout).strip()
    if not raw:
        raise RuntimeError(f"empty response from OpenAI API. stderr={stderr_text[:500]}")

    try:
        obj = json.loads(raw)
    except Exception:
        raise RuntimeError(f"invalid JSON response: {raw[:800]}")

    try:
        return obj["choices"][0]["message"]["content"]
    except Exception:
        raise RuntimeError(f"unexpected response: {raw[:800]}")


# =========================================================
# PIPELINE
# =========================================================
def run_api1(api_key: str, frames_dir: Path, event_name: str, model: str, out_dir: Path) -> Dict[str, Any]:
    # 1秒1枚 最大60
    imgs = list_jpegs(frames_dir)[:60]
    if not imgs:
        raise RuntimeError("no images in frames_dir")

    user_text = f"""以下はイベント {event_name} の静止画です。
最初の画像を 0 秒、次を 1 秒…と数える「相対秒」で start_sec/end_sec を返してください。
この静止画だけを見て、crop_x,start_sec,end_sec を JSONのみで返してください。
"""
    resp = openai_chat_images(api_key=api_key, model=model, system=API1_SYSTEM, user_text=user_text, image_paths=imgs, max_tokens=300)

    save_json(out_dir / "request.json", {"model": model, "system": API1_SYSTEM, "user_text": user_text, "images": [str(p) for p in imgs]})
    save_json(out_dir / "response.json", {"raw": resp})

    # JSON抽出（簡易）
    try:
        obj = json.loads(resp)
    except Exception:
        # たまに ```json ...``` が来る可能性を拾う
        m = None
        import re
        mm = re.search(r"\{.*\}", resp, flags=re.S)
        if mm:
            m = mm.group(0)
        if not m:
            raise
        obj = json.loads(m)

    save_json(out_dir / "decision_raw.json", obj)
    return obj


def run_api2(api_key: str, frames_dir: Path, event_name: str, model: str, out_dir: Path,
             crop_x: int, start_sec: int, end_sec: int, prompt_file: str = None) -> Dict[str, Any]:
    imgs_all = list_jpegs(frames_dir)
    if not imgs_all:
        raise RuntimeError("no images in frames_dir")

    # API1で確定した区間だけ
    start_sec = clamp_int(start_sec, 0, len(imgs_all) - 1)
    end_sec = clamp_int(end_sec, start_sec + 1, len(imgs_all))
    imgs = imgs_all[start_sec:end_sec]
    if not imgs:
        imgs = imgs_all[:min(9, len(imgs_all))]

    user_text = f"""以下はイベント {event_name} の指定区間の静止画です。
これだけを世界の全てとして扱い、title/description を JSONのみで返してください。
"""

    crop_context = build_api2_crop_context(crop_x)
    system_prompt = crop_context + "\n\n" + get_api2_system_prompt(prompt_file)
    resp = openai_chat_images(
        api_key=api_key,
        model=model,
        system=system_prompt,
        user_text=user_text,
        image_paths=imgs,
        max_tokens=500,
    )

    save_json(out_dir / "api2_request.json", {"model": model, "system": system_prompt, "user_text": user_text, "images": [str(p) for p in imgs]})
    save_json(out_dir / "api2_response.json", {"raw": resp})

    # JSON抽出
    try:
        obj = json.loads(resp)
    except Exception:
        import re
        mm = re.search(r"\{.*\}", resp, flags=re.S)
        if not mm:
            raise
        obj = json.loads(mm.group(0))

    save_json(out_dir / "api2_response_obj.json", obj)
    return obj


def build_final_decision(decision_raw: Dict[str, Any], api2_obj: Dict[str, Any]) -> Dict[str, Any]:
    # API1: crop_x/start_sec/end_sec
    crop_x = int(decision_raw.get("crop_x"))
    start_sec = int(decision_raw.get("start_sec"))
    end_sec = int(decision_raw.get("end_sec"))

    title = str(api2_obj.get("title", "")).strip()
    description = str(api2_obj.get("description", "")).strip()

    return {
        "crop_x": crop_x,
        "start_sec_rel": start_sec,
        "end_sec_rel": end_sec,
        "title": title,
        "description": description,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--frames-dir", type=str, default=None)
    ap.add_argument("--event-name", type=str, default=None)
    ap.add_argument("--event-dir", type=str, default=".")
    ap.add_argument("--api1-model", type=str, default=DEFAULT_API1_MODEL)
    ap.add_argument("--api2-model", type=str, default=DEFAULT_API2_MODEL)
    ap.add_argument("--step", type=int, default=2, help="1=API1 only, 2=API1+API2")
    ap.add_argument("--api2-prompt-file", type=str, default=None, help="external system prompt for API2")
    ap.add_argument("--max-retry", type=int, default=3)
    args = ap.parse_args()

    event_dir = Path(args.event_dir).resolve()
    api_dir = event_dir / "api"
    safe_mkdir(api_dir)

    if args.frames_dir:
        frames_dir = Path(args.frames_dir).resolve()
    else:
        # 既存運用：event_dir から frames_360/<event_name> を推測する用途があるならここを調整
        # 今は必須にしておく（曖昧にしない）
        die("--frames-dir is required", 2)

    if not args.event_name:
        # frames_dir 名を event_name に
        event_name = frames_dir.name
    else:
        event_name = args.event_name

    api_key = get_api_key()

    # 既に最新 decision があればそれを採用（API再実行せず）
    latest_dec = find_latest_decision(api_dir)
    if latest_dec and args.step == 2:
        print(f"[INFO] decision.json exists -> {latest_dec}")
        return

    v = next_version(api_dir)
    out_dir = api_dir / f"v{v}"
    safe_mkdir(out_dir)

    # API1: retry with numeric checks (STRICT: never proceed on all-fail)
    last_err = ""
    decision_raw = None
    passed_try_dir = None

    imgs_all = list_jpegs(frames_dir)
    if not imgs_all:
        raise RuntimeError("no frames in frames_dir")

    for i in range(args.max_retry):
        try:
            # 失敗の痕跡を vN 直下に残さないため、try用サブディレクトリへ保存
            tmp_dir = out_dir / f"try{i+1}"
            if tmp_dir.exists():
                shutil.rmtree(tmp_dir, ignore_errors=True)
            safe_mkdir(tmp_dir)

            cand = run_api1(api_key, frames_dir, event_name, args.api1_model, tmp_dir)

            # numeric checks
            crop_x = int(cand.get("crop_x"))
            start_sec = int(cand.get("start_sec"))
            end_sec = int(cand.get("end_sec"))

            if end_sec > len(imgs_all):
                raise RuntimeError(f"end_sec({end_sec}) > frames({len(imgs_all)})")
            if start_sec < 0 or start_sec >= len(imgs_all):
                raise RuntimeError(f"start_sec({start_sec}) out of range for frames({len(imgs_all)})")
            if not ensure_duration_ok(start_sec, end_sec):
                raise RuntimeError(f"duration invalid: {end_sec-start_sec} (must be 9..15)")

            # 合格：このtryを採用
            decision_raw = cand
            passed_try_dir = tmp_dir
            break

        except Exception as e:
            last_err = str(e)
            print(f"[WARN] API1 failed/retry {i+1}/{args.max_retry}: {last_err}")
            # 失敗tryは掃除（残したいなら消さずにコメントアウトでOK）
            try:
                if 'tmp_dir' in locals() and tmp_dir.exists():
                    shutil.rmtree(tmp_dir, ignore_errors=True)
            except Exception:
                pass
            time.sleep(1)

    if decision_raw is None or passed_try_dir is None:
        # 全敗：絶対に先へ進まない
        raise RuntimeError("API1 failed (all retries): " + last_err)

    # 合格したtryの証跡を vN 直下へ昇格（API2や後工程が迷わない）
    for name in ["request.json", "response.json", "decision_raw.json"]:
        src = passed_try_dir / name
        if src.is_file():
            shutil.move(str(src), str(out_dir / name))
    # tryフォルダを消す（try2/try3なども残っていれば掃除）
    try:
        shutil.rmtree(passed_try_dir, ignore_errors=True)
        for d in out_dir.glob("try*"):
            if d.is_dir():
                shutil.rmtree(d, ignore_errors=True)
    except Exception:
        pass

    if args.step == 1:
        print("[DONE] step=1")
        return

    # API2
    crop_x = int(decision_raw.get("crop_x"))
    start_sec = int(decision_raw.get("start_sec"))
    end_sec = int(decision_raw.get("end_sec"))

    api2_obj = run_api2(api_key, frames_dir, event_name, args.api2_model, out_dir, crop_x, start_sec, end_sec, args.api2_prompt_file)


    final_decision = build_final_decision(decision_raw, api2_obj)
    save_json(out_dir / "decision.json", final_decision)

    print("[DONE] step=2")


if __name__ == "__main__":
    main()

