#!/usr/bin/env python3
"""
publish_yolo: yolo_event_queue.jsonl -> (YOLO判定) -> (crop+API2) -> decision_post.json -> yolo_queue.jsonl

- dequeue方式：先頭1行だけ処理して、成功/失敗に関わらず先頭は消す（=詰まり防止）
- reject は絶対に yolo_queue に入れない
- 保険：session_dir/frames_dir/event_dir が /media/sf_REC 側を向いていたら、
        /media/sf_masaos_mov/<SESSION> に全置換して存在確認してから実行
"""

import argparse
import json
import re
import subprocess
from pathlib import Path
from typing import Optional, Tuple

CTRL_BASE = Path("/media/sf_REC")  # 司令塔（固定）
POSTING = CTRL_BASE / "posting"

EVENT_Q = POSTING / "yolo_event_queue.jsonl"
NEXT_Q  = POSTING / "yolo_queue.jsonl"

YOLO_API1 = CTRL_BASE / "scripts" / "experimental" / "yolo_api1_like.py"
YOLO_CROP_API2 = CTRL_BASE / "scripts" / "experimental" / "yolo_crop_and_api2.py"

DEFAULT_API2_SYSTEM = CTRL_BASE / "prompts" / "api2_system_yolo.txt"
FALLBACK_API2_SYSTEM = CTRL_BASE / "prompts" / "api2_system.txt"


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


def strip_code_fences(s: str) -> str:
    s = s.strip()
    s = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", s)
    s = re.sub(r"\s*```$", "", s)
    return s.strip()


def parse_api2_output(desc_path: Path) -> Tuple[str, str, str]:
    raw = desc_path.read_text(encoding="utf-8").strip()
    txt = strip_code_fences(raw)

    try:
        obj = json.loads(txt)
        title = str(obj.get("title", "")).strip()
        desc  = str(obj.get("description", "")).strip()
        if title and desc:
            return title, desc, "json_ok"
    except Exception:
        pass

    m_title = re.search(r"^title\s*[:：]\s*(.+)$", txt, re.IGNORECASE | re.MULTILINE)
    m_desc  = re.search(r"^description\s*[:：]\s*(.+)$", txt, re.IGNORECASE | re.MULTILINE)
    if m_title and m_desc:
        title = m_title.group(1).strip()
        desc = m_desc.group(1).strip()
        return title, desc, "kv_ok"

    return "（API2_PARSE_FAIL）", raw, "raw_fallback"


def run_cmd(cmd: list[str]) -> int:
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    print(p.stdout.rstrip())
    return p.returncode


def normalize_paths(item: dict, mov_base: Path) -> dict:
    """
    保険：キューに /media/sf_REC/<SESSION> が混ざっても、
    /media/sf_masaos_mov/<SESSION> に全置換して存在する方を採用する。
    対象：session_dir / frames_dir / event_dir
    """
    def _swap(p: Path) -> Path:
        # 既に存在するならそのまま
        if p.exists():
            return p

        session_name = p.parts[-1] if p.name else p.name
        # session_dir の場合は name がセッション名
        # frames_dir/event_dir の場合も、上位にセッション名が入っているはずなので p.parts から拾う
        # 最優先：.../<SESSION>/frames_360/<EVENT> or .../<SESSION>/events/<EVENT>
        # p.parts を舐めて最初に 20xx-.. を探す
        sess = None
        for part in p.parts[::-1]:
            if re.match(r"^20\d{2}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}$", part):
                sess = part
                break

        if sess is None:
            # 最後の手：p.name をセッション名候補にしてみる
            sess = p.name

        # 置換候補
        # 1) session直下
        cand_session = mov_base / sess
        if cand_session.exists():
            # p が session_dir のとき
            if (p / "frames_360").exists() or (p / "events").exists():
                return cand_session

            # p が frames_dir のとき
            if "frames_360" in p.parts:
                idx = p.parts.index("frames_360")
                tail = Path(*p.parts[idx:])  # frames_360/<EVENT>/...
                cand = cand_session / tail
                if cand.exists():
                    return cand

            # p が event_dir のとき
            if "events" in p.parts:
                idx = p.parts.index("events")
                tail = Path(*p.parts[idx:])  # events/<EVENT>/...
                cand = cand_session / tail
                if cand.exists():
                    return cand

        return p

    out = dict(item)
    for k in ["session_dir", "frames_dir", "event_dir"]:
        if k in out and out[k]:
            out[k] = str(_swap(Path(out[k])))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max", type=int, default=1, help="process N items per run (default 1)")
    ap.add_argument("--api2-system", default=str(DEFAULT_API2_SYSTEM), help="api2 system prompt path (yolo)")
    ap.add_argument("--api2-model", default="gpt-4o-mini")
    ap.add_argument("--api2-max-tokens", type=int, default=220)
    ap.add_argument("--labels", default="cat", help="labels for masao proxy (pass-through to yolo_api1_like)")
    ap.add_argument("--conf-min", type=float, default=0.40)
    ap.add_argument("--target-ratio", type=float, default=0.70)
    ap.add_argument("--dry-run", action="store_true", help="do not write yolo_queue.jsonl")

    # ★追加：素材ルート（正）
    ap.add_argument("--mov-base", default="/media/sf_masaos_mov",
                    help="materials base dir (default: /media/sf_masaos_mov)")

    args = ap.parse_args()
    mov_base = Path(args.mov_base)

    api2_system_path = Path(args.api2_system)
    if not api2_system_path.exists():
        print(f"[WARN] api2 system not found: {api2_system_path}")
        print(f"[WARN] fallback to: {FALLBACK_API2_SYSTEM}")
        api2_system_path = FALLBACK_API2_SYSTEM

    for _ in range(args.max):
        item = read_first_jsonl_and_dequeue(EVENT_Q)
        if item is None:
            print("[EMPTY] yolo_event_queue is empty")
            return

        # ★保険：素材パスを全置換（存在する方へ寄せる）
        item = normalize_paths(item, mov_base)

        session_dir = Path(item["session_dir"])
        event = item["event"]
        frames_dir = Path(item["frames_dir"])
        event_dir = Path(item["event_dir"])
        route = item.get("route", "yolo")

        yolo_dir = event_dir / "yolo" / "v1"
        decision_json = yolo_dir / "decision.json"
        done_flag = yolo_dir / ".yolo_done"
        reject_flag = yolo_dir / ".yolo_reject"

        print("=" * 60)
        print(f"[ITEM] {session_dir.name} / {event} route={route}")
        print(f"[PATH] session_dir={session_dir}")
        print(f"[PATH] frames_dir={frames_dir}")
        print(f"[PATH] event_dir={event_dir}")

        # 1) YOLO(API1相当)
        if not done_flag.exists() and not reject_flag.exists():
            safe_mkdir(yolo_dir)
            rc = run_cmd([
                "python3", str(YOLO_API1),
                "--frames-dir", str(frames_dir),
                "--event-dir", str(event_dir),
                "--labels", args.labels,
                "--conf-min", str(args.conf_min),
                "--target-ratio", str(args.target_ratio),
            ])
            if rc != 0:
                print("[WARN] yolo_api1_like returned non-zero (skip enqueue)")
                continue

        if reject_flag.exists():
            print("[REJECT] yolo_reject exists -> not enqueued")
            continue

        if not decision_json.exists():
            print(f"[WARN] decision.json not found: {decision_json} -> not enqueued")
            continue

        # 2) crop + API2
        rc = run_cmd([
            "python3", str(YOLO_CROP_API2),
            "--session-dir", str(session_dir),
            "--event", event,
            "--api2",
            "--api2-system", str(api2_system_path),
            "--api2-model", args.api2_model,
            "--api2-max-tokens", str(args.api2_max_tokens),
        ])
        if rc != 0:
            print("[WARN] yolo_crop_and_api2 returned non-zero -> not enqueued")
            continue

        # 3) decision_post.json 作成
        api2_desc = yolo_dir / "api2" / "v1" / "description.txt"
        if not api2_desc.exists():
            print(f"[WARN] api2 description missing: {api2_desc} -> not enqueued")
            continue

        ydec = json.loads(decision_json.read_text(encoding="utf-8"))
        title, description, parse_mode = parse_api2_output(api2_desc)

        decision_post = yolo_dir / "decision_post.json"
        post_obj = {
            "start_sec_rel": int(ydec["start_sec_rel"]),
            "end_sec_rel": int(ydec["end_sec_rel"]),
            "crop_x": int(ydec["crop_x"]),
            "crop_w": float(ydec.get("crop_w", 202.5)),
            "title": title,
            "description": description,
            "api2_parse_mode": parse_mode,
            "route": route,
            "session_dir": str(session_dir),
            "event": event,
            "yolo_version": "v1",
        }
        decision_post.write_text(json.dumps(post_obj, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[OK] wrote: {decision_post}")

        # 4) 次キューへ
        if args.dry_run:
            print("[DRY] skip enqueue yolo_queue")
            continue

        append_jsonl(NEXT_Q, {
            "session_dir": str(session_dir),
            "event": event,
            "event_dir": str(event_dir),
            "decision_post_path": str(decision_post),
            "route": route,
        })
        print("[PUBLISH] enqueued to yolo_queue.jsonl")


if __name__ == "__main__":
    main()
