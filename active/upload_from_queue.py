#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, Tuple

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request


# ====== paths / constants ======
DEFAULT_QUEUE_PATH = "/media/sf_REC/posting/queue.jsonl"
DEFAULT_TOKEN_PATH = "/media/sf_REC/keys/youtube/token.json"

# 固定再生リスト（まさおAIショート切り抜き動画）
PLAYLIST_ID = "PLvSj66EpFnyfn0tMREkXv33zjDn1edic-"

JST = timezone(timedelta(hours=9))

GORO_BLOCK = (
    "プロジェクトメンバーのGPT五郎です。\n"
    "まさおのライブ配信を低解像度の映像でフレーム単位に解析し、動きが強い区間を自動で拾って7〜15秒に整えています。\n"
    "n8n と FFmpeg を使って実験運用中なので、可愛い瞬間や違和感があればコメントで教えてもらえると助かります。"
)

HASHTAGS = "#まさお #AI切り抜き #ショート動画 #自動編集 #n8n #FFmpeg #shorts"


# ====== small utils ======
def eprint(*args):
    print(*args, file=sys.stderr)

def read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def write_text(path: str, text: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

def file_exists(path: str) -> bool:
    try:
        return os.path.exists(path)
    except Exception:
        return False

def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def now_jst() -> datetime:
    return datetime.now(JST)

def parse_session_start_from_path(any_path_under_session: str) -> Optional[datetime]:
    """
    any_path_under_session: /media/sf_masaos_mov/YYYY-MM-DD_HH-MM-SS/...
    """
    parts = any_path_under_session.split("/")
    for p in parts:
        if len(p) == 19 and p[4] == "-" and p[7] == "-" and p[10] == "_" and p[13] == "-" and p[16] == "-":
            try:
                return datetime.strptime(p, "%Y-%m-%d_%H-%M-%S").replace(tzinfo=JST)
            except Exception:
                return None
    return None

def parse_event_abs_seconds(event_name: str) -> Optional[Tuple[int, int]]:
    """
    event_name: "03531_03546"
    """
    try:
        a, b = event_name.split("_")
        return int(a), int(b)
    except Exception:
        return None

def build_time_line(video_path: str, decision_path: str) -> str:
    """
    例：
    2025年12月19日 06:59頃のライブ配信中の一場面です
    """
    # event_name は decision_path から取るのが確実
    # .../events/03531_03546/api/v1/decision.json
    parts = decision_path.split("/")
    event_name = None
    for i, p in enumerate(parts):
        if p == "events" and i + 1 < len(parts):
            event_name = parts[i + 1]
            break

    sess_start = parse_session_start_from_path(decision_path) or parse_session_start_from_path(video_path)
    if (event_name is None) or (sess_start is None):
        # 取れない場合はフォールバック（壊さない）
        return ""

    sec_pair = parse_event_abs_seconds(event_name)
    if sec_pair is None:
        return ""

    s0, s1 = sec_pair
    center = (s0 + s1) // 2
    t = sess_start + timedelta(seconds=center)
    return f"{t.strftime('%Y年%m月%d日 %H:%M')}頃のライブ配信中の一場面です"

def build_description(decision: Dict[str, Any], video_path: str, decision_path: str) -> str:
    taro = (decision.get("description") or "").rstrip()
    time_line = build_time_line(video_path, decision_path)

    blocks = []
    if taro:
        blocks.append(taro)
    if time_line:
        blocks.append(time_line)
    blocks.append(GORO_BLOCK)
    blocks.append(HASHTAGS)
    return "\n\n".join(blocks)


# ====== YouTube API ======
def load_creds(token_path: str) -> Credentials:
    if not file_exists(token_path):
        raise FileNotFoundError(f"token.json not found: {token_path}")
    creds = Credentials.from_authorized_user_file(token_path, scopes=["https://www.googleapis.com/auth/youtube"])
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        # refresh後は保存しておく（勝手に更新される可能性がある）
        write_text(token_path, creds.to_json())
    return creds

def youtube_build(token_path: str):
    creds = load_creds(token_path)
    return build("youtube", "v3", credentials=creds)

def upload_video(
    youtube,
    video_path: str,
    title: str,
    description: str,
    publish_at_rfc3339: str,
    privacy_status: str = "private",
) -> str:
    body = {
        "snippet": {
            "title": title,
            "description": description,
            "categoryId": "15",  # Pets & Animals
        },
        "status": {
            "privacyStatus": privacy_status,
            "publishAt": publish_at_rfc3339,
            "selfDeclaredMadeForKids": False,
        },
    }
    media = MediaFileUpload(video_path, mimetype="video/mp4", resumable=True)
    req = youtube.videos().insert(part="snippet,status", body=body, media_body=media)

    resp = None
    while resp is None:
        status, resp = req.next_chunk()
    return resp["id"]

def add_to_playlist(youtube, video_id: str, playlist_id: str) -> None:
    body = {
        "snippet": {
            "playlistId": playlist_id,
            "resourceId": {"kind": "youtube#video", "videoId": video_id},
        }
    }
    youtube.playlistItems().insert(part="snippet", body=body).execute()


# ====== queue handling ======
def load_queue_lines(queue_path: str):
    if not file_exists(queue_path):
        return []
    lines = []
    with open(queue_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            lines.append(line)
    return lines

def dequeue_first_line(queue_path: str) -> Tuple[Optional[str], int]:
    lines = load_queue_lines(queue_path)
    if not lines:
        return None, 0
    first = lines[0]
    remaining = lines[1:]
    with open(queue_path, "w", encoding="utf-8") as f:
        for l in remaining:
            f.write(l + "\n")
    return first, len(remaining)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--queue", default=DEFAULT_QUEUE_PATH)
    ap.add_argument("--token", default=DEFAULT_TOKEN_PATH)
    ap.add_argument("--print_desc", action="store_true")
    ap.add_argument("--dry_run", action="store_true")
    ap.add_argument("--max", type=int, default=1, help="process up to N items (default 1)")
    ap.add_argument("--sleep", type=int, default=5, help="sleep seconds between uploads (default 5)")
    ap.add_argument("--dequeue", action="store_true", help="dequeue on success/skip (default ON unless print/dry)")
    args = ap.parse_args()

    max_n = max(args.max, 1)

    # 安全：print_desc / dry_run はキューを進めない
    if args.print_desc or args.dry_run:
        args.dequeue = False
    else:
        # 通常は dequeue するのが正
        if not args.dequeue:
            args.dequeue = True

    processed = done = skipped = errors = 0

    youtube = None
    if not (args.print_desc or args.dry_run):
        youtube = youtube_build(args.token)

    for _ in range(max_n):
        item_line, remaining = (None, 0)
        if args.dequeue:
            item_line, remaining = dequeue_first_line(args.queue)
        else:
            lines = load_queue_lines(args.queue)
            item_line = lines[0] if lines else None
            remaining = max(len(lines) - 1, 0)

        if not item_line:
            break

        processed += 1
        try:
            item = json.loads(item_line)
        except Exception:
            errors += 1
            eprint("[ERROR] invalid json line")
            continue

        video_path = item.get("video_path")
        decision_path = item.get("decision_path")
        published_flag_path = item.get("published_flag_path")
        publish_at = item.get("publishAt")

        print("=" * 60)
        print(f"[JOB] {video_path}")
        print(f"  publishAt: {publish_at}")

        # 必須チェック
        if not video_path or not decision_path or not published_flag_path or not publish_at:
            print("[SKIP] missing required fields")
            skipped += 1
            continue

        if file_exists(published_flag_path):
            print(f"[SKIP] already published: {published_flag_path}")
            skipped += 1
            continue

        if not file_exists(video_path):
            print(f"[SKIP] video missing: {video_path}")
            skipped += 1
            continue

        if not file_exists(decision_path):
            print(f"[SKIP] decision missing: {decision_path}")
            skipped += 1
            continue

        decision = load_json(decision_path)
        title = (decision.get("title") or "").strip()
        if not title:
            title = "まさおのワンシーン"

        desc = build_description(decision, video_path, decision_path)

        if args.print_desc:
            print("[PRINT_DESC]")
            print("---- title ----")
            print(title)
            print("---- description ----")
            print(desc)
            skipped += 1
            continue

        if args.dry_run:
            print("[DRY RUN] would upload")
            skipped += 1
            continue

        try:
            video_id = upload_video(
                youtube,
                video_path=video_path,
                title=title,
                description=desc,
                publish_at_rfc3339=publish_at,
                privacy_status="private",
            )
            # playlist 追加
            add_to_playlist(youtube, video_id, PLAYLIST_ID)

            # .published 作成（ログとして残す）
            log = {"video_id": video_id, "publishAt": publish_at, "uploaded_at": now_jst().isoformat()}
            write_text(published_flag_path, json.dumps(log, ensure_ascii=False) + "\n")

            print(f"[DONE] uploaded video_id={video_id}")
            done += 1

            # 実際のアップロード時だけ sleep
            time.sleep(max(args.sleep, 0))

        except HttpError as e:
            errors += 1
            eprint("[ERROR] HttpError:", str(e))
        except Exception as e:
            errors += 1
            eprint("[ERROR]", repr(e))

    print(f"[SUMMARY] processed={processed} done={done} skipped={skipped} errors={errors}")


if __name__ == "__main__":
    main()