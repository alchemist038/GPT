#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from common_win import load_config, read_jsonl, write_jsonl_atomic

JST = timezone(timedelta(hours=9))
DEFAULT_PLAYLIST_ID = "PLvSj66EpFnyfn0tMREkXv33zjDn1edic-"
DEFAULT_TOKEN_PATH = r"D:\OBS\REC\keys\youtube\token.json"


def now_jst_iso() -> str:
    return datetime.now(JST).replace(microsecond=0).isoformat()


def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_creds(token_path: Path) -> Credentials:
    creds = Credentials.from_authorized_user_file(str(token_path), scopes=["https://www.googleapis.com/auth/youtube"])
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        token_path.write_text(creds.to_json(), encoding="utf-8")
    return creds



def normalize_publish_at_rfc3339(raw: str) -> str:
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=JST)
    else:
        dt = dt.astimezone(JST)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
def upload_video(youtube, video_path: Path, title: str, description: str, publish_at_rfc3339: str) -> str:
    body = {
        "snippet": {
            "title": title,
            "description": description,
            "categoryId": "15",
        },
        "status": {
            "privacyStatus": "private",
            "publishAt": publish_at_rfc3339,
            "selfDeclaredMadeForKids": False,
        },
    }

    media = MediaFileUpload(str(video_path), mimetype="video/mp4", resumable=True)
    req = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
    resp = None
    while resp is None:
        _, resp = req.next_chunk()
    return str(resp["id"])


def add_to_playlist(youtube, video_id: str, playlist_id: str) -> None:
    body = {
        "snippet": {
            "playlistId": playlist_id,
            "resourceId": {"kind": "youtube#video", "videoId": video_id},
        }
    }
    youtube.playlistItems().insert(part="snippet", body=body).execute()


def build_description(decision: Dict[str, Any]) -> str:
    desc = str(decision.get("description") or "").strip()
    tags = "#まさお #うさぎ #rabbit #bunny #shorts"
    if desc:
        return f"{desc}\n\n{tags}"
    return tags


def dequeue_items(queue_path: Path, max_n: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    rows = read_jsonl(queue_path)
    return rows[:max_n], rows[max_n:]


def main() -> None:
    ap = argparse.ArgumentParser(description="Upload from WIN upload queue")
    ap.add_argument("--config", default=str(Path(__file__).resolve().parents[1] / "config.json"))
    ap.add_argument("--max", type=int, default=1)
    ap.add_argument("--sleep", type=int, default=3)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--token", default="")
    ap.add_argument("--playlist", default="")
    ap.add_argument("--ignore-published-flag", action="store_true")
    args = ap.parse_args()

    conf = load_config(args.config)
    queue_path = Path(conf["upload_queue"])
    token_path = Path(args.token or conf.get("youtube_token", DEFAULT_TOKEN_PATH))
    playlist_id = args.playlist or conf.get("playlist_id", DEFAULT_PLAYLIST_ID)

    if not queue_path.exists():
        print(f"[INFO] upload queue not found: {queue_path}")
        return

    jobs, remaining = dequeue_items(queue_path, max(1, args.max))
    if not jobs:
        print("[INFO] upload queue empty")
        return

    youtube = None
    if not args.dry_run:
        if not token_path.exists():
            raise SystemExit(f"token not found: {token_path}")
        creds = load_creds(token_path)
        youtube = build("youtube", "v3", credentials=creds)

    done = 0
    skipped = 0
    for item in jobs:
        video_path = Path(item.get("video_path", ""))
        decision_path = Path(item.get("decision_path", ""))
        published_flag_path = Path(item.get("published_flag_path", ""))
        publish_at_raw = str(item.get("publishAt", ""))

        print("=" * 60)
        print(f"[JOB] {video_path}")
        print(f"publishAt(raw)={publish_at_raw}")

        if not video_path.exists():
            print("[SKIP] video missing")
            skipped += 1
            continue
        if not decision_path.exists():
            print("[SKIP] decision missing")
            skipped += 1
            continue
        if published_flag_path.exists() and not args.ignore_published_flag:
            print("[SKIP] already published flag exists")
            skipped += 1
            continue

        decision = load_json(decision_path)
        title = str(decision.get("title") or "まさおのワンシーン").strip()
        description = build_description(decision)

        if args.dry_run:
            print(f"[DRY] title={title}")
            done += 1
            continue

        try:
            publish_at = normalize_publish_at_rfc3339(publish_at_raw)
        except Exception as e:
            print(f"[SKIP] invalid publishAt: {publish_at_raw} ({e})")
            skipped += 1
            continue

        print(f"publishAt(utc)={publish_at}")
        video_id = upload_video(youtube, video_path, title, description, publish_at)
        print(f"[UPLOADED] video_id={video_id}")

        try:
            add_to_playlist(youtube, video_id, playlist_id)
            print(f"[PLAYLIST] added {playlist_id}")
        except Exception as e:
            print(f"[WARN] playlist add failed: {e}")

        published_flag_path.parent.mkdir(parents=True, exist_ok=True)
        published_flag_path.write_text(f"uploaded_at={now_jst_iso()}\nvideo_id={video_id}\n", encoding="utf-8")
        done += 1

        if args.sleep > 0:
            time.sleep(args.sleep)

    if args.dry_run:
        print("[DRY] upload queue unchanged")
    else:
        write_jsonl_atomic(queue_path, remaining)
        print(f"[OK] queue updated remaining={len(remaining)}")

    print(f"[SUMMARY] done={done} skipped={skipped}")


if __name__ == "__main__":
    main()


