#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from common_win import load_config

JST = timezone(timedelta(hours=9))
DEFAULT_TOKEN_PATH = r"D:\OBS\REC\keys\youtube\token.json"


def console_safe(text: str) -> str:
    enc = getattr(__import__("sys").stdout, "encoding", None) or "utf-8"
    try:
        return text.encode(enc, errors="replace").decode(enc, errors="replace")
    except Exception:
        return text


def load_creds(token_path: Path) -> Credentials:
    creds = Credentials.from_authorized_user_file(str(token_path), scopes=["https://www.googleapis.com/auth/youtube"])
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        token_path.write_text(creds.to_json(), encoding="utf-8")
    return creds


def normalize_rfc3339(raw: str) -> str:
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=JST)
    else:
        dt = dt.astimezone(JST)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_description(args: argparse.Namespace) -> str:
    if args.description_file:
        return Path(args.description_file).read_text(encoding="utf-8").strip()
    return str(args.description_text or "").strip()


def chunks(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def get_upload_playlist_id(youtube) -> str:
    resp = youtube.channels().list(part="contentDetails", mine=True, maxResults=1).execute()
    items = resp.get("items", [])
    if not items:
        raise SystemExit("channel(mine=true) not found")
    return str(items[0]["contentDetails"]["relatedPlaylists"]["uploads"])


def list_recent_video_ids(youtube, max_results: int) -> List[str]:
    playlist_id = get_upload_playlist_id(youtube)
    out: List[str] = []
    page_token: Optional[str] = None
    while len(out) < max_results:
        req = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=playlist_id,
            maxResults=min(50, max_results - len(out)),
            pageToken=page_token,
        )
        resp = req.execute()
        for item in resp.get("items", []):
            vid = item.get("contentDetails", {}).get("videoId")
            if isinstance(vid, str) and vid:
                out.append(vid)
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return out


def fetch_video_details(youtube, video_ids: List[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for group in chunks(video_ids, 50):
        resp = youtube.videos().list(part="snippet,status", id=",".join(group), maxResults=50).execute()
        out.extend(resp.get("items", []))
    return out


def missing_description(video: Dict[str, Any]) -> bool:
    desc = str(video.get("snippet", {}).get("description", "")).strip()
    return not desc


def missing_publish_at(video: Dict[str, Any]) -> bool:
    status = video.get("status", {})
    privacy = str(status.get("privacyStatus", "")).strip().lower()
    publish_at = str(status.get("publishAt", "")).strip()
    return privacy == "private" and (not publish_at)


def update_description(youtube, video: Dict[str, Any], description: str, dry_run: bool) -> bool:
    snippet = video.get("snippet", {})
    vid = str(video.get("id"))
    title = str(snippet.get("title", "")).strip()
    category_id = str(snippet.get("categoryId", "22")).strip() or "22"
    body = {
        "id": vid,
        "snippet": {
            "title": title,
            "description": description,
            "categoryId": category_id,
        },
    }
    if dry_run:
        return True
    youtube.videos().update(part="snippet", body=body).execute()
    return True


def build_status_payload(video: Dict[str, Any], privacy_status: str, publish_at: Optional[str]) -> Dict[str, Any]:
    status_src = video.get("status", {})
    status: Dict[str, Any] = {
        "privacyStatus": privacy_status,
    }
    if "selfDeclaredMadeForKids" in status_src:
        status["selfDeclaredMadeForKids"] = bool(status_src["selfDeclaredMadeForKids"])
    if "publicStatsViewable" in status_src:
        status["publicStatsViewable"] = bool(status_src["publicStatsViewable"])
    if "embeddable" in status_src:
        status["embeddable"] = bool(status_src["embeddable"])
    if "license" in status_src:
        status["license"] = str(status_src["license"])
    if publish_at:
        status["publishAt"] = publish_at
    return {"id": str(video.get("id")), "status": status}


def update_status(youtube, body: Dict[str, Any], dry_run: bool) -> bool:
    if dry_run:
        return True
    youtube.videos().update(part="status", body=body).execute()
    return True


def choose_targets(videos: List[Dict[str, Any]], include_ok: bool) -> List[Dict[str, Any]]:
    if include_ok:
        return videos
    out: List[Dict[str, Any]] = []
    for v in videos:
        if missing_description(v) or missing_publish_at(v):
            out.append(v)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit/update YouTube video description and publishAt")
    ap.add_argument("--config", default=str(Path(__file__).resolve().parents[1] / "config.json"))
    ap.add_argument("--token", default="")
    ap.add_argument("--max", type=int, default=20, help="number of recent uploads to inspect")
    ap.add_argument("--video-id", action="append", default=[], help="specific video id (repeatable)")
    ap.add_argument("--include-ok", action="store_true", help="include videos without missing fields")
    ap.add_argument("--description-text", default="", help="description text to apply")
    ap.add_argument("--description-file", default="", help="read description text from file")
    ap.add_argument("--fill-missing-description", action="store_true")
    ap.add_argument("--fill-missing-publish-at", default="", help="RFC3339/ISO datetime start")
    ap.add_argument("--reschedule-start", default="", help="force schedule start datetime")
    ap.add_argument("--pitch-hours", type=float, default=3.0)
    ap.add_argument("--publish", action="store_true", help="set privacyStatus=public")
    ap.add_argument("--private", action="store_true", help="set privacyStatus=private")
    ap.add_argument("--commit", action="store_true", help="execute updates (default: dry-run)")
    args = ap.parse_args()

    if args.publish and args.private:
        raise SystemExit("--publish and --private are mutually exclusive")
    if args.reschedule_start and args.publish:
        raise SystemExit("--reschedule-start and --publish cannot be used together")
    if args.fill_missing_publish_at and args.publish:
        raise SystemExit("--fill-missing-publish-at and --publish cannot be used together")

    conf = load_config(args.config)
    token_path = Path(args.token or conf.get("youtube_token", DEFAULT_TOKEN_PATH))
    if not token_path.exists():
        raise SystemExit(f"token not found: {token_path}")

    description = parse_description(args)
    dry_run = not args.commit

    creds = load_creds(token_path)
    youtube = build("youtube", "v3", credentials=creds)

    video_ids = list(dict.fromkeys([str(x).strip() for x in args.video_id if str(x).strip()]))
    if not video_ids:
        video_ids = list_recent_video_ids(youtube, max(1, args.max))
    if not video_ids:
        print("[INFO] no videos found")
        return

    videos = fetch_video_details(youtube, video_ids)
    targets = choose_targets(videos, include_ok=args.include_ok)
    if not targets:
        print("[INFO] no target videos (missing description/publishAt)")
        return

    print(f"[INFO] dry_run={dry_run} inspected={len(videos)} target={len(targets)}")
    for v in targets:
        vid = str(v.get("id"))
        sn = v.get("snippet", {})
        st = v.get("status", {})
        title_disp = console_safe(str(sn.get("title", "")).strip()[:40])
        print(
            f"[TARGET] {vid} title={title_disp} "
            f"desc_missing={missing_description(v)} publishAt_missing={missing_publish_at(v)} "
            f"privacy={st.get('privacyStatus', '')} publishAt={st.get('publishAt', '')}"
        )

    update_count = 0
    for idx, v in enumerate(targets):
        vid = str(v.get("id"))

        if args.fill_missing_description and description and missing_description(v):
            ok = update_description(youtube, v, description, dry_run=dry_run)
            if ok:
                print(f"[UPDATE] description {vid}")
                update_count += 1

        if args.fill_missing_publish_at and missing_publish_at(v):
            ts = normalize_rfc3339(args.fill_missing_publish_at)
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00")) + timedelta(hours=idx * float(args.pitch_hours))
            body = build_status_payload(
                v,
                privacy_status="private",
                publish_at=dt.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            )
            if update_status(youtube, body, dry_run=dry_run):
                print(f"[UPDATE] publishAt(set missing) {vid} => {body['status'].get('publishAt')}")
                update_count += 1

        if args.reschedule_start:
            ts = normalize_rfc3339(args.reschedule_start)
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00")) + timedelta(hours=idx * float(args.pitch_hours))
            body = build_status_payload(
                v,
                privacy_status="private",
                publish_at=dt.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            )
            if update_status(youtube, body, dry_run=dry_run):
                print(f"[UPDATE] publishAt(reschedule) {vid} => {body['status'].get('publishAt')}")
                update_count += 1

        if args.publish:
            body = build_status_payload(v, privacy_status="public", publish_at=None)
            if update_status(youtube, body, dry_run=dry_run):
                print(f"[UPDATE] publish {vid}")
                update_count += 1
        elif args.private:
            body = build_status_payload(v, privacy_status="private", publish_at=str(v.get("status", {}).get("publishAt", "")) or None)
            if update_status(youtube, body, dry_run=dry_run):
                print(f"[UPDATE] private {vid}")
                update_count += 1

    print(f"[SUMMARY] updates={update_count} dry_run={dry_run}")
    if dry_run:
        print("[NEXT] add --commit to apply changes")


if __name__ == "__main__":
    main()


