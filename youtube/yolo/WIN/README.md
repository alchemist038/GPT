# WIN Pipeline (Isolated)

This folder is a Windows-only pipeline workspace.
Existing scripts under project root were not modified.

## Files

- `config.example.json`: template config
- `config.json`: runtime config (create from template)
- `scripts/build_candidates_win.py`: build `candidates_20s.jsonl` per session
- `scripts/pick_global_candidates_win.py`: global picker across `E:\masaos_mov`
- `scripts/run_event_queue_pipeline_yolo_win.py`: render pipeline with JPEG review gate
- `ui/app.py`: simple desktop launcher (tkinter)
- `launch_ui.ps1`: PowerShell launcher

## Key Behavior

1. `build_candidates_win.py` is independent line.
2. `pick_global_candidates_win.py` scans all `candidates_20s.jsonl` and supports:
- `random`
- `motion`
- `band`
- `hybrid`
3. Pipeline supports pre-API review:
- `approve`
- `defer` -> writes to `deferred_queue`
- `reject` -> writes to `rejected_queue`

## Setup

1. Copy config:
```powershell
Copy-Item .\WIN\config.example.json .\WIN\config.json
```

2. Edit `WIN\config.json`:
- `base_dir`
- `bgm_path`
- `fontfile`
- `api_script`
- `api2_prompt_file`
- queue paths

3. Ensure commands are available:
- `python`
- `ffmpeg`

## Launch UI

```powershell
cd D:\OBS\REC\scripts\youtube\yolo\WIN
.\launch_ui.ps1
```

## CLI Examples

Build candidates:
```powershell
python .\WIN\scripts\build_candidates_win.py --config .\WIN\config.json --base-dir E:\masaos_mov
```

Global pick:
```powershell
python .\WIN\scripts\pick_global_candidates_win.py --config .\WIN\config.json --base-dir E:\masaos_mov --mode band --total 8 --no-overlap
```

Pipeline with manual review:
```powershell
python .\WIN\scripts\run_event_queue_pipeline_yolo_win.py --config .\WIN\config.json --review-before-api --review-action prompt
```

## Notes

- Candidate files are updated with `picked_at`/`pick_id` after pick.
- Existing root scripts are untouched.
- If `review-action prompt` is used, pipeline waits for your keyboard action in terminal.

## Uploaded Video Management

Manage uploaded videos (audit/fix description/schedule/publish):

```powershell
# audit recent 20 uploads (dry-run)
python .\WIN\scripts\manage_youtube_videos_win.py --config .\WIN\config.json --max 20

# fill only missing descriptions (dry-run)
python .\WIN\scripts\manage_youtube_videos_win.py --config .\WIN\config.json --fill-missing-description --description-file .\desc.txt

# set missing publishAt from JST start time with 3h interval (dry-run)
python .\WIN\scripts\manage_youtube_videos_win.py --config .\WIN\config.json --fill-missing-publish-at 2026-02-19T09:00:00+09:00 --pitch-hours 3

# reschedule target videos and apply changes
python .\WIN\scripts\manage_youtube_videos_win.py --config .\WIN\config.json --video-id VIDEO_ID_1 --video-id VIDEO_ID_2 --reschedule-start 2026-02-20T09:00:00+09:00 --pitch-hours 3 --commit

# publish target videos now and apply changes
python .\WIN\scripts\manage_youtube_videos_win.py --config .\WIN\config.json --video-id VIDEO_ID_1 --publish --commit
```
