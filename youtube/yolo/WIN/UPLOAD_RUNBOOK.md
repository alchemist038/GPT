# 3本アップ作業ログと繰り返し手順

更新日: 2026-02-18
対象環境: `D:\OBS\REC\scripts\youtube\yolo\WIN`

## 今回実施したアップ結果（3本）

- `07080_07100` -> `eLVmHJ0PSmA`
- `21660_21680` -> `tI713DNf4lU`
- `11840_11860` -> `5DM2SeuASX4`

公開時刻（JST）:

- `2026-02-20T22:00:00+09:00`
- `2026-02-21T02:00:00+09:00`
- `2026-02-21T06:00:00+09:00`

送信時のUTC変換ログ（参考）:

- `2026-02-20T13:00:00Z`
- `2026-02-20T17:00:00Z`
- `2026-02-20T21:00:00Z`

## 繰り返し手順（CLI）

1. 候補作成

```powershell
python scripts\build_candidates_win.py --config config.json --base-dir E:\masaos_mov
```

2. ピック（例: 動き優先 / 3本 / 4時間おき）

```powershell
python scripts\pick_global_candidates_win.py --config config.json --base-dir E:\masaos_mov --mode motion --total 3 --no-overlap --start 2026-02-20T22:00:00 --pitch-hours 4
```

3. パイプライン実行（レビューは自動承認）

```powershell
python scripts\run_event_queue_pipeline_yolo_win.py --config config.json --review-before-api --review-action approve --max 10
```

4. アップロード

```powershell
python scripts\upload_from_queue_win.py --config config.json --max 10
```

## 運用メモ

- `prompt` は同じCLIで入力待ちになるため、通常は `approve` を推奨。
- `lock exists` が出た場合は、先行プロセス終了を確認してから再実行。
- 日本時間を確実に扱うため、`publishAt` は `+09:00` 付きで扱う実装に修正済み。
- 完了確認は以下:

```powershell
Get-Content data\event_queue_yolo_win.jsonl | Measure-Object -Line
Get-Content data\upload_queue_yolo_win.jsonl | Measure-Object -Line
```

両方 `0` なら当該バッチは処理完了。
