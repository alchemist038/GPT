# UI仕様書 (現行)

対象: `youtube/yolo/WIN/ui/app.py`

## 1. 概要
- アプリ名: `YOLO Windows パイプライン起動`
- 実装: Python `tkinter`
- 目的: WINパイプラインの主要4処理（候補作成/全体ピック/パイプライン/アップロード）をGUIから実行する
- 作業ディレクトリ: `ROOT = youtube/yolo/WIN`

## 2. 起動
- 起動スクリプト: `youtube/yolo/WIN/launch_ui.ps1`
- 直接起動: `python youtube/yolo/WIN/ui/app.py`
- 初期ウィンドウサイズ: `980x820`

## 3. 画面構成
### 3.1 設定ファイル
- 入力: `設定ファイル` (`config_path`)
- 初期値: `ROOT/config.json`
- ボタン:
  - `参照`: JSONファイル選択
  - `設定再読込`: config再読込（API関連設定を再反映）

### 3.2 ベースフォルダ
- 入力: `ベースフォルダ` (`base_dir`)
- 初期値: `E:\masaos_mov`
- ボタン: `参照`（フォルダ選択）

### 3.3 APIキー参照
- 入力:
  - `envファイル` (`api_env_file`) 初期値: `ROOT/.env.win`
  - `変数名` (`api_key_env_name`) 初期値: `OPENAI_API_KEY`
  - `キー` (`api_key_value`) マスク表示
- ボタン:
  - `参照`: envファイル選択
  - `キー保存`: envファイルへ `KEY=VALUE` 形式で保存

### 3.4 全体ピック
- 入力:
  - `モード` (`pick_mode`): `random | motion | band | hybrid`（初期値 `band`）
  - `件数` (`pick_total`) 初期値 `8`
  - `シード` (`pick_seed`) 初期値 `42`
  - `公開開始時刻` (`publish_start`) 初期値 `2026/02/26 02:00`
  - `公開間隔(時間)` (`publish_pitch_hours`) 初期値 空

### 3.5 パイプライン確認
- 入力:
  - `API 実行前に JPEG を確認` (`review_before_api`) 初期値 ON
  - `確認後の処理` (`review_action`): `prompt | approve | defer | reject`（初期値 `prompt`）

### 3.6 実行ボタン
- `1) 候補作成`
- `2) 全体ピック`
- `3) パイプライン実行`
- `4) アップロード`
- `WIN フォルダを開く`

### 3.7 今回 Pick されたフォルダ
- `readonly` コンボボックス (`picked_folder`)
- ボタン:
  - `選択フォルダを開く`
  - `一覧更新`（event_queueから再読込）

### 3.8 ログ表示
- 画面下部 `Text` に標準出力/標準エラー統合ログを逐次表示
- 非同期更新周期: 100ms

## 4. 実行コマンド仕様
すべて `cwd=ROOT` でサブプロセス起動。環境変数は `api_env_file` を読み込んで上書き。

### 4.1 候補作成 (`run_build`)
`python scripts/build_candidates_win.py --config <config_path> --base-dir <base_dir>`

### 4.2 全体ピック (`run_pick`)
基本:
`python scripts/pick_global_candidates_win.py --config <config_path> --base-dir <base_dir> --mode <pick_mode> --total <pick_total> --seed <pick_seed> --no-overlap`

追加:
- `publish_start` が有効なら `--start <normalized>` を付与
- `publish_pitch_hours` 入力時のみ `--pitch-hours <value>` を付与

### 4.3 パイプライン実行 (`run_pipeline`)
基本:
`python scripts/run_event_queue_pipeline_yolo_win.py --config <config_path>`

追加:
- `review_before_api=ON` のとき `--review-before-api`
- 常に `--review-action <review_action>`

### 4.4 アップロード (`run_upload`)
`python scripts/upload_from_queue_win.py --config <config_path>`

## 5. 補助動作
- `publish_start` は以下を許容して `YYYY-MM-DDTHH:MM:SS` へ正規化
  - `YYYY/MM/DD HH:MM`
  - `YYYY-MM-DD HH:MM`
  - `YYYY-MM-DDTHH:MM`
- `run_pick` 成功時、event_queueの増分から `event_dir` を抽出してコンボ更新
- `選択フォルダを開く`:
  - 対象が存在すればそのフォルダを開く
  - なければ親`events`フォルダを開く（存在時）

## 6. エラーハンドリング
- config読込失敗: ログに `[WARN] config load failed`
- env読込失敗: ログに `[WARN] env read failed`
- event_queue未設定: ログに `[WARN] event_queue path missing in config`
- コマンド終了コードは `[exit] <code>` として出力

## 7. 既知の仕様上の注意
- UIからは `scripts/manage_youtube_videos_win.py` を直接実行しない（現行ボタン未実装）
- APIキー保存は既存envキー順序を保持せず、読込済み辞書順で再出力
- 長時間処理はバックグラウンドスレッドで実行されるため、UIはブロックしない
