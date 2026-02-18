# 正の実行パス（2026-01-04）
# cron が呼ぶのは active/ と youtube/ のみ

[analyze]
/media/sf_REC/scripts/active/analyze_y2_events.py

[enqueue daily A/Y]
/media/sf_REC/scripts/active/enqueue_daily_YA.py

[event_queue -> generate -> upload_queue]
/media/sf_REC/scripts/youtube/run_event_queue_pipeline.py

[upload]
/media/sf_REC/scripts/youtube/upload_from_queue.py

[core engines]
/media/sf_REC/scripts/core/api_decision_pipeline.py
/media/sf_REC/scripts/core/render_short_from_decision.py

[dormant (do not cron)]
/media/sf_REC/scripts/dormant/
