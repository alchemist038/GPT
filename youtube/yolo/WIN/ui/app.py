#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import queue
import subprocess
import threading
from datetime import datetime, timedelta, timezone
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, ttk
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
DEFAULT_CONFIG = ROOT / "config.json"
DEFAULT_API_ENV = ROOT / ".env.win"
JST = timezone(timedelta(hours=9))


class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("YOLO Windows パイプライン起動")
        self.geometry("980x820")

        self.log_queue: "queue.Queue[str]" = queue.Queue()

        self.config_path = tk.StringVar(value=str(DEFAULT_CONFIG))
        self.base_dir = tk.StringVar(value="E:\\masaos_mov")
        self.pick_mode = tk.StringVar(value="band")
        self.pick_total = tk.StringVar(value="8")
        self.pick_seed = tk.StringVar(value="42")
        self.publish_start = tk.StringVar(value="2026/02/26 02:00")
        self.publish_pitch_hours = tk.StringVar(value="")
        self.review_before_api = tk.BooleanVar(value=True)
        self.review_action = tk.StringVar(value="prompt")
        self.picked_folder = tk.StringVar(value="")

        self.manual_min_motion = tk.StringVar(value="40")
        self.manual_min_hits = tk.StringVar(value="18")
        self.manual_limit = tk.StringVar(value="300")
        self.manual_candidates: list[dict[str, Any]] = []

        self.api_env_file = tk.StringVar(value=str(DEFAULT_API_ENV))
        self.api_key_env_name = tk.StringVar(value="OPENAI_API_KEY")
        self.api_key_value = tk.StringVar(value="")

        self._build_ui()
        self.reload_api_settings_from_config(log_warn=False)
        self.after(100, self._drain_logs)

    def _build_ui(self) -> None:
        frm = ttk.Frame(self, padding=12)
        frm.pack(fill="both", expand=True)

        row = 0
        ttk.Label(frm, text="設定ファイル:").grid(row=row, column=0, sticky="w")
        ttk.Entry(frm, textvariable=self.config_path, width=95).grid(row=row, column=1, sticky="ew", padx=4)
        ttk.Button(frm, text="参照", command=self._pick_config).grid(row=row, column=2)
        ttk.Button(frm, text="設定再読込", command=lambda: self.reload_api_settings_from_config(log_warn=True)).grid(row=row, column=3, padx=4)
        row += 1

        ttk.Label(frm, text="ベースフォルダ:").grid(row=row, column=0, sticky="w")
        ttk.Entry(frm, textvariable=self.base_dir, width=95).grid(row=row, column=1, sticky="ew", padx=4)
        ttk.Button(frm, text="参照", command=self._pick_base).grid(row=row, column=2)
        row += 1

        api_box = ttk.LabelFrame(frm, text="APIキー参照")
        api_box.grid(row=row, column=0, columnspan=4, sticky="ew", pady=8)
        ttk.Label(api_box, text="envファイル").grid(row=0, column=0, padx=6, pady=4, sticky="w")
        ttk.Entry(api_box, textvariable=self.api_env_file, width=82).grid(row=0, column=1, padx=4, sticky="ew")
        ttk.Button(api_box, text="参照", command=self._pick_api_env).grid(row=0, column=2, padx=4)
        ttk.Label(api_box, text="変数名").grid(row=1, column=0, padx=6, pady=4, sticky="w")
        ttk.Entry(api_box, textvariable=self.api_key_env_name, width=20).grid(row=1, column=1, padx=4, sticky="w")
        ttk.Label(api_box, text="キー").grid(row=1, column=2, padx=6, sticky="e")
        ttk.Entry(api_box, textvariable=self.api_key_value, width=36, show="*").grid(row=1, column=3, padx=4, sticky="w")
        ttk.Button(api_box, text="キー保存", command=self.save_api_key_to_env).grid(row=1, column=4, padx=6)
        api_box.columnconfigure(1, weight=1)
        row += 1

        pick_box = ttk.LabelFrame(frm, text="全体ピック")
        pick_box.grid(row=row, column=0, columnspan=4, sticky="ew", pady=8)
        ttk.Label(pick_box, text="モード").grid(row=0, column=0, padx=6, pady=4)
        ttk.Combobox(pick_box, textvariable=self.pick_mode, values=["random", "motion", "band", "hybrid"], width=10).grid(row=0, column=1)
        ttk.Label(pick_box, text="件数").grid(row=0, column=2, padx=6)
        ttk.Entry(pick_box, textvariable=self.pick_total, width=8).grid(row=0, column=3)
        ttk.Label(pick_box, text="シード").grid(row=0, column=4, padx=6)
        ttk.Entry(pick_box, textvariable=self.pick_seed, width=8).grid(row=0, column=5)
        ttk.Label(pick_box, text="公開開始時刻").grid(row=1, column=0, padx=6, pady=4, sticky="w")
        ttk.Entry(pick_box, textvariable=self.publish_start, width=18).grid(row=1, column=1, padx=2, sticky="w")
        ttk.Label(pick_box, text="公開間隔(時間)").grid(row=1, column=2, padx=6, sticky="w")
        ttk.Entry(pick_box, textvariable=self.publish_pitch_hours, width=8).grid(row=1, column=3, sticky="w")
        row += 1

        review_box = ttk.LabelFrame(frm, text="パイプライン確認")
        review_box.grid(row=row, column=0, columnspan=4, sticky="ew", pady=8)
        ttk.Checkbutton(review_box, text="API 実行前に JPEG を確認", variable=self.review_before_api).grid(row=0, column=0, padx=6, pady=4)
        ttk.Label(review_box, text="確認後の処理").grid(row=0, column=1, padx=6)
        ttk.Combobox(review_box, textvariable=self.review_action, values=["prompt", "approve", "defer", "reject"], width=10).grid(row=0, column=2)
        row += 1

        actions = ttk.Frame(frm)
        actions.grid(row=row, column=0, columnspan=4, sticky="ew", pady=8)
        ttk.Button(actions, text="1) 候補作成", command=self.run_build).pack(side="left", padx=4)
        ttk.Button(actions, text="2) 全体ピック", command=self.run_pick).pack(side="left", padx=4)
        ttk.Button(actions, text="3) パイプライン実行", command=self.run_pipeline).pack(side="left", padx=4)
        ttk.Button(actions, text="4) アップロード", command=self.run_upload).pack(side="left", padx=4)
        ttk.Button(actions, text="WIN フォルダを開く", command=self.open_win_folder).pack(side="left", padx=4)
        row += 1

        picked_box = ttk.LabelFrame(frm, text="今回 Pick されたフォルダ")
        picked_box.grid(row=row, column=0, columnspan=4, sticky="ew", pady=8)
        self.picked_combo = ttk.Combobox(picked_box, textvariable=self.picked_folder, state="readonly", width=110)
        self.picked_combo.grid(row=0, column=0, padx=6, pady=4, sticky="ew")
        ttk.Button(picked_box, text="選択フォルダを開く", command=self.open_selected_picked_folder).grid(row=0, column=1, padx=6)
        ttk.Button(picked_box, text="一覧更新", command=self.reload_picked_from_queue).grid(row=0, column=2, padx=6)
        picked_box.columnconfigure(0, weight=1)
        row += 1

        manual_box = ttk.LabelFrame(frm, text="Manual Candidate Select (Pick from list to queue)")
        manual_box.grid(row=row, column=0, columnspan=4, sticky="nsew", pady=8)

        ttk.Label(manual_box, text="min motion").grid(row=0, column=0, padx=6, pady=4, sticky="w")
        ttk.Entry(manual_box, textvariable=self.manual_min_motion, width=8).grid(row=0, column=1, sticky="w")
        ttk.Label(manual_box, text="min hits").grid(row=0, column=2, padx=6, pady=4, sticky="w")
        ttk.Entry(manual_box, textvariable=self.manual_min_hits, width=8).grid(row=0, column=3, sticky="w")
        ttk.Label(manual_box, text="max rows").grid(row=0, column=4, padx=6, pady=4, sticky="w")
        ttk.Entry(manual_box, textvariable=self.manual_limit, width=8).grid(row=0, column=5, sticky="w")
        ttk.Button(manual_box, text="Load Candidates", command=self.load_manual_candidates).grid(row=0, column=6, padx=6)
        ttk.Button(manual_box, text="Enqueue Selected", command=self.enqueue_selected_candidates).grid(row=0, column=7, padx=6)

        self.manual_listbox = tk.Listbox(manual_box, selectmode=tk.EXTENDED, height=10)
        self.manual_listbox.grid(row=1, column=0, columnspan=8, padx=6, pady=(4, 6), sticky="nsew")
        sb = ttk.Scrollbar(manual_box, orient="vertical", command=self.manual_listbox.yview)
        sb.grid(row=1, column=8, sticky="ns", pady=(4, 6))
        self.manual_listbox.configure(yscrollcommand=sb.set)
        manual_box.columnconfigure(7, weight=1)
        manual_box.rowconfigure(1, weight=1)
        row += 1

        self.log_text = tk.Text(frm, wrap="word", height=20)
        self.log_text.grid(row=row, column=0, columnspan=4, sticky="nsew")

        frm.columnconfigure(1, weight=1)
        frm.rowconfigure(row - 1, weight=1)
        frm.rowconfigure(row, weight=1)

    def _pick_config(self) -> None:
        p = filedialog.askopenfilename(filetypes=[("JSON", "*.json"), ("All", "*.*")])
        if p:
            self.config_path.set(p)
            self.reload_api_settings_from_config(log_warn=True)

    def _pick_base(self) -> None:
        p = filedialog.askdirectory()
        if p:
            self.base_dir.set(p)

    def _pick_api_env(self) -> None:
        p = filedialog.askopenfilename(filetypes=[("Env", "*.env*"), ("All", "*.*")])
        if p:
            self.api_env_file.set(p)

    def _log(self, line: str) -> None:
        self.log_queue.put(line)

    def _drain_logs(self) -> None:
        while True:
            try:
                line = self.log_queue.get_nowait()
            except queue.Empty:
                break
            self.log_text.insert("end", line + "\n")
            self.log_text.see("end")
        self.after(100, self._drain_logs)

    def _read_config(self) -> dict:
        try:
            with Path(self.config_path.get()).open("r", encoding="utf-8-sig") as f:
                return json.load(f)
        except Exception as e:
            self._log(f"[WARN] config load failed: {e}")
            return {}

    def reload_api_settings_from_config(self, log_warn: bool) -> None:
        conf = self._read_config()
        if not conf:
            return

        env_file = conf.get("api_env_file")
        if isinstance(env_file, str) and env_file.strip():
            self.api_env_file.set(env_file.strip())
        elif log_warn:
            self._log(f"[INFO] api_env_file 未設定。既定値を使用: {self.api_env_file.get()}")

        env_name = conf.get("api_key_env_name")
        if isinstance(env_name, str) and env_name.strip():
            self.api_key_env_name.set(env_name.strip())

    def _parse_env_file(self, env_path: Path) -> dict[str, str]:
        vals: dict[str, str] = {}
        if not env_path.exists():
            return vals
        try:
            with env_path.open("r", encoding="utf-8") as f:
                for raw in f:
                    line = raw.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    key = k.strip()
                    if not key:
                        continue
                    vals[key] = v.strip().strip('"').strip("'")
        except Exception as e:
            self._log(f"[WARN] env read failed: {e}")
        return vals

    def _build_subprocess_env(self) -> dict[str, str]:
        env = os.environ.copy()
        env_path = Path(self.api_env_file.get().strip() or str(DEFAULT_API_ENV))
        env_vals = self._parse_env_file(env_path)
        env.update(env_vals)
        return env

    def save_api_key_to_env(self) -> None:
        key_name = self.api_key_env_name.get().strip() or "OPENAI_API_KEY"
        key_val = self.api_key_value.get().strip()
        if not key_val:
            self._log("[WARN] APIキーが空です")
            return

        env_path = Path(self.api_env_file.get().strip() or str(DEFAULT_API_ENV))
        env_vals = self._parse_env_file(env_path)
        env_vals[key_name] = key_val

        lines = [f"{k}={v}" for k, v in env_vals.items()]
        env_path.parent.mkdir(parents=True, exist_ok=True)
        env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        self.api_key_value.set("")
        self._log(f"[OK] APIキー保存: {env_path} ({key_name})")

    def _run_cmd(self, cmd: list[str], on_done: Callable[[int], None] | None = None) -> None:
        def worker() -> None:
            self._log("$ " + " ".join(cmd))
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                cwd=str(ROOT),
                env=self._build_subprocess_env(),
            )
            assert proc.stdout is not None
            for line in proc.stdout:
                self._log(line.rstrip("\n"))
            rc = proc.wait()
            self._log(f"[exit] {rc}")
            if on_done is not None:
                self.after(0, lambda: on_done(rc))

        threading.Thread(target=worker, daemon=True).start()

    def _normalize_start(self, raw: str) -> str:
        s = raw.strip()
        if not s:
            return ""

        if " " in s and ":" in s:
            parts = s.split()
            if len(parts) >= 2:
                date_part = parts[0].replace("/", "-")
                time_part = parts[1]
                d = date_part.split("-")
                t = time_part.split(":")
                if len(d) == 3 and len(t) in (2, 3):
                    try:
                        yy = int(d[0])
                        mm = int(d[1])
                        dd = int(d[2])
                        hh = int(t[0])
                        mi = int(t[1])
                        ss = int(t[2]) if len(t) == 3 else 0
                        return f"{yy:04d}-{mm:02d}-{dd:02d}T{hh:02d}:{mi:02d}:{ss:02d}"
                    except ValueError:
                        pass

        s2 = s.replace("/", "-")
        if " " in s2 and "T" not in s2:
            s2 = s2.replace(" ", "T", 1)
        if "T" in s2 and len(s2) == 16:
            s2 += ":00"
        return s2

    def _event_queue_path(self) -> Path | None:
        conf = self._read_config()
        q = conf.get("event_queue")
        if not q:
            self._log("[WARN] event_queue path missing in config")
            return None
        return Path(q)

    def _read_event_rows(self, queue_path: Path) -> list[dict]:
        if not queue_path.exists():
            return []
        rows: list[dict] = []
        try:
            with queue_path.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.lstrip("\ufeff").strip()
                    if not line:
                        continue
                    rows.append(json.loads(line))
        except Exception as e:
            self._log(f"[WARN] failed to read queue: {e}")
            return []
        return rows

    def _set_picked_folders(self, event_dirs: list[str]) -> None:
        if not event_dirs:
            self._log("[INFO] pick結果から新規フォルダを検出できませんでした")
            return
        self.picked_combo["values"] = event_dirs
        self.picked_folder.set(event_dirs[0])
        self._log(f"[OK] picked folders loaded: {len(event_dirs)}")

    def reload_picked_from_queue(self) -> None:
        q = self._event_queue_path()
        if q is None:
            return
        rows = self._read_event_rows(q)
        event_dirs: list[str] = []
        for row in rows:
            ev = row.get("event_dir")
            if isinstance(ev, str) and ev:
                event_dirs.append(ev)
        event_dirs = list(dict.fromkeys(event_dirs))
        self._set_picked_folders(event_dirs)


    def _collect_manual_candidates(self) -> list[dict[str, Any]]:
        conf = self._read_config()
        if not conf:
            return []

        base_dir_raw = self.base_dir.get().strip() or str(conf.get("base_dir", ""))
        if not base_dir_raw:
            self._log("[WARN] base_dir is empty")
            return []

        base_dir = Path(base_dir_raw)
        if not base_dir.exists():
            self._log(f"[WARN] base_dir not found: {base_dir}")
            return []

        candidates_name = str(conf.get("candidates_name", "candidates_20s.jsonl"))

        try:
            min_motion = float(self.manual_min_motion.get().strip() or "-1e9")
            min_hits = float(self.manual_min_hits.get().strip() or "-1e9")
            limit = int(self.manual_limit.get().strip() or "300")
        except ValueError:
            self._log("[WARN] Invalid manual candidate filter values")
            return []

        out: list[dict[str, Any]] = []
        for cand_path in base_dir.rglob(candidates_name):
            try:
                with cand_path.open("r", encoding="utf-8") as f:
                    for idx, raw in enumerate(f):
                        line = raw.strip()
                        if not line:
                            continue
                        try:
                            row = json.loads(line)
                        except json.JSONDecodeError:
                            continue

                        if row.get("picked_at"):
                            continue
                        if row.get("video_id"):
                            continue
                        if "start_abs" not in row or "end_abs" not in row:
                            continue

                        motion = float(row.get("motion", -1.0))
                        hits = float(row.get("hits", -1.0))
                        if motion < min_motion or hits < min_hits:
                            continue

                        row2 = dict(row)
                        row2["_source_path"] = str(cand_path)
                        row2["_source_index"] = idx
                        row2["_session_dir"] = str(cand_path.parent)
                        out.append(row2)
            except OSError as e:
                self._log(f"[WARN] candidate read failed: {cand_path} ({e})")

        out.sort(key=lambda r: (float(r.get("motion", -1.0)), float(r.get("hits", -1.0))), reverse=True)
        if limit > 0:
            out = out[:limit]
        return out

    def load_manual_candidates(self) -> None:
        self.manual_candidates = self._collect_manual_candidates()
        self.manual_listbox.delete(0, tk.END)

        for row in self.manual_candidates:
            s = int(row.get("start_abs", 0))
            e = int(row.get("end_abs", 0))
            ev = f"{s:05d}_{e:05d}"
            motion = float(row.get("motion", -1.0))
            hits = float(row.get("hits", -1.0))
            sess = Path(str(row.get("_session_dir", ""))).name
            self.manual_listbox.insert(tk.END, f"{ev} | motion={motion:.3f} | hits={hits:.0f} | session={sess}")

        self._log(f"[OK] manual candidates loaded: {len(self.manual_candidates)}")

    def _parse_publish_start_jst(self) -> datetime:
        raw = self._normalize_start(self.publish_start.get())
        if not raw:
            return datetime.now(JST).replace(microsecond=0) + timedelta(minutes=5)

        try:
            dt = datetime.fromisoformat(raw)
        except ValueError:
            self._log(f"[WARN] invalid publish start; fallback now+5m: {raw}")
            return datetime.now(JST).replace(microsecond=0) + timedelta(minutes=5)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=JST)
        else:
            dt = dt.astimezone(JST)
        return dt

    def enqueue_selected_candidates(self) -> None:
        sel = list(self.manual_listbox.curselection())
        if not sel:
            self._log("[INFO] select candidates first")
            return

        conf = self._read_config()
        if not conf:
            return

        q = self._event_queue_path()
        if q is None:
            return

        try:
            pitch_raw = self.publish_pitch_hours.get().strip()
            pitch_hours = float(pitch_raw) if pitch_raw else float(conf.get("publish_pitch_hours", 3.0))
        except ValueError:
            self._log("[WARN] invalid publish pitch hours")
            return

        start_dt = self._parse_publish_start_jst()
        route = str(conf.get("route", "yolo_win"))

        selected_rows = [self.manual_candidates[i] for i in sel]
        picked_at = datetime.now(JST).replace(microsecond=0).isoformat()
        pick_id = picked_at.replace(":", "").replace("-", "")

        by_file: dict[str, list[int]] = {}
        for row in selected_rows:
            src = str(row.get("_source_path", ""))
            idx = int(row.get("_source_index", -1))
            if src and idx >= 0:
                by_file.setdefault(src, []).append(idx)

        for src, idxs in by_file.items():
            p2 = Path(src)
            try:
                src_rows = []
                with p2.open("r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            src_rows.append(json.loads(line))
                for idx in idxs:
                    if 0 <= idx < len(src_rows):
                        src_rows[idx]["picked_at"] = picked_at
                        src_rows[idx]["pick_id"] = pick_id
                p2.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in src_rows), encoding="utf-8")
            except Exception as e:
                self._log(f"[WARN] pick mark failed: {p2} ({e})")

        q.parent.mkdir(parents=True, exist_ok=True)
        with q.open("a", encoding="utf-8") as f:
            for i, row in enumerate(selected_rows):
                s = int(row.get("start_abs", 0))
                e = int(row.get("end_abs", 0))
                event_name = f"{s:05d}_{e:05d}"
                session_dir = Path(str(row.get("_session_dir", "")))
                event_dir = session_dir / "events" / event_name
                publish_at = (start_dt + timedelta(hours=i * pitch_hours)).isoformat()
                out_row = {
                    "session_dir": str(session_dir),
                    "event_name": event_name,
                    "event_dir": str(event_dir),
                    "frames_dir": str(event_dir / "images_cropped"),
                    "publishAt": publish_at,
                    "route": route,
                    "motion": row.get("motion"),
                    "hits": row.get("hits"),
                    "pick_reason": "manual_list_select",
                }
                f.write(json.dumps(out_row, ensure_ascii=False) + "\n")
                self._log(f"[QUEUE] {event_name} motion={float(row.get('motion', -1.0)):.3f} hits={float(row.get('hits', -1.0)):.0f} publishAt={publish_at}")

        self._log(f"[OK] selected candidates enqueued: {len(selected_rows)}")

    def run_build(self) -> None:
        cmd = [
            "python",
            str(SCRIPTS / "build_candidates_win.py"),
            "--config",
            self.config_path.get(),
            "--base-dir",
            self.base_dir.get(),
        ]
        self._run_cmd(cmd)

    def run_pick(self) -> None:
        q = self._event_queue_path()
        before_rows = self._read_event_rows(q) if q is not None else []

        cmd = [
            "python",
            str(SCRIPTS / "pick_global_candidates_win.py"),
            "--config",
            self.config_path.get(),
            "--base-dir",
            self.base_dir.get(),
            "--mode",
            self.pick_mode.get(),
            "--total",
            self.pick_total.get(),
            "--seed",
            self.pick_seed.get(),
            "--no-overlap",
        ]
        start = self._normalize_start(self.publish_start.get())
        if start:
            cmd.extend(["--start", start])
        pitch_hours = self.publish_pitch_hours.get().strip()
        if pitch_hours:
            cmd.extend(["--pitch-hours", pitch_hours])

        def on_done(rc: int) -> None:
            if rc != 0:
                self._log("[WARN] pick failed; picked folder list was not updated")
                return
            if q is None:
                return
            after_rows = self._read_event_rows(q)
            new_rows = after_rows[len(before_rows) :] if len(after_rows) >= len(before_rows) else after_rows
            event_dirs: list[str] = []
            for row in new_rows:
                ev = row.get("event_dir")
                if isinstance(ev, str) and ev:
                    event_dirs.append(ev)
            event_dirs = list(dict.fromkeys(event_dirs))
            self._set_picked_folders(event_dirs)

        self._run_cmd(cmd, on_done=on_done)

    def run_pipeline(self) -> None:
        cmd = ["python", str(SCRIPTS / "run_event_queue_pipeline_yolo_win.py"), "--config", self.config_path.get()]
        if self.review_before_api.get():
            cmd.append("--review-before-api")
        cmd.extend(["--review-action", self.review_action.get()])
        self._run_cmd(cmd)

    def run_upload(self) -> None:
        cmd = ["python", str(SCRIPTS / "upload_from_queue_win.py"), "--config", self.config_path.get()]
        self._run_cmd(cmd)

    def open_win_folder(self) -> None:
        subprocess.Popen(["explorer", str(ROOT)])

    def open_selected_picked_folder(self) -> None:
        p = self.picked_folder.get().strip()
        if not p:
            self._log("[INFO] 開く対象がありません。まず全体ピックを実行するか、一覧更新してください")
            return
        target = Path(p)
        if target.exists():
            subprocess.Popen(["explorer", str(target)])
            return

        parent_events = target.parent
        if parent_events.exists():
            self._log(f"[INFO] eventフォルダ未作成のため親を開きます: {parent_events}")
            subprocess.Popen(["explorer", str(parent_events)])
            return

        self._log(f"[WARN] フォルダが見つかりません: {target}")


if __name__ == "__main__":
    App().mainloop()



