#!/usr/bin/env python3
# YOLO(cat代理)で「区間(9-15s) + 固定crop_x」を決める（API1相当のローカル版）
# 目的：まさおセンター最優先。人は切れてOK。discardあり（9秒未満はreject）。
import json
import statistics
from pathlib import Path

from ultralytics import YOLO
from PIL import Image

DEFAULT_MODEL = "yolov8n.pt"

def safe_mkdir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def clamp(x, lo, hi):
    return max(lo, min(hi, x))

def load_image_size(img_path: Path):
    with Image.open(img_path) as im:
        return im.size  # (W, H)

def best_det_for_labels(result, labels_set):
    """
    result: ultralytics result[0]
    returns: (conf, (x1,y1,x2,y2), label) or (0,None,None)
    """
    boxes = result.boxes
    if boxes is None or len(boxes) == 0:
        return 0.0, None, None
    names = result.names
    best = (0.0, None, None)
    for b in boxes:
        cls_id = int(b.cls[0])
        label = names.get(cls_id, str(cls_id))
        if label not in labels_set:
            continue
        conf = float(b.conf[0])
        xyxy = [float(x) for x in b.xyxy[0].tolist()]
        if conf > best[0]:
            best = (conf, xyxy, label)
    return best

def window_score(confs, s, e):
    # s..e inclusive
    return sum(confs[s:e+1])

def window_hit_ratio(confs, s, e, conf_min):
    n = e - s + 1
    hits = sum(1 for i in range(s, e+1) if confs[i] >= conf_min)
    return hits / n if n > 0 else 0.0

def pick_best_window(confs, min_len, max_len, conf_min):
    """
    9-15秒の範囲で、sum(conf)最大の窓を選ぶ。
    tie-break: hit_ratioが高い方 → 長い方
    """
    n = len(confs)
    best = None
    for L in range(min_len, max_len+1):
        for s in range(0, n - L + 1):
            e = s + L - 1
            sc = window_score(confs, s, e)
            hr = window_hit_ratio(confs, s, e, conf_min)
            cand = (sc, hr, L, s, e)
            if best is None or cand > best:
                best = cand
    return best  # (score, hit_ratio, L, s, e)

def compute_crop_x_from_centers(centers_x, crop_w, W):
    if not centers_x:
        return None
    cx = statistics.median(centers_x)
    crop_x = cx - (crop_w / 2.0)
    crop_x = clamp(crop_x, 0.0, W - crop_w)
    # crop_xはpxなので整数が扱いやすい
    return int(round(crop_x))

def inside_ratio_for_crop(bboxes, s, e, crop_x, crop_w, confs, conf_min):
    """
    bbox中心が枠内に入る率（conf>=conf_minのフレームだけ対象）
    """
    ok = 0
    total = 0
    left = crop_x
    right = crop_x + crop_w
    for i in range(s, e+1):
        if confs[i] < conf_min:
            continue
        bb = bboxes[i]
        if not bb:
            continue
        x1, y1, x2, y2 = bb
        cx = (x1 + x2) / 2.0
        total += 1
        if left <= cx <= right:
            ok += 1
    if total == 0:
        return 0.0
    return ok / total

def trim_window_to_meet_ratio(confs, bboxes, s, e, conf_min, target_ratio, min_len, crop_w, W):
    """
    crop_xをその都度計算し直しつつ、端から削って target_ratio を満たすまで調整。
    目的：まさおセンター固定で「入ってる率」を上げる。
    """
    while (e - s + 1) >= min_len:
        # 現区間のcrop_x算出
        centers = []
        for i in range(s, e+1):
            if confs[i] < conf_min:
                continue
            bb = bboxes[i]
            if not bb:
                continue
            x1, y1, x2, y2 = bb
            centers.append((x1 + x2) / 2.0)
        crop_x = compute_crop_x_from_centers(centers, crop_w, W)
        if crop_x is None:
            # 検出が無いのでトリムしても無理
            return None

        ratio = inside_ratio_for_crop(bboxes, s, e, crop_x, crop_w, confs, conf_min)
        if ratio >= target_ratio:
            return (s, e, crop_x, ratio)

        # 端を削る候補（左 or 右）
        if (e - s + 1) == min_len:
            break

        # 左を削った場合
        s1, e1 = s + 1, e
        # 右を削った場合
        s2, e2 = s, e - 1

        # それぞれの「改善度」を比較（ratio優先、次にscore）
        def eval_window(ss, ee):
            centers2 = []
            for i in range(ss, ee+1):
                if confs[i] < conf_min:
                    continue
                bb = bboxes[i]
                if not bb:
                    continue
                x1, y1, x2, y2 = bb
                centers2.append((x1 + x2) / 2.0)
            cx2 = compute_crop_x_from_centers(centers2, crop_w, W)
            if cx2 is None:
                return (-1.0, -1.0, None)  # worst
            r2 = inside_ratio_for_crop(bboxes, ss, ee, cx2, crop_w, confs, conf_min)
            sc2 = window_score(confs, ss, ee)
            return (r2, sc2, cx2)

        r1, sc1, cx1 = eval_window(s1, e1)
        r2, sc2, cx2 = eval_window(s2, e2)

        # ratio優先、次にscore
        if (r1, sc1) >= (r2, sc2):
            s, e = s1, e1
        else:
            s, e = s2, e2

    return None

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--frames-dir", required=True, help=".../frames_360/<EVENT>")
    ap.add_argument("--event-dir", required=True, help=".../events/<EVENT>")
    ap.add_argument("--model", default=DEFAULT_MODEL)
    ap.add_argument("--labels", default="cat", help='comma separated labels treated as "masao proxy" (default: cat)')
    ap.add_argument("--conf-min", type=float, default=0.40, help="conf threshold for counting as hit (tune here)")
    ap.add_argument("--target-ratio", type=float, default=0.70, help="bbox center inside crop ratio target (tune here)")
    ap.add_argument("--min-sec", type=int, default=9)
    ap.add_argument("--max-sec", type=int, default=15)
    ap.add_argument("--max-frames", type=int, default=60)
    ap.add_argument("--every", type=int, default=1, help="process every Nth frame (1=all). if >1, time resolution changes.")
    ap.add_argument("--crop-w", type=float, default=202.5, help="analysis crop width in px for 640x360")
    args = ap.parse_args()

    frames_dir = Path(args.frames_dir)
    event_dir = Path(args.event_dir)

    out_dir = event_dir / "yolo" / "v1"
    safe_mkdir(out_dir)

    done_flag = out_dir / ".yolo_done"
    reject_flag = out_dir / ".yolo_reject"
    reject_reason = out_dir / "reject_reason.json"
    yolo_jsonl = out_dir / "yolo_frames.jsonl"
    decision_json = out_dir / "decision.json"

    # 既にdoneなら何もしない
    if done_flag.exists():
        print(f"[SKIP] already done: {done_flag}")
        return

    # 入力画像
    imgs = sorted(frames_dir.glob("*.jpg"))
    if not imgs:
        raise SystemExit(f"[ERR] no jpg: {frames_dir}")

    imgs = imgs[:args.max_frames]
    imgs = imgs[::max(1, args.every)]
    n = len(imgs)
    if n < args.min_sec:
        reject_flag.write_text("", encoding="utf-8")
        reject_reason.write_text(json.dumps({"reason":"too_few_frames","num_frames":n}, ensure_ascii=False, indent=2), encoding="utf-8")
        print("[REJECT] too few frames")
        return

    # 画像サイズ（analysis世界）
    W, H = load_image_size(imgs[0])

    labels_set = set([s.strip() for s in args.labels.split(",") if s.strip()])
    model = YOLO(args.model)

    confs = [0.0] * n
    bboxes = [None] * n
    det_labels = [None] * n

    # YOLO実行＆ログ化
    with yolo_jsonl.open("w", encoding="utf-8") as f:
        for i, p in enumerate(imgs):
            r = model.predict(source=str(p), conf=0.01, verbose=False)[0]  # conf_minは後段で使う
            conf, bb, lab = best_det_for_labels(r, labels_set)
            confs[i] = conf
            bboxes[i] = bb
            det_labels[i] = lab
            row = {
                "idx": i,
                "frame": p.name,
                "conf": conf,
                "label": lab,
                "bbox_xyxy": bb,
            }
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    # 9-15秒のベスト窓選定
    best = pick_best_window(confs, args.min_sec, min(args.max_sec, n), args.conf_min)
    if best is None:
        reject_flag.write_text("", encoding="utf-8")
        reject_reason.write_text(json.dumps({"reason":"no_window"}, ensure_ascii=False, indent=2), encoding="utf-8")
        print("[REJECT] no window")
        return

    score, hit_ratio, L, s, e = best

    # crop_xと「枠内率」達成までトリム
    trimmed = trim_window_to_meet_ratio(
        confs, bboxes, s, e,
        conf_min=args.conf_min,
        target_ratio=args.target_ratio,
        min_len=args.min_sec,
        crop_w=args.crop_w,
        W=W
    )
    if trimmed is None:
        reject_flag.write_text("", encoding="utf-8")
        reject_reason.write_text(json.dumps({
            "reason":"cannot_meet_ratio",
            "picked_window": {"s":s,"e":e,"len":L,"score":score,"hit_ratio":hit_ratio},
            "conf_min": args.conf_min,
            "target_ratio": args.target_ratio
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        print("[REJECT] cannot meet ratio (trimmed below min_sec)")
        return

    s2, e2, crop_x, inside_ratio = trimmed
    L2 = e2 - s2 + 1

    decision = {
        "start_sec_rel": int(s2),
        "end_sec_rel": int(e2 + 1),  # endは排他的にする（君の既存decisionに合わせやすい）
        "duration_sec": int(L2),
        "crop_x": int(crop_x),
        "analysis_width": int(W),
        "analysis_height": int(H),
        "crop_w": float(args.crop_w),
        "method": "yolo_cat_proxy_api1_like",
        "model": args.model,
        "labels": sorted(list(labels_set)),
        "conf_min": float(args.conf_min),
        "target_ratio": float(args.target_ratio),
        "inside_ratio_achieved": float(inside_ratio),
        "note": "human may be cut; masao center prioritized"
    }

    decision_json.write_text(json.dumps(decision, ensure_ascii=False, indent=2), encoding="utf-8")
    done_flag.write_text("", encoding="utf-8")

    # rejectが残っていたら消しておく
    if reject_flag.exists():
        try: reject_flag.unlink()
        except: pass

    print("[OK] wrote:")
    print(f" - {yolo_jsonl}")
    print(f" - {decision_json}")
    print(f" - {done_flag}")

if __name__ == "__main__":
    main()
