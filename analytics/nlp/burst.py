from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta
from math import sqrt

from sqlalchemy import select
from sqlalchemy.orm import Session

from analytics.statistical.time_series import video_bucket_start
from database.models import CleanDanmu, RawDanmu


def detect_bursty_tokens(
    db: Session,
    platform: str,
    video_id: str,
    pipeline_run_id: int,
    bucket_sec: int = 10,
    token_top_k: int = 200,
    burst_top_k: int = 30,
    z_threshold: float = 3.0,
    min_count: int = 10,
) -> dict:
    stmt = (
        select(RawDanmu.video_ts, CleanDanmu.tokens_json)
        .join(CleanDanmu, CleanDanmu.raw_id == RawDanmu.id)
        .where(
            CleanDanmu.platform == platform,
            CleanDanmu.video_id == video_id,
            CleanDanmu.pipeline_run_id == pipeline_run_id,
            RawDanmu.platform == platform,
            RawDanmu.video_id == video_id,
        )
        .order_by(RawDanmu.video_ts.asc())
    )

    bucket_tokens: dict[datetime, Counter[str]] = defaultdict(Counter)
    total_counter: Counter[str] = Counter()
    for video_ts, tokens in db.execute(stmt).all():
        if video_ts is None or not tokens:
            continue
        b = video_bucket_start(float(video_ts), bucket_sec)
        for t in tokens:
            if not t:
                continue
            bucket_tokens[b][t] += 1
            total_counter[t] += 1

    candidates = [t for t, _ in total_counter.most_common(token_top_k)]
    buckets = sorted(bucket_tokens.keys())
    if not buckets or not candidates:
        return {"items": []}

    items: list[dict] = []
    for token in candidates:
        series = [bucket_tokens[b].get(token, 0) for b in buckets]
        mean, std = _mean_std(series)
        if std <= 0:
            continue
        max_idx, max_val = max(enumerate(series), key=lambda x: x[1])
        if max_val < min_count:
            continue
        z = (max_val - mean) / std
        if z < z_threshold:
            continue
        segment = _segment_for_token(buckets, series, mean, std, z_threshold, min_count, bucket_sec)
        items.append(
            {
                "token": token,
                "peak_bucket_start_sec": int(buckets[max_idx].timestamp()),
                "peak_count": int(max_val),
                "z_score": float(z),
                "segment": segment,
            }
        )

    items.sort(key=lambda x: x["z_score"], reverse=True)
    return {"items": items[:burst_top_k]}


def _mean_std(values: list[int]) -> tuple[float, float]:
    if not values:
        return (0.0, 0.0)
    mean = sum(values) / len(values)
    var = sum((v - mean) ** 2 for v in values) / max(len(values) - 1, 1)
    return (float(mean), float(sqrt(var)))


def _segment_for_token(
    buckets: list[datetime], series: list[int], mean: float, std: float, z_threshold: float, min_count: int, bucket_sec: int
) -> dict:
    above = [v >= min_count and (v - mean) / std >= z_threshold for v in series]
    best = None
    current_start = None
    current_peak = 0
    current_peak_idx = -1
    for i, ok in enumerate(above):
        if ok:
            if current_start is None:
                current_start = i
                current_peak = series[i]
                current_peak_idx = i
            else:
                if series[i] > current_peak:
                    current_peak = series[i]
                    current_peak_idx = i
        else:
            if current_start is not None:
                best = _pick_best_segment(best, current_start, i - 1, current_peak, current_peak_idx, buckets, bucket_sec)
                current_start = None
                current_peak = 0
                current_peak_idx = -1
    if current_start is not None:
        best = _pick_best_segment(best, current_start, len(series) - 1, current_peak, current_peak_idx, buckets, bucket_sec)
    return best or {}


def _pick_best_segment(
    best: dict | None,
    start_i: int,
    end_i: int,
    peak: int,
    peak_i: int,
    buckets: list[datetime],
    bucket_sec: int,
) -> dict:
    seg = {
        "start_sec": int(buckets[start_i].timestamp()),
        "end_sec": int((buckets[end_i] + timedelta(seconds=bucket_sec)).timestamp()),
        "peak_sec": int(buckets[peak_i].timestamp()) if peak_i >= 0 else int(buckets[start_i].timestamp()),
        "peak_count": int(peak),
    }
    if best is None:
        return seg
    if seg["peak_count"] > best.get("peak_count", 0):
        return seg
    return best
