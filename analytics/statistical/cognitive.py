from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
from math import log

from sqlalchemy import select
from sqlalchemy.orm import Session

from analytics.statistical.time_series import video_bucket_start
from database.models import CleanDanmu, RawDanmu


def cognitive_metrics_by_bucket(
    db: Session, platform: str, video_id: str, pipeline_run_id: int, bucket_sec: int
) -> dict[str, list[tuple[datetime, float]]]:
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

    token_counts_by_bucket: dict[datetime, Counter[str]] = defaultdict(Counter)
    total_tokens_by_bucket: Counter[datetime] = Counter()
    unique_tokens_by_bucket: dict[datetime, set[str]] = defaultdict(set)
    for video_ts, tokens in db.execute(stmt).all():
        if video_ts is None:
            continue
        bucket_start = video_bucket_start(float(video_ts), bucket_sec)
        if not tokens:
            continue
        for t in tokens:
            if not t:
                continue
            token_counts_by_bucket[bucket_start][t] += 1
            total_tokens_by_bucket[bucket_start] += 1
            unique_tokens_by_bucket[bucket_start].add(t)

    buckets = sorted(set(total_tokens_by_bucket.keys()) | set(token_counts_by_bucket.keys()))
    tokens_per_sec: list[tuple[datetime, float]] = []
    entropy: list[tuple[datetime, float]] = []
    unique_ratio: list[tuple[datetime, float]] = []
    for b in buckets:
        total = float(total_tokens_by_bucket[b])
        tokens_per_sec.append((b, total / float(bucket_sec) if bucket_sec > 0 else 0.0))
        uniq = float(len(unique_tokens_by_bucket[b]))
        unique_ratio.append((b, (uniq / total) if total > 0 else 0.0))
        entropy.append((b, _shannon_entropy(token_counts_by_bucket[b])))

    return {"cognitive_tokens_per_sec": tokens_per_sec, "cognitive_entropy": entropy, "cognitive_unique_ratio": unique_ratio}


def _shannon_entropy(counter: Counter[str]) -> float:
    total = sum(counter.values())
    if total <= 0:
        return 0.0
    h = 0.0
    for c in counter.values():
        p = c / total
        if p > 0:
            h -= p * log(p)
    return float(h)

