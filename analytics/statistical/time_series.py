from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from database.models import CleanDanmu, RawDanmu


def video_bucket_start(video_ts_sec: float, bucket_sec: int) -> datetime:
    bucket = int(video_ts_sec // bucket_sec) * bucket_sec
    return datetime.fromtimestamp(bucket, tz=timezone.utc)


def count_by_time_bucket(
    db: Session, platform: str, video_id: str, pipeline_run_id: int, bucket_sec: int
) -> list[tuple[datetime, int]]:
    stmt = (
        select(RawDanmu.video_ts)
        .join(CleanDanmu, CleanDanmu.raw_id == RawDanmu.id)
        .where(
            CleanDanmu.platform == platform,
            CleanDanmu.video_id == video_id,
            CleanDanmu.pipeline_run_id == pipeline_run_id,
            RawDanmu.platform == platform,
            RawDanmu.video_id == video_id,
        )
    )
    counts: Counter[datetime] = Counter()
    for (video_ts,) in db.execute(stmt).all():
        if video_ts is None:
            continue
        counts[video_bucket_start(float(video_ts), bucket_sec)] += 1
    return sorted(counts.items(), key=lambda x: x[0])


def sentiment_ratio_by_bucket(
    db: Session, platform: str, video_id: str, pipeline_run_id: int, bucket_sec: int, label: str
) -> list[tuple[datetime, float]]:
    time_stmt = (
        select(RawDanmu.video_ts, CleanDanmu.sentiment_label)
        .join(CleanDanmu, CleanDanmu.raw_id == RawDanmu.id)
        .where(
            CleanDanmu.platform == platform,
            CleanDanmu.video_id == video_id,
            CleanDanmu.pipeline_run_id == pipeline_run_id,
            RawDanmu.platform == platform,
            RawDanmu.video_id == video_id,
        )
    )
    total: Counter[datetime] = Counter()
    matched: Counter[datetime] = Counter()
    for video_ts, s_label in db.execute(time_stmt).all():
        if video_ts is None:
            continue
        bucket_start = video_bucket_start(float(video_ts), bucket_sec)
        total[bucket_start] += 1
        if s_label == label:
            matched[bucket_start] += 1
    series: list[tuple[datetime, float]] = []
    for bucket_start in sorted(total.keys()):
        denom = total[bucket_start]
        series.append((bucket_start, matched[bucket_start] / denom if denom else 0.0))
    return series


def user_activity_summary(db: Session, platform: str, video_id: str, pipeline_run_id: int, top_n: int = 20) -> dict:
    raw_ids_stmt = select(CleanDanmu.raw_id).where(
        CleanDanmu.platform == platform,
        CleanDanmu.video_id == video_id,
        CleanDanmu.pipeline_run_id == pipeline_run_id,
    )
    subq = raw_ids_stmt.subquery()
    user_stmt = (
        select(RawDanmu.user_id_hash, func.count(RawDanmu.id))
        .where(
            RawDanmu.platform == platform,
            RawDanmu.video_id == video_id,
            RawDanmu.id.in_(select(subq.c.raw_id)),
            RawDanmu.user_id_hash.is_not(None),
        )
        .group_by(RawDanmu.user_id_hash)
        .order_by(func.count(RawDanmu.id).desc())
    )
    rows = list(db.execute(user_stmt).all())
    unique_users = len(rows)
    top = [{"user_id_hash": u, "count": int(c)} for (u, c) in rows[:top_n]]
    total_msgs = int(sum(int(c) for (_, c) in rows))
    top10_share = float(sum(int(c) for (_, c) in rows[:10]) / total_msgs) if total_msgs else 0.0
    return {"unique_users": unique_users, "top_users": top, "top10_share": top10_share}

