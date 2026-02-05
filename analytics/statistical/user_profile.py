from __future__ import annotations

from collections import Counter, defaultdict
from statistics import mean

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from database.models import CleanDanmu, RawDanmu


def danmu_type_distribution(db: Session, platform: str, video_id: str, pipeline_run_id: int) -> dict:
    stmt = (
        select(CleanDanmu.danmu_type, func.count(CleanDanmu.id))
        .where(
            CleanDanmu.platform == platform,
            CleanDanmu.video_id == video_id,
            CleanDanmu.pipeline_run_id == pipeline_run_id,
        )
        .group_by(CleanDanmu.danmu_type)
        .order_by(func.count(CleanDanmu.id).desc())
    )
    items = [{"type": t or "unknown", "count": int(c)} for t, c in db.execute(stmt).all()]
    total = sum(i["count"] for i in items)
    return {"total": int(total), "items": items}


def user_segmentation_summary(db: Session, platform: str, video_id: str, pipeline_run_id: int, top_n: int = 20) -> dict:
    stmt = (
        select(RawDanmu.user_id_hash, CleanDanmu.content_norm)
        .join(CleanDanmu, CleanDanmu.raw_id == RawDanmu.id)
        .where(
            CleanDanmu.platform == platform,
            CleanDanmu.video_id == video_id,
            CleanDanmu.pipeline_run_id == pipeline_run_id,
            RawDanmu.platform == platform,
            RawDanmu.video_id == video_id,
            RawDanmu.user_id_hash.is_not(None),
        )
        .order_by(RawDanmu.id.asc())
    )

    counts: Counter[str] = Counter()
    lengths: dict[str, list[int]] = defaultdict(list)
    content_uniqs: dict[str, set[str]] = defaultdict(set)
    for user_hash, content in db.execute(stmt).all():
        if not user_hash or not content:
            continue
        counts[user_hash] += 1
        lengths[user_hash].append(len(content))
        content_uniqs[user_hash].add(content)

    segments: dict[str, list[str]] = defaultdict(list)
    segment_stats: dict[str, dict] = defaultdict(dict)
    for user_hash, c in counts.items():
        avg_len = mean(lengths[user_hash]) if lengths[user_hash] else 0.0
        uniq_ratio = len(content_uniqs[user_hash]) / c if c > 0 else 0.0
        seg = _classify_user(c, avg_len, uniq_ratio)
        segments[seg].append(user_hash)
        segment_stats[user_hash] = {"count": int(c), "avg_len": float(avg_len), "unique_ratio": float(uniq_ratio), "segment": seg}

    segment_counts = {k: len(v) for k, v in segments.items()}
    top_users = [{"user_id_hash": u, **segment_stats[u]} for u, _ in counts.most_common(top_n)]
    return {"segment_counts": segment_counts, "top_users": top_users}


def _classify_user(count: int, avg_len: float, unique_ratio: float) -> str:
    if count <= 1:
        return "low"
    if count >= 50 and unique_ratio < 0.6:
        return "spam_suspect"
    if count >= 50:
        return "heavy"
    if count >= 10:
        return "active"
    if unique_ratio < 0.5:
        return "repeat_suspect"
    return "normal"

