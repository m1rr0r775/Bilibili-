from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from analytics.nlp.burst import detect_bursty_tokens
from analytics.nlp.keywords import top_keywords
from analytics.social.mentions import build_mention_network_summary
from analytics.statistical.peak import detect_peaks
from analytics.statistical.cognitive import cognitive_metrics_by_bucket
from analytics.statistical.time_series import count_by_time_bucket, sentiment_ratio_by_bucket, user_activity_summary
from analytics.statistical.user_profile import danmu_type_distribution, user_segmentation_summary
from database.models import MetricsSummary, MetricsTimeSeries, PipelineRun


logger = logging.getLogger(__name__)


def run_analysis(db: Session, platform: str, video_id: str, pipeline_run_id: int) -> None:
    run = db.execute(select(PipelineRun).where(PipelineRun.id == pipeline_run_id)).scalars().first()
    if run is None:
        raise ValueError(f"pipeline_run 不存在: {pipeline_run_id}")

    db.execute(delete(MetricsTimeSeries).where(MetricsTimeSeries.pipeline_run_id == pipeline_run_id))
    db.execute(delete(MetricsSummary).where(MetricsSummary.pipeline_run_id == pipeline_run_id))
    db.commit()

    for bucket_sec in (10, 60):
        count_series = count_by_time_bucket(db, platform=platform, video_id=video_id, pipeline_run_id=pipeline_run_id, bucket_sec=bucket_sec)
        for bucket_start, count in count_series:
            db.add(
                MetricsTimeSeries(
                    platform=platform,
                    video_id=video_id,
                    metric_name="danmu_count",
                    bucket_start=bucket_start,
                    bucket_sec=bucket_sec,
                    value=float(count),
                    pipeline_run_id=pipeline_run_id,
                )
            )
        for label, name in (("positive", "sentiment_positive_ratio"), ("negative", "sentiment_negative_ratio")):
            ratio_series = sentiment_ratio_by_bucket(
                db, platform=platform, video_id=video_id, pipeline_run_id=pipeline_run_id, bucket_sec=bucket_sec, label=label
            )
            for bucket_start, ratio in ratio_series:
                db.add(
                    MetricsTimeSeries(
                        platform=platform,
                        video_id=video_id,
                        metric_name=name,
                        bucket_start=bucket_start,
                        bucket_sec=bucket_sec,
                        value=float(ratio),
                        pipeline_run_id=pipeline_run_id,
                    )
                )
        db.commit()

    count_10s = count_by_time_bucket(db, platform=platform, video_id=video_id, pipeline_run_id=pipeline_run_id, bucket_sec=10)
    peaks = detect_peaks(count_10s)
    peak_segments = [
        {"start_sec": int(p.start.timestamp()), "end_sec": int(p.end.timestamp()), "peak_count": int(p.peak_value)} for p in peaks
    ]
    db.add(
        MetricsSummary(
            platform=platform,
            video_id=video_id,
            metric_name="high_energy_segments",
            value_json={"segments": peak_segments, "count": len(peak_segments)},
            pipeline_run_id=pipeline_run_id,
        )
    )

    keywords = top_keywords(db, platform=platform, video_id=video_id, pipeline_run_id=pipeline_run_id, top_k=50)
    db.add(
        MetricsSummary(
            platform=platform,
            video_id=video_id,
            metric_name="top_keywords",
            value_json={"items": keywords},
            pipeline_run_id=pipeline_run_id,
        )
    )

    user_summary = user_activity_summary(db, platform=platform, video_id=video_id, pipeline_run_id=pipeline_run_id)
    db.add(
        MetricsSummary(
            platform=platform,
            video_id=video_id,
            metric_name="user_activity",
            value_json=user_summary,
            pipeline_run_id=pipeline_run_id,
        )
    )

    cognitive = cognitive_metrics_by_bucket(db, platform=platform, video_id=video_id, pipeline_run_id=pipeline_run_id, bucket_sec=10)
    for metric_name, series in cognitive.items():
        for bucket_start, v in series:
            db.add(
                MetricsTimeSeries(
                    platform=platform,
                    video_id=video_id,
                    metric_name=metric_name,
                    bucket_start=bucket_start,
                    bucket_sec=10,
                    value=float(v),
                    pipeline_run_id=pipeline_run_id,
                )
            )
    db.commit()

    mention_network = build_mention_network_summary(db, platform=platform, video_id=video_id, pipeline_run_id=pipeline_run_id)
    db.add(
        MetricsSummary(
            platform=platform,
            video_id=video_id,
            metric_name="danmu_mention_network",
            value_json=mention_network,
            pipeline_run_id=pipeline_run_id,
        )
    )

    bursts = detect_bursty_tokens(db, platform=platform, video_id=video_id, pipeline_run_id=pipeline_run_id, bucket_sec=10)
    db.add(
        MetricsSummary(
            platform=platform,
            video_id=video_id,
            metric_name="danmu_bursty_tokens",
            value_json=bursts,
            pipeline_run_id=pipeline_run_id,
        )
    )

    segments = user_segmentation_summary(db, platform=platform, video_id=video_id, pipeline_run_id=pipeline_run_id)
    db.add(
        MetricsSummary(
            platform=platform,
            video_id=video_id,
            metric_name="danmu_user_segments",
            value_json=segments,
            pipeline_run_id=pipeline_run_id,
        )
    )

    type_dist = danmu_type_distribution(db, platform=platform, video_id=video_id, pipeline_run_id=pipeline_run_id)
    db.add(
        MetricsSummary(
            platform=platform,
            video_id=video_id,
            metric_name="danmu_type_distribution",
            value_json=type_dist,
            pipeline_run_id=pipeline_run_id,
        )
    )

    db.add(
        MetricsSummary(
            platform=platform,
            video_id=video_id,
            metric_name="analysis_meta",
            value_json={"generated_at": datetime.now(tz=timezone.utc).isoformat(), "pipeline_run_id": pipeline_run_id},
            pipeline_run_id=pipeline_run_id,
        )
    )
    db.commit()
    logger.info("analysis done platform=%s video_id=%s pipeline_run_id=%s", platform, video_id, pipeline_run_id)
