from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from data_pipeline.cleaner.spam_filter import is_spam
from data_pipeline.cleaner.text_cleaner import is_empty_or_noise, normalize_text
from data_pipeline.transformer.sentiment import score_sentiment
from data_pipeline.transformer.tokenizer import tokenize
from database.models import CleanDanmu, PipelineRun, RawDanmu


logger = logging.getLogger(__name__)


def run_pipeline(db: Session, platform: str, video_id: str, config_json: dict[str, Any] | None = None) -> PipelineRun:
    run = PipelineRun(platform=platform, video_id=video_id, status="RUNNING", config_json=config_json)
    db.add(run)
    db.flush()

    db.execute(delete(CleanDanmu).where(CleanDanmu.platform == platform, CleanDanmu.video_id == video_id))
    db.commit()

    stmt = select(RawDanmu).where(RawDanmu.platform == platform, RawDanmu.video_id == video_id).order_by(RawDanmu.id.asc())
    result = db.execute(stmt).scalars()

    inserted = 0
    skipped = 0
    for raw in result:
        content_norm = normalize_text(raw.content)
        if is_empty_or_noise(content_norm) or is_spam(content_norm):
            skipped += 1
            continue
        tokens = tokenize(content_norm)
        sentiment_label, sentiment_score = score_sentiment(tokens)
        danmu_type = _map_bilibili_danmu_type(platform, raw.raw_json)
        clean = CleanDanmu(
            raw_id=raw.id,
            platform=platform,
            video_id=video_id,
            content_norm=content_norm,
            tokens_json=tokens,
            sentiment_label=sentiment_label,
            sentiment_score=sentiment_score,
            danmu_type=danmu_type,
            pipeline_run_id=run.id,
        )
        db.add(clean)
        inserted += 1
        if inserted % 2000 == 0:
            db.commit()
            logger.info("pipeline %s inserted=%s skipped=%s", run.id, inserted, skipped)

    db.commit()
    run.status = "SUCCEEDED"
    run.finished_at = datetime.now(tz=timezone.utc)
    db.add(run)
    db.commit()
    logger.info("pipeline done %s inserted=%s skipped=%s", run.id, inserted, skipped)
    return run


def _map_bilibili_danmu_type(platform: str, raw_json: dict[str, Any]) -> str | None:
    if platform != "bilibili":
        return None
    mode = raw_json.get("mode")
    if mode == 1:
        return "scroll"
    if mode == 4:
        return "bottom"
    if mode == 5:
        return "top"
    return "other"
