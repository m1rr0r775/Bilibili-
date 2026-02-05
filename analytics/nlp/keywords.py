from __future__ import annotations

from collections import Counter

from sqlalchemy import select
from sqlalchemy.orm import Session

from database.models import CleanDanmu


_STOP_TOKENS = {
    "的",
    "了",
    "啊",
    "呀",
    "吗",
    "吧",
    "和",
    "是",
    "我",
    "你",
    "他",
    "她",
    "它",
    "这",
    "那",
    "就",
    "都",
    "也",
    "不",
    "很",
    "在",
    "有",
}


def top_keywords(db: Session, platform: str, video_id: str, pipeline_run_id: int, top_k: int = 50) -> list[dict]:
    stmt = select(CleanDanmu.tokens_json).where(
        CleanDanmu.platform == platform,
        CleanDanmu.video_id == video_id,
        CleanDanmu.pipeline_run_id == pipeline_run_id,
    )
    counter: Counter[str] = Counter()
    for (tokens,) in db.execute(stmt).all():
        if not tokens:
            continue
        for t in tokens:
            if not t or t in _STOP_TOKENS:
                continue
            if len(t) == 1 and not t.isalnum():
                continue
            counter[t] += 1
    return [{"token": k, "count": int(v)} for k, v in counter.most_common(top_k)]

