from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from database.models import RawDanmu


class RawDanmuRepository:
    def __init__(self, db: Session) -> None:
        self._db = db

    def insert_one(self, item: RawDanmu) -> bool:
        try:
            with self._db.begin_nested():
                self._db.add(item)
                self._db.flush()
        except IntegrityError:
            return False
        return True

    def list_by_video(self, platform: str, video_id: str, limit: int = 1000) -> list[RawDanmu]:
        stmt = (
            select(RawDanmu)
            .where(RawDanmu.platform == platform, RawDanmu.video_id == video_id)
            .order_by(RawDanmu.video_ts.asc(), RawDanmu.id.asc())
            .limit(limit)
        )
        return list(self._db.execute(stmt).scalars().all())
