from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from database.models import Video


class VideoRepository:
    def __init__(self, db: Session) -> None:
        self._db = db

    def get_by_platform_video_id(self, platform: str, video_id: str) -> Video | None:
        stmt = select(Video).where(Video.platform == platform, Video.video_id == video_id)
        return self._db.execute(stmt).scalars().first()

    def get_or_create(self, platform: str, video_id: str, title: str | None = None, metadata_json: dict | None = None) -> Video:
        existing = self.get_by_platform_video_id(platform=platform, video_id=video_id)
        if existing is not None:
            return existing
        video = Video(platform=platform, video_id=video_id, title=title, metadata_json=metadata_json)
        self._db.add(video)
        self._db.flush()
        return video

