from __future__ import annotations

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from database.models import CrawlTask


class CrawlTaskRepository:
    def __init__(self, db: Session) -> None:
        self._db = db

    def create(self, platform: str, video_id: str, task_type: str, cursor_json: dict | None = None) -> CrawlTask:
        task = CrawlTask(platform=platform, video_id=video_id, task_type=task_type, status="PENDING", cursor_json=cursor_json)
        self._db.add(task)
        self._db.flush()
        return task

    def get(self, task_id: int) -> CrawlTask | None:
        stmt = select(CrawlTask).where(CrawlTask.id == task_id)
        return self._db.execute(stmt).scalars().first()

    def list_pending(self, limit: int = 10) -> list[CrawlTask]:
        stmt = select(CrawlTask).where(CrawlTask.status == "PENDING").order_by(CrawlTask.created_at.asc()).limit(limit)
        return list(self._db.execute(stmt).scalars().all())

    def update_status(
        self,
        task_id: int,
        status: str,
        cursor_json: dict | None = None,
        last_error: str | None = None,
        retry_count: int | None = None,
    ) -> None:
        values: dict[str, object] = {"status": status}
        if cursor_json is not None:
            values["cursor_json"] = cursor_json
        if last_error is not None:
            values["last_error"] = last_error
        if retry_count is not None:
            values["retry_count"] = retry_count
        self._db.execute(update(CrawlTask).where(CrawlTask.id == task_id).values(**values))

