from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any

import aiohttp
from sqlalchemy.orm import Session

from crawlers.platforms.registry import create_adapter
from crawlers.utils import dedup_hash, user_hash
from database.models import RawDanmu
from database.repositories.crawl_task_repo import CrawlTaskRepository
from database.repositories.raw_danmu_repo import RawDanmuRepository
from database.session import SessionLocal


logger = logging.getLogger(__name__)


async def run_pending_tasks_once(limit: int = 3) -> int:
    processed = 0
    db: Session = SessionLocal()
    try:
        task_repo = CrawlTaskRepository(db)
        pending = task_repo.list_pending(limit=limit)
        for task in pending:
            processed += 1
            try:
                task_repo.update_status(task.id, "RUNNING")
                db.commit()
                await _run_one_task(db=db, task_id=task.id)
                task_repo.update_status(task.id, "SUCCEEDED")
                db.commit()
            except Exception as e:
                db.rollback()
                task_repo.update_status(task.id, "FAILED", last_error=str(e), retry_count=(task.retry_count or 0) + 1)
                db.commit()
                logger.exception("task failed: %s", task.id)
    finally:
        db.close()
    return processed


async def _run_one_task(db: Session, task_id: int) -> None:
    task_repo = CrawlTaskRepository(db)
    task = task_repo.get(task_id)
    if task is None:
        return
    async with aiohttp.ClientSession() as session:
        adapter = create_adapter(task.platform, session=session)
        canonical_video_id = await adapter.resolve_video(task.video_id)
        cursor: dict[str, Any] = dict(task.cursor_json or {})
        raw_repo = RawDanmuRepository(db)

        current_segment: int | None = None
        inserted_since_commit = 0
        last_cursor: dict[str, Any] = dict(cursor)
        async for event in adapter.crawl_history(canonical_video_id, cursor=cursor):
            seg = event.raw_payload.get("segment_index")
            if isinstance(seg, int):
                if current_segment is None:
                    current_segment = seg
                elif seg != current_segment:
                    await _commit_segment(db, task_repo, task_id, last_cursor, inserted_since_commit, current_segment)
                    inserted_since_commit = 0
                    current_segment = seg
            user_id_h = user_hash(event.platform, event.user_id)
            d_hash = dedup_hash(event.platform, canonical_video_id, event.content, event.video_ts, user_id_h)
            item = RawDanmu(
                platform=event.platform,
                video_id=canonical_video_id,
                content=event.content,
                video_ts=event.video_ts,
                send_time=event.send_time,
                user_id_hash=user_id_h,
                user_level=event.user_level,
                like_count=event.like_count,
                dedup_hash=d_hash,
                raw_json=event.raw_payload,
            )
            inserted = raw_repo.insert_one(item)
            if inserted:
                inserted_since_commit += 1
            last_cursor = _cursor_from_event(last_cursor, event.raw_payload)
            if inserted_since_commit >= 2000:
                db.commit()
                inserted_since_commit = 0

        if inserted_since_commit > 0:
            db.commit()
        if current_segment is not None:
            last_cursor["segment_index"] = current_segment + 1
        task_repo.update_status(task_id, status="RUNNING", cursor_json=last_cursor)
        db.commit()


async def _commit_segment(
    db: Session, task_repo: CrawlTaskRepository, task_id: int, cursor: dict[str, Any], inserted_count: int, seg: int
) -> None:
    db.commit()
    new_cursor = dict(cursor)
    new_cursor["segment_index"] = seg + 1
    new_cursor["last_commit_at"] = datetime.utcnow().isoformat()
    new_cursor["inserted_in_segment"] = inserted_count
    task_repo.update_status(task_id, status="RUNNING", cursor_json=new_cursor)
    db.commit()


def _cursor_from_event(cursor: dict[str, Any], raw_payload: dict[str, Any]) -> dict[str, Any]:
    next_cursor = dict(cursor)
    for key in ("cid", "duration"):
        if key in raw_payload:
            next_cursor[key] = raw_payload.get(key)
    return next_cursor


async def run_forever(poll_interval_sec: float = 2.0) -> None:
    while True:
        processed = await run_pending_tasks_once()
        if processed == 0:
            await asyncio.sleep(poll_interval_sec)
