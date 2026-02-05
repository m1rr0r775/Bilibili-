from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import timezone
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from analytics.runner import run_analysis
from cache.file_backend import FileCacheBackend
from config.logging import configure_logging
from config.settings import settings
from crawlers.platforms.registry import create_adapter
from crawlers.scheduler import run_forever
from data_pipeline.loader.pipeline_runner import run_pipeline
from database.init_db import init_db
from database.models import MetricsSummary, MetricsTimeSeries, PipelineRun
from database.repositories.crawl_task_repo import CrawlTaskRepository
from database.repositories.video_repo import VideoRepository
from database.session import get_db
from visualization.report.html_report import generate_html_report


logger = logging.getLogger(__name__)


class CreateCrawlTaskRequest(BaseModel):
    platform: str = Field(default="bilibili")
    video_input: str
    task_type: str = Field(default="history")


class CreateCrawlTaskResponse(BaseModel):
    task_id: int
    platform: str
    video_id: str
    status: str


class RunAnalyticsRequest(BaseModel):
    platform: str = Field(default="bilibili")
    video_id: str


class RunAnalyticsResponse(BaseModel):
    pipeline_run_id: int


class GenerateReportResponse(BaseModel):
    path: str


app = FastAPI(title="弹幕爬取与数据分析系统", version="0.1.0")
templates = Jinja2Templates(directory="visualization/dashboard/templates")
cache_backend = FileCacheBackend(settings.cache_dir)


@app.on_event("startup")
async def _startup() -> None:
    configure_logging(settings.log_level)
    init_db()
    app.state.poller_task = asyncio.create_task(run_forever())


@app.on_event("shutdown")
async def _shutdown() -> None:
    task: asyncio.Task | None = getattr(app.state, "poller_task", None)
    if task is not None:
        task.cancel()
        with contextlib.suppress(BaseException):
            await task


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/tasks/crawl", response_model=CreateCrawlTaskResponse)
async def create_crawl_task(req: CreateCrawlTaskRequest, db: Session = Depends(get_db)) -> CreateCrawlTaskResponse:
    adapter = create_adapter(req.platform)
    canonical_video_id = await adapter.resolve_video(req.video_input)
    VideoRepository(db).get_or_create(platform=req.platform, video_id=canonical_video_id)
    task = CrawlTaskRepository(db).create(platform=req.platform, video_id=canonical_video_id, task_type=req.task_type)
    db.commit()
    return CreateCrawlTaskResponse(task_id=task.id, platform=task.platform, video_id=task.video_id, status=task.status)


@app.get("/tasks/{task_id}")
def get_task(task_id: int, db: Session = Depends(get_db)) -> dict[str, Any]:
    task = CrawlTaskRepository(db).get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="task not found")
    return {
        "id": task.id,
        "platform": task.platform,
        "video_id": task.video_id,
        "task_type": task.task_type,
        "status": task.status,
        "cursor_json": task.cursor_json,
        "retry_count": task.retry_count,
        "last_error": task.last_error,
        "created_at": task.created_at,
        "updated_at": task.updated_at,
    }


@app.post("/tasks/{task_id}/retry")
def retry_task(task_id: int, db: Session = Depends(get_db)) -> dict[str, Any]:
    repo = CrawlTaskRepository(db)
    task = repo.get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="task not found")
    repo.update_status(task_id, status="PENDING", last_error=None)
    db.commit()
    return {"id": task_id, "status": "PENDING"}


@app.post("/analytics/run", response_model=RunAnalyticsResponse)
def run_pipeline_and_analysis(req: RunAnalyticsRequest, db: Session = Depends(get_db)) -> RunAnalyticsResponse:
    run = run_pipeline(db, platform=req.platform, video_id=req.video_id, config_json=None)
    run_analysis(db, platform=req.platform, video_id=req.video_id, pipeline_run_id=run.id)
    cache_backend.cleanup()
    return RunAnalyticsResponse(pipeline_run_id=run.id)


@app.get("/analytics/time_series")
def get_time_series(
    platform: str,
    video_id: str,
    metric_name: str,
    bucket_sec: int = 10,
    db: Session = Depends(get_db),
) -> list[dict[str, Any]]:
    stmt = (
        select(MetricsTimeSeries)
        .where(
            MetricsTimeSeries.platform == platform,
            MetricsTimeSeries.video_id == video_id,
            MetricsTimeSeries.metric_name == metric_name,
            MetricsTimeSeries.bucket_sec == bucket_sec,
        )
        .order_by(MetricsTimeSeries.bucket_start.asc())
    )
    rows = db.execute(stmt).scalars().all()
    result: list[dict[str, Any]] = []
    for r in rows:
        bucket_start = r.bucket_start
        if bucket_start.tzinfo is None:
            bucket_start = bucket_start.replace(tzinfo=timezone.utc)
        result.append(
            {
                "bucket_start": r.bucket_start,
                "x_sec": int(bucket_start.timestamp()),
                "value": r.value,
                "pipeline_run_id": r.pipeline_run_id,
            }
        )
    return result


@app.get("/analytics/summary")
def get_summary(platform: str, video_id: str, metric_name: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    stmt = (
        select(MetricsSummary)
        .where(MetricsSummary.platform == platform, MetricsSummary.video_id == video_id, MetricsSummary.metric_name == metric_name)
        .order_by(MetricsSummary.pipeline_run_id.desc())
        .limit(1)
    )
    row = db.execute(stmt).scalars().first()
    if row is None:
        raise HTTPException(status_code=404, detail="summary not found")
    return {"metric_name": row.metric_name, "value": row.value_json, "pipeline_run_id": row.pipeline_run_id}


@app.get("/pipeline/latest")
def get_latest_pipeline(platform: str, video_id: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    stmt = (
        select(PipelineRun)
        .where(PipelineRun.platform == platform, PipelineRun.video_id == video_id)
        .order_by(PipelineRun.id.desc())
        .limit(1)
    )
    run = db.execute(stmt).scalars().first()
    if run is None:
        raise HTTPException(status_code=404, detail="pipeline_run not found")
    return {"pipeline_run_id": run.id, "status": run.status, "started_at": run.started_at, "finished_at": run.finished_at}


@app.post("/cache/cleanup")
def cleanup_cache() -> dict[str, str]:
    cache_backend.cleanup()
    return {"status": "ok"}


@app.post("/report/html", response_model=GenerateReportResponse)
def generate_report(req: RunAnalyticsRequest, db: Session = Depends(get_db)) -> GenerateReportResponse:
    out_dir = settings.data_dir / "reports"
    output_path = out_dir / f"{req.platform}_{req.video_id}.html"
    path = generate_html_report(db, platform=req.platform, video_id=req.video_id, output_path=output_path)
    return GenerateReportResponse(path=str(path))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("visualization.dashboard.server:app", host="127.0.0.1", port=8000, reload=False)
