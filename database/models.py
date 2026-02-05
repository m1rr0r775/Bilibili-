from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import JSON, DateTime, Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Video(Base):
    __tablename__ = "video"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    platform: Mapped[str] = mapped_column(String(32), nullable=False)
    video_id: Mapped[str] = mapped_column(String(128), nullable=False)
    title: Mapped[str | None] = mapped_column(String(512), nullable=True)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (UniqueConstraint("platform", "video_id", name="uq_video_platform_video_id"),)


class CrawlTask(Base):
    __tablename__ = "crawl_task"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    platform: Mapped[str] = mapped_column(String(32), nullable=False)
    video_id: Mapped[str] = mapped_column(String(128), nullable=False)
    task_type: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    cursor_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    __table_args__ = (Index("ix_crawl_task_platform_video", "platform", "video_id"),)


class RawDanmu(Base):
    __tablename__ = "raw_danmu"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    platform: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    video_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    video_ts: Mapped[float] = mapped_column(Float, nullable=False, index=True)
    send_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    user_id_hash: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    user_level: Mapped[int | None] = mapped_column(Integer, nullable=True)
    like_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    dedup_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    raw_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    clean_items: Mapped[list["CleanDanmu"]] = relationship("CleanDanmu", back_populates="raw")

    __table_args__ = (
        UniqueConstraint("platform", "video_id", "dedup_hash", name="uq_raw_danmu_dedup"),
        Index("ix_raw_danmu_platform_video_ts", "platform", "video_id", "video_ts"),
    )


class PipelineRun(Base):
    __tablename__ = "pipeline_run"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    platform: Mapped[str] = mapped_column(String(32), nullable=False)
    video_id: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    config_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (Index("ix_pipeline_run_platform_video", "platform", "video_id"),)


class CleanDanmu(Base):
    __tablename__ = "clean_danmu"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    raw_id: Mapped[int] = mapped_column(ForeignKey("raw_danmu.id"), nullable=False, index=True)
    platform: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    video_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    content_norm: Mapped[str] = mapped_column(Text, nullable=False)
    tokens_json: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    sentiment_label: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    sentiment_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    danmu_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    cleaned_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    pipeline_run_id: Mapped[int] = mapped_column(ForeignKey("pipeline_run.id"), nullable=False, index=True)

    raw: Mapped[RawDanmu] = relationship("RawDanmu", back_populates="clean_items")

    __table_args__ = (Index("ix_clean_danmu_platform_video_ts", "platform", "video_id"),)


class MetricsTimeSeries(Base):
    __tablename__ = "metrics_time_series"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    platform: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    video_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    metric_name: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    bucket_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    bucket_sec: Mapped[int] = mapped_column(Integer, nullable=False)
    value: Mapped[float] = mapped_column(Float, nullable=False)
    pipeline_run_id: Mapped[int] = mapped_column(ForeignKey("pipeline_run.id"), nullable=False, index=True)

    __table_args__ = (
        UniqueConstraint(
            "platform",
            "video_id",
            "metric_name",
            "bucket_start",
            "bucket_sec",
            "pipeline_run_id",
            name="uq_metrics_ts_unique",
        ),
    )


class MetricsSummary(Base):
    __tablename__ = "metrics_summary"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    platform: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    video_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    metric_name: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    value_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    pipeline_run_id: Mapped[int] = mapped_column(ForeignKey("pipeline_run.id"), nullable=False, index=True)

    __table_args__ = (
        UniqueConstraint(
            "platform",
            "video_id",
            "metric_name",
            "pipeline_run_id",
            name="uq_metrics_summary_unique",
        ),
    )
