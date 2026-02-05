from __future__ import annotations

from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape
from sqlalchemy import select
from sqlalchemy.orm import Session

from database.models import MetricsSummary


def generate_html_report(db: Session, platform: str, video_id: str, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    summaries = _load_summaries(db, platform=platform, video_id=video_id)
    env = Environment(
        loader=FileSystemLoader("visualization/report/templates"),
        autoescape=select_autoescape(["html"]),
    )
    tpl = env.get_template("report.html")
    html = tpl.render(platform=platform, video_id=video_id, summaries=summaries)
    output_path.write_text(html, encoding="utf-8")
    return output_path


def _load_summaries(db: Session, platform: str, video_id: str) -> dict[str, Any]:
    stmt = (
        select(MetricsSummary)
        .where(MetricsSummary.platform == platform, MetricsSummary.video_id == video_id)
        .order_by(MetricsSummary.pipeline_run_id.desc())
    )
    rows = db.execute(stmt).scalars().all()
    result: dict[str, Any] = {}
    for r in rows:
        if r.metric_name not in result:
            result[r.metric_name] = r.value_json
    return result

