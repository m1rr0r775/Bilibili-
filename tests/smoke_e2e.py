from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from analytics.runner import run_analysis
from crawlers.utils import sha256_hex
from data_pipeline.loader.pipeline_runner import run_pipeline
from database.init_db import init_db
from database.models import RawDanmu, Video
from database.session import SessionLocal
from sqlalchemy import delete
from visualization.dashboard.server import get_time_series
from visualization.report.html_report import generate_html_report


def main() -> None:
    init_db()
    db = SessionLocal()
    try:
        platform = "bilibili"
        video_id = "BV_SMOKE_TEST"

        db.execute(delete(Video).where(Video.platform == platform, Video.video_id == video_id))
        db.execute(delete(RawDanmu).where(RawDanmu.platform == platform, RawDanmu.video_id == video_id))
        db.commit()

        db.add(Video(platform=platform, video_id=video_id, title="smoke"))
        db.commit()

        _seed_raw_danmu(db, platform, video_id)

        danmu_run = run_pipeline(db, platform=platform, video_id=video_id, config_json={"smoke": True})
        run_analysis(db, platform=platform, video_id=video_id, pipeline_run_id=danmu_run.id)

        danmu_ts = get_time_series(platform=platform, video_id=video_id, metric_name="danmu_count", bucket_sec=10, db=db)
        assert all(int(r["x_sec"]) >= 0 for r in danmu_ts), "danmu x_sec should be non-negative"

        out = Path("data/reports/smoke_report.html")
        generate_html_report(db, platform=platform, video_id=video_id, output_path=out)
        assert out.exists(), "report should exist"

        print("smoke ok")
        print("danmu_count points:", len(danmu_ts))
        print("report:", str(out))
    finally:
        db.close()


def _seed_raw_danmu(db, platform: str, video_id: str) -> None:
    rows = [
        (0.2, "开场@up 主好", 1),
        (5.1, "哈哈哈哈", 1),
        (10.0, "前方高能", 5),
        (12.3, "这也太强了@朋友", 1),
        (40.0, "有点尬", 4),
        (41.0, "笑死", 1),
        (80.0, "爷青回", 1),
    ]
    for i, (ts, content, mode) in enumerate(rows, start=1):
        d_hash = sha256_hex(f"{platform}|{video_id}|{i}")
        db.add(
            RawDanmu(
                platform=platform,
                video_id=video_id,
                content=content,
                video_ts=float(ts),
                send_time=datetime.now(tz=timezone.utc),
                user_id_hash=sha256_hex(f"{platform}:user{i%3}"),
                user_level=None,
                like_count=None,
                dedup_hash=d_hash,
                raw_json={"mode": mode, "segment_index": 1, "cid": 1, "duration": 120.0},
            )
        )
    db.commit()


if __name__ == "__main__":
    main()

