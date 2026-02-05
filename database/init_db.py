from __future__ import annotations

from sqlalchemy import create_engine

from config.settings import settings
from database.models import Base


def init_db() -> None:
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    settings.cache_dir.mkdir(parents=True, exist_ok=True)
    connect_args: dict[str, object] = {}
    if settings.database_url.startswith("sqlite:"):
        connect_args = {"check_same_thread": False}
    engine = create_engine(settings.database_url, future=True, pool_pre_ping=True, connect_args=connect_args)
    Base.metadata.create_all(bind=engine)

