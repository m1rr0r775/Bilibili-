from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class DanmuEvent(BaseModel):
    platform: str
    video_id: str
    content: str
    video_ts: float = Field(ge=0)
    send_time: datetime | None = None
    user_id: str | None = None
    user_level: int | None = None
    like_count: int | None = None
    raw_payload: dict[str, Any]


class PlatformAdapter(ABC):
    platform: str

    @abstractmethod
    async def resolve_video(self, input_value: str) -> str:
        raise NotImplementedError

    @abstractmethod
    async def crawl_history(self, video_id: str, cursor: dict[str, Any] | None = None) -> AsyncIterator[DanmuEvent]:
        raise NotImplementedError

    async def crawl_live(self, video_id: str) -> AsyncIterator[DanmuEvent]:
        raise NotImplementedError
