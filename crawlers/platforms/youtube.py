from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from crawlers.platforms.base import DanmuEvent, PlatformAdapter


class YouTubeAdapter(PlatformAdapter):
    platform = "youtube"

    async def resolve_video(self, input_value: str) -> str:
        return input_value.strip()

    async def crawl_history(self, video_id: str, cursor: dict[str, Any] | None = None) -> AsyncIterator[DanmuEvent]:
        raise NotImplementedError

