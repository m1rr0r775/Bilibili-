from __future__ import annotations

from typing import Any

from crawlers.platforms.base import PlatformAdapter
from crawlers.platforms.bilibili import BilibiliAdapter
from crawlers.platforms.douyin import DouyinAdapter
from crawlers.platforms.youtube import YouTubeAdapter


def create_adapter(platform: str, **kwargs: Any) -> PlatformAdapter:
    if platform == "bilibili":
        return BilibiliAdapter(session=kwargs.get("session"))
    if platform == "youtube":
        return YouTubeAdapter()
    if platform == "douyin":
        return DouyinAdapter()
    raise ValueError(f"不支持的平台: {platform}")
