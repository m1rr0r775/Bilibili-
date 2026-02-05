from __future__ import annotations

import re
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from math import ceil
from typing import Any

import aiohttp

from crawlers.platforms.base import DanmuEvent, PlatformAdapter
from crawlers.platforms.bilibili_proto import DmSegMobileReply


class BilibiliAdapter(PlatformAdapter):
    platform = "bilibili"

    def __init__(self, session: aiohttp.ClientSession | None = None) -> None:
        self._session = session

    async def resolve_video(self, input_value: str) -> str:
        input_value = input_value.strip()
        bvid = self._extract_bvid(input_value)
        if bvid:
            return bvid
        avid = self._extract_avid(input_value)
        if avid:
            return avid
        if input_value.isdigit():
            return f"cid:{input_value}"
        return input_value

    async def crawl_history(self, video_id: str, cursor: dict[str, Any] | None = None) -> AsyncIterator[DanmuEvent]:
        cursor = cursor or {}
        cid = cursor.get("cid")
        duration = cursor.get("duration")
        if cid is None or duration is None:
            resolved = await self._resolve_cid_and_duration(video_id)
            cid = resolved.get("cid") or cid
            duration = resolved.get("duration") or duration
        if cid is None:
            raise ValueError(f"无法解析 cid: {video_id}")

        start_segment_index = int(cursor.get("segment_index") or 1)
        max_segments = None
        if isinstance(duration, (int, float)) and duration > 0:
            max_segments = ceil(float(duration) / 360.0)

        empty_streak = 0
        segment_index = start_segment_index
        while True:
            if max_segments is not None and segment_index > max_segments:
                break
            payload = await self._fetch_seg(cid=int(cid), segment_index=segment_index)
            reply = DmSegMobileReply.FromString(payload)
            if not getattr(reply, "elems", None):
                empty_streak += 1
                if max_segments is not None or empty_streak >= 3:
                    break
                segment_index += 1
                continue
            empty_streak = 0
            for elem in reply.elems:
                content = getattr(elem, "content", "") or ""
                progress_ms = int(getattr(elem, "progress", 0) or 0)
                ctime = int(getattr(elem, "ctime", 0) or 0)
                send_time = datetime.fromtimestamp(ctime, tz=timezone.utc) if ctime > 0 else None
                raw_payload = {
                    "cid": int(cid),
                    "duration": float(duration) if isinstance(duration, (int, float)) else None,
                    "segment_index": segment_index,
                    "id": int(getattr(elem, "id", 0) or 0),
                    "progress": progress_ms,
                    "mode": int(getattr(elem, "mode", 0) or 0),
                    "fontsize": int(getattr(elem, "fontsize", 0) or 0),
                    "color": int(getattr(elem, "color", 0) or 0),
                    "midHash": str(getattr(elem, "midHash", "") or ""),
                    "content": content,
                    "ctime": ctime,
                    "weight": int(getattr(elem, "weight", 0) or 0),
                    "pool": int(getattr(elem, "pool", 0) or 0),
                    "attr": int(getattr(elem, "attr", 0) or 0),
                }
                yield DanmuEvent(
                    platform=self.platform,
                    video_id=video_id,
                    content=content,
                    video_ts=progress_ms / 1000.0,
                    send_time=send_time,
                    user_id=raw_payload.get("midHash") or None,
                    user_level=None,
                    like_count=None,
                    raw_payload=raw_payload,
                )
            segment_index += 1

    async def _fetch_seg(self, cid: int, segment_index: int) -> bytes:
        url = "https://api.bilibili.com/x/v2/dm/web/seg.so"
        params = {"type": 1, "oid": cid, "segment_index": segment_index}
        session = await self._get_session()
        headers = {"User-Agent": _DEFAULT_UA, "Referer": "https://www.bilibili.com"}
        async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            resp.raise_for_status()
            return await resp.read()

    async def _resolve_cid_and_duration(self, video_id: str) -> dict[str, Any]:
        bvid = self._extract_bvid(video_id)
        avid = self._extract_avid(video_id)
        if not bvid and not avid:
            m = re.fullmatch(r"cid:(\d+)", video_id)
            if m:
                return {"cid": int(m.group(1)), "duration": None}
            return {}

        url = "https://api.bilibili.com/x/web-interface/view"
        params: dict[str, str] = {}
        if bvid:
            params["bvid"] = bvid
        else:
            params["aid"] = avid.removeprefix("av")
        session = await self._get_session()
        headers = {"User-Agent": _DEFAULT_UA, "Referer": "https://www.bilibili.com"}
        async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            resp.raise_for_status()
            data = await resp.json()
        if data.get("code") != 0:
            return {}
        view_data = data.get("data") or {}
        pages = view_data.get("pages") or []
        if not pages:
            return {}
        page0 = pages[0] or {}
        return {"cid": int(page0.get("cid")), "duration": float(page0.get("duration") or 0)}

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    @staticmethod
    def _extract_bvid(text: str) -> str | None:
        m = re.search(r"(BV[0-9A-Za-z]{10})", text)
        return m.group(1) if m else None

    @staticmethod
    def _extract_avid(text: str) -> str | None:
        m = re.search(r"(av\d+)", text, flags=re.IGNORECASE)
        return m.group(1).lower() if m else None


_DEFAULT_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
