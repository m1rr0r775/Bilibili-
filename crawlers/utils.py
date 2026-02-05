from __future__ import annotations

import hashlib


def sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def user_hash(platform: str, user_id: str | None) -> str | None:
    if not user_id:
        return None
    return sha256_hex(f"{platform}:{user_id}")


def dedup_hash(platform: str, video_id: str, content: str, video_ts: float, user_id_hash: str | None) -> str:
    user_part = user_id_hash or ""
    return sha256_hex(f"{platform}|{video_id}|{video_ts:.3f}|{user_part}|{content}")

