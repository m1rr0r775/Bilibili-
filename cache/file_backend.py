from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from cache.backend import CacheBackend
from crawlers.utils import sha256_hex


class FileCacheBackend(CacheBackend):
    def __init__(self, cache_dir: Path) -> None:
        self._dir = cache_dir
        self._dir.mkdir(parents=True, exist_ok=True)

    def get_json(self, key: str) -> dict[str, Any] | None:
        path = self._path_for_key(key)
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def set_json(self, key: str, value: dict[str, Any]) -> None:
        path = self._path_for_key(key)
        path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")

    def cleanup(self) -> None:
        if not self._dir.exists():
            return
        for p in self._dir.glob("**/*"):
            if p.is_file():
                p.unlink(missing_ok=True)

    def _path_for_key(self, key: str) -> Path:
        name = sha256_hex(key) + ".json"
        return self._dir / name

