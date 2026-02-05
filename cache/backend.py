from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class CacheBackend(ABC):
    @abstractmethod
    def get_json(self, key: str) -> dict[str, Any] | None:
        raise NotImplementedError

    @abstractmethod
    def set_json(self, key: str, value: dict[str, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def cleanup(self) -> None:
        raise NotImplementedError

