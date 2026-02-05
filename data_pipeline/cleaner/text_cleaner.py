from __future__ import annotations

import re


_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")
_WS_RE = re.compile(r"\s+")


def normalize_text(text: str) -> str:
    text = text.strip()
    text = _CONTROL_RE.sub("", text)
    text = _WS_RE.sub(" ", text)
    return text


def is_empty_or_noise(text: str) -> bool:
    if not text:
        return True
    non_space = text.replace(" ", "")
    if not non_space:
        return True
    if len(non_space) <= 1:
        return True
    return False

