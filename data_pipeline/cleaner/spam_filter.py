from __future__ import annotations

import re


_URL_RE = re.compile(r"(https?://|www\.)", flags=re.IGNORECASE)
_REPEAT_CHAR_RE = re.compile(r"(.)\1{6,}")


def is_spam(text: str) -> bool:
    if _URL_RE.search(text):
        return True
    if _REPEAT_CHAR_RE.search(text):
        return True
    if len(text) >= 200:
        return True
    return False

