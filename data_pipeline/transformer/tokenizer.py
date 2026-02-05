from __future__ import annotations

import jieba


def tokenize(text: str) -> list[str]:
    return [t.strip() for t in jieba.lcut(text) if t.strip()]

