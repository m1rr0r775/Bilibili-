from __future__ import annotations


_POS_WORDS = {
    "好",
    "喜欢",
    "爱了",
    "感动",
    "牛",
    "强",
    "厉害",
    "笑死",
    "哈哈",
    "可爱",
    "舒服",
    "赞",
    "精彩",
    "神",
    "绝了",
}

_NEG_WORDS = {
    "差",
    "烂",
    "难看",
    "无聊",
    "尬",
    "烦",
    "恶心",
    "离谱",
    "气死",
    "失望",
    "垃圾",
    "不行",
    "拉胯",
}


def score_sentiment(tokens: list[str]) -> tuple[str, float]:
    if not tokens:
        return ("neutral", 0.0)
    pos = sum(1 for t in tokens if t in _POS_WORDS)
    neg = sum(1 for t in tokens if t in _NEG_WORDS)
    score = float(pos - neg)
    if score > 0:
        return ("positive", score)
    if score < 0:
        return ("negative", score)
    return ("neutral", 0.0)

