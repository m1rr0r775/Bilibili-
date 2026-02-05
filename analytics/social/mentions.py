from __future__ import annotations

import re
from collections import Counter, defaultdict

from sqlalchemy import select
from sqlalchemy.orm import Session

from database.models import CleanDanmu, RawDanmu


_MENTION_RE = re.compile(r"@([0-9A-Za-z_\u4e00-\u9fff\-]{1,20})")


def build_mention_network_summary(db: Session, platform: str, video_id: str, pipeline_run_id: int, top_n: int = 20) -> dict:
    stmt = (
        select(RawDanmu.user_id_hash, CleanDanmu.content_norm)
        .join(CleanDanmu, CleanDanmu.raw_id == RawDanmu.id)
        .where(
            CleanDanmu.platform == platform,
            CleanDanmu.video_id == video_id,
            CleanDanmu.pipeline_run_id == pipeline_run_id,
            RawDanmu.platform == platform,
            RawDanmu.video_id == video_id,
        )
    )

    edge_counter: Counter[tuple[str, str]] = Counter()
    out_counter: Counter[str] = Counter()
    in_counter: Counter[str] = Counter()
    unique_targets_by_sender: dict[str, set[str]] = defaultdict(set)

    messages = 0
    for sender_hash, content in db.execute(stmt).all():
        messages += 1
        if not sender_hash or not content:
            continue
        targets = [m.group(1) for m in _MENTION_RE.finditer(content)]
        if not targets:
            continue
        for t in targets:
            edge_counter[(sender_hash, t)] += 1
            out_counter[sender_hash] += 1
            in_counter[t] += 1
            unique_targets_by_sender[sender_hash].add(t)

    edges = int(sum(edge_counter.values()))
    unique_senders = len(out_counter)
    unique_targets = len(in_counter)
    top_senders = [{"user_id_hash": k, "out_mentions": int(v), "unique_targets": len(unique_targets_by_sender[k])} for k, v in out_counter.most_common(top_n)]
    top_targets = [{"target": k, "in_mentions": int(v)} for k, v in in_counter.most_common(top_n)]
    top_edges = [{"from_user_id_hash": a, "to_target": b, "count": int(c)} for (a, b), c in edge_counter.most_common(top_n)]

    return {
        "messages": messages,
        "edges": edges,
        "unique_senders": unique_senders,
        "unique_targets": unique_targets,
        "top_senders": top_senders,
        "top_targets": top_targets,
        "top_edges": top_edges,
    }
