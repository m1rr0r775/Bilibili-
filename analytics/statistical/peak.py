from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True)
class PeakSegment:
    start: datetime
    end: datetime
    peak_value: int


def detect_peaks(series: list[tuple[datetime, int]], z_threshold: float = 2.0, min_count: int = 10) -> list[PeakSegment]:
    if not series:
        return []
    values = [v for _, v in series]
    mean = sum(values) / len(values)
    var = sum((v - mean) ** 2 for v in values) / max(len(values) - 1, 1)
    std = var**0.5
    if std == 0:
        return []
    flagged = [(t, v) for (t, v) in series if v >= min_count and (v - mean) / std >= z_threshold]
    if not flagged:
        return []
    flagged_set = {t for t, _ in flagged}
    bucket_sec = _infer_bucket_sec(series)

    segments: list[PeakSegment] = []
    current_start = None
    current_end = None
    current_peak = 0
    for t, v in series:
        if t in flagged_set:
            if current_start is None:
                current_start = t
                current_end = t
                current_peak = v
            else:
                current_end = t
                current_peak = max(current_peak, v)
        else:
            if current_start is not None and current_end is not None:
                segments.append(PeakSegment(start=current_start, end=current_end + timedelta(seconds=bucket_sec), peak_value=current_peak))
                current_start = None
                current_end = None
                current_peak = 0
    if current_start is not None and current_end is not None:
        segments.append(PeakSegment(start=current_start, end=current_end + timedelta(seconds=bucket_sec), peak_value=current_peak))
    return segments


def _infer_bucket_sec(series: list[tuple[datetime, int]]) -> int:
    if len(series) < 2:
        return 10
    delta = (series[1][0] - series[0][0]).total_seconds()
    return int(delta) if delta > 0 else 10

