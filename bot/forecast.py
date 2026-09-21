"""Pure route-frequency calculations used by the Telegram forecast view."""
from __future__ import annotations

from datetime import datetime, timedelta
from statistics import median
from typing import Any, Iterable, List, Optional, Sequence, Tuple


def _percentile(values: Sequence[float], q: float) -> Optional[float]:
    if not values:
        return None
    if len(values) == 1:
        return float(values[0])
    ordered = sorted(float(v) for v in values)
    position = (len(ordered) - 1) * q
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def calculate_route_forecasts(rows: Iterable[Any], *, min_occurrences: int = 3, limit: int = 10) -> List[dict]:
    grouped = {}
    for row in rows:
        route_from = str(row["route_from"] or "").strip()
        route_to = str(row["route_to"] or "").strip()
        created_at = row["created_at"]
        if not route_from or not route_to or created_at is None:
            continue
        region_from = str(row["route_from_region"] or "").strip() if "route_from_region" in row else ""
        region_to = str(row["route_to_region"] or "").strip() if "route_to_region" in row else ""
        key = (route_from.lower(), route_to.lower(), region_from.lower(), region_to.lower())
        grouped.setdefault(
            key,
            {"route_from": route_from, "route_to": route_to, "region_from": region_from, "region_to": region_to, "times": []},
        )["times"].append(created_at)

    forecasts: List[dict] = []
    for data in grouped.values():
        times = sorted(data["times"])
        count = len(times)
        intervals_hours = [
            (later - earlier).total_seconds() / 3600.0
            for earlier, later in zip(times, times[1:])
            if later > earlier
        ]
        item = {
            "route_from": data["route_from"],
            "route_to": data["route_to"],
            "region_from": data["region_from"],
            "region_to": data["region_to"],
            "count": count,
            "first_seen": times[0],
            "last_seen": times[-1],
            "median_interval_hours": None,
            "next_expected": None,
            "expected_window": None,
        }
        if count >= min_occurrences and intervals_hours:
            med = float(median(intervals_hours))
            low = _percentile(intervals_hours, 0.25) or med
            high = _percentile(intervals_hours, 0.75) or med
            next_expected = times[-1] + timedelta(hours=med)
            window = (times[-1] + timedelta(hours=low), times[-1] + timedelta(hours=high))
            item.update(
                {
                    "median_interval_hours": med,
                    "next_expected": next_expected,
                    "expected_window": window,
                }
            )
        forecasts.append(item)

    forecasts.sort(key=lambda item: (item["count"], item["last_seen"]), reverse=True)
    return forecasts[:limit]
