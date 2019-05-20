import time
from typing import List  # noqa: F401

from lahja.tools.benchmark.typing import CrunchedMeasureEntry, RawMeasureEntry, Total


class LocalStatistic:
    def __init__(self) -> None:
        self._entries: List[RawMeasureEntry] = []

    def add(self, entry: RawMeasureEntry) -> None:
        self._entries.append(entry)

    def crunch(self, caption: str) -> Total:
        crunched_entries = []
        for entry in self._entries:
            crunched_entries.append(
                CrunchedMeasureEntry(
                    entry.sent_at, entry.received_at, entry.received_at - entry.sent_at
                )
            )

        fastest = min(n.duration for n in crunched_entries)
        slowest = max(n.duration for n in crunched_entries)
        first_sent = min(n.sent_at for n in crunched_entries)
        last_received = max(n.received_at for n in crunched_entries)
        total_duration = last_received - first_sent
        total_aggregated_time = sum(n.duration for n in crunched_entries)
        avg = total_aggregated_time / len(crunched_entries)

        return Total(
            caption=caption,
            num_total=len(crunched_entries),
            duration_slowest=slowest,
            duration_fastest=fastest,
            first_sent=first_sent,
            last_received=last_received,
            total_duration=total_duration,
            total_aggregated_time=total_aggregated_time,
            duration_avg=avg,
        )


class GlobalStatistic:
    def __init__(self) -> None:
        self._created_at = time.time()
        self._entries: List[Total] = []

    def __len__(self) -> int:
        return len(self._entries)

    def add(self, total: Total) -> None:
        self._entries.append(total)
