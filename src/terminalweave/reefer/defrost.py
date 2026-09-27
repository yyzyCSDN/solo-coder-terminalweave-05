from __future__ import annotations
from bisect import insort
from dataclasses import dataclass
from datetime import datetime, timedelta
from terminalweave.core.clock import ensure_utc
from terminalweave.core.errors import ValidationError

@dataclass(frozen=True)
class DefrostEvent:
    start: datetime
    end: datetime
    temperature_rise_c: float

    def __post_init__(self):
        object.__setattr__(self, 'start', ensure_utc(self.start))
        object.__setattr__(self, 'end', ensure_utc(self.end))
        if self.end <= self.start or self.temperature_rise_c < 0:
            raise ValidationError('invalid defrost event')

class DefrostTimeline:
    """Defrost windows registered for one container, queried by event time.

    Registration is idempotent: re-registering an identical event is a
    no-op, so replayed controller messages cannot duplicate a window.
    """

    def __init__(self):
        self.events: list[DefrostEvent] = []

    def register(self, event: DefrostEvent) -> bool:
        if event in self.events:
            return False
        insort(self.events, event, key=lambda e: (e.start, e.end))
        return True

    def covers(self, at: datetime) -> bool:
        at = ensure_utc(at)
        return any(e.start <= at < e.end for e in self.events)

class DefrostClassifier:

    def __init__(self, max_normal_duration: timedelta=timedelta(minutes=45), max_normal_rise_c: float=8.0):
        self.max_duration = max_normal_duration
        self.max_rise = max_normal_rise_c

    def classify(self, event: DefrostEvent) -> str:
        if event.end - event.start > self.max_duration and event.temperature_rise_c > self.max_rise:
            return 'abnormal_long_hot'
        if event.end - event.start > self.max_duration:
            return 'abnormal_long'
        if event.temperature_rise_c > self.max_rise:
            return 'abnormal_hot'
        return 'normal'

    def duty_ratio(self, events: list[DefrostEvent], window_start: datetime, window_end: datetime) -> float:
        window_start = ensure_utc(window_start)
        window_end = ensure_utc(window_end)
        if window_end <= window_start:
            raise ValidationError('invalid window')
        seconds = 0.0
        for e in events:
            start = max(window_start, e.start)
            end = min(window_end, e.end)
            if start < end:
                seconds += (end - start).total_seconds()
        return seconds / (window_end - window_start).total_seconds()
