from __future__ import annotations
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from terminalweave.core.clock import ensure_utc
from terminalweave.core.errors import ValidationError
from terminalweave.reefer.defrost import DefrostTimeline

class AlarmState(str, Enum):
    NORMAL = 'normal'
    PENDING = 'pending'
    ACTIVE = 'active'
    RECOVERING = 'recovering'

@dataclass(frozen=True)
class ReeferPolicy:
    low_c: float
    high_c: float
    trigger_after: timedelta
    clear_after: timedelta
    hysteresis_c: float = 0.5
    max_gap: timedelta | None = None

    def __post_init__(self) -> None:
        if self.low_c >= self.high_c:
            raise ValidationError('low temperature must be below high temperature')
        if self.trigger_after.total_seconds() < 0 or self.clear_after.total_seconds() < 0 or self.hysteresis_c < 0:
            raise ValidationError('invalid reefer alarm policy')
        if self.max_gap is not None and self.max_gap.total_seconds() <= 0:
            raise ValidationError('max telemetry gap must be positive')

@dataclass(frozen=True)
class ReeferReading:
    at: datetime
    temperature_c: float
    power_on: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, 'at', ensure_utc(self.at))
        if not -80 <= self.temperature_c <= 80:
            raise ValidationError('temperature outside supported range')

@dataclass(frozen=True)
class StreamGap:
    """A stream interruption: no readings between two event times."""
    start: datetime
    end: datetime

@dataclass
class ExcursionEpisode:
    """A confirmed out-of-range excursion, recorded by event time.

    Once confirmed the episode is frozen: late or repeated readings can
    never rewrite its start/confirm times, and clearance is appended at
    most once.
    """
    started_at: datetime
    confirmed_at: datetime
    cleared_at: datetime | None = None

    @property
    def open(self) -> bool:
        return self.cleared_at is None

@dataclass
class ReeferAlarm:
    state: AlarmState = AlarmState.NORMAL
    state_since: datetime | None = None
    last_reading: ReeferReading | None = None
    excursions: int = 0

class ReeferMonitor:
    """Event-time alarm timeline for one reefer container.

    Readings may arrive out of order or duplicated; the timeline is always
    evaluated in event-time order, so a brief excursion that is reported
    late cannot accumulate into a sustained alarm. Conclusions once
    confirmed (an ExcursionEpisode or its clearance) are frozen at an
    event-time watermark: data arriving at or before that watermark is
    stored but cannot change what was concluded, and cannot raise the
    same excursion twice.
    """

    def __init__(self, policy: ReeferPolicy, defrost: DefrostTimeline | None = None):
        self.policy = policy
        self.defrost = defrost if defrost is not None else DefrostTimeline()
        self.alarm = ReeferAlarm()
        self.episodes: list[ExcursionEpisode] = []
        self._readings: list[ReeferReading] = []
        self._frozen_state = AlarmState.NORMAL
        self._frozen_until: datetime | None = None
        self._frozen_since: datetime | None = None

    def _outside(self, reading: ReeferReading) -> bool:
        if not reading.power_on or reading.temperature_c < self.policy.low_c:
            return True
        if reading.temperature_c > self.policy.high_c:
            # a registered defrost window explains a high-side rise
            return not self.defrost.covers(reading.at)
        return False

    def _inside_clear_band(self, reading: ReeferReading) -> bool:
        return reading.power_on and self.policy.low_c + self.policy.hysteresis_c <= reading.temperature_c <= self.policy.high_c - self.policy.hysteresis_c

    def ingest(self, reading: ReeferReading) -> AlarmState:
        series = self._readings
        i = bisect_left(series, reading.at, key=lambda r: r.at)
        if i < len(series) and series[i].at == reading.at:
            if series[i] == reading:
                return self.alarm.state
            raise ValidationError('conflicting reading at the same event time')
        series.insert(i, reading)
        self._replay()
        return self.alarm.state

    def refresh(self) -> AlarmState:
        """Re-evaluate the unconfirmed tail, e.g. after a late defrost registration."""
        self._replay()
        return self.alarm.state

    def _freeze(self, state: AlarmState, at: datetime) -> None:
        self._frozen_state = state
        self._frozen_until = at
        self._frozen_since = at

    def _replay(self) -> None:
        policy = self.policy
        state = self._frozen_state
        state_since = self._frozen_since
        if self._frozen_until is None:
            start = 0
            prev_at = None
        else:
            start = bisect_right(self._readings, self._frozen_until, key=lambda r: r.at)
            prev_at = self._readings[start - 1].at if start else None
        for reading in self._readings[start:]:
            at = reading.at
            if prev_at is not None and policy.max_gap is not None and at - prev_at > policy.max_gap:
                # stream interruption: unconfirmed durations never accumulate across a gap
                if state == AlarmState.PENDING:
                    state, state_since = AlarmState.NORMAL, at
                elif state == AlarmState.RECOVERING:
                    state, state_since = AlarmState.ACTIVE, at
            outside = self._outside(reading)
            clear_band = self._inside_clear_band(reading)
            if state == AlarmState.NORMAL:
                if outside:
                    state, state_since = AlarmState.PENDING, at
            elif state == AlarmState.PENDING:
                if not outside:
                    state, state_since = AlarmState.NORMAL, at
                elif state_since is not None and at - state_since >= policy.trigger_after:
                    self.episodes.append(ExcursionEpisode(started_at=state_since, confirmed_at=at))
                    state, state_since = AlarmState.ACTIVE, at
                    self._freeze(AlarmState.ACTIVE, at)
            elif state == AlarmState.ACTIVE:
                if clear_band:
                    state, state_since = AlarmState.RECOVERING, at
            elif state == AlarmState.RECOVERING:
                if outside:
                    state, state_since = AlarmState.ACTIVE, at
                elif clear_band and state_since is not None and at - state_since >= policy.clear_after:
                    state, state_since = AlarmState.NORMAL, at
                    self.episodes[-1].cleared_at = at
                    self._freeze(AlarmState.NORMAL, at)
            prev_at = at
        self.alarm.state = state
        self.alarm.state_since = state_since
        self.alarm.last_reading = self._readings[-1] if self._readings else None
        self.alarm.excursions = len(self.episodes)

    @property
    def gaps(self) -> tuple[StreamGap, ...]:
        if self.policy.max_gap is None:
            return ()
        return tuple(
            StreamGap(a.at, b.at)
            for a, b in zip(self._readings, self._readings[1:])
            if b.at - a.at > self.policy.max_gap
        )

    def seconds_in_state(self, now: datetime) -> float:
        now = ensure_utc(now)
        if self.alarm.state_since is None:
            return 0.0
        return max(0.0, (now - self.alarm.state_since).total_seconds())
