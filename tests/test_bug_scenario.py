from datetime import datetime, timedelta, timezone

import pytest

from terminalweave.core.errors import ValidationError
from terminalweave.reefer.defrost import DefrostEvent, DefrostTimeline
from terminalweave.reefer.monitoring import AlarmState, ReeferMonitor, ReeferPolicy, ReeferReading
from terminalweave.reefer.telemetry import TelemetryBuffer, TelemetryPoint
from terminalweave.reefer.workorders import ReeferDispatcher, ReeferWorkBoard


UTC = timezone.utc
D = datetime(2026, 1, 1, tzinfo=UTC)
POLICY = ReeferPolicy(-1, 1, timedelta(minutes=10), timedelta(minutes=5), 0.2)


def point(at, temperature):
    return TelemetryPoint("CONT1", at, temperature, temperature, 0.0, True)


def reading(at, temperature, power_on=True):
    return ReeferReading(at, temperature, power_on)


def episode_key(monitor):
    return [(e.started_at, e.confirmed_at, e.cleared_at) for e in monitor.episodes]


def test_late_and_duplicate_telemetry_do_not_replace_confirmed_latest_state():
    buffer = TelemetryBuffer()
    recovered = point(D + timedelta(minutes=20), 0.0)
    delayed_excursion = point(D + timedelta(minutes=10), 8.0)
    buffer.ingest(recovered)
    buffer.ingest(delayed_excursion)
    buffer.ingest(delayed_excursion)
    assert buffer.latest("CONT1") == recovered
    assert len(buffer.points["CONT1"]) == 2


def test_conflicting_telemetry_at_same_event_time_is_rejected():
    buffer = TelemetryBuffer()
    buffer.ingest(point(D, 1.0))
    with pytest.raises(ValidationError):
        buffer.ingest(point(D, 2.0))
    assert buffer.latest("CONT1").temperature_c == 1.0


def test_window_stats_follow_event_time_not_arrival_order():
    buffer = TelemetryBuffer()
    buffer.ingest(point(D + timedelta(minutes=10), 4.0))
    buffer.ingest(point(D, 2.0))
    assert buffer.mean_offset("CONT1", timedelta(minutes=15)) == pytest.approx(3.0)


def test_brief_late_excursion_does_not_accumulate_into_sustained_alarm():
    m = ReeferMonitor(POLICY)
    assert m.ingest(reading(D + timedelta(minutes=20), 0.0)) == AlarmState.NORMAL
    # late arrival: 8C at D+10, already recovered by D+20 -> brief, below trigger_after
    assert m.ingest(reading(D + timedelta(minutes=10), 8.0)) == AlarmState.NORMAL
    assert m.alarm.excursions == 0
    assert m.episodes == []
    # replaying the same late reading changes nothing
    assert m.ingest(reading(D + timedelta(minutes=10), 8.0)) == AlarmState.NORMAL
    assert m.alarm.excursions == 0


def test_out_of_order_sustained_excursion_confirms_once_and_disposes_once():
    m = ReeferMonitor(POLICY)
    board = ReeferWorkBoard()
    dispatcher = ReeferDispatcher("CONT1", m, board)
    m.ingest(reading(D + timedelta(minutes=12), 5.0))
    m.ingest(reading(D, 5.0))  # late: excursion actually began at D
    assert m.alarm.state == AlarmState.ACTIVE
    assert m.alarm.excursions == 1
    assert episode_key(m) == [(D, D + timedelta(minutes=12), None)]
    opened = dispatcher.sync()
    assert len(opened) == 1
    assert opened[0].opened_at == D + timedelta(minutes=12)  # event time, not arrival time
    # duplicate and late replays produce no further disposals
    m.ingest(reading(D, 5.0))
    m.ingest(reading(D + timedelta(minutes=12), 5.0))
    assert dispatcher.sync() == ()
    assert dispatcher.sync() == ()
    assert len(board.orders) == 1
    # recovery computed by event time: sustained in-band readings
    m.ingest(reading(D + timedelta(minutes=15), 0.0))
    m.ingest(reading(D + timedelta(minutes=21), 0.0))
    assert m.alarm.state == AlarmState.NORMAL
    assert m.episodes[0].cleared_at == D + timedelta(minutes=21)
    # old excursion data replayed after recovery: no new episode, no new disposal
    m.ingest(reading(D + timedelta(minutes=6), 6.0))
    assert m.alarm.excursions == 1
    assert dispatcher.sync() == ()
    assert len(board.orders) == 1


def test_clear_requires_sustained_time_inside_hysteresis_band():
    m = ReeferMonitor(POLICY)
    m.ingest(reading(D, 5.0))
    m.ingest(reading(D + timedelta(minutes=11), 5.0))
    assert m.alarm.state == AlarmState.ACTIVE
    # 0.9 is inside [low, high] but outside the clear band (high - hysteresis = 0.8)
    assert m.ingest(reading(D + timedelta(minutes=13), 0.9)) == AlarmState.ACTIVE
    assert m.ingest(reading(D + timedelta(minutes=16), 0.5)) == AlarmState.RECOVERING
    assert m.ingest(reading(D + timedelta(minutes=20), 0.9)) == AlarmState.RECOVERING
    # 6 minutes inside the clear band since D+16 >= clear_after
    assert m.ingest(reading(D + timedelta(minutes=22), 0.0)) == AlarmState.NORMAL
    assert m.episodes[0].cleared_at == D + timedelta(minutes=22)


def test_stream_gap_resets_unconfirmed_duration_and_is_reported():
    policy = ReeferPolicy(-1, 1, timedelta(minutes=10), timedelta(minutes=5), 0.2, max_gap=timedelta(minutes=15))
    m = ReeferMonitor(policy)
    m.ingest(reading(D, 5.0))
    m.ingest(reading(D + timedelta(minutes=6), 5.0))
    # 34 minutes of silence > max_gap: pending duration must not accumulate across it
    assert m.ingest(reading(D + timedelta(minutes=40), 5.0)) == AlarmState.PENDING
    assert m.alarm.excursions == 0
    gaps = m.gaps
    assert len(gaps) == 1
    assert (gaps[0].start, gaps[0].end) == (D + timedelta(minutes=6), D + timedelta(minutes=40))
    # duration restarts from the post-gap reading
    assert m.ingest(reading(D + timedelta(minutes=51), 5.0)) == AlarmState.ACTIVE
    assert episode_key(m) == [(D + timedelta(minutes=40), D + timedelta(minutes=51), None)]


def test_stream_gap_during_recovery_restarts_clear_accumulation():
    policy = ReeferPolicy(-1, 1, timedelta(minutes=10), timedelta(minutes=5), 0.2, max_gap=timedelta(minutes=15))
    m = ReeferMonitor(policy)
    m.ingest(reading(D, 5.0))
    m.ingest(reading(D + timedelta(minutes=11), 5.0))
    assert m.alarm.state == AlarmState.ACTIVE
    m.ingest(reading(D + timedelta(minutes=13), 0.0))
    assert m.alarm.state == AlarmState.RECOVERING
    # gap interrupts the recovery evidence; clear time restarts afterwards
    m.ingest(reading(D + timedelta(minutes=40), 0.0))
    assert m.alarm.state == AlarmState.RECOVERING
    assert m.episodes[0].open
    m.ingest(reading(D + timedelta(minutes=44), 0.0))
    assert m.alarm.state == AlarmState.RECOVERING
    m.ingest(reading(D + timedelta(minutes=46), 0.0))
    assert m.alarm.state == AlarmState.NORMAL
    assert m.episodes[0].cleared_at == D + timedelta(minutes=46)


def test_defrost_window_suppresses_high_side_excursion_by_event_time():
    defrost = DefrostTimeline()
    m = ReeferMonitor(POLICY, defrost)
    assert defrost.register(DefrostEvent(D, D + timedelta(minutes=30), 6.0))
    assert not defrost.register(DefrostEvent(D, D + timedelta(minutes=30), 6.0))  # idempotent
    for i in range(1, 5):
        assert m.ingest(reading(D + timedelta(minutes=5 * i), 6.0)) == AlarmState.NORMAL
    assert m.alarm.excursions == 0
    # the same temperature outside the defrost window alarms as usual
    assert m.ingest(reading(D + timedelta(minutes=40), 6.0)) == AlarmState.PENDING


def test_late_defrost_registration_reevaluates_only_unconfirmed_state():
    defrost = DefrostTimeline()
    m = ReeferMonitor(POLICY, defrost)
    m.ingest(reading(D, 6.0))
    m.ingest(reading(D + timedelta(minutes=5), 6.0))
    assert m.alarm.state == AlarmState.PENDING
    defrost.register(DefrostEvent(D - timedelta(minutes=1), D + timedelta(minutes=30), 6.0))
    assert m.refresh() == AlarmState.NORMAL
    assert m.alarm.excursions == 0


def test_confirmed_episode_is_immutable_to_late_data():
    m = ReeferMonitor(POLICY)
    m.ingest(reading(D, 5.0))
    m.ingest(reading(D + timedelta(minutes=11), 5.0))
    assert m.alarm.state == AlarmState.ACTIVE
    # late in-band readings inside the excursion span cannot erase the confirmed episode
    m.ingest(reading(D + timedelta(minutes=5), 0.0))
    m.ingest(reading(D + timedelta(minutes=8), 0.0))
    assert m.alarm.state == AlarmState.ACTIVE
    assert m.alarm.excursions == 1
    assert episode_key(m) == [(D, D + timedelta(minutes=11), None)]


def test_conflicting_reading_at_same_event_time_is_rejected():
    m = ReeferMonitor(POLICY)
    m.ingest(reading(D, 5.0))
    with pytest.raises(ValidationError):
        m.ingest(reading(D, 9.0))
    # an exact duplicate is simply ignored
    assert m.ingest(reading(D, 5.0)) == AlarmState.PENDING
    assert m.alarm.excursions == 0


def test_shuffled_arrival_yields_the_same_timeline_as_in_order():
    script = [(0, 0.0), (5, 5.0), (16, 5.0), (18, 0.0), (24, 0.0), (30, 0.0)]

    def build(order):
        m = ReeferMonitor(POLICY)
        for idx in order:
            minute, temp = script[idx]
            m.ingest(reading(D + timedelta(minutes=minute), temp))
        return m

    in_order = build(range(len(script)))
    shuffled = build([4, 1, 3, 0, 5, 2])
    assert shuffled.alarm.state == in_order.alarm.state == AlarmState.NORMAL
    assert episode_key(shuffled) == episode_key(in_order)
    assert shuffled.alarm.excursions == in_order.alarm.excursions == 1
