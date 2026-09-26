from datetime import datetime, timedelta, timezone

from terminalweave.reefer.telemetry import TelemetryBuffer, TelemetryPoint


UTC = timezone.utc
D = datetime(2026, 1, 1, tzinfo=UTC)


def point(at, temperature):
    return TelemetryPoint("CONT1", at, temperature, temperature, 0.0, True)


def test_late_and_duplicate_telemetry_do_not_replace_confirmed_latest_state():
    buffer = TelemetryBuffer()
    recovered = point(D + timedelta(minutes=20), 0.0)
    delayed_excursion = point(D + timedelta(minutes=10), 8.0)
    buffer.ingest(recovered)
    buffer.ingest(delayed_excursion)
    buffer.ingest(delayed_excursion)
    assert buffer.latest("CONT1") == recovered
    assert len(buffer.points["CONT1"]) == 2
