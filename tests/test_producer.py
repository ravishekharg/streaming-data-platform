"""Producer record generation (no Kafka needed)."""
import json
import random
from datetime import datetime, timezone

from producer.telemetry_producer import bad_event, valid_event

NOW = datetime(2026, 10, 19, 12, 0, tzinfo=timezone.utc)


def test_valid_event_shape_and_ranges():
    rng = random.Random(1)
    for _ in range(500):
        e = valid_event(rng, 50, NOW, late=False)
        assert e["device_id"].startswith("dev-") and len(e["device_id"]) == 8
        assert e["metric"] in ("temperature_c", "battery_pct", "signal_rssi_dbm")
        assert e["event_time"].endswith("Z")


def test_bad_events_cover_every_kind_and_are_deterministic():
    kinds = {bad_event(random.Random(i), 50, NOW)[1] for i in range(200)}
    assert kinds == {"malformed_json", "value_out_of_range", "unknown_metric", "missing_device_id",
                     "event_time_in_future"}
    assert bad_event(random.Random(7), 50, NOW) == bad_event(random.Random(7), 50, NOW)


def test_malformed_payload_is_not_json():
    for i in range(200):
        payload, kind = bad_event(random.Random(i), 50, NOW)
        if kind == "malformed_json":
            try:
                json.loads(payload)
                raise AssertionError("expected invalid JSON")
            except json.JSONDecodeError:
                pass
