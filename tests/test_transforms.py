"""Unit tests for parsing, validation, dedupe and aggregation (local Spark, no Kafka/S3)."""
import json
from datetime import datetime, timedelta, timezone

from pyspark.sql import Row

from sdp.transforms import bronze_to_silver, hourly_aggregates

NOW = datetime.now(timezone.utc).replace(microsecond=0)


def ev(**over):
    e = {"event_id": "e-1", "device_id": "dev-0001", "site": "pune", "metric": "battery_pct", "value": 55.0,
         "unit": "%", "event_time": (NOW - timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
         "schema_version": "1"}
    e.update(over)
    return json.dumps(e)


def bronze(spark, payloads):
    rows = [Row(kafka_key=None, payload=p, topic="t", kafka_partition=0, kafka_offset=i,
                kafka_ts=NOW, ingest_ts=NOW) for i, p in enumerate(payloads)]
    return spark.createDataFrame(rows)


def reasons(rejects):
    return sorted(r.reason for r in rejects.collect())


def test_valid_event_goes_to_silver(spark):
    silver, rejects = bronze_to_silver(bronze(spark, [ev()]))
    rows = silver.collect()
    assert len(rows) == 1 and rejects.count() == 0
    r = rows[0]
    assert (r.event_id, r.device_id, r.metric, r.value) == ("e-1", "dev-0001", "battery_pct", 55.0)
    assert r.event_ts is not None and r.kafka_offset == 0


def test_each_bad_record_gets_the_right_reason(spark):
    payloads = [
        ev(event_id="a")[:25],                                   # truncated JSON
        ev(event_id="b", value=250.0),                           # battery above 100
        ev(event_id="c", metric="humidity_pct"),                 # unknown metric
        ev(event_id="d", device_id=None),                        # missing device
        ev(event_id="e", device_id="sensor-9"),                  # bad device id format
        ev(event_id="f", event_time=(NOW + timedelta(hours=2)).isoformat()),   # future
        ev(event_id="g", event_time="not-a-time"),               # unparseable time
        ev(event_id=None),                                       # missing event id
        "{}",                                                    # empty object
    ]
    silver, rejects = bronze_to_silver(bronze(spark, payloads))
    assert silver.count() == 0
    assert reasons(rejects) == sorted([
        "malformed_json", "value_out_of_range", "unknown_metric", "missing_device_id", "missing_device_id",
        "event_time_in_future", "bad_event_time", "missing_event_id", "malformed_json"])


def test_rejects_keep_raw_payload_and_offsets(spark):
    _, rejects = bronze_to_silver(bronze(spark, [ev(value=999.0)]))
    r = rejects.collect()[0]
    assert json.loads(r.payload)["value"] == 999.0 and r.kafka_offset == 0 and r.reason == "value_out_of_range"


def test_duplicates_in_a_batch_are_collapsed(spark):
    silver, _ = bronze_to_silver(bronze(spark, [ev(), ev(), ev(event_id="e-2")]))
    assert sorted(r.event_id for r in silver.collect()) == ["e-1", "e-2"]


def test_late_events_are_accepted(spark):
    late = ev(event_time=(NOW - timedelta(hours=3)).isoformat().replace("+00:00", "Z"))
    silver, rejects = bronze_to_silver(bronze(spark, [late]))
    assert silver.count() == 1 and rejects.count() == 0


def test_metric_ranges_are_inclusive(spark):
    payloads = [ev(event_id="lo", value=0.0), ev(event_id="hi", value=100.0),
                ev(event_id="rssi", metric="signal_rssi_dbm", value=-130.0, unit="dBm")]
    silver, rejects = bronze_to_silver(bronze(spark, payloads))
    assert silver.count() == 3 and rejects.count() == 0


def test_hourly_aggregates(spark):
    base = NOW.replace(minute=0, second=0)
    payloads = [ev(event_id=f"x{i}", value=float(v), event_time=(base + timedelta(minutes=i)).isoformat())
                for i, v in enumerate([10, 20, 30, 40])]
    silver, _ = bronze_to_silver(bronze(spark, payloads))
    g = hourly_aggregates(silver).selectExpr("*", "date_format(hour, 'yyyy-MM-dd HH:mm') AS hour_utc").collect()
    assert len(g) == 1
    r = g[0]
    assert (r.samples, r.min_value, r.max_value, r.avg_value) == (4, 10.0, 40.0, 25.0)
    assert r.hour_utc == base.strftime("%Y-%m-%d %H:%M")      # session time zone is UTC
