"""Event contract for the device-telemetry topic and DDL for the lakehouse tables."""
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# JSON value of each Kafka record. Everything arrives as a string or number; validation happens in quality.py.
TELEMETRY_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("device_id", StringType()),
    StructField("site", StringType()),
    StructField("metric", StringType()),
    StructField("value", DoubleType()),
    StructField("unit", StringType()),
    StructField("event_time", StringType()),       # ISO-8601, UTC
    StructField("schema_version", StringType()),
])

METRIC_RANGES = {            # metric -> (min, max, unit): physically plausible values
    "temperature_c": (-40.0, 85.0, "C"),
    "battery_pct": (0.0, 100.0, "%"),
    "signal_rssi_dbm": (-130.0, -20.0, "dBm"),
}


def ddl(catalog: str) -> list[str]:
    return [
        f"CREATE NAMESPACE IF NOT EXISTS {catalog}.bronze",
        f"CREATE NAMESPACE IF NOT EXISTS {catalog}.silver",
        f"CREATE NAMESPACE IF NOT EXISTS {catalog}.gold",
        f"CREATE NAMESPACE IF NOT EXISTS {catalog}.quarantine",
        f"""CREATE TABLE IF NOT EXISTS {catalog}.bronze.telemetry_raw (
              kafka_key STRING, payload STRING, topic STRING, kafka_partition INT, kafka_offset BIGINT,
              kafka_ts TIMESTAMP, ingest_ts TIMESTAMP)
            USING iceberg PARTITIONED BY (days(ingest_ts))""",
        f"""CREATE TABLE IF NOT EXISTS {catalog}.silver.telemetry (
              event_id STRING NOT NULL, device_id STRING NOT NULL, site STRING, metric STRING NOT NULL,
              value DOUBLE NOT NULL, unit STRING, event_ts TIMESTAMP NOT NULL, ingest_ts TIMESTAMP,
              kafka_partition INT, kafka_offset BIGINT)
            USING iceberg PARTITIONED BY (days(event_ts), metric)
            TBLPROPERTIES ('write.merge.mode'='merge-on-read', 'format-version'='2')""",
        f"""CREATE TABLE IF NOT EXISTS {catalog}.quarantine.telemetry_rejects (
              payload STRING, reason STRING, kafka_partition INT, kafka_offset BIGINT, ingest_ts TIMESTAMP)
            USING iceberg PARTITIONED BY (days(ingest_ts))""",
        f"""CREATE TABLE IF NOT EXISTS {catalog}.gold.device_metric_hourly (
              device_id STRING, site STRING, metric STRING, hour TIMESTAMP, samples BIGINT,
              avg_value DOUBLE, min_value DOUBLE, max_value DOUBLE, p95_value DOUBLE, updated_at TIMESTAMP)
            USING iceberg PARTITIONED BY (days(hour))""",
    ]
