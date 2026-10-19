"""Silver -> gold hourly aggregates. Recomputes the last N hours of event time and MERGEs them, so it is safe to
re-run (same result) and it picks up late-arriving events for hours already aggregated.

  python -m jobs.build_gold --hours 48
"""
import argparse

from pyspark.sql import functions as F

from sdp.config import SETTINGS
from sdp.spark import build_session
from sdp.transforms import hourly_aggregates


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=48, help="event-time window to recompute")
    args = ap.parse_args()
    spark = build_session("build-gold")
    silver = spark.table(SETTINGS.table("silver", "telemetry")).where(
        F.col("event_ts") >= F.expr(f"date_trunc('hour', current_timestamp() - INTERVAL {args.hours} HOURS)"))
    hourly_aggregates(silver).createOrReplaceTempView("fresh_gold")
    spark.sql(f"""
        MERGE INTO {SETTINGS.table('gold', 'device_metric_hourly')} t
        USING fresh_gold s
        ON t.device_id = s.device_id AND t.metric = s.metric AND t.hour = s.hour
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    n = spark.table(SETTINGS.table("gold", "device_metric_hourly")).count()
    print(f"gold rows: {n}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
