"""Iceberg table maintenance: streaming writes many small files and snapshots, so compact and expire regularly.
Run daily (Kubernetes CronJob / ScheduledSparkApplication).

  python -m jobs.maintenance --retain-days 7
"""
import argparse
from datetime import datetime, timedelta, timezone

from sdp.config import SETTINGS
from sdp.spark import build_session

TABLES = [("bronze", "telemetry_raw"), ("silver", "telemetry"), ("quarantine", "telemetry_rejects"),
          ("gold", "device_metric_hourly")]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--retain-days", type=int, default=7)
    args = ap.parse_args()
    spark = build_session("table-maintenance")
    cat = SETTINGS.catalog
    # procedure arguments must be literals, so compute the cutoff here (UTC, explicit offset)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=args.retain_days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    for layer, name in TABLES:
        t = f"{layer}.{name}"
        # bin-pack small files into ~128 MB files
        spark.sql(f"CALL {cat}.system.rewrite_data_files(table => '{t}', "
                  f"options => map('target-file-size-bytes','134217728','min-input-files','2'))").show(truncate=False)
        # merge-on-read deletes in silver leave delete files; fold them back in
        if name == "telemetry":
            spark.sql(f"CALL {cat}.system.rewrite_position_delete_files(table => '{t}')").show(truncate=False)
        spark.sql(f"CALL {cat}.system.expire_snapshots(table => '{t}', "
                  f"older_than => TIMESTAMP '{cutoff}', retain_last => 5)"
                  ).show(truncate=False)
        spark.sql(f"CALL {cat}.system.rewrite_manifests(table => '{t}')").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
