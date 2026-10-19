"""Data-quality gate over the lakehouse tables. Writes a JSON report and exits 1 if any check fails, so a
scheduler (or CI) stops downstream jobs instead of publishing bad numbers.

  python -m jobs.dq_checks --max-reject-rate 0.10 --report dq_report.json
"""
import argparse
import json
import sys

from sdp.config import SETTINGS
from sdp.spark import build_session


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-reject-rate", type=float, default=0.10)
    ap.add_argument("--report", default="dq_report.json")
    args = ap.parse_args()
    spark = build_session("dq-checks")
    s, b, q, g = (SETTINGS.table(*t) for t in
                  [("silver", "telemetry"), ("bronze", "telemetry_raw"), ("quarantine", "telemetry_rejects"),
                   ("gold", "device_metric_hourly")])
    one = lambda sql: spark.sql(sql).collect()[0][0]   # noqa: E731
    metrics = {
        "bronze_rows": one(f"SELECT count(*) FROM {b}"),
        "silver_rows": one(f"SELECT count(*) FROM {s}"),
        "reject_rows": one(f"SELECT count(*) FROM {q}"),
        "silver_duplicate_event_ids": one(
            f"SELECT count(*) FROM (SELECT event_id FROM {s} GROUP BY event_id HAVING count(*) > 1)"),
        "silver_null_keys": one(f"SELECT count(*) FROM {s} WHERE event_id IS NULL OR device_id IS NULL "
                                f"OR event_ts IS NULL OR value IS NULL"),
        "gold_samples_total": one(f"SELECT coalesce(sum(samples), 0) FROM {g}"),
        "gold_vs_silver_mismatch_hours": one(f"""
            SELECT count(*) FROM (
              SELECT device_id, metric, date_trunc('hour', event_ts) AS hour, count(*) AS n FROM {s}
              GROUP BY 1, 2, 3) x
            FULL OUTER JOIN {g} y ON x.device_id = y.device_id AND x.metric = y.metric AND x.hour = y.hour
            WHERE coalesce(x.n, -1) <> coalesce(y.samples, -2)"""),
        "rejects_by_reason": {r[0]: r[1] for r in spark.sql(
            f"SELECT reason, count(*) FROM {q} GROUP BY reason ORDER BY 2 DESC").collect()},
    }
    processed = metrics["silver_rows"] + metrics["reject_rows"]
    metrics["reject_rate"] = round(metrics["reject_rows"] / processed, 4) if processed else 0.0
    checks = {
        "no_duplicate_event_ids": metrics["silver_duplicate_event_ids"] == 0,
        "no_null_keys": metrics["silver_null_keys"] == 0,
        "gold_reconciles_with_silver": metrics["gold_vs_silver_mismatch_hours"] == 0,
        "reject_rate_below_threshold": metrics["reject_rate"] <= args.max_reject_rate,
        "silver_not_empty": metrics["silver_rows"] > 0,
    }
    report = {"metrics": metrics, "checks": checks, "passed": all(checks.values())}
    with open(args.report, "w") as fh:
        json.dump(report, fh, indent=2, default=str)
    print(json.dumps(report, indent=2, default=str), flush=True)
    spark.stop()
    sys.exit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
