"""Compare what the producer sent (expectations.json) with what landed in the lakehouse, queried through Trino.
Exits 1 on any mismatch.  python scripts/verify_e2e.py --expect expectations.json [--label "after replay"]"""
import argparse
import json
import sys

import trino


def q(cur, sql):
    cur.execute(sql)
    return cur.fetchall()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--expect", default="expectations.json")
    ap.add_argument("--label", default="first run")
    ap.add_argument("--out", default="e2e_result.json")
    args = ap.parse_args()
    exp = json.load(open(args.expect))
    cur = trino.dbapi.connect(host="localhost", port=8080, user="ci", catalog="lake").cursor()
    got = {
        "bronze_rows": q(cur, "SELECT count(*) FROM bronze.telemetry_raw")[0][0],
        "silver_rows": q(cur, "SELECT count(*) FROM silver.telemetry")[0][0],
        "silver_distinct_event_ids": q(cur, "SELECT count(DISTINCT event_id) FROM silver.telemetry")[0][0],
        "reject_rows": q(cur, "SELECT count(*) FROM quarantine.telemetry_rejects")[0][0],
        "rejects_by_reason": dict(q(cur, "SELECT reason, count(*) FROM quarantine.telemetry_rejects GROUP BY reason")),
        "gold_samples_total": q(cur, "SELECT coalesce(sum(samples), 0) FROM gold.device_metric_hourly")[0][0],
        "late_events_in_silver": q(cur, "SELECT count(*) FROM silver.telemetry "
                                        "WHERE event_ts < ingest_ts - INTERVAL '55' MINUTE")[0][0],
    }
    checks = {
        "bronze == every record sent": got["bronze_rows"] == exp["sent"],
        "silver == unique valid events": got["silver_rows"] == exp["valid_unique"],
        "silver has no duplicate event_id": got["silver_rows"] == got["silver_distinct_event_ids"],
        "quarantine == bad records": got["reject_rows"] == exp["bad"],
        "reject reasons match": got["rejects_by_reason"] == exp["rejects_by_reason"],
        "gold reconciles with silver": got["gold_samples_total"] == got["silver_rows"],
        "late events kept": got["late_events_in_silver"] >= exp["late"],
    }
    result = {"label": args.label, "expected": exp, "got": got, "checks": checks, "passed": all(checks.values())}
    json.dump(result, open(args.out, "w"), indent=2)
    print(f"--- {args.label} ---")
    for name, ok in checks.items():
        print(f"{'PASS' if ok else 'FAIL'}  {name}")
    print(json.dumps(got, indent=2))
    sys.exit(0 if result["passed"] else 1)


if __name__ == "__main__":
    main()
