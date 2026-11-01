"""Check an erasure (jobs/forget_device.py) from the outside, through Trino: the device has no rows in any table, the
other devices' data is untouched, and time travel to the snapshot from before the erasure no longer works.
Exits 1 on any failure.  python scripts/verify_forget.py --report forget_report.json"""
import argparse
import json
import sys

import trino


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", default="forget_report.json")
    ap.add_argument("--out", default="e2e_forget.json")
    args = ap.parse_args()
    rep = json.load(open(args.report))
    d = rep["device_id"]
    cur = trino.dbapi.connect(host="localhost", port=8080, user="ci", catalog="lake").cursor()

    def count(sql):
        cur.execute(sql)
        return cur.fetchall()[0][0]

    where = {
        "bronze.telemetry_raw": f"(kafka_key = '{d}' OR json_extract_scalar(payload, '$.device_id') = '{d}' "
                                f"OR payload LIKE '%\"{d}\"%')",
        "quarantine.telemetry_rejects": f"(json_extract_scalar(payload, '$.device_id') = '{d}' "
                                        f"OR payload LIKE '%\"{d}\"%')",
        "silver.telemetry": f"device_id = '{d}'",
        "gold.device_metric_hourly": f"device_id = '{d}'",
    }
    checks, got = {}, {}
    for table, t in rep["tables"].items():
        now = count(f"SELECT count(*) FROM {table} WHERE {where[table]}")
        total = count(f"SELECT count(*) FROM {table}")
        try:
            old = count(f"SELECT count(*) FROM {table} FOR VERSION AS OF {t['pre_erasure_snapshot']} "
                        f"WHERE {where[table]}")
            travel = f"still readable: {old} rows"
        except trino.exceptions.TrinoQueryError as e:
            old, travel = None, f"refused ({e.error_name})"
        got[table] = {"rows_before": t["rows_before"], "rows_now": now, "total_now": total, "time_travel": travel}
        checks[f"{table}: no rows for {d}"] = now == 0
        checks[f"{table}: only {d}'s rows removed"] = total == t["total_before"] - t["rows_before"]
        checks[f"{table}: pre-erasure snapshot gone (time travel refused)"] = old is None
    checks["the device had data to erase"] = rep["tables"]["silver.telemetry"]["rows_before"] > 0
    result = {"device_id": d, "tables": got, "checks": checks, "passed": all(checks.values())}
    json.dump(result, open(args.out, "w"), indent=2)
    for k, v in checks.items():
        print(("PASS " if v else "FAIL ") + k)
    print(json.dumps(got, indent=2))
    sys.exit(0 if result["passed"] else 1)


if __name__ == "__main__":
    main()
