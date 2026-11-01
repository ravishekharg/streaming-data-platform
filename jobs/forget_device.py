"""Erase one device's data from the lakehouse, for real (GDPR / DPDP "right to erasure", or a customer contract ending).

An Iceberg DELETE only writes a new snapshot. The old data files stay in storage and stay readable through time
travel (`VERSION AS OF`) until three more things happen, so this job does all four, per table:

  1. DELETE the device's rows (bronze and quarantine by the raw payload/key, silver and gold by device_id)
  2. rewrite data files that still carry delete files (silver is merge-on-read: a DELETE there only adds
     position-delete files, the rows are still physically in the old data files)
  3. expire every snapshot but the current one, so no snapshot references the old files any more
  4. remove orphan files: delete from storage every file no snapshot references

Then it checks that no remaining snapshot of any table returns a row for the device, and writes a report with the
pre-erasure snapshot ids (verify_forget.py proves time travel to them is gone).

Run it when nothing else writes the tables (stop the stream first): orphan removal can delete files that a
concurrent writer has written but not committed yet. That's why Iceberg's default keeps files younger than 3 days.

  python -m jobs.forget_device --device-id dev-0042 --report forget_report.json
"""
import argparse
import json
from datetime import datetime, timezone

from sdp.config import SETTINGS
from sdp.spark import build_session


def predicates(device_id: str) -> dict[str, str]:
    d = device_id.replace("'", "''")
    payload_match = f"(get_json_object(payload, '$.device_id') = '{d}' OR payload LIKE '%\"{d}\"%')"
    return {
        "bronze.telemetry_raw": f"(kafka_key = '{d}' OR {payload_match})",
        "quarantine.telemetry_rejects": payload_match,   # malformed JSON can't be parsed: match the raw text too
        "silver.telemetry": f"device_id = '{d}'",
        "gold.device_metric_hourly": f"device_id = '{d}'",
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--device-id", required=True)
    ap.add_argument("--report", default="forget_report.json")
    args = ap.parse_args()
    spark = build_session("forget-device")
    cat = SETTINGS.catalog
    report = {"device_id": args.device_id, "tables": {}}

    def scalar(sql):
        return spark.sql(sql).collect()[0][0]

    for table, where in predicates(args.device_id).items():
        t = f"{cat}.{table}"
        before_snapshot = scalar(f"SELECT snapshot_id FROM {t}.snapshots ORDER BY committed_at DESC LIMIT 1")
        rows = scalar(f"SELECT count(*) FROM {t} WHERE {where}")
        total = scalar(f"SELECT count(*) FROM {t}")
        spark.sql(f"DELETE FROM {t} WHERE {where}")
        # fold delete files into rewritten data files, so the deleted rows are no longer in any live file
        spark.sql(f"CALL {cat}.system.rewrite_data_files(table => '{table}', "
                  f"options => map('delete-file-threshold', '1', 'rewrite-all', 'true'))").collect()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        expired = spark.sql(f"CALL {cat}.system.expire_snapshots(table => '{table}', "
                            f"older_than => TIMESTAMP '{now}', retain_last => 1)").collect()[0]
        orphans = spark.sql(f"CALL {cat}.system.remove_orphan_files(table => '{table}', "
                            f"older_than => TIMESTAMP '{now}')").collect()
        # every snapshot that still exists (only the current one now) must hold nothing for the device
        snapshots = [r[0] for r in spark.sql(f"SELECT snapshot_id FROM {t}.snapshots").collect()]
        left = sum(scalar(f"SELECT count(*) FROM {t} VERSION AS OF {s} WHERE {where}") for s in snapshots)
        report["tables"][table] = {
            "rows_before": rows, "total_before": total, "total_after": scalar(f"SELECT count(*) FROM {t}"),
            "pre_erasure_snapshot": before_snapshot, "snapshots_left": len(snapshots),
            "rows_left_in_any_snapshot": left,
            "data_files_expired": int(expired["deleted_data_files_count"]),
            "delete_files_expired": int(expired["deleted_position_delete_files_count"]),
            "orphan_files_removed": len(orphans),
        }
        print(table, report["tables"][table])
    json.dump(report, open(args.report, "w"), indent=2)
    spark.stop()


if __name__ == "__main__":
    main()
