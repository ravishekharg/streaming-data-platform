# Design decisions

## Why a lakehouse on Iceberg instead of Hive tables

The pipelines I built earlier wrote Hive tables and were scheduled by Oozie. They worked, but a few problems kept
coming back:

| Hive-style tables | Iceberg |
|---|---|
| A job failing halfway leaves partial partitions | atomic commits: readers see the old or the new snapshot, never half |
| Upserts mean rewriting whole partitions | `MERGE INTO` with merge-on-read deletes |
| Partition columns leak into every query (`WHERE dt='...'`) | hidden partitioning: `days(event_ts)`; queries filter on `event_ts` |
| Schema changes are risky | column adds/renames are metadata-only, tracked by ID |
| No undo | time travel and rollback to a snapshot |
| Listing millions of files in HDFS/S3 is slow | manifests track files; no directory listing |

The engines are interchangeable on top: Spark writes, and Trino, Athena or Spark read.

## Exactly-once without Kafka transactions

Structured Streaming gives at-least-once delivery into `foreachBatch`: after a failure the last micro-batch is
replayed from the checkpointed offsets. Instead of relying on that being rare, every sink write is an idempotent
upsert:

- **bronze** and **quarantine**: `MERGE ... ON (topic, partition, offset) WHEN NOT MATCHED THEN INSERT`. A Kafka
  record lands once, however often it is re-read.
- **silver**: `MERGE ... ON event_id WHEN NOT MATCHED THEN INSERT`, after a per-batch `row_number()` dedupe. This
  also absorbs producer resends, which carry the same `event_id` at a different offset.

CI proves it: it deletes the checkpoint and re-reads the whole topic from `earliest`, then asserts identical counts.

Trade-off: `MERGE` costs more than append (it joins against the target). Bronze is partitioned by ingest day and the
join is on the partition's recent data, so the cost stays bounded. At very high volume, switch bronze to plain
append plus a dedupe view, and keep `MERGE` for silver only.

## Validation as data

Rules are `(reason, SQL condition)` pairs evaluated in order; the first failure is the reason. Adding a rule is a
one-line change with a test, and the quarantine table answers "how much are we rejecting and why" directly.
Malformed JSON needs care: since Spark 3.0, `from_json` returns a struct of nulls rather than `NULL`, so the rule
checks `to_json(parsed) <> '{}'`.

## Late data

Silver keeps every valid event regardless of lateness (no watermark drop). Gold is recomputed for the last 48 hours
of event time and `MERGE`d, so an event that arrives 3 hours late updates the correct hourly row on the next run.
The window is a tunable trade-off between correctness and cost. Events later than the window can be backfilled by
running `build_gold --hours N`.

## Data-quality gate

`dq_checks` runs after gold and fails the run if:
- silver has duplicate `event_id`s or null keys;
- gold does not reconcile with silver (per device, metric and hour);
- the reject rate exceeds a threshold;
- silver is empty.

In production it would block the downstream publish step and alert.

## Small files and table health

Micro-batches every 10 s create many small files and snapshots. Daily maintenance:
- bin-packs files to ~128 MB;
- rewrites position-delete files;
- expires snapshots older than 7 days (keeps the last 5);
- rewrites manifests.

Kafka keeps 7 days of retention, which is the replay window if the lakehouse ever has to be rebuilt from the topic.

## Local vs cloud

| Component | Local / CI | Kubernetes | AWS managed |
|---|---|---|---|
| Kafka | `apache/kafka` KRaft, 1 node | Strimzi, 3 KRaft nodes | MSK Serverless (IAM auth) |
| Spark | pip `pyspark` on the host | Spark Operator | EMR Serverless |
| Catalog | Iceberg REST | Iceberg REST (DB-backed) | Glue Data Catalog |
| Storage | MinIO | S3 | S3 |
| SQL | Trino | Trino (Helm) | Athena / Trino |
