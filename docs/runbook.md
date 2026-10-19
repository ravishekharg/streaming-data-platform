# Operations runbook

## The streaming job died / was redeployed
Nothing to do: it restarts from the checkpoint (Spark Operator `restartPolicy: Always`). Writes are idempotent, so a
replayed micro-batch does not duplicate data.

## The checkpoint is lost or corrupted
Delete it and restart. The job re-reads the topic from `earliest`; the `MERGE`s skip records already written. This is
safe within Kafka retention (7 days). CI exercises exactly this.

## Reject rate alert fired
```sql
SELECT reason, count(*), min(ingest_ts), max(ingest_ts) FROM quarantine.telemetry_rejects
WHERE ingest_ts > current_timestamp - INTERVAL '1' HOUR GROUP BY reason ORDER BY 2 DESC;
SELECT payload FROM quarantine.telemetry_rejects WHERE reason = 'value_out_of_range' LIMIT 20;
```
- A new reason after a device firmware release usually means a contract change. Agree the schema (bump
  `schema_version`), then add or adjust the rule with a test.
- To reprocess fixed records, re-publish the payloads from quarantine to the topic (they get new offsets; silver dedupes
  on `event_id`).

## Gold numbers look wrong for a past day
Recompute a wider window: `python -m jobs.build_gold --hours 168`. It is idempotent. Then run `dq_checks`.

## A bad write needs undoing
Find the snapshot before it and roll back:
```sql
SELECT snapshot_id, committed_at, operation FROM lake.silver.`telemetry`.snapshots ORDER BY committed_at DESC;  -- Spark
CALL lake.system.rollback_to_snapshot('silver.telemetry', <snapshot_id>);
```

## Queries got slow
Usually small files or too many snapshots: check `SELECT count(*) FROM silver."telemetry$files"` in Trino, then run
`jobs.maintenance` (or check why the daily ScheduledSparkApplication failed).

## Consumer lag growing
- Check executor CPU and memory, and `maxOffsetsPerTrigger` (default 50,000).
- Scale: more partitions on the topic and more executors (dynamic allocation is on).
- Kafka lag: `kafka-consumer-groups.sh` is not used by Structured Streaming; read the offsets in the checkpoint, or use
  the `stream-ingest` query progress metrics (`inputRowsPerSecond` vs `processedRowsPerSecond`).
