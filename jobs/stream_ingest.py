"""Kafka -> Iceberg bronze, silver and quarantine with Spark Structured Streaming.

Each micro-batch (foreachBatch):
  1. MERGEs the raw records into bronze on (partition, offset): a replayable audit copy,
  2. parses and validates them; invalid rows are MERGEd into quarantine (same key) with a reason,
  3. MERGEs valid rows into silver on event_id.
Every write is an idempotent upsert, so a retried micro-batch, a producer resend, or even a full replay of the topic
after losing the checkpoint cannot create duplicates: effectively exactly-once into all three tables.

  python -m jobs.stream_ingest --once      # process everything available, then stop (CI / backfills)
  python -m jobs.stream_ingest             # run continuously, 10 s micro-batches
"""
import argparse

from pyspark.sql import DataFrame

from sdp.config import SETTINGS
from sdp.spark import build_session
from sdp.transforms import bronze_to_silver, to_bronze


def write_batch(batch: DataFrame, batch_id: int) -> None:
    spark = batch.sparkSession
    bronze = to_bronze(batch).persist()
    if bronze.isEmpty():
        bronze.unpersist()
        return
    bronze.createOrReplaceTempView("incoming_bronze")
    spark.sql(f"""
        MERGE INTO {SETTINGS.table('bronze', 'telemetry_raw')} t
        USING incoming_bronze s
        ON t.topic = s.topic AND t.kafka_partition = s.kafka_partition AND t.kafka_offset = s.kafka_offset
        WHEN NOT MATCHED THEN INSERT *
    """)
    silver, rejects = bronze_to_silver(bronze)
    rejects.createOrReplaceTempView("incoming_rejects")
    spark.sql(f"""
        MERGE INTO {SETTINGS.table('quarantine', 'telemetry_rejects')} t
        USING incoming_rejects s
        ON t.kafka_partition = s.kafka_partition AND t.kafka_offset = s.kafka_offset
        WHEN NOT MATCHED THEN INSERT *
    """)
    silver.createOrReplaceTempView("incoming_silver")
    spark.sql(f"""
        MERGE INTO {SETTINGS.table('silver', 'telemetry')} t
        USING incoming_silver s
        ON t.event_id = s.event_id
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"batch {batch_id}: bronze={bronze.count()} rejects={rejects.count()} silver_candidates={silver.count()}",
          flush=True)
    bronze.unpersist()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="process all available data then stop")
    args = ap.parse_args()
    spark = build_session("stream-ingest")
    source = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", SETTINGS.kafka_bootstrap)
              .option("subscribe", SETTINGS.topic)
              .option("startingOffsets", "earliest")
              .option("maxOffsetsPerTrigger", SETTINGS.max_offsets_per_trigger)
              .option("failOnDataLoss", "false")
              .load())
    writer = (source.writeStream.foreachBatch(write_batch)
              .option("checkpointLocation", f"{SETTINGS.checkpoint_dir}/stream_ingest")
              .queryName("telemetry-ingest"))
    query = writer.trigger(availableNow=True).start() if args.once else \
        writer.trigger(processingTime="10 seconds").start()
    query.awaitTermination()


if __name__ == "__main__":
    main()
