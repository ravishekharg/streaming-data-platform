"""Pure DataFrame transformations shared by the streaming and batch jobs (unit-tested without Kafka or S3)."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .quality import split_valid_invalid
from .schema import TELEMETRY_SCHEMA


def to_bronze(kafka_df: DataFrame) -> DataFrame:
    """Kafka source rows -> bronze rows (raw payload kept byte-for-byte for replay and audit)."""
    return kafka_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("payload"),
        "topic",
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.current_timestamp().alias("ingest_ts"),
    )


def parse(bronze_df: DataFrame) -> DataFrame:
    """Bronze -> typed columns. Malformed JSON yields parsed = NULL (caught by the malformed_json rule)."""
    parsed = bronze_df.withColumn("parsed", F.from_json("payload", TELEMETRY_SCHEMA, {"mode": "PERMISSIVE"}))
    return parsed.select(
        "payload", "parsed", "kafka_partition", "kafka_offset", "ingest_ts",
        F.col("parsed.event_id").alias("event_id"),
        F.col("parsed.device_id").alias("device_id"),
        F.col("parsed.site").alias("site"),
        F.col("parsed.metric").alias("metric"),
        F.col("parsed.value").alias("value"),
        F.col("parsed.unit").alias("unit"),
        F.to_timestamp("parsed.event_time").alias("event_ts"),
    )


def dedupe_latest(df: DataFrame) -> DataFrame:
    """Keep one row per event_id inside a micro-batch (the producer may resend). Highest offset wins."""
    w = Window.partitionBy("event_id").orderBy(F.col("kafka_partition").desc(), F.col("kafka_offset").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


SILVER_COLUMNS = ["event_id", "device_id", "site", "metric", "value", "unit", "event_ts", "ingest_ts",
                  "kafka_partition", "kafka_offset"]
REJECT_COLUMNS = ["payload", "reason", "kafka_partition", "kafka_offset", "ingest_ts"]


def bronze_to_silver(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid, rejects = split_valid_invalid(parse(bronze_df))
    return dedupe_latest(valid).select(*SILVER_COLUMNS), rejects.select(*REJECT_COLUMNS)


def hourly_aggregates(silver_df: DataFrame) -> DataFrame:
    """Silver -> gold: per device, metric and hour. percentile_approx keeps it cheap on large data."""
    return (silver_df
            .groupBy("device_id", "site", "metric", F.date_trunc("hour", "event_ts").alias("hour"))
            .agg(F.count("*").alias("samples"),
                 F.round(F.avg("value"), 3).alias("avg_value"),
                 F.min("value").alias("min_value"),
                 F.max("value").alias("max_value"),
                 F.percentile_approx("value", 0.95).alias("p95_value"))
            .withColumn("updated_at", F.current_timestamp()))
