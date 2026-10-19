"""Runtime settings, read from environment variables so the same jobs run locally, in CI and on Kubernetes."""
import os
from dataclasses import dataclass

SPARK_VERSION = "3.5.5"
ICEBERG_VERSION = "1.8.1"
PACKAGES = ",".join([
    f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}",
    f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}",
    f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
])


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    topic: str = os.getenv("TELEMETRY_TOPIC", "device-telemetry")
    catalog: str = os.getenv("ICEBERG_CATALOG", "lake")
    catalog_uri: str = os.getenv("ICEBERG_REST_URI", "http://localhost:8181")
    warehouse: str = os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse/")
    s3_endpoint: str = os.getenv("S3_ENDPOINT", "http://localhost:9000")
    checkpoint_dir: str = os.getenv("CHECKPOINT_DIR", "./checkpoints")
    max_offsets_per_trigger: int = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "50000"))

    def table(self, layer: str, name: str) -> str:
        return f"{self.catalog}.{layer}.{name}"


SETTINGS = Settings()
