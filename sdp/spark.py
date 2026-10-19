"""SparkSession factory. Catalog options:
  ICEBERG_CATALOG_TYPE=rest  (default) Iceberg REST catalog + S3-compatible storage (SeaweedFS locally)
  ICEBERG_CATALOG_TYPE=glue  AWS Glue Data Catalog + S3 (EMR Serverless / EKS on AWS)
SDP_JARS_BUNDLED=1 skips the Maven download when the jars are baked into the image (see Dockerfile)."""
import os

from pyspark.sql import SparkSession

from .config import PACKAGES, SETTINGS, Settings


def build_session(app_name: str, s: Settings = SETTINGS, with_lake: bool = True) -> SparkSession:
    b = (SparkSession.builder.appName(app_name)
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "8")))
    if not with_lake:                         # unit tests: plain local Spark, no downloads
        return (b.master("local[2]").config("spark.ui.enabled", "false")
                .config("spark.driver.memory", "512m").getOrCreate())
    if not os.getenv("SDP_JARS_BUNDLED"):
        b = b.config("spark.jars.packages", PACKAGES).config("spark.jars.ivy", "/tmp/.ivy2")
    c = f"spark.sql.catalog.{s.catalog}"
    b = (b.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config(c, "org.apache.iceberg.spark.SparkCatalog")
         .config(f"{c}.warehouse", s.warehouse)
         .config(f"{c}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"))
    if os.getenv("ICEBERG_CATALOG_TYPE", "rest") == "glue":
        b = b.config(f"{c}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    else:
        b = (b.config(f"{c}.type", "rest").config(f"{c}.uri", s.catalog_uri)
             .config(f"{c}.s3.endpoint", s.s3_endpoint).config(f"{c}.s3.path-style-access", "true"))
    return b.getOrCreate()
