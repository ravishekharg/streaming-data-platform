"""Create namespaces and tables (idempotent)."""
from sdp.config import SETTINGS
from sdp.schema import ddl
from sdp.spark import build_session

if __name__ == "__main__":
    spark = build_session("init-tables")
    for stmt in ddl(SETTINGS.catalog):
        spark.sql(stmt)
    for layer in ("bronze", "silver", "gold", "quarantine"):
        spark.sql(f"SHOW TABLES IN {SETTINGS.catalog}.{layer}").show(truncate=False)
    spark.stop()
