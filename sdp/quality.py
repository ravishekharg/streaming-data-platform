"""Record-level validation. Each rule is a SQL boolean expression over the parsed columns; the first rule a row
fails becomes its rejection reason. Rules are data, so they are easy to review, test and extend."""
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .schema import METRIC_RANGES

_range_sql = " OR ".join(
    f"(metric = '{m}' AND value BETWEEN {lo} AND {hi})" for m, (lo, hi, _) in METRIC_RANGES.items())

# (reason, condition that must be TRUE for a valid row)
RULES: list[tuple[str, str]] = [
    # Spark 3+ from_json returns a struct of all-null fields (not NULL) for unparseable input
    ("malformed_json", "parsed IS NOT NULL AND to_json(parsed) <> '{}'"),
    ("missing_event_id", "event_id IS NOT NULL AND length(event_id) > 0"),
    ("missing_device_id", "device_id IS NOT NULL AND device_id RLIKE '^dev-[0-9]{4}$'"),
    ("unknown_metric", f"metric IN ({', '.join(repr(m) for m in METRIC_RANGES)})"),
    ("value_missing", "value IS NOT NULL AND NOT isnan(value)"),
    ("value_out_of_range", _range_sql),
    ("bad_event_time", "event_ts IS NOT NULL"),
    ("event_time_in_future", "event_ts <= ingest_ts + INTERVAL 5 MINUTES"),
]


def _reason_column() -> Column:
    expr = None
    for reason, ok in reversed(RULES):          # first failing rule wins
        cond = ~F.coalesce(F.expr(ok), F.lit(False))
        expr = F.when(cond, F.lit(reason)) if expr is None else F.when(cond, F.lit(reason)).otherwise(expr)
    return expr


def split_valid_invalid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Input needs the parsed struct column `parsed`, the flattened fields, event_ts and ingest_ts.
    Returns (valid rows, rejected rows with a `reason`)."""
    tagged = df.withColumn("reason", _reason_column())
    return tagged.where("reason IS NULL").drop("reason"), tagged.where("reason IS NOT NULL")
