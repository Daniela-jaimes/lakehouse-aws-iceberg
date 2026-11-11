"""Data quality gate: separates valid rows from rejects and fails above the threshold."""
from pyspark.sql import functions as F

from helpers.config import MAX_REJECT_RATE


def quality_gate(df, entity, rules, rejects_path, max_rate=MAX_REJECT_RATE):
    """
    rules: list of (reason, bad_condition). A row is rejected when a condition is true;
    the first matching reason is stored in the _reject_reason column.
    """
    reason = F.coalesce(*[F.when(bad, F.lit(name)) for name, bad in rules])
    flagged = df.withColumn("_reject_reason", reason).persist()

    counts = flagged.agg(
        F.count(F.lit(1)).alias("total"),
        F.count("_reject_reason").alias("rejected"),   # count() skips nulls
    ).first()
    total, rejected = counts["total"], counts["rejected"]
    rate = rejected / total if total else 0.0
    print(f"[QUALITY] entity={entity} total={total} rejected={rejected} rate={rate:.4f}")

    (flagged.filter(F.col("_reject_reason").isNotNull())
            .write.mode("overwrite").parquet(rejects_path))

    if rate > max_rate:
        raise ValueError(f"{entity}: reject rate {rate:.2%} is above {max_rate:.0%}")

    return flagged.filter(F.col("_reject_reason").isNull()).drop("_reject_reason")
