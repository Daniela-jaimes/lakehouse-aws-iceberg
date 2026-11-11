"""Small column-level functions to type, normalize and deduplicate data."""
from pyspark.sql import Window
from pyspark.sql import functions as F

EMAIL_REGEX = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"


def clean_text(column):
    """Trim, collapse repeated spaces and turn empty text into null."""
    cleaned = F.trim(F.regexp_replace(column, r"\s+", " "))
    return F.when(cleaned != "", cleaned)


def title_text(column):
    """clean_text + 'juan perez' / 'JUAN PEREZ' -> 'Juan Perez'."""
    return F.initcap(clean_text(column))


def lower_text(column):
    """clean_text + lower case (emails, status, payment method)."""
    return F.lower(clean_text(column))


def is_valid_email(column):
    """True / False, never null."""
    return F.coalesce(column.rlike(EMAIL_REGEX), F.lit(False))


def deduplicate(df, key, order_column):
    """Keep one row per key: the one with the highest order_column."""
    window = Window.partitionBy(key).orderBy(F.col(order_column).desc_nulls_last())
    return (df.withColumn("_rn", F.row_number().over(window))
              .filter(F.col("_rn") == 1)
              .drop("_rn"))
