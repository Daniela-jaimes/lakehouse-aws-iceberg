"""Surrogate keys and the 'Unknown' row of each dimension."""
from pyspark.sql import functions as F


def surrogate_key(*columns):
    """Deterministic bigint key: same natural key -> same surrogate key on every run."""
    return F.abs(F.xxhash64(*columns))


def add_unknown_row(df, key_column, labels):
    """Append the row with key -1. labels: {column: text} for the descriptive columns."""
    values = []
    for field in df.schema.fields:
        if field.name == key_column:
            values.append(-1)
        else:
            values.append(labels.get(field.name))
    unknown = df.sparkSession.createDataFrame([tuple(values)], df.schema)
    return df.unionByName(unknown)
