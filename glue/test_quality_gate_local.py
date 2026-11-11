
import pytest

from pyspark.sql import SparkSession, functions as F
from pyspark import StorageLevel


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("TestQualityGateLocal")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


def quality_gate(df, cond, max_reject=0.05):
    """Versión local de quality_gate para probar la lógica."""

    cached = (
        df.withColumn("_ok", F.coalesce(cond, F.lit(False)))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    try:
        total = cached.count()
        bad = cached.filter(~F.col("_ok"))
        n_bad = bad.count()

        if total and n_bad / total > max_reject:
            raise ValueError("Se superó el umbral de rechazos")

        valid = (
            cached.filter("_ok")
            .drop("_ok")
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        valid.count()

        return valid

    finally:
        cached.unpersist()


def test_quality_gate_separa_validos(spark):
    df = spark.createDataFrame(
        [
            (1, 100.0),
            (2, 200.0),
            (3, -10.0),
        ],
        ["id", "amount"]
    )

    result = quality_gate(
        df,
        F.col("amount") > 0,
        max_reject=0.40
    )

    try:
        assert result.count() == 2
        assert result.filter(F.col("amount") <= 0).count() == 0

    finally:
        result.unpersist()


def test_quality_gate_falla_si_supera_umbral(spark):
    df = spark.createDataFrame(
        [
            (1, 100.0),
            (2, 200.0),
            (3, -10.0),
            (4, -20.0),
        ],
        ["id", "amount"]
    )

    with pytest.raises(
        ValueError,
        match="Se superó el umbral"
    ):
        quality_gate(
            df,
            F.col("amount") > 0,
            max_reject=0.25
        )
