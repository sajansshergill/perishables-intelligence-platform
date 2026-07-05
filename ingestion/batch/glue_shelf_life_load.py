# Shelf-life reference batch load placeholder.
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

PRODUCT_ID_RE = r"^P[0-9]+$"


def transform_shelf_life(df: DataFrame) -> DataFrame:
    """Clean, validate and derive SCD2 columns from stacked shelf-life revisions."""
    cleaned = (
        df
        .withColumn("product_id", F.upper(F.trim(F.col("product_id"))))
        # try_cast tolerates dirty cells (returns null) rather than failing the
        # whole job under ANSI mode — nulls are quarantined by the filter below.
        .withColumn("shelf_life_days", F.expr("try_cast(shelf_life_days as int)"))
        .withColumn("effective_from", F.expr("try_cast(effective_from as date)"))
    )

    valid = cleaned.filter(
        F.col("product_id").isNotNull()
        & F.col("product_id").rlike(PRODUCT_ID_RE)
        & (F.col("shelf_life_days") > 0)
        & F.col("effective_from").isNotNull()
    )

    # Collapse accidental duplicates: one revision per (product, effective_from),
    # keeping the largest shelf_life if a day was recorded twice.
    per_day = Window.partitionBy("product_id", "effective_from").orderBy(
        F.col("shelf_life_days").desc()
    )
    one_per_day = (
        valid
        .withColumn("_rn", F.row_number().over(per_day))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # SCD2 close: a record is valid until the next revision for that product
    # begins; the record with no successor is the current one.
    history = Window.partitionBy("product_id").orderBy("effective_from")
    scd2 = (
        one_per_day
        .withColumn("effective_to", F.lead("effective_from").over(history))
        .withColumn("is_current", F.col("effective_to").isNull())
        .withColumn("ingested_at", F.current_timestamp())
    )

    return scd2.select(
        "product_id", "shelf_life_days", "effective_from",
        "effective_to", "is_current", "ingested_at",
    )


def run() -> None:
    import sys
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_path", "target_path"])
    sc = SparkContext.getOrCreate()
    glue = GlueContext(sc)
    spark = glue.spark_session
    job = Job(glue)
    job.init(args["JOB_NAME"], args)

    raw = spark.read.option("header", True).csv(args["source_path"])
    conformed = transform_shelf_life(raw)
    # Full rebuild: the dimension is derived from the complete history each run.
    conformed.write.mode("overwrite").parquet(args["target_path"])
    job.commit()


if __name__ == "__main__":
    run()