# Supplier and warehouse batch load placeholder.
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

SUPPLIER_ID_RE = r"^SUP[0-9]+$"


def transform_suppliers(df: DataFrame, load_date: str) -> DataFrame:
    """Clean, validate, dedupe and stamp a raw supplier extract."""
    cleaned = (
        df
        .withColumn("supplier_id", F.upper(F.trim(F.col("supplier_id"))))
        .withColumn("supplier_name", F.trim(F.col("supplier_name")))
        # try_cast quarantines dirty cells to null instead of crashing the job.
        .withColumn("lead_time_days", F.expr("try_cast(lead_time_days as int)"))
        .withColumn("reliability", F.expr("try_cast(reliability as double)"))
        .withColumn("extract_ts", F.expr("try_cast(extract_ts as timestamp)"))
    )

    valid = cleaned.filter(
        F.col("supplier_id").isNotNull()
        & F.col("supplier_id").rlike(SUPPLIER_ID_RE)
        & (F.col("lead_time_days") >= 0)
        & F.col("reliability").between(0.0, 1.0)
    )

    # Keep the freshest record per supplier (late-arriving corrections win).
    latest = Window.partitionBy("supplier_id").orderBy(F.col("extract_ts").desc_nulls_last())
    deduped = (
        valid
        .withColumn("_rn", F.row_number().over(latest))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    return (
        deduped
        .withColumn("load_date", F.lit(load_date))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
        .select(
            "supplier_id", "supplier_name", "lead_time_days", "reliability",
            "extract_ts", "load_date", "ingested_at", "source_file",
        )
    )


def run() -> None:
    import sys
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_path", "target_path", "load_date"])
    sc = SparkContext.getOrCreate()
    glue = GlueContext(sc)
    spark = glue.spark_session
    job = Job(glue)
    job.init(args["JOB_NAME"], args)

    raw = spark.read.option("header", True).csv(args["source_path"])
    conformed = transform_suppliers(raw, args["load_date"])
    conformed.write.mode("overwrite").partitionBy("load_date").parquet(args["target_path"])
    job.commit()


if __name__ == "__main__":
    run()