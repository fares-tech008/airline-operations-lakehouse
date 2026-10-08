from datetime import datetime
from pyspark.sql.types import *


audit_schema = StructType([
    StructField("pipeline_name", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("duration_seconds", DoubleType(), True),
    StructField("rows_processed", LongType(), True),
    StructField("status", StringType(), True),
    StructField("error_message", StringType(), True)
])


def start_audit():

    return datetime.now()


def end_audit(
    spark,
    pipeline_name,
    start_time,
    rows_processed,
    status,
    error_message=None
):

    end_time = datetime.now()

    duration = (
        end_time - start_time
    ).total_seconds()

    audit_record = [
        (
            pipeline_name,
            start_time,
            end_time,
            duration,
            rows_processed,
            status,
            error_message
        )
    ]

    audit_df = spark.createDataFrame(
        audit_record,
        schema=audit_schema
    )

    audit_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("gold.pipeline_audit")

    print(
        f"{pipeline_name} logged successfully"
    )
