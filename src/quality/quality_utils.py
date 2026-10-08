from datetime import datetime
from pyspark.sql.functions import col


def log_check(
    check_name,
    table_name,
    failed_count,
    spark
):
    status = "PASS" if failed_count == 0 else "FAIL"

    result = [
        (
            check_name,
            table_name,
            status,
            failed_count,
            datetime.now()
        )
    ]

    result_df = spark.createDataFrame(
        result,
        [
            "check_name",
            "table_name",
            "status",
            "failed_records",
            "execution_time"
        ]
    )

    result_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("gold.data_quality_results")

    print(
        f"{check_name}: {status} ({failed_count} failures)"
    )


def check_duplicates(df):

    total_rows = df.count()

    distinct_rows = df.distinct().count()

    duplicates = total_rows - distinct_rows

    return duplicates


def check_nulls(
    df,
    column_name
):

    return df.filter(
        col(column_name).isNull()
    ).count()
