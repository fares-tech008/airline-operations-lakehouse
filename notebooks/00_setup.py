# Creates the "ops" schema and the file tracker.

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.ops")

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.ops.file_tracker (
    file_name       STRING,
    file_path       STRING,
    file_size       BIGINT,
    rows_loaded     BIGINT,
    processed_time  TIMESTAMP
) USING DELTA
""")


spark.sql("DROP TABLE IF EXISTS workspace.bronze.bronze_flights")
