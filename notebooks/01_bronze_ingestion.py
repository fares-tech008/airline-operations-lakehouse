%run ./00_audit_helpers



from datetime import datetime
started = datetime.now()



from datetime import datetime
from pyspark.sql import functions as F

RAW_PATH = "/Volumes/workspace/bronze/airline_raw_files"
BRONZE_TABLE = "workspace.bronze.bronze_flights"
TRACKER_TABLE = "workspace.ops.file_tracker"

TRACKER_SCHEMA = (
    "file_name string, file_path string, file_size long, "
    "rows_loaded long, processed_time timestamp"
)


available = [
    {"name": f.name, "path": f.path, "size": f.size}
    for f in dbutils.fs.ls(RAW_PATH)
    if f.name.lower().endswith(".csv")
]

# already loaded
processed = {r["file_name"]: r["file_size"]
             for r in spark.table(TRACKER_TABLE).collect()}

new_files = sorted(
    [f for f in available if processed.get(f["name"]) != f["size"]],
    key=lambda f: f["name"],
)

print(f"{len(available)} files in folder, {len(processed)} already loaded, {len(new_files)} to load")
for f in new_files:
    print("  ->", f["name"])


def sql_quote(text):
    return text.replace("'", "''")


total_rows = 0

for f in new_files:
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(f["path"])
        .withColumn("_source_file", F.lit(f["path"]))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    rows = df.count()

    if spark.catalog.tableExists(BRONZE_TABLE):
        spark.sql(
            f"DELETE FROM {BRONZE_TABLE} WHERE _source_file = '{sql_quote(f['path'])}'")

    df.write.format("delta").mode("append").option(
        "mergeSchema", "true").saveAsTable(BRONZE_TABLE)

    spark.sql(
        f"DELETE FROM {TRACKER_TABLE} WHERE file_name = '{sql_quote(f['name'])}'")
    spark.createDataFrame(
        [(f["name"], f["path"], int(f["size"]), int(rows), datetime.now())],
        TRACKER_SCHEMA,
    ).write.format("delta").mode("append").saveAsTable(TRACKER_TABLE)

    total_rows += rows
    print(f"loaded {f['name']}: {rows:,} rows")

print(f"Rows loaded this run: {total_rows:,}")


display(
    spark.table(BRONZE_TABLE)
         .groupBy("_source_file").count()
         .orderBy("_source_file")
)
display(spark.table(TRACKER_TABLE).orderBy("file_name"))



log_step("01_bronze", "SUCCESS",
         rows=spark.table("workspace.bronze.bronze_flights").count(),
         started=started, message="rows = total rows in Bronze after this run")