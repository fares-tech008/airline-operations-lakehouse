%run ./00_audit_helpers



from datetime import datetime

STEPS = [
    "01_bronze_ingestion",
    "02_silver_transformation",
    "03_gold_layer",
    "04_data_quality",
]

pipeline_started = datetime.now()
print("Pipeline run_id:", RUN_ID)





# run them in order, stop at the first failure

for name in STEPS:
    print(f"--> starting {name}")
    try:
        dbutils.notebook.run(f"./{name}", 3600, {"run_id": RUN_ID})
    except Exception as e:
        log_step("orchestrator", "FAILED", started=pipeline_started,
                 message=f"Stopped at {name}: {e}")
        raise Exception(f"Pipeline stopped at {name}: {e}")
    print(f"<-- finished {name}")

log_step("orchestrator", "SUCCESS", started=pipeline_started,
         message=f"All {len(STEPS)} steps finished")





# the result

display(spark.sql(f"""
    SELECT * FROM workspace.ops.run_summary WHERE run_id = '{RUN_ID}'
"""))
display(spark.sql(f"""
    SELECT step_name, status, rows_processed, duration_seconds
    FROM workspace.ops.audit_log WHERE run_id = '{RUN_ID}' ORDER BY started_at
"""))