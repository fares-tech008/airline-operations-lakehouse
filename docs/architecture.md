# Architecture

![Architecture](architecture.png)

## Flow

```text
Raw CSV files (Databricks Volume)
        |
        v   01_bronze_ingestion   incremental, raw text + lineage
bronze.bronze_flights
        |
        v   02_silver_transformation   typed, cleaned, deduplicated
silver.silver_flights
        |
        v   03_gold_layer   star schema + analytics tables
gold.fact_flights  gold.dim_date  gold.dim_airport
gold.monthly_trends  gold.airport_performance  gold.route_performance  gold.delay_causes
        |
        v   04_data_quality   16 checks, ERROR stops the run
ops.dq_results
```

Every step writes to `ops.audit_log`. `ops.run_summary` (view) summarises one row per run. `06_run_pipeline` runs steps 01 to 04 with a shared `run_id`.

## Design decisions

| Decision | Why |
|---|---|
| Bronze keeps everything as raw text | Bronze should hold the source as it arrived, so a bad value never stops the load |
| `try_cast` in Silver | A bad value becomes NULL instead of failing 7 million rows |
| Exact duplicates are removed in Silver, not Bronze | Bronze keeps the raw truth. Cleaning belongs in Silver |
| `IS_DELAYED` is NULL for cancelled, diverted or unknown flights | "No arrival delay" is not the same as "on time". Counting it as 0 made delay rates too low |
| Delay rate = delayed flights / completed flights | Cancelled and diverted flights have no arrival delay to measure |
| `dim_airport` is built from origin and destination | Some airports only appear as a destination. Origin-only left holes in the dimension |
| `dim_date` is a full calendar | A calendar built from the fact table skips days without flights |
| Incremental loading by file name and size | A changed file is detected and its old rows are replaced, so a retry never duplicates data |
| Tracker is written only after the data landed | A crash in the middle cannot mark a file as processed |
| Quality results are saved before the gate raises | A failed run still leaves a record of why |
| Rules in `00_rules.py`, shared by notebooks and tests | The tests check the exact code the pipeline runs |

## Gold star schema

```text
dim_date (DATE_KEY) <--- fact_flights ---> dim_airport (AIRPORT_CODE)   [ORIGIN]
                                      \--> dim_airport (AIRPORT_CODE)   [DEST]
```

`fact_flights` grain: one row per flight.

## Operations tables (`ops` schema)

| Object | Type | Content |
|---|---|---|
| `ops.file_tracker` | table | Files loaded (name, size, rows, time) |
| `ops.audit_log` | table | One row per step per run |
| `ops.dq_results` | table | One row per check per run |
| `ops.run_summary` | view | One row per run with status and quality score |

## Known limitations

See the Limitations section of the main README.
