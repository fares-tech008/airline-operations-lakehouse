# Airline Operations Lakehouse Architecture

## Overview

The Airline Operations Lakehouse is built using a Medallion Architecture pattern on Databricks.

The platform processes airline operations data from raw CSV files and transforms it through Bronze, Silver, and Gold layers before exposing analytical datasets and operational monitoring metrics.

---

## Data Source

Source:

- U.S. Department of Transportation (DOT)
- Bureau of Transportation Statistics (BTS)

Dataset Characteristics:

- 12 monthly CSV files
- More than 7 million flight records
- Airline operations and delay metrics

---

## Medallion Architecture

### Bronze Layer

Purpose:

- Ingest raw files
- Preserve source data
- Validate schema
- Remove duplicates

Output:

```text
bronze.bronze_flights
```

---

### Silver Layer

Purpose:

- Clean and standardize data
- Handle missing values
- Apply business transformations

Derived Columns:

```text
IS_DELAYED
DISTANCE_CATEGORY
```

Output:

```text
silver.silver_flights
```

---

### Gold Layer

Purpose:

- Deliver analytics-ready datasets
- Support reporting and business analysis

Core Tables:

```text
gold.fact_flights
gold.dim_airport
gold.dim_date
```

---

## Star Schema Design

### Fact Table

```text
fact_flights
```

Contains:

- Flight activity
- Delay information
- Operational metrics

### Dimensions

```text
dim_airport
dim_date
```

Benefits:

- Faster analytical queries
- Simplified reporting
- Scalable warehouse design

---

## Data Quality Framework

Purpose:

Validate data before downstream consumption.

Checks:

- Null validation
- Duplicate detection
- Business rule validation
- Data freshness validation

Output Table:

```text
gold.data_quality_results
```

---

## Audit Logging Framework

Purpose:

Track pipeline execution metadata.

Captured Metrics:

- Pipeline name
- Start time
- End time
- Runtime
- Status
- Rows processed

Output Table:

```text
gold.pipeline_audit
```

---

## Incremental Loading Framework

Purpose:

Avoid reprocessing previously ingested files.

Features:

- File tracking
- Metadata storage
- New file detection

Output Table:

```text
gold.file_tracker
```

---

## Pipeline Monitoring Framework

Purpose:

Provide operational visibility.

Tracked Metrics:

- Runtime
- Quality score
- Failed checks
- Processed rows

Output Table:

```text
gold.pipeline_metrics
```

---

## Platform Orchestration

Purpose:

Coordinate framework execution and provide a centralized health view of the platform.

Responsibilities:

- Quality monitoring
- Audit monitoring
- Incremental load tracking
- Health evaluation

---

## Technology Stack

### Platform

- Databricks
- Delta Lake

### Languages

- Python
- SQL

### Processing

- Apache Spark
- PySpark

### Version Control

- Git
- GitHub
