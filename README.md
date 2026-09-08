# NYC Taxi Data Pipeline

An end-to-end data engineering project built with **Azure Databricks, PySpark, Delta Lake, and Azure Data Lake Storage**.

The pipeline ingests NYC Yellow Taxi trip data, processes it through a **Medallion Architecture (Bronze → Silver → Gold)**, and produces an analytics-ready **star schema**.

The project demonstrates incremental ingestion, Structured Streaming, data quality handling, incremental batch processing, dimensional modeling, and Databricks orchestration.

---

## Architecture

```text
NYC Yellow Taxi Parquet Files
            │
            ▼
       ADLS Gen2 - Raw
            │
            │ Auto Loader
            │ Structured Streaming
            │ AvailableNow
            ▼
       ┌───────────┐
       │  BRONZE   │
       │ Raw Delta │
       └───────────┘
            │
            │ Delta Structured Streaming
            │ AvailableNow
            ▼
       ┌───────────┐
       │  SILVER   │────────────► Quarantine
       │ Cleaned   │              Invalid trips
       └───────────┘
            │
            │ Incremental Batch
            │ High-Water Mark
            ▼
       ┌───────────┐
       │   GOLD    │
       │Star Schema│
       └───────────┘
```

---

## Tech Stack

- Python
- PySpark
- Azure Databricks
- Delta Lake
- Unity Catalog
- Azure Data Lake Storage Gen2
- Databricks Auto Loader
- Spark Structured Streaming
- Databricks Jobs
- Databricks Declarative Automation Bundles
- Databricks Connect
- uv

---

## Pipeline

### Raw → Bronze

NYC Yellow Taxi data is stored as Parquet files in **Azure Data Lake Storage Gen2**.

The files are incrementally ingested using **Databricks Auto Loader**.

Auto Loader tracks previously processed files using a Structured Streaming checkpoint, preventing files from being processed multiple times.

The Bronze layer preserves the source data while adding ingestion metadata including:

- `ingestion_ts`
- `source_file`

The stream uses:

```python
.trigger(availableNow=True)
```

`AvailableNow` processes all currently unprocessed files and then terminates. This provides incremental Structured Streaming semantics while allowing the pipeline to operate like a scheduled batch job.

Auto Loader also maintains a separate `schemaLocation` for inferred schema metadata.

---

### Bronze → Silver

Bronze data is read incrementally using **Delta Structured Streaming**.

The Silver layer performs data cleaning and derives additional fields such as:

- `duration_minutes`

Records are separated into valid and invalid datasets.

```text
                    ┌──► Valid ─────► Silver
Bronze ─► Transform │
                    └──► Invalid ───► Quarantine
```

Trips with invalid durations are written to a separate **quarantine Delta table** rather than being silently discarded.

Silver and quarantine are separate Structured Streaming queries and therefore maintain separate checkpoints.

This layer also uses:

```python
.trigger(availableNow=True)
```

The checkpoint allows each execution to continue from the previous streaming progress instead of processing previously handled Bronze data again.

---

### Silver → Gold

The Gold layer is implemented using **incremental batch processing**.

A Delta table called `pipeline_state` stores the last successfully processed ingestion timestamp.

Example:

```text
pipeline_name                  last_processed_ingestion_ts
----------------------------------------------------------------
silver_to_fact_taxi_trips      2026-09-08 18:05:00
```

On each pipeline execution:

1. Read the previous high-water mark.
2. Filter Silver for records with a newer `ingestion_ts`.
3. Transform the new records.
4. Append them to the Gold fact table.
5. Calculate the maximum processed `ingestion_ts`.
6. Update the high-water mark in `pipeline_state`.

Conceptually:

```text
Silver
   │
   ▼
Read High-Water Mark
   │
   ▼
Filter New Records
   │
   ▼
Transform
   │
   ▼
Append to Fact Table
   │
   ▼
Update High-Water Mark
```

This demonstrates manual state management for incremental batch processing.

---

## Gold Data Model

The Gold layer contains an analytics-ready **star schema**.

```text
                         dim_date
                            ▲
                            │
                     pickup/dropoff
                            │
                            │
dim_location ◄────── fact_taxi_trips ──────► dim_payment_type
      ▲                     │
      │                     │
 pickup/dropoff             ▼
                       dim_rate_code
```

---

## Fact Table

### `fact_taxi_trips`

**Grain: one row represents one taxi trip.**

The fact table contains trip-level measures and foreign keys used to connect to the dimensions.

Examples include:

- Pickup timestamp
- Dropoff timestamp
- Pickup date key
- Dropoff date key
- Pickup location key
- Dropoff location key
- Payment type key
- Rate code key
- Passenger count
- Trip distance
- Trip duration
- Fare amount
- Tip amount
- Toll amount
- Total amount
- Congestion surcharge
- Airport fee

The original pickup and dropoff timestamps are retained alongside the date keys to preserve the exact event timestamps.

---

## Dimension Tables

### `dim_date`

Calendar dimension containing:

- Date key
- Calendar date
- Year
- Quarter
- Month
- Month name
- Day
- Day of week
- Day of week number
- Weekend indicator

The dimension is generated for a predefined calendar range.

---

### `dim_location`

Built from the NYC Taxi Zone reference dataset.

Contains:

- Location ID
- Borough
- Zone
- Service zone

The same dimension is referenced for both pickup and dropoff locations:

```text
pickup_location_key  ──► dim_location
dropoff_location_key ──► dim_location
```

---

### `dim_payment_type`

Maps NYC TLC payment type codes to descriptive values such as:

- Credit card
- Cash
- No charge
- Dispute
- Unknown
- Voided trip

---

### `dim_rate_code`

Maps NYC TLC rate codes to descriptive values such as:

- Standard rate
- JFK
- Newark
- Nassau or Westchester
- Negotiated fare
- Group ride

---

## Incremental Processing Strategy

Different incremental processing techniques are intentionally used across the pipeline.

| Layer | Method | State Management |
|---|---|---|
| Raw → Bronze | Auto Loader | Checkpoint |
| Bronze → Silver | Delta Structured Streaming | Checkpoint |
| Silver → Gold | Incremental Batch | High-water mark |

### Raw → Bronze

Auto Loader tracks which source files have already been processed.

### Bronze → Silver

Structured Streaming tracks Delta source progress through checkpoints.

### Silver → Gold

The pipeline manually stores the latest processed `ingestion_ts` in a Delta state table and uses it as a high-water mark on subsequent runs.

This allows the project to demonstrate multiple approaches to incremental data engineering.

---

## Data Quality

Invalid trip durations are separated from valid records during Silver processing.

```text
Valid duration
      │
      ▼
Silver

Invalid duration
      │
      ▼
Quarantine
```

The quarantine table preserves rejected records for inspection rather than dropping them from the pipeline.

---

## Orchestration

The entire pipeline is orchestrated through a single Python entry point.

```text
main()
  │
  ├── ingest_bronze()
  │
  ├── ingest_silver()
  │
  ├── build_dim_date()
  │
  ├── build_dim_location()
  │
  ├── build_dim_payment_type()
  │
  ├── build_dim_rate_code()
  │
  └── build_fact_taxi_trips()
```

Static dimensions are created only when they do not already exist.

The incremental fact pipeline runs on every execution and determines whether new Silver records are available using its high-water mark.

---

## Databricks Jobs

The pipeline is executed as a **Databricks Job**.

Runtime configuration such as the catalog, raw data location, schema metadata location, and checkpoint locations is passed into the application rather than hardcoded into the transformation logic.

This keeps the pipeline code independent of a specific environment.

---

## Databricks Declarative Automation Bundles

The Databricks Job and environment-specific configuration are defined using **Databricks Declarative Automation Bundles**.

The project can therefore be validated and deployed using the Databricks CLI.

Example:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run nyc_taxi_job -t dev
```

---

## Project Structure

```text
nyc_taxi_pipeline/
│
├── databricks.yml
├── pyproject.toml
├── uv.lock
│
├── resources/
│   └── job.yml
│
└── src/
    └── nyc_taxi_pipeline/
        │
        ├── main.py
        │
        ├── bronze/
        │   └── ingest_bronze.py
        │
        ├── silver/
        │   └── ingest_silver.py
        │
        └── gold/
            ├── build_dim_date.py
            ├── build_dim_location.py
            ├── build_dim_payment_type.py
            ├── build_dim_rate_code.py
            └── build_fact_taxi_trips.py
```

---

## Key Concepts Demonstrated

This project demonstrates:

- End-to-end ETL pipeline development
- Medallion Architecture
- Incremental file ingestion
- Databricks Auto Loader
- Spark Structured Streaming
- `AvailableNow` streaming triggers
- Streaming checkpoints
- Delta Lake
- Unity Catalog
- Data quality and quarantine handling
- Incremental batch processing
- High-water mark state management
- Dimensional modeling
- Star schema design
- Fact and dimension tables
- PySpark transformations
- Azure Data Lake Storage
- Databricks Jobs
- Databricks Declarative Automation Bundles
- Databricks Connect
- Environment-based configuration

---

## Dataset

This project uses the **NYC Taxi & Limousine Commission Yellow Taxi Trip Records**.

The trip dataset contains information such as pickup and dropoff timestamps, locations, passenger counts, trip distances, payment types, fares, tips, and other taxi trip information.

A separate NYC Taxi Zone lookup dataset is used to enrich location IDs with borough and zone information.

---

## Summary

This project implements an end-to-end Azure Databricks data pipeline from raw cloud storage to an analytics-ready dimensional model.

It combines file-based incremental ingestion, Delta Structured Streaming, data-quality handling, incremental batch processing, and dimensional modeling to demonstrate several common data engineering patterns within a single pipeline.