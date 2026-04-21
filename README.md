# OpenAQ Data Warehouse (PostgreSQL + Python)

A lightweight Data Warehouse built on top of OpenAQ API data using PostgreSQL and Python.
The project demonstrates a full ETL pipeline, dimensional modeling, and a basic analytics layer for air quality measurements.

## Architecture

The warehouse follows a layered approach:

* **stg** — raw JSON data from the OpenAQ API
* **proc** — normalized and deduplicated data
* **dm** — star schema (dimensions + fact table)
* **svc** — ETL metadata, logs, and analytical views

## Data Model

* **Fact table:** `fact_measurement` (partitioned by month)
* **Dimensions:**

  * `dim_date`
  * `dim_location`
  * `dim_parameter`

The model supports efficient time-series analysis of air quality metrics such as PM2.5, PM10, and NO2.

## ETL Flow

1. Raw data is ingested into `stg.measurements_raw`
2. Data is normalized into `proc.measurements_clean`
3. Dimensions are populated using upsert logic
4. The fact table is loaded incrementally with deduplication
5. Aggregates are exposed through materialized views

### Main entry point

```sql
SELECT * FROM svc.etl_end_to_end('openaq_pipeline');
```

## Features

* Incremental and idempotent ETL
* Monthly partitioning for large fact tables
* Deduplication using hash keys
* ETL run tracking and error logging
* Materialized views for faster aggregation
* Ready-to-use analytical view for BI tools

## Usage

1. Deploy the schema using the provided SQL script
2. Load raw data into `stg.measurements_raw`
3. Register each load in `stg.load_registry`
4. Run the ETL pipeline

## Tech Stack

* PostgreSQL 17+
* Python
* OpenAQ API

## Purpose

This project is a practical example of building a simple but scalable DWH solution. It demonstrates ETL design patterns, dimensional modeling in PostgreSQL, and can be extended for BI dashboards or scheduled orchestration.
