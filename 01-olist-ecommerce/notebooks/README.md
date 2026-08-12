# Notebooks

Databricks notebooks implementing the Olist pipeline's medallion architecture (Bronze → Silver → Quality Gate → Gold).

- **01_ingest_bronze.py** — Ingests raw Olist CSVs into Bronze Delta tables with explicit schemas.
- **02_transform_silver_v2.py** — Applies schema enforcement, business rule validation, FK integrity checks, and quarantine logic to produce Silver tables.
- **03_quality_gate.py** — Checks rejection rate across Silver tables; halts the pipeline (via Airflow) if any table exceeds a 5% rejection threshold.
- **04_transform_gold.py** — Builds the Gold star schema: dimension tables, fact table, SCD Type 2 on `dim_seller`, and 5 business KPIs.

These notebooks are orchestrated by Airflow — see [`/airflow`](../airflow) for the DAG that triggers each one in sequence as separate Databricks Jobs.
