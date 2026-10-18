# Spotify: End-to-End Azure Data Engineering Project

Spotify is an end-to-end Azure data platform that moves streaming data from **Azure SQL** into **ADLS Gen2** using **Azure Data Factory**, transforms it with **Azure Databricks** (Auto Loader, Structured Streaming, Delta Lake, Unity Catalog), applies **CDC / SCD Type 2** to dimensions, and serves curated data to **Power BI**. The Databricks project is deployed with **Databricks Asset Bundles** and all code is version-controlled in **GitHub**.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Tech Stack](#tech-stack)
4. [Pipeline Walkthrough](#pipeline-walkthrough)
5. [Governance](#governance)
6. [Deployment](#deployment)
7. [Repository Structure](#repository-structure)
8. [Skills Demonstrated](#skills-demonstrated)
9. [GitHub Repositories](#github-repositories)
10. [Author](#author)

---

## Project Overview

* **Goal:** Turn a relational Spotify streaming source into a governed cloud lakehouse for analytics and BI
* **Source:** Azure SQL (`DimUser`, `DimArtist`, `DimDate`, `DimTrack`, `FactStream`)
* **Ingestion:** Azure Data Factory
* **Storage:** ADLS Gen2 (`bronze` and `silver` containers) and Delta Lake
* **Processing:** Azure Databricks (PySpark, Auto Loader, Structured Streaming, serverless pipeline)
* **Governance:** Unity Catalog
* **Deployment:** Databricks Asset Bundles
* **Reporting:** Power BI
* **Version control:** GitHub

---

## Architecture

```text
                 ┌──────────────────┐
                 │    Azure SQL     │
                 │  Source Tables   │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │ Azure Data       │
                 │ Factory (ADF)    │
                 └────────┬─────────┘
                          │  parquet
                          ▼
                 ┌──────────────────┐
                 │  ADLS Gen2       │
                 │  bronze          │
                 │  (raw parquet)   │
                 └────────┬─────────┘
                          │  Auto Loader (cloudFiles)
                          ▼
                 ┌──────────────────┐
                 │  Databricks      │
                 │  Silver          │
                 │  Clean, Dedup,   │
                 │  Delta tables    │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │  Databricks      │
                 │  Gold pipeline   │
                 │  Auto CDC,       │
                 │  SCD Type 2      │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │    Power BI      │
                 └──────────────────┘

   Unity Catalog ──── governance for silver and gold tables
   Asset Bundles ──── deployment of the Databricks project
   GitHub ─────────── version control for ADF and Databricks code
```

---

## Tech Stack

| Service / Tool           | Role                                  |
| ------------------------ | ------------------------------------- |
| Azure SQL                | Source database                       |
| Azure Data Factory       | Ingestion and orchestration           |
| ADLS Gen2                | Data lake storage                     |
| Azure Databricks         | Transformation with PySpark           |
| Auto Loader              | Incremental file ingestion            |
| Spark Structured Streaming | Streaming reads and writes          |
| Delta Lake               | Reliable lakehouse storage            |
| Unity Catalog            | Data governance and cataloging        |
| SCD Type 2 / Auto CDC    | Historical dimension processing       |
| Databricks Asset Bundles | Deployment and project management     |
| Power BI                 | Analytics and visualization           |
| GitHub                   | Source control for ADF and Databricks |

---

## Pipeline Walkthrough

### Step 1: Source (Azure SQL)
Spotify source tables: `DimUser`, `DimArtist`, `DimDate`, `DimTrack`, `FactStream`.

### Step 2: Ingestion (Azure Data Factory)
* Connects to Azure SQL and extracts source data
* Loads data into ADLS Gen2 as parquet
* Orchestrates the pipelines and lands data in the `bronze` container
* ADF code lives in a separate repository (see [GitHub Repositories](#github-repositories))

### Step 3: Bronze Layer (ADLS Gen2)
Raw landing layer between Azure Data Factory and Databricks, one folder per table.

```text
abfss://bronze@storagemikeproject.dfs.core.windows.net/
├── DimUser
├── DimArtist
├── DimDate
├── DimTrack
└── FactStream
```

### Step 4: Silver Layer (Databricks)
The Silver notebook (`utils/Silver.ipynb`) processes all five tables with the same pattern:

* Reads Bronze parquet with **Auto Loader** (`cloudFiles`) and a per-table `schemaLocation`
* Drops the technical `_rescued_data` column using the reusable `dropColumns` helper from `utils/transformation.py`
* Removes duplicates on the table's business key with `dropDuplicates`
* Writes Delta in `append` mode with a per-table `checkpointLocation`
* Runs with `trigger(availableNow=True)` for batch-style incremental processing, then `awaitTermination()`

| Table      | Business Key | Silver Output (ADLS Gen2)                 |
| ---------- | ------------ | ----------------------------------------- |
| DimUser    | user_id      | `silver/DimUser/data`                     |
| DimArtist  | artist_id    | `silver/DimArtist/data`                   |
| DimDate    | date_key     | `silver/DimDate/data`                     |
| DimTrack   | track_id     | `silver/DimTrack/data`                    |
| FactStream | stream_id    | `silver/FactStream/data`                  |

Reusable helper (`utils/transformation.py`):

```python
class reusable:
    def dropColumns(self, df, columns):
        df = df.drop(*columns)
        return df
```

Silver pattern (shown for `DimUser`, repeated for every table):

```python
df_user = (
    spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "abfss://silver@storagemikeproject.dfs.core.windows.net/DimUser/schema")
        .load("abfss://bronze@storagemikeproject.dfs.core.windows.net/DimUser")
)

df_user = reusable().dropColumns(df_user, ["_rescued_data"])
df_user = df_user.dropDuplicates(["user_id"])

query = (
    df_user.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", "abfss://silver@storagemikeproject.dfs.core.windows.net/DimUser/checkpoint")
        .trigger(availableNow=True)
        .start("abfss://silver@storagemikeproject.dfs.core.windows.net/DimUser/data")
)
query.awaitTermination()
```

### Step 5: Gold Layer (Databricks Pipeline)
The Gold layer runs as the `spotify_lab_etl` Databricks pipeline (serverless), defined in `resources/spotify_lab_etl.pipeline.yml`. It loads every transformation file under `transformations/` (`DimDate.py`, `DimTrack.py`, `DimUser.py`, `FactStream.py`).

* Reads Silver tables with streaming transformations
* Data quality expectations
* Dimensional modelling
* **Auto CDC** with **SCD Type 2** history on dimensions

Example for `DimUser`, where history is kept instead of overwritten:

```text
user_id | user_name | ... | current_record
------------------------------------------
101     | Michael   | ... | true
101     | Mike      | ... | false
```

### Step 6: Output (Power BI)
Curated Gold tables are consumed by Power BI for reporting.

---

## Governance

Unity Catalog provides centralized governance and a structured namespace for the project:

```text
Catalog
└── spotify_catalog
    ├── bronze
    ├── silver
    └── gold
```

The Gold pipeline writes to the catalog and schema set through the bundle variables `${var.catalog}` and `${var.schema}`.

---

## Deployment

The Databricks project is managed with Databricks Asset Bundles. The pipeline resource:

```yaml
resources:
  pipelines:
    spotify_lab_etl:
      name: spotify_lab_etl
      catalog: ${var.catalog}
      schema: ${var.schema}
      serverless: true
      root_path: "../src/spotify_lab_etl"
      libraries:
        - glob:
            include: ../src/spotify_lab_etl/transformations/**
      environment:
        dependencies:
          - --editable ${workspace.file_path}
```

* Serverless compute, no cluster to manage
* Catalog and schema are bundle variables, so the same pipeline can target different environments
* Dependencies come from `pyproject.toml` through an editable install

```bash
databricks bundle validate
databricks bundle deploy --target dev
```

---

## Repository Structure

```text
spotify-azure-data-engineering/
│
├── README.md
├── databricks.yml
├── pyproject.toml
│
├── resources/
│   └── spotify_lab_etl.pipeline.yml
│
├── src/
│   ├── silver/
│   │   └── silver_dimensions.ipynb   (exploration notebook)
│   └── spotify_lab_etl/
│       └── transformations/
│           ├── DimDate.py
│           ├── DimTrack.py
│           ├── DimUser.py
│           └── FactStream.py
│
├── utils/
│   ├── Silver.ipynb              (Silver processing, all 5 tables)
│   └── transformation.py
│
├── tests/
├── AGENTS.md
└── CLAUDE.md
```

---

## Skills Demonstrated

* ETL / ELT pipeline design
* Azure Data Factory orchestration
* ADLS Gen2 data lake layout
* Databricks, PySpark and Spark Structured Streaming
* Auto Loader and incremental processing with checkpointing
* Reusable PySpark helper classes (`reusable.dropColumns`)
* Delta Lake, deduplication and data cleansing
* Data quality expectations
* CDC and SCD Type 2 with dimensional modelling
* Unity Catalog governance
* Databricks Asset Bundles with serverless pipelines
* Power BI reporting
* Git / GitHub workflow

---

## GitHub Repositories

* **Databricks (Silver / Gold, pipeline and deployment config):** https://github.com/MichaelJohnsonlmj/spotify-azure-data-engineering
* **Azure Data Factory (pipelines, datasets, linked services):** https://github.com/MichaelJohnsonlmj/adf-mikeazureproject

---

## Author

**Michael Johnson**
Azure Data Engineering | SQL | PL/SQL | Python | PySpark | Databricks | Azure | Power BI
