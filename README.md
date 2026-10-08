# Spotify Azure Data Engineering Project

End-to-end Azure Data Engineering project built using **Azure SQL, Azure Data Factory, ADLS Gen2, Azure Databricks, Delta Lake, Unity Catalog, and Power BI**.

The project demonstrates a modern cloud data pipeline that ingests Spotify streaming data from Azure SQL, moves it through an ADLS Gen2 data lake, transforms the data using Databricks, applies dimensional modelling and CDC/SCD Type 2 concepts, and prepares curated data for Power BI reporting.

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
                    │    Ingestion     │
                    └────────┬─────────┘
                             │
                             ▼
              ┌─────────────────────────────┐
              │         ADLS Gen2           │
              │                             │
              │          Bronze             │
              │       Raw Source Data       │
              └──────────────┬──────────────┘
                             │
                             ▼
              ┌─────────────────────────────┐
              │        Databricks           │
              │                             │
              │          Silver             │
              │ Cleaning / Deduplication    │
              │     Delta transformations  │
              └──────────────┬──────────────┘
                             │
                             ▼
              ┌─────────────────────────────┐
              │        Databricks           │
              │                             │
              │           Gold              │
              │ Dimensional Model / CDC     │
              │        SCD Type 2            │
              └──────────────┬──────────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │     Power BI     │
                    │   BI Reporting   │
                    └──────────────────┘
```

---

## Project Flow

### 1. Source — Azure SQL

Spotify-related source data is maintained in Azure SQL.

The project contains dimension and fact-style tables including:

* DimUser
* DimArtist
* DimDate
* DimTrack
* FactStream

Example source flow:

```text
Azure SQL
   │
   ├── DimUser
   ├── DimArtist
   ├── DimDate
   ├── DimTrack
   └── FactStream
```

---

## 2. Data Ingestion — Azure Data Factory

Azure Data Factory is used to orchestrate ingestion from Azure SQL into ADLS Gen2.

Responsibilities include:

* Connecting to Azure SQL
* Extracting source data
* Loading data into ADLS Gen2
* Pipeline orchestration
* Moving data into the Bronze layer

The ADF implementation is maintained in a separate repository:

**ADF Repository**

https://github.com/MichaelJohnsonlmj/adf-mikeazureproject

---

## 3. Bronze Layer — ADLS Gen2

The Bronze layer stores the ingested source data in its raw form.

```text
ADLS Gen2
└── Bronze
    ├── DimUser
    ├── DimArtist
    ├── DimDate
    ├── DimTrack
    └── FactStream
```

The Bronze layer acts as the landing layer between Azure Data Factory and Databricks.

---

## 4. Silver Layer — Databricks

Azure Databricks is used to transform Bronze data into clean Silver Delta data.

The Silver layer performs activities such as:

* Reading data from ADLS Gen2
* Auto Loader-based ingestion
* Schema management
* Removing `_rescued_data`
* Deduplication
* Data cleansing
* Transformation
* Writing Delta data
* Checkpoint management

The Silver processing uses `availableNow=True` for batch-style incremental processing.

### Silver Tables

| Table      | Business Key |
| ---------- | ------------ |
| DimUser    | user_id      |
| DimArtist  | artist_id    |
| DimDate    | date_key     |
| DimTrack   | track_id     |
| FactStream | stream_id    |

Example Silver processing pattern:

```text
ADLS Bronze
     │
     ▼
Databricks Auto Loader
     │
     ▼
Data Cleaning
     │
     ▼
Deduplication
     │
     ▼
Delta Lake
     │
     ▼
Silver
```

---

## 5. Gold Layer — Databricks

The Gold layer provides curated data for analytics and reporting.

The Gold implementation uses Databricks pipeline functionality and applies CDC/SCD concepts to dimension data.

### Gold Responsibilities

* Reading Silver tables
* Data quality expectations
* Streaming transformations
* Dimension modelling
* Auto CDC
* SCD Type 2 history
* Curated analytical tables

### SCD Type 2

For dimensions such as `DimUser`, changes to records can be maintained historically rather than simply overwriting the previous value.

Conceptually:

```text
DimUser

user_id | user_name | ... | current_record
------------------------------------------------
101     | Michael   | ... | true
101     | Mike      | ... | false
```

This allows historical changes to dimension attributes to be retained.

---

## 6. Unity Catalog

Unity Catalog is used for centralized governance and organization of Databricks data assets.

The project uses:

```text
Catalog
└── spotify_catalog
    ├── bronze
    ├── silver
    └── gold
```

Silver tables include:

* `dimartist`
* `dimdate`
* `dimtrack`
* `dimuser`
* `factstream`

Unity Catalog provides a structured namespace for the project and supports centralized data governance.

---

## 7. Delta Lake

Delta Lake is used as the storage format for the transformed data.

Benefits used in the project include:

* Reliable transactional storage
* Schema management
* Incremental processing
* Historical data handling
* Integration with Databricks
* Support for CDC/SCD processing

---

## 8. Databricks Asset Bundles

The Databricks project is managed using Databricks Asset Bundles.

The repository contains:

```text
databricks.yml
pyproject.toml
resources/
src/
tests/
utils/
```

Development deployment is configured through the Databricks bundle configuration.

Example:

```bash
databricks bundle validate
```

Development deployment:

```bash
databricks bundle deploy --target dev
```

---

## 9. Repository Structure

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
│   │
│   ├── silver/
│   │   └── silver_dimensions.ipynb
│   │
│   └── gold/
│       └── transformations/
│           ├── DimDate.py
│           ├── DimTrack.py
│           ├── DimUser.py
│           └── FactStream.py
│
├── utils/
│   ├── Silver.ipynb
│   └── transformation.py
│
├── tests/
│
├── AGENTS.md
└── CLAUDE.md
```

---

## 10. Technologies Used

| Technology               | Purpose                             |
| ------------------------ | ----------------------------------- |
| Azure SQL                | Source database                     |
| Azure Data Factory       | Data ingestion and orchestration    |
| ADLS Gen2                | Cloud data lake                     |
| Azure Databricks         | Data engineering and transformation |
| PySpark                  | Distributed data processing         |
| Auto Loader              | Incremental file ingestion          |
| Delta Lake               | Reliable lakehouse storage          |
| Unity Catalog            | Data governance and cataloging      |
| Databricks Asset Bundles | Deployment and project management   |
| SCD Type 2 / Auto CDC    | Historical dimension processing     |
| Power BI                 | Analytics and visualization         |
| Git / GitHub             | Version control                     |

---

## 11. Key Data Engineering Concepts Demonstrated

This project demonstrates practical experience with:

* ETL / ELT pipelines
* Azure Data Factory
* ADLS Gen2
* Azure SQL
* Databricks
* PySpark
* Spark Structured Streaming
* Auto Loader
* Delta Lake
* Unity Catalog
* Data cleansing
* Deduplication
* Incremental processing
* Checkpointing
* Data quality expectations
* CDC
* SCD Type 2
* Dimensional modelling
* Databricks Asset Bundles
* Git and GitHub
* Power BI

---

## 12. GitHub Repositories

### Databricks — Spotify Azure Data Engineering

https://github.com/MichaelJohnsonlmj/spotify-azure-data-engineering

Contains the Databricks Silver/Gold implementation, transformations, utilities, pipeline configuration, and deployment configuration.

### Azure Data Factory

https://github.com/MichaelJohnsonlmj/adf-mikeazureproject

Contains the Azure Data Factory ingestion implementation, including pipelines, datasets, linked services, and ADF configuration.

---

## 13. End-to-End Data Flow

The complete pipeline follows this process:

```text
1. Azure SQL
      ↓
2. Azure Data Factory
      ↓
3. ADLS Gen2 Bronze
      ↓
4. Databricks Auto Loader
      ↓
5. Silver transformations
      ↓
6. Delta Lake
      ↓
7. Unity Catalog
      ↓
8. Gold transformations
      ↓
9. CDC / SCD Type 2
      ↓
10. Power BI
```

The project is designed to demonstrate how a traditional relational source can be transformed into a governed cloud lakehouse architecture suitable for analytics and BI workloads.

---

## Author

**Michael Johnson**

Azure Data Engineering | SQL | PL/SQL | Python | PySpark | Databricks | Azure | Power BI
