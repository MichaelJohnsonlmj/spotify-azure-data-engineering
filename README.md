# CareSync: End-to-End Azure Data Engineering Project

CareSync is an end-to-end Azure data platform that moves data from **Azure SQL** into **ADLS Gen2**, transforms it with **Azure Databricks**, secures credentials with **Azure Key Vault**, and sends automated pipeline alerts through **Azure Logic Apps**. Code is version-controlled in **GitHub**.

> Items in `[brackets]` are placeholders. Replace them with your actual details.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Tech Stack](#tech-stack)
4. [Pipeline Walkthrough](#pipeline-walkthrough)
5. [Security](#security)
6. [Monitoring and Alerting](#monitoring-and-alerting)
7. [Repository Structure](#repository-structure)
8. [Skills Demonstrated](#skills-demonstrated)
9. [Author](#author)

---

## Project Overview

* **Goal:** `[one line on what CareSync solves, e.g. consolidating [domain] data for analytics]`
* **Source:** Azure SQL (`[N]` tables)
* **Ingestion:** Azure Data Factory
* **Storage:** ADLS Gen2
* **Processing:** Azure Databricks (PySpark)
* **Security:** Azure Key Vault
* **Alerting:** Azure Logic Apps
* **Version control:** GitHub

---

## Architecture

```text
                 ┌──────────────────┐
                 │    Azure SQL     │
                 └────────┬─────────┘
                          │
                          ▼
┌───────────┐    ┌──────────────────┐    ┌──────────────────┐
│ Key Vault │───▶│ Azure Data       │───▶│   Logic Apps     │
│ (secrets) │    │ Factory (ADF)    │    │ (alert emails)   │
└───────────┘    └────────┬─────────┘    └──────────────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │  ADLS Gen2       │
                 │  Raw / Bronze    │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │  Databricks      │
                 │  Transformations │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │  ADLS Gen2       │
                 │  Curated / Gold  │
                 └──────────────────┘
```

---

## Tech Stack

| Service            | Role                                     |
| ------------------ | ---------------------------------------- |
| Azure SQL          | Source database                          |
| Azure Data Factory | Ingestion and orchestration              |
| ADLS Gen2          | Data lake storage                        |
| Azure Databricks   | Transformation with PySpark              |
| Azure Key Vault    | Secrets and credential management        |
| Azure Logic Apps   | Success / failure notifications          |
| GitHub             | Source control for ADF and Databricks    |

---

## Pipeline Walkthrough

### Step 1: Source (Azure SQL)
CareSync tables: `[Table 1]`, `[Table 2]`, `[Table 3]`, `[Table 4]`.

### Step 2: Ingestion (Azure Data Factory)
* Linked services for Azure SQL and ADLS Gen2
* Copy activities / parameterised pipelines `[describe if used]`
* Triggers: `[schedule / manual / event]`
* Loads raw data into the lake

### Step 3: Raw Layer (ADLS Gen2)
```text
ADLS Gen2
└── [container]
    ├── bronze/
    ├── silver/
    └── gold/
```

### Step 4: Transformation (Azure Databricks)
* Reads raw data from ADLS Gen2
* Cleaning, type fixes, deduplication
* Business transformations and joins
* Writes curated output `[Delta / Parquet]`

### Step 5: Output
Curated data ready for `[Power BI / reporting / downstream use]`.

---

## Security

Azure Key Vault holds all secrets, so nothing sensitive is hardcoded:

* Azure SQL connection credentials
* ADLS Gen2 access keys / details
* Databricks access token
* Used by ADF linked services and Databricks secret scope

---

## Monitoring and Alerting

Azure Logic Apps sends `[email / Teams]` notifications when ADF pipelines succeed or fail, triggered from the pipeline via a web activity.

---

## Repository Structure

```text
caresync-azure-data-engineering/
│
├── README.md
│
├── adf/
│   ├── pipeline/
│   ├── dataset/
│   ├── linkedService/
│   └── trigger/
│
├── databricks/
│   └── notebooks/
│       ├── bronze_to_silver
│       └── silver_to_gold
│
├── logic-apps/
│   └── pipeline-alert-workflow.json
│
└── docs/
    └── architecture.png
```

> Adjust to match your real repo layout.

---

## Skills Demonstrated

* ETL / ELT pipeline design
* Azure Data Factory orchestration
* ADLS Gen2 data lake layout
* Databricks and PySpark transformations
* Secure secrets management with Key Vault
* Automated alerting with Logic Apps
* Git / GitHub workflow

---

## Author

**Michael Johnson**
Azure Data Engineering | SQL | PL/SQL | Python | PySpark | Databricks | Azure | Power BI
