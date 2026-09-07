# 🏥 Healthcare Patient Analytics Pipeline

## 🚀 Project Overview

The **Healthcare Patient Analytics Pipeline** is an end-to-end Data Engineering project designed to ingest, process, transform, validate, and analyze healthcare patient data to generate reliable analytics and actionable insights.

The pipeline follows the **Medallion Architecture (Bronze → Silver → Gold)** using **Azure Databricks and Delta Lake**. Healthcare CSV files are ingested through **Azure Data Factory**, stored in **Azure Data Lake Storage Gen2**, processed in Databricks, transformed using **dbt**, orchestrated using **Apache Airflow**, and consumed through analytics dashboards and alerting systems.

The project focuses on building a scalable and reliable healthcare data platform with data quality validation, transformation, monitoring, and automated pipeline execution.

---

## 🎯 Project Objectives

Healthcare organizations generate large volumes of patient, hospital, diagnosis, laboratory, and vital-sign data. Raw healthcare data often contains missing values, duplicate records, inconsistent formats, and invalid values.

The main objectives of this project are:

* Build an end-to-end healthcare data engineering pipeline.
* Ingest healthcare CSV datasets using Azure Data Factory.
* Store raw and processed data in Azure Data Lake Storage Gen2.
* Implement Medallion Architecture using Databricks.
* Create Bronze Delta tables for raw healthcare data.
* Clean and standardize healthcare data in the Silver layer.
* Build analytics-ready Gold tables using dbt.
* Implement data quality and validation checks.
* Create healthcare analytics dashboards.
* Automate pipeline execution using Apache Airflow.
* Implement Slack alerts for pipeline failures and important events.
* Provide reliable datasets for healthcare analytics and decision-making.

---

## 🏗 Lakehouse Architecture

The pipeline follows a modern **Azure Lakehouse Data Engineering Architecture**.

### Architecture Flow

```text
Healthcare CSV Files
        │
        ▼
Azure Data Factory
        │
        ▼
Azure Data Lake Storage Gen2
        │
        ├── staging/
        │      └── CSV files
        │
        └── parquet/
               └── Parquet files
        │
        ▼
Azure Databricks
        │
        ▼
┌──────────────────────────────┐
│       Bronze Layer           │
│     Raw Delta Tables         │
└──────────────────────────────┘
        │
        ▼
┌──────────────────────────────┐
│       Silver Layer           │
│  Cleaned & Validated Data    │
│          dbt                 │
└──────────────────────────────┘
        │
        ▼
┌──────────────────────────────┐
│        Gold Layer            │
│ Analytics & Business Metrics │
│          dbt                 │
└──────────────────────────────┘
        │
        ├──────────────► Dashboard
        │
        └──────────────► Slack Alerts

Apache Airflow
      │
      └── Pipeline Orchestration & Monitoring
```

---

## 🛠 Technology Stack

| Technology                   | Purpose                                |
| ---------------------------- | -------------------------------------- |
| Azure Data Lake Storage Gen2 | Cloud data lake storage                |
| Azure Data Factory           | Data ingestion and file transformation |
| Azure Databricks             | Data engineering and processing        |
| Apache Spark / PySpark       | Distributed data processing            |
| Delta Lake                   | Reliable table storage                 |
| Unity Catalog                | Data governance and table management   |
| dbt                          | SQL transformations and data modelling |
| Apache Airflow               | Pipeline orchestration                 |
| Slack                        | Pipeline alerts and notifications      |
| Git / GitHub                 | Version control                        |
| SQL                          | Data transformation and analytics      |

---

## 📂 Dataset

### Dataset Used

**Healthcare Patient Analytics Dataset**

The project uses healthcare datasets containing patient, hospital, diagnosis, laboratory, and vital information.

### Source CSV Files

* `hospital_info.csv`
* `lab_results.csv`
* `patient_demographics.csv`
* `patient_diagnosis.csv`
* `patient_vitals.csv`

### Data Flow

```text
CSV
 │
 ▼
ADF Pipeline
 │
 ▼
ADLS Gen2 - Staging
 │
 ▼
Parquet
 │
 ▼
Databricks Bronze
 │
 ▼
dbt Silver
 │
 ▼
dbt Gold
```

---

# 🏗 ELT Design (Medallion Architecture)

## 🥉 Bronze Layer – Raw Data Ingestion

The Bronze layer stores the raw healthcare data in Delta format.

### Bronze Processing

* Read healthcare Parquet files from ADLS Gen2.
* Load raw data into Databricks.
* Preserve the original source information.
* Create Delta tables for healthcare datasets.
* Add ingestion metadata where required.
* Maintain the raw layer for traceability and auditing.

### Bronze Tables

| Bronze Tables          |
| ---------------------- |
| `patient_demographics` |
| `hospital_info`        |
| `patient_diagnosis`    |
| `patient_vitals`       |
| `lab_results`          |

---

## 🥈 Silver Layer – Data Cleaning & Transformation

The Silver layer contains cleaned, standardized, and validated healthcare data.

Transformations are implemented using **dbt SQL models** on Databricks.

### Silver Processing

* Remove invalid records.
* Handle NULL and blank values.
* Trim unnecessary spaces.
* Standardize text formats.
* Standardize healthcare attributes.
* Validate patient identifiers.
* Remove duplicate records.
* Apply appropriate data types.
* Validate diagnosis and laboratory information.
* Clean patient vital measurements.
* Prepare reliable datasets for analytics.

### Silver Models

| Silver Tables                 |
| ----------------------------- |
| `silver_patient_demographics` |
| `silver_hospital_info`        |
| `silver_patient_diagnosis`    |
| `silver_patient_vitals`       |
| `silver_lab_results`          |

---

## 🥇 Gold Layer – Analytics & Insights

The Gold layer contains business-ready healthcare datasets used for analytics and dashboards.

The Gold layer is created using **dbt SQL models**.

### Gold Processing

* Combine patient and hospital information.
* Analyze patient diagnosis information.
* Analyze laboratory results.
* Analyze patient vital measurements.
* Generate healthcare metrics.
* Create aggregated analytical datasets.
* Prepare dashboard-ready tables.
* Support healthcare trend analysis.

### Gold Models

| Gold Tables               |
| ------------------------- |
| `gold_patient_analysis`   |
| `gold_hospital_analysis`  |
| `gold_diagnosis_analysis` |
| `gold_lab_analysis`       |
| `gold_vitals_analysis`    |
| `gold_cost_analysis`      |

---

# 📊 Data Models

The project uses a structured analytical data model to support healthcare reporting and analytics.

### Analytical Model

```text
                    ┌─────────────────────┐
                    │    Hospital Info     │
                    └──────────┬──────────┘
                               │
                               ▼
┌──────────────────┐     ┌─────────────────────┐
│ Patient          │────►│ Healthcare Patient  │
│ Demographics     │     │ Analytics           │
└──────────────────┘     └──────────┬──────────┘
                                    │
             ┌──────────────────────┼──────────────────────┐
             ▼                      ▼                      ▼
   ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
   │ Patient         │    │ Patient         │    │ Laboratory      │
   │ Diagnosis       │    │ Vitals          │    │ Results         │
   └─────────────────┘    └─────────────────┘    └─────────────────┘
```

---

# 📊 Business Insights

## Descriptive Analytics

The pipeline supports descriptive healthcare analytics such as:

* Patient population analysis.
* Hospital-wise patient distribution.
* Diagnosis distribution.
* Patient vital trends.
* Laboratory result analysis.
* Healthcare activity trends.

---

## Diagnostic Analytics

The pipeline can be used to analyze:

* Diagnosis patterns.
* Hospital performance.
* Patient health indicators.
* Laboratory result variations.
* Vital-sign abnormalities.
* Relationship between patient characteristics and diagnoses.

---

## Advanced Analytics

The Gold layer can support:

* High-risk patient identification.
* Hospital-wise healthcare analysis.
* Diagnosis trend analysis.
* Patient health monitoring.
* Cost analysis.
* Healthcare performance metrics.

---

# 📊 Dashboards

The Gold layer provides analytics-ready datasets for dashboards.

### Dashboard Areas

* Patient Overview
* Hospital Analysis
* Diagnosis Analysis
* Patient Vital Analysis
* Laboratory Analysis
* Healthcare Cost Analysis
* Patient Risk Analysis
* Healthcare Trends

The dashboards consume the processed **Gold-layer datasets** rather than raw healthcare data.

---

# 🧰 Data Build Tool (dbt)

dbt is used for SQL-based transformations and modelling in the **Silver and Gold layers**.

### dbt Implementation

* Connected dbt with Databricks.
* Created Silver transformation models.
* Created Gold analytical models.
* Used modular SQL transformations.
* Standardized healthcare fields.
* Implemented reusable transformation logic.
* Materialized models as Delta tables.
* Created structured analytics datasets.
* Applied data quality tests.
* Used dbt models as the foundation for dashboard analytics.

### dbt Layer

```text
Bronze Delta Tables
        │
        ▼
      dbt
        │
        ├── Silver Models
        │      │
        │      ▼
        │  Cleaned Data
        │
        └── Gold Models
               │
               ▼
        Analytics Tables
```

---

# 🔄 Apache Airflow – Pipeline Orchestration

The pipeline execution is automated using **Apache Airflow**.

### Workflow Tasks

1. Start healthcare pipeline.
2. Trigger data ingestion.
3. Validate source files.
4. Process Bronze layer.
5. Execute dbt Silver models.
6. Execute dbt Gold models.
7. Perform data quality checks.
8. Validate pipeline execution.
9. Send Slack notifications.
10. Log pipeline execution status.

### Pipeline Flow

```text
Airflow
   │
   ▼
Data Ingestion
   │
   ▼
Bronze Processing
   │
   ▼
dbt Silver
   │
   ▼
dbt Gold
   │
   ▼
Data Quality Checks
   │
   ├────────► Success ───────► Slack
   │
   └────────► Failure ───────► Slack Alert
```

---

# ⚠ Alerts, Monitoring & Logging

Slack is integrated into the pipeline for automated notifications.

### Alerts Include

* Pipeline failure alerts.
* Task failure notifications.
* Data quality failure notifications.
* Successful pipeline completion.
* Important pipeline execution events.

### Monitoring

* Airflow DAG monitoring.
* Databricks job monitoring.
* dbt execution monitoring.
* Data quality monitoring.
* Pipeline execution logs.

---

# ✅ Data Quality & Testing

Data quality checks are implemented across the pipeline to improve data reliability.

### Validation Checks

* Schema validation.
* Null-value checks.
* Duplicate detection.
* Patient ID validation.
* Data type validation.
* Row-count validation.
* Invalid-value detection.
* dbt model tests.
* Pipeline execution validation.

### Data Quality Flow

```text
Raw Data
   │
   ▼
Schema Validation
   │
   ▼
Null Checks
   │
   ▼
Duplicate Checks
   │
   ▼
Transformation Validation
   │
   ▼
Gold Data Validation
   │
   ▼
Dashboard Ready
```

---

# 🔐 Data Governance

The project uses **Databricks Unity Catalog** for structured data management and governance.

### Catalog Structure

```text
healthcare_catalog
│
├── bronze
│
├── silver
│
├── gold
│
└── logs
```

Unity Catalog provides centralized management of the healthcare data assets and supports controlled access to the different layers.

---

# 👨‍💻 My Role

### Role: Data Engineer

Responsibilities included:

* Designed and implemented the healthcare data pipeline.
* Worked with Azure Data Factory for data ingestion.
* Loaded healthcare data into ADLS Gen2.
* Created Bronze Delta tables in Databricks.
* Implemented Silver transformations using dbt.
* Developed Gold analytical models using dbt.
* Implemented SQL-based data transformations.
* Performed data quality and validation checks.
* Worked with Unity Catalog and Databricks schemas.
* Prepared analytics-ready datasets for dashboards.
* Implemented Airflow pipeline orchestration.
* Integrated Slack alerts for pipeline monitoring.
* Monitored pipeline execution and troubleshooting.
* Maintained project code using Git and GitHub.

---

# 📈 Key Outcomes

* Built an end-to-end healthcare data engineering pipeline.
* Implemented Azure-based Lakehouse architecture.
* Created Bronze, Silver, and Gold data layers.
* Automated healthcare data ingestion.
* Created reusable dbt transformation models.
* Built analytics-ready healthcare datasets.
* Implemented data quality validation.
* Added Airflow pipeline orchestration.
* Integrated Slack monitoring and alerts.
* Improved reliability and maintainability of healthcare analytics processing.

---

# 🔮 Future Enhancements

* Implement incremental data processing.
* Add advanced patient risk scoring.
* Implement machine learning-based healthcare predictions.
* Add automated CI/CD for dbt and Databricks.
* Improve real-time healthcare monitoring.
* Add additional data quality frameworks.
* Implement comprehensive pipeline observability.
* Expand healthcare analytics dashboards.

---

# 📌 Conclusion

This project demonstrates an end-to-end **Healthcare Patient Analytics Data Engineering Pipeline** built using modern Azure and Databricks technologies.

The pipeline combines **Azure Data Factory, ADLS Gen2, Databricks, Delta Lake, Unity Catalog, dbt, Apache Airflow, Slack, SQL, and Git/GitHub** to create a scalable and reliable healthcare analytics platform.

By implementing the **Medallion Architecture**, the project transforms raw healthcare data into clean, validated, and analytics-ready datasets while providing automated orchestration, monitoring, data quality validation, and business insights.
