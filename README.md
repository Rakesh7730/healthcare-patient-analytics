# 🏥 Healthcare Patient Analytics Pipeline

## 📌 Introduction

The **Healthcare Patient Analytics Pipeline** is an end-to-end Data Engineering project designed to ingest, process, transform, validate, and analyze healthcare patient data using **Microsoft Azure, Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, Delta Lake, PySpark, dbt, Apache Airflow, and Slack**.

The project follows a **Medallion Architecture consisting of Bronze, Silver, and Gold layers** to transform raw healthcare records into clean, reliable, and analytics-ready datasets.

The source data consists of healthcare CSV datasets containing information related to:

* Patient Demographics
* Patient Vitals
* Patient Diagnosis
* Laboratory Results
* Hospital Information

The source CSV files are ingested using **Azure Data Factory** and stored in **Azure Data Lake Storage Gen2**. The files are converted from CSV to Parquet format before being processed in Azure Databricks.

The **Bronze layer** stores raw healthcare data in Delta format. The **Silver and Gold layers are developed using dbt**, where SQL-based transformations, cleansing, validation, standardization, and analytical transformations are performed.

**Apache Airflow** is used for workflow orchestration and scheduling, while **Slack** is used for pipeline notifications and alerts.

The project demonstrates practical Data Engineering concepts including **ETL/ELT, Medallion Architecture, Delta Lake, cloud storage, SQL transformations, data quality, orchestration, monitoring, data governance, and version control**.

---

# 🎯 Project Objective

The main objective of this project is to design and implement a scalable healthcare data engineering pipeline that:

* Ingests healthcare CSV files.
* Uses Azure Data Factory for data ingestion.
* Stores source files in ADLS Gen2.
* Converts CSV files into Parquet format.
* Processes raw data using Azure Databricks.
* Implements a Bronze, Silver, and Gold architecture.
* Stores Bronze data using Delta Lake.
* Uses dbt for Silver transformations.
* Uses dbt for Gold transformations.
* Performs data cleansing and standardization.
* Removes duplicate records.
* Handles missing and invalid values.
* Validates healthcare data.
* Integrates multiple healthcare datasets.
* Creates analytics-ready Gold datasets.
* Performs data quality checks.
* Uses Apache Airflow for orchestration.
* Uses Slack for pipeline notifications.
* Uses Unity Catalog for data governance.
* Provides data for dashboard-based analysis.
* Maintains source code using Git and GitHub.

---

# 🏗️ Project Architecture

The solution follows a **Medallion Architecture** implemented using Azure Data Factory, ADLS Gen2, Azure Databricks, Delta Lake, dbt, Apache Airflow, Dashboard, and Slack.

## High-Level Data Flow

```text
                  Healthcare CSV Files
                           │
                           ▼
                  Azure Data Factory
                           │
                           ▼
                       ADLS Gen2
                    CSV → Parquet
                           │
                           ▼
                   Azure Databricks
                           │
                           ▼
                        BRONZE
                     Raw Delta Data
                           │
                           ▼
                          dbt
                           │
                           ▼
                        SILVER
                  Cleaned & Validated
                           │
                           ▼
                          dbt
                           │
                           ▼
                         GOLD
                  Analytics & Metrics
                           │
                           ▼
                      Dashboard
                           │
                           ▼
                   Slack Notifications
```

---

# 🖼️ Architecture Diagram

The detailed architecture diagram is maintained inside the repository:

```text
architecture/
└── healthcare_pipeline_architecture.png
```

The architecture represents the complete data flow from source healthcare CSV files through ingestion, processing, transformation, analytics, dashboard consumption, and notifications.

---

# 🔷 High-Level Design (HLD)

The High-Level Design represents the major components of the healthcare analytics platform.

```text
                       ┌────────────────────────────┐
                       │    HEALTHCARE DATA SOURCE  │
                       │                            │
                       │  • Patient Demographics    │
                       │  • Patient Vitals          │
                       │  • Patient Diagnosis       │
                       │  • Lab Results             │
                       │  • Hospital Information    │
                       └─────────────┬──────────────┘
                                     │
                                     ▼
                       ┌────────────────────────────┐
                       │     AZURE DATA FACTORY     │
                       │                            │
                       │      Data Ingestion        │
                       │      CSV → Parquet         │
                       └─────────────┬──────────────┘
                                     │
                                     ▼
                       ┌────────────────────────────┐
                       │         ADLS GEN2          │
                       │                            │
                       │       Staging / Parquet    │
                       └─────────────┬──────────────┘
                                     │
                                     ▼
                  ┌──────────────────────────────────────┐
                  │           AZURE DATABRICKS            │
                  │                                      │
                  │             BRONZE                    │
                  │          Raw Delta Data               │
                  │               │                       │
                  └───────────────┼───────────────────────┘
                                  │
                                  ▼
                         ┌─────────────────┐
                         │       DBT       │
                         │                 │
                         │ SQL Transform.  │
                         └────────┬────────┘
                                  │
                                  ▼
                             ┌──────────┐
                             │  SILVER  │
                             │ Cleaned  │
                             └────┬─────┘
                                  │
                                  ▼
                         ┌─────────────────┐
                         │       DBT       │
                         │                 │
                         │ Analytics SQL   │
                         └────────┬────────┘
                                  │
                                  ▼
                             ┌──────────┐
                             │   GOLD   │
                             │Analytics │
                             └────┬─────┘
                                  │
                                  ▼
                             Dashboard
                                  │
                                  ▼
                         Slack Notifications
```

---

# 🧩 Major HLD Components

| Component            | Responsibility                               |
| -------------------- | -------------------------------------------- |
| Healthcare CSV Files | Source healthcare records                    |
| Azure Data Factory   | Data ingestion and CSV-to-Parquet processing |
| ADLS Gen2            | Cloud data lake storage                      |
| Azure Databricks     | Distributed data processing and Bronze layer |
| Delta Lake           | Reliable storage for processed data          |
| PySpark              | Data processing and transformation           |
| dbt                  | Silver and Gold SQL transformations          |
| Apache Airflow       | Workflow orchestration and scheduling        |
| Dashboard            | Visualization and analytical consumption     |
| Slack                | Pipeline notifications and alerts            |
| Unity Catalog        | Data governance and metadata management      |
| Git/GitHub           | Version control and source management        |

---

# 🔶 Low-Level Design (LLD)

The Low-Level Design explains how individual components process the healthcare data.

---

# 1. Data Sources

The project uses healthcare CSV datasets containing multiple types of patient and hospital information.

### Source Datasets

```text
patient_demographics.csv
patient_vitals.csv
patient_diagnosis.csv
lab_results.csv
hospital_info.csv
```

These files represent different healthcare entities and are processed through the data pipeline.

---

# 2. Data Ingestion — Azure Data Factory

Azure Data Factory is used as the initial ingestion layer.

The source CSV files are loaded into **ADLS Gen2**.

### Processing Flow

```text
Healthcare CSV Files
        │
        ▼
Azure Data Factory
        │
        ▼
ADLS Gen2 - Staging
        │
        ▼
CSV → Parquet
        │
        ▼
ADLS Gen2 - Parquet
```

### Responsibilities

* Source file ingestion.
* Data movement.
* CSV-to-Parquet conversion.
* Pipeline orchestration.
* Dependency management.
* Pipeline monitoring.

---

# 3. ADLS Gen2 — Data Lake Storage

**Azure Data Lake Storage Gen2** is used as the cloud storage layer.

The project uses separate areas for source and processed files.

```text
ADLS Gen2
│
├── staging/
│   ├── patient_demographics/
│   ├── patient_vitals/
│   ├── patient_diagnosis/
│   ├── lab_results/
│   └── hospital_info/
│
└── parquet/
    ├── patient_demographics/
    ├── patient_vitals/
    ├── patient_diagnosis/
    ├── lab_results/
    └── hospital_info/
```

Parquet is used as an optimized storage format before Databricks processing.

---

# 4. Bronze Layer — Azure Databricks

The Bronze layer is implemented using **Azure Databricks**.

The Bronze layer stores raw healthcare records with minimal transformation.

### Bronze Responsibilities

* Read Parquet files from ADLS Gen2.
* Preserve source information.
* Create Delta tables.
* Maintain raw healthcare data.
* Provide a reliable foundation for downstream transformations.
* Support data traceability and reprocessing.

### Bronze Architecture

```text
                    ADLS Gen2
                        │
                        ▼
                   Parquet Files
                        │
                        ▼
                 Azure Databricks
                        │
                        ▼
                    BRONZE
                        │
             ┌──────────┼──────────┐
             ▼          ▼          ▼
        Demographics  Vitals   Diagnosis
             │          │          │
             └──────────┼──────────┘
                        │
                 Lab Results
                        │
                 Hospital Info
```

### Bronze Tables

```text
healthcare_catalog.bronze.patient_demographics

healthcare_catalog.bronze.patient_vitals

healthcare_catalog.bronze.patient_diagnosis

healthcare_catalog.bronze.lab_results

healthcare_catalog.bronze.hospital_info
```

---

# 5. Silver Layer — dbt

The Silver layer is implemented using **dbt**.

dbt reads the Bronze tables and applies SQL transformations to create clean and validated healthcare datasets.

### Silver Processing

```text
Bronze Delta Tables
        │
        ▼
       dbt
        │
        ├── Data Cleaning
        ├── Data Validation
        ├── Standardization
        ├── Deduplication
        ├── Data Type Conversion
        └── Business Rules
        │
        ▼
      SILVER
```

### Silver Transformations

The Silver layer performs:

* Null value handling.
* Duplicate removal.
* Data type conversion.
* Column standardization.
* Patient ID validation.
* Date standardization.
* Invalid record identification.
* Data cleansing.
* Schema validation.
* Business-rule validation.
* Data enrichment.
* Dataset integration.

### Example Silver Models

```text
dbt/models/silver/

silver_patient_demographics.sql
silver_patient_vitals.sql
silver_patient_diagnosis.sql
silver_lab_results.sql
silver_hospital_info.sql
```

---

# 6. Gold Layer — dbt

The Gold layer is also implemented using **dbt**.

The purpose of the Gold layer is to create business-ready healthcare analytics.

### Gold Processing

```text
Silver Tables
     │
     ▼
    dbt
     │
     ├── Aggregation
     ├── Metric Calculation
     ├── Business Logic
     └── Analytical Transformation
     │
     ▼
   GOLD
```

### Potential Gold Metrics

The Gold layer can provide metrics such as:

* Total patient count.
* Patient count by gender.
* Patient count by age group.
* Patient count by hospital.
* Diagnosis distribution.
* Average vital measurements.
* Abnormal vital counts.
* Laboratory result statistics.
* High-risk patient count.
* Disease distribution.
* Hospital-level patient statistics.

### Example Gold Model

```text
dbt/models/gold/

health_metrics.sql
```

Example output:

```text
Metric                    Value
-----------------------------------
Total Patients            ...
Male Patients             ...
Female Patients           ...
High Risk Patients        ...
Abnormal Vital Records    ...
```

---

# 7. dbt Project Structure

The dbt project is organized into staging, Silver, and Gold models.

```text
dbt/
│
├── models/
│   ├── staging/
│   │   ├── stg_patient_demographics.sql
│   │   ├── stg_patient_vitals.sql
│   │   ├── stg_patient_diagnosis.sql
│   │   ├── stg_lab_results.sql
│   │   └── stg_hospital_info.sql
│   │
│   ├── silver/
│   │   ├── silver_patient_demographics.sql
│   │   ├── silver_patient_vitals.sql
│   │   ├── silver_patient_diagnosis.sql
│   │   ├── silver_lab_results.sql
│   │   └── silver_hospital_info.sql
│   │
│   └── gold/
│       └── health_metrics.sql
│
├── tests/
│
├── macros/
│
├── seeds/
│
├── dbt_project.yml
│
└── profiles.yml.example
```

---

# 8. Data Quality Checks

Data quality is applied throughout the pipeline.

The project validates:

* Required columns.
* Null values.
* Duplicate records.
* Patient IDs.
* Data types.
* Invalid dates.
* Invalid numerical values.
* Schema consistency.
* Unexpected values.
* Relationships between datasets.
* Missing healthcare records.

### Data Quality Flow

```text
Input Data
    │
    ▼
Schema Validation
    │
    ▼
Null Validation
    │
    ▼
Duplicate Check
    │
    ▼
Data Type Validation
    │
    ▼
Business Rule Validation
    │
 ┌──┴───────┐
 ▼          ▼
Valid      Invalid
 │          │
 ▼          ▼
Silver    Error/Log
```

---

# 9. dbt Testing

dbt tests are used to validate transformation outputs.

Common tests include:

```text
not_null
unique
accepted_values
relationships
```

### Example Validation

```text
Patient ID
    │
    ├── NOT NULL
    └── UNIQUE

Hospital ID
    │
    └── RELATIONSHIP VALIDATION
```

These tests help ensure that Silver and Gold datasets meet expected quality requirements.

---

# 10. Error Handling

The pipeline identifies invalid records and processing failures.

### Error Handling Strategy

```text
                    Input
                      │
                      ▼
                  Validation
                      │
                ┌─────┴─────┐
                ▼           ▼
              Valid       Invalid
                │           │
                ▼           ▼
            Processing   Error Log
                │
                ▼
             Silver
```

Potential error information includes:

* Error timestamp.
* Pipeline name.
* Job name.
* Error type.
* Error message.
* Dataset name.
* Processing stage.
* Affected record information.

---

# 11. Apache Airflow — Orchestration

**Apache Airflow** is used to orchestrate the healthcare data pipeline.

Airflow manages task dependencies and workflow execution.

### Airflow Flow

```text
                Airflow DAG
                    │
                    ▼
             Start Pipeline
                    │
                    ▼
             Trigger ADF
                    │
                    ▼
             Data Ingestion
                    │
                    ▼
              Databricks
                    │
                    ▼
               dbt Silver
                    │
                    ▼
                dbt Gold
                    │
                    ▼
                Dashboard
                    │
                    ▼
             Slack Notification
```

### Airflow Responsibilities

* Workflow orchestration.
* Scheduling.
* Task dependency management.
* Pipeline execution.
* Retry handling.
* Failure handling.
* Monitoring.
* Triggering downstream tasks.

---

# 12. Dashboard

The Gold-layer data is used as the source for the project's **healthcare analytics dashboard**.

The dashboard provides an analytical view of the processed healthcare data.

### Dashboard Flow

```text
Gold Tables
     │
     ▼
Dashboard
     │
     ├── Patient Analytics
     ├── Diagnosis Analytics
     ├── Vital Analytics
     ├── Laboratory Analytics
     └── Hospital Analytics
```

The dashboard can be used to monitor healthcare metrics and identify useful trends from the processed data.

Dashboard screenshots can be stored in:

```text
dashboard/
└── screenshots/
```

---

# 13. Slack Notifications

**Slack** is used for pipeline notifications.

Slack notifications can provide information about pipeline execution status.

### Notification Flow

```text
Airflow
   │
   ▼
Pipeline Execution
   │
 ┌─┴──────────┐
 ▼            ▼
SUCCESS      FAILURE
 │            │
 ▼            ▼
Dashboard    Slack
             Alert
```

Notifications can be configured for:

* Pipeline success.
* Pipeline failure.
* Databricks job failure.
* dbt execution failure.
* Data quality failure.
* Pipeline completion.
* Task failure.

Slack provides a simple way to receive pipeline status without continuously monitoring Airflow.

---

# 14. Unity Catalog

**Unity Catalog** is used for data governance and centralized management of Databricks data assets.

The project follows a logical catalog and schema structure.

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

Unity Catalog can provide:

* Access control.
* Metadata management.
* Data discovery.
* Data lineage.
* Table governance.
* Permission management.

---

# 15. Delta Lake

Delta Lake is used as the storage format for the Databricks data layers.

Delta Lake provides:

* ACID transactions.
* Schema enforcement.
* Schema evolution where required.
* Time travel.
* Reliable reads and writes.
* Efficient analytical processing.

The Bronze layer is maintained as Delta data, while the dbt Silver and Gold models are materialized into the target Databricks environment.

---

# 16. Pipeline Execution Flow

The complete execution sequence is:

```text
1. Healthcare CSV Files
          │
          ▼
2. Azure Data Factory
          │
          ▼
3. ADLS Gen2
          │
          ▼
4. CSV → Parquet
          │
          ▼
5. Azure Databricks
          │
          ▼
6. Bronze Delta Tables
          │
          ▼
7. dbt Staging
          │
          ▼
8. dbt Silver Models
          │
          ▼
9. dbt Gold Models
          │
          ▼
10. Dashboard
          │
          ▼
11. Slack Notification
```

Apache Airflow orchestrates the workflow and manages task dependencies.

---

# 17. Performance Optimization

The project can use the following optimization techniques:

* Efficient Spark transformations.
* Appropriate partitioning.
* Partition pruning.
* Predicate pushdown.
* Incremental processing.
* Delta optimization.
* Avoiding unnecessary shuffles.
* Efficient SQL transformations.
* Appropriate dbt materialization strategies.
* Caching only when beneficial.

Optimization decisions should be based on actual data volume and query requirements.

---

# 18. Security Considerations

Sensitive credentials should never be committed to GitHub.

The following information must not be hardcoded:

* Azure credentials.
* ADLS access keys.
* Databricks tokens.
* dbt credentials.
* Database passwords.
* Connection strings.
* Service principal secrets.
* Slack webhook URLs.

The repository should use:

* Environment variables.
* Secret management.
* Azure Key Vault where applicable.
* Databricks secrets where applicable.
* Secure CI/CD secrets.

The `.gitignore` file should prevent sensitive configuration files from being committed.

---

# 📁 Project Structure

```text
healthcare-patient-analytics/
│
├── README.md
├── .gitignore
│
├── architecture/
│   └── healthcare_pipeline_architecture.png
│
├── data/
│   └── csv/
│       ├── patient_demographics.csv
│       ├── patient_vitals.csv
│       ├── patient_diagnosis.csv
│       ├── lab_results.csv
│       └── hospital_info.csv
│
├── adf/
│   └── pipelines/
│
├── airflow/
│   └── dags/
│       └── healthcare_pipeline.py
│
├── databricks/
│   └── bronze/
│       ├── 01_Bronze_Patient_Demographics.sql
│       ├── 02_Bronze_Patient_Vitals.sql
│       ├── 03_Bronze_Patient_Diagnosis.sql
│       ├── 04_Bronze_Lab_Results.sql
│       └── 05_Bronze_Hospital_Info.sql
│
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   ├── silver/
│   │   └── gold/
│   │
│   ├── tests/
│   ├── macros/
│   ├── seeds/
│   ├── dbt_project.yml
│   └── profiles.yml.example
│
├── dashboard/
│   └── screenshots/
│
├── slack/
│   └── notifications/
│
└── requirements.txt
```

---

# 🛠️ Technology Stack

## ☁️ Cloud

* Microsoft Azure
* Azure Data Factory
* Azure Data Lake Storage Gen2
* Azure Databricks

## 🔥 Data Engineering

* Apache Spark
* PySpark
* Delta Lake
* Parquet
* Medallion Architecture

## 🔄 Data Transformation

* dbt
* SQL
* PySpark

## 🔁 Orchestration

* Apache Airflow
* Azure Data Factory

## 📊 Analytics

* Healthcare Analytics Dashboard
* Databricks SQL / Gold-layer data

## 📢 Notifications

* Slack

## 🔐 Governance

* Unity Catalog
* Data Governance
* Access Control
* Data Lineage

## 🔧 Version Control

* Git
* GitHub

---

# 💼 Business Use Cases

## Patient Analytics

Analyze patient demographics, vitals, diagnosis, and laboratory information.

## Hospital Analytics

Analyze patient distribution and healthcare activity across hospitals.

## Diagnosis Analysis

Identify diagnosis patterns and disease distributions.

## Vital Analysis

Analyze patient vital measurements and identify abnormal values.

## Laboratory Analysis

Analyze laboratory results and identify abnormal or significant results.

## Patient Risk Analysis

Gold-layer transformations can be used to identify potentially high-risk patient groups based on defined business rules.

## Healthcare Reporting

The Gold layer provides analytics-ready data for dashboard-based reporting.

---

# 🚀 Expected Outcomes

The project demonstrates the ability to build an end-to-end cloud Data Engineering solution that:

* Uses Azure Data Factory for ingestion.
* Uses ADLS Gen2 for cloud storage.
* Converts CSV files into Parquet.
* Uses Azure Databricks for Bronze processing.
* Implements Medallion Architecture.
* Uses Delta Lake.
* Uses dbt for Silver transformations.
* Uses dbt for Gold transformations.
* Performs data cleansing and validation.
* Implements dbt tests.
* Handles invalid records.
* Uses Apache Airflow for orchestration.
* Uses Slack for notifications.
* Uses Unity Catalog for governance.
* Produces analytics-ready healthcare datasets.
* Supports dashboard-based analysis.
* Uses Git and GitHub for version control.

---

# 🔮 Future Enhancements

The project can be extended by:

* Implementing incremental data loading.
* Adding automated CI/CD pipelines.
* Adding GitHub Actions.
* Improving dashboard functionality.
* Adding real-time monitoring.
* Adding automated data quality reports.
* Adding advanced patient risk analytics.
* Implementing machine learning models.
* Adding predictive healthcare analytics.
* Adding automated anomaly detection.
* Improving Airflow monitoring.
* Adding additional Slack alert conditions.
* Expanding healthcare datasets.
* Adding comprehensive dbt documentation.

---

# 👨‍💻 Skills Demonstrated

This project demonstrates practical experience with:

```text
Azure Cloud
Azure Data Factory
Azure Data Lake Storage Gen2
Azure Databricks
Apache Spark
PySpark
Python
SQL
Delta Lake
Parquet
Medallion Architecture
dbt
Apache Airflow
Unity Catalog
Databricks SQL
Slack
Git
GitHub
ETL / ELT
Data Transformation
Data Quality
Data Validation
Error Handling
Workflow Orchestration
Cloud Data Engineering
Data Governance
Analytics
Dashboard Development
```

---

# 🏁 Conclusion

The **Healthcare Patient Analytics Pipeline** demonstrates an end-to-end Azure Data Engineering architecture for transforming raw healthcare records into reliable, clean, and analytics-ready datasets.

The pipeline combines **Azure Data Factory, ADLS Gen2, Azure Databricks, Delta Lake, dbt, Apache Airflow, Unity Catalog, Dashboard analytics, Slack, and Git/GitHub**.

The architecture separates data processing into **Bronze, Silver, and Gold layers**.

The **Bronze layer is implemented using Azure Databricks**, while the **Silver and Gold layers are implemented using dbt**.

Apache Airflow provides workflow orchestration and scheduling, while Slack provides pipeline notifications.

The resulting Gold-layer datasets can be consumed by the healthcare dashboard to provide meaningful analytical insights.

The project demonstrates practical knowledge of modern **Cloud Data Engineering, ETL/ELT, SQL transformation, data quality, orchestration, governance, monitoring, and analytics**.

---

# ⭐ Project Highlights

> **Source** → Healthcare CSV Files
> **Ingestion** → Azure Data Factory
> **Storage** → Azure Data Lake Storage Gen2
> **File Format** → Parquet
> **Processing** → Azure Databricks + PySpark
> **Architecture** → Bronze → Silver → Gold
> **Bronze** → Azure Databricks
> **Silver** → dbt
> **Gold** → dbt
> **Storage** → Delta Lake
> **Governance** → Unity Catalog
> **Orchestration** → Apache Airflow
> **Analytics** → Healthcare Dashboard
> **Notifications** → Slack
> **Version Control** → Git + GitHub
> **Business Objective** → Healthcare Patient Analytics
