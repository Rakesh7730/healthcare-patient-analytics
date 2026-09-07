# 🏥 Healthcare Patient Analytics Pipeline

## 🚀 Project Overview

The **Healthcare Patient Analytics Pipeline** is an end-to-end Data Engineering project designed to ingest, transform, validate, and analyze healthcare data for hospital and patient analytics.

The project uses a modern **Lakehouse architecture** with **Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, Delta Lake, Unity Catalog, and dbt** to transform raw healthcare datasets into clean, validated, and analytics-ready data.

The pipeline processes hospital information, patient demographics, diagnosis records, laboratory results, and patient vital measurements through a **Medallion Architecture (Bronze → Silver → Gold)**.

The project focuses on:

* Reliable ingestion of healthcare source data
* Raw data storage and traceability
* Data cleansing and standardization
* Deduplication and validation
* Healthcare business transformations
* Patient risk-score calculation
* Readmission-risk analysis
* Hospital performance analysis
* Laboratory abnormality analysis
* Patient vitals trend analysis
* Healthcare cost analysis
* Data-quality auditing
* Pipeline monitoring and failure alerting
* CI/CD support through Azure DevOps

---

# 🎯 Project Objectives

The main objectives of the Healthcare Patient Analytics Pipeline are:

* Build a scalable healthcare analytics data pipeline.
* Ingest multiple healthcare datasets using **Azure Data Factory (ADF)**.
* Convert source CSV files into Parquet format.
* Store the processed source files in **Azure Data Lake Storage Gen2**.
* Implement Bronze, Silver, and Gold layers using Medallion Architecture.
* Register Bronze data as external tables using **Databricks Unity Catalog**.
* Clean and standardize healthcare data using **dbt**.
* Remove duplicate healthcare records.
* Apply data-type casting and validation.
* Identify invalid and physically impossible values.
* Generate patient risk scores.
* Categorize patient readmission risk.
* Create hospital performance metrics.
* Generate healthcare cost and treatment analytics.
* Build analytics-ready Gold marts.
* Implement data-quality audit tables.
* Validate Gold and Silver datasets using automated tests.
* Support pipeline orchestration and monitoring.
* Generate Slack notifications for pipeline failures.
* Support CI/CD using Azure DevOps.

---

# 🏗 Lakehouse Architecture

The project follows a modern **Azure Lakehouse Architecture**.

```text
                  ┌──────────────────────────┐
                  │   Healthcare CSV Files   │
                  │                          │
                  │ hospital_info            │
                  │ lab_results              │
                  │ patient_demographics     │
                  │ patient_diagnosis        │
                  │ patient_vitals           │
                  └────────────┬─────────────┘
                               │
                               ▼
                  ┌──────────────────────────┐
                  │    Azure Data Factory    │
                  │                          │
                  │ Ingestion + Conversion   │
                  │       CSV → Parquet      │
                  └────────────┬─────────────┘
                               │
                               ▼
                  ┌──────────────────────────┐
                  │     ADLS Gen2            │
                  │                          │
                  │       Parquet Container  │
                  └────────────┬─────────────┘
                               │
                               ▼
        ┌────────────────────────────────────────────┐
        │             Azure Databricks               │
        │                                            │
        │              Unity Catalog                │
        └────────────────────┬───────────────────────┘
                             │
                             ▼
                  ┌──────────────────────────┐
                  │     🥉 Bronze Layer      │
                  │                          │
                  │ Raw External Tables      │
                  └────────────┬─────────────┘
                               │
                               ▼
                  ┌──────────────────────────┐
                  │      🥈 Silver Layer     │
                  │                          │
                  │ Cleaning + Validation    │
                  │ Deduplication + Standard │
                  │          dbt              │
                  └────────────┬─────────────┘
                               │
                               ▼
                  ┌──────────────────────────┐
                  │       🥇 Gold Layer      │
                  │                          │
                  │ Star Schema + Gold Marts │
                  │          dbt              │
                  └────────────┬─────────────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
              ▼                ▼                ▼
        ┌───────────┐    ┌────────────┐   ┌───────────┐
        │ Dashboards│    │  Analytics │   │   Alerts  │
        └───────────┘    └────────────┘   └───────────┘
```

---

# 🛠 Technology Stack

| Technology                       | Purpose                                       |
| -------------------------------- | --------------------------------------------- |
| **Azure Data Lake Storage Gen2** | Cloud data lake storage                       |
| **Azure Data Factory**           | Data ingestion and CSV-to-Parquet conversion  |
| **Azure Databricks**             | Lakehouse data processing                     |
| **Apache Spark / SQL**           | Data processing and transformation            |
| **Delta Lake**                   | Structured and reliable data storage          |
| **Unity Catalog**                | Data governance and external table management |
| **dbt Cloud**                    | Silver and Gold SQL transformations           |
| **Apache Airflow**               | Pipeline orchestration                        |
| **Azure DevOps**                 | CI/CD and repository integration              |
| **Slack**                        | Pipeline failure notifications                |
| **Python / Pytest**              | Data-quality testing                          |
| **Git / GitHub**                 | Version control                               |

---

# 📂 Dataset

## Dataset Source

**Simulated Healthcare Patient Data**

The project dataset is based on the **Kaggle Heart Disease Dataset** and is organized into multiple healthcare domains.

## Datasets Used

### 🏥 Hospital Information

`hospital_info.csv`

Contains hospital-level information such as:

* Hospital ID
* Hospital name
* City
* State
* Bed capacity
* ICU beds
* Staff count
* Infection rate
* Utilization rate
* Average wait time
* Equipment score
* Patient load
* Surgery count
* Emergency cases
* Record date

### 👤 Patient Demographics

`patient_demographics.csv`

Contains patient-level demographic and lifestyle information including:

* Patient ID
* Age
* Gender
* Age group
* Lifestyle risk
* Smoking index
* Alcohol index
* Exercise hours
* Sleep hours
* Diet score
* Health score
* Record date

### 🩺 Patient Diagnosis

`patient_diagnosis.csv`

Contains clinical diagnosis and risk-related information including:

* Patient ID
* Hospital ID
* Diagnosis code
* Severity score
* Risk probability
* Readmission risk
* Comorbidity score
* Doctor information
* Treatment cost
* Medication count
* Recovery days
* Record date

### 🧪 Laboratory Results

`lab_results.csv`

Contains laboratory observations such as:

* Patient ID
* Hospital ID
* Laboratory test
* Laboratory values
* Hemoglobin
* Platelets
* WBC
* RBC
* Creatinine
* Technician information
* Test cost
* Record date

### ❤️ Patient Vitals

`patient_vitals.csv`

Contains patient vital measurements such as:

* Patient ID
* Hospital ID
* Heart rate
* Blood pressure
* Oxygen level
* BMI
* Glucose
* Vital measurements
* Record date

---

# 🏗 ELT Design – Medallion Architecture

The pipeline is organized into three major layers:

```text
Raw Healthcare Data
       │
       ▼
🥉 Bronze
       │
       ▼
🥈 Silver
       │
       ▼
🥇 Gold
       │
       ▼
Healthcare Analytics
```

---

# 🥉 Bronze Layer – Raw Data Ingestion

## Purpose

The Bronze layer provides the raw representation of healthcare source data.

The Bronze implementation uses **Parquet files stored in ADLS Gen2** and registers them as external tables in Databricks Unity Catalog.

## Bronze Processing

* Healthcare CSV files are ingested through Azure Data Factory.
* Source files are converted to Parquet.
* Parquet files are stored in the ADLS Gen2 Parquet container.
* Databricks Unity Catalog external tables are created over the Parquet files.
* Raw source information is preserved for traceability.
* The Bronze layer provides the foundation for downstream dbt transformations.

## Bronze Tables

```text
bronze.hospital_info
bronze.lab_results
bronze.patient_demographics
bronze.patient_diagnosis
bronze.patient_vitals
```

## Bronze Architecture

```text
CSV Files
   │
   ▼
Azure Data Factory
   │
   ▼
ADLS Gen2
   │
   ▼
Parquet Files
   │
   ▼
Unity Catalog External Tables
   │
   ▼
Bronze Layer
```

---

# 🥈 Silver Layer – Data Cleaning & Transformation

The Silver layer is implemented using **dbt SQL models** on top of the Bronze tables.

## Purpose

The Silver layer transforms raw healthcare data into consistent, validated, and analytics-ready datasets.

## Transformations

The Silver layer performs:

* NULL handling
* Blank-value handling
* Data-type casting
* String trimming
* Data standardization
* Duplicate detection and removal
* Record-date standardization
* Key validation
* Numeric validation
* Range validation
* Data-quality flag generation
* Orphan-record detection
* Readmission-risk categorization
* Healthcare field standardization

## Deduplication

Duplicate records are handled using `ROW_NUMBER()`-based deduplication patterns over business keys such as:

```text
Patient + Record Date
Hospital + Record Date
Patient + Hospital + Record Date + Diagnosis
Patient + Hospital + Record Date + Laboratory Test
Patient + Hospital + Record Date
```

## Data Quality Flags

Invalid or suspicious records can be identified using a `data_quality_flag`.

Examples include:

```text
Invalid values
Negative values
Orphan records
NULL keys
Invalid healthcare measurements
```

## Silver Models

```text
silver_hospital_info
silver_lab_results
silver_patient_demographics
silver_patient_diagnosis
silver_patient_vitals
```

---

# 🥇 Gold Layer – Analytics & Insights

The Gold layer contains the business-ready healthcare analytical models.

Gold transformations are implemented using **dbt SQL models**.

The Gold layer contains both:

1. **Dimensional/star-schema models**
2. **Business analytical marts**

---

# ⭐ Gold Data Model

The analytical model is based on a star-schema approach.

```text
                    ┌────────────────────┐
                    │    DIM_PATIENT     │
                    └─────────┬──────────┘
                              │
                              │
┌────────────────┐            │            ┌────────────────┐
│  DIM_HOSPITAL  │────────────┼────────────│    DIM_DATE    │
└────────────────┘            │            └────────────────┘
                              │
                              ▼
                   ┌─────────────────────┐
                   │ FACT_HEALTH_METRICS │
                   └──────────┬──────────┘
                              │
                              ▼
                   ┌─────────────────────┐
                   │   DIM_OBSERVATION   │
                   └─────────────────────┘
```

---

# 📐 Gold Dimension Models

## `dim_patient`

Provides the patient dimension for analytical reporting.

It represents patient-level demographic and lifestyle information.

---

## `dim_hospital`

Provides hospital-level analytical attributes including:

* Hospital ID
* Hospital name
* City
* State
* Bed capacity
* ICU beds
* Staff count
* Infection rate
* Utilization rate
* Wait time
* Equipment score
* Patient load
* Surgery count
* Emergency cases
* Latest record date

---

## `dim_date`

Provides date-related analytical attributes including:

* Date
* Year
* Quarter
* Month
* Month name
* Other calendar attributes

It supports time-based healthcare analysis.

---

## `dim_observations`

Combines healthcare observations from:

* Diagnosis
* Laboratory results
* Patient vitals

The model standardizes different observation types into a common analytical structure.

Observation types include:

```text
DIAGNOSIS
LAB
VITAL
```

---

# 📊 Fact Table – `fact_health_metrics`

The central Gold fact table is:

```text
fact_health_metrics
```

This table combines patient, hospital, date, observation, diagnosis, laboratory, and vital information into a common analytical structure.

## Key Metrics

The fact table supports metrics such as:

* Patient risk score
* Risk probability
* Severity score
* Readmission risk
* Comorbidity score
* Lifestyle risk
* Smoking index
* Alcohol index
* Health score
* Treatment cost
* Test cost
* Insurance claim
* Hospital utilization
* Patient load
* Infection rate
* Equipment score
* Vital indicators
* Laboratory indicators

---

# 🧮 Patient Risk Score

The project generates a composite **Patient Risk Score** using healthcare risk-related attributes.

The calculation uses factors including:

* `risk_probability`
* `severity_score`
* `comorbidity_score`
* `lifestyle_risk`
* `smoking_index`
* `alcohol_index`

The resulting score is used to support patient-risk analysis and healthcare reporting.

---

# 🏥 Gold Business Marts

The project contains eight business-oriented Gold marts.

## 1. Patient Risk Summary

```text
gold_patient_risk_summary
```

Provides patient risk analysis using dimensions such as:

* Age group
* Gender
* Patient count
* Risk probability
* Severity
* Patient risk score

---

## 2. Hospital Performance Scorecard

```text
gold_hospital_performance_scorecard
```

Provides hospital-level performance metrics including:

* Observation count
* Patient count
* Infection rate
* Utilization rate
* Average wait time
* Equipment score
* Patient load
* Surgery count
* Emergency cases
* Average patient risk score

---

## 3. Vitals Trend Analysis

```text
gold_vitals_trend_analysis
```

Supports analysis of patient vital measurements and healthcare trends.

The analysis can be used for:

* Vital-sign monitoring
* Patient health trends
* Abnormal vital identification
* Risk analysis

---

## 4. Cost Analysis

```text
gold_cost_analysis
```

Provides financial and treatment-related analytics.

Metrics include:

* Observation count
* Patient count
* Total treatment cost
* Total test cost
* Total insurance claim
* Average treatment cost
* Average test cost
* Average insurance claim
* Average patient risk score

Analysis can be performed by:

* Hospital
* City
* State
* Diagnosis

---

## 5. Readmission Risk Distribution

```text
gold_readmission_risk_distribution
```

Provides analysis of patient readmission-risk categories.

Risk categories include:

```text
Low
Medium
High
```

This enables healthcare teams to analyze the distribution of patients across different readmission-risk levels.

---

## 6. Lifestyle Health Correlation

```text
gold_lifestyle_health_correlation
```

Analyzes relationships between lifestyle attributes and health outcomes.

Metrics include:

* Smoking index
* Alcohol index
* Exercise hours
* Sleep hours
* Diet score
* Lifestyle risk
* Health score
* Risk probability
* Patient risk score

Analysis is grouped by:

* Age group
* Gender

---

## 7. Laboratory Abnormality Rate

```text
gold_lab_abnormality_rate
```

Provides laboratory-quality and abnormality analysis.

Metrics include:

* Laboratory observation count
* Patient count
* Abnormal laboratory count
* Abnormal laboratory percentage

The analysis is grouped by laboratory test name.

---

## 8. Monthly & Quarterly Trend

```text
gold_monthly_quarterly_trend
```

Provides time-based healthcare analytics.

Metrics include:

* Observation count
* Patient count
* Average risk probability
* Average severity score
* Average patient risk score
* Total treatment cost
* Total test cost
* Total insurance claim

Analysis is available by:

* Year
* Quarter
* Month
* Month name

---

# 📊 Business Insights

## Descriptive Analytics

The Gold layer supports descriptive analysis including:

* Patient population analysis
* Hospital-wise patient distribution
* Diagnosis distribution
* Patient risk distribution
* Readmission-risk distribution
* Laboratory abnormality rates
* Patient vitals trends
* Healthcare costs
* Monthly and quarterly healthcare trends

---

## Diagnostic Analytics

The project supports diagnostic analysis such as:

* Hospital utilization analysis
* Hospital performance comparison
* Diagnosis and treatment analysis
* Laboratory abnormality analysis
* Patient lifestyle and health analysis
* Patient risk analysis
* Readmission-risk analysis
* Healthcare cost analysis

---

## Advanced Analytics

The Gold layer provides a foundation for advanced healthcare analytics including:

* High-risk patient identification
* Readmission-risk analysis
* Patient health-risk scoring
* Hospital performance scoring
* Lifestyle-health correlation
* Laboratory abnormality monitoring
* Time-based risk analysis
* Healthcare cost analysis

---

# 📊 Dashboards

The repository contains three dashboard images under:

```text
DashBoards/
```

Files:

```text
dashboard1.png
dashboard2.png
dash board3.png
```

The dashboards are designed around the Gold analytical datasets.

### Dashboard Analytics

The dashboard layer supports:

* Patient overview
* Patient risk analysis
* Hospital performance
* Healthcare cost analysis
* Diagnosis analysis
* Laboratory analysis
* Vital trends
* Readmission-risk analysis
* Lifestyle and health analysis
* Time-based healthcare trends

---

# 🧰 Data Build Tool – dbt

**dbt Cloud** is used to implement the transformation layer.

dbt is responsible for transforming Bronze datasets into Silver and Gold analytical models.

## dbt Silver

The Silver dbt models perform:

* Data cleaning
* Data standardization
* Data-type conversion
* Deduplication
* Data-quality flagging
* Key validation
* Healthcare field transformation

## dbt Gold

The Gold dbt models create:

* Dimension tables
* Fact table
* Healthcare analytical marts
* Business aggregations
* Patient risk metrics
* Hospital performance metrics
* Cost analysis
* Laboratory analytics
* Vitals analytics
* Lifestyle-health analytics
* Time-based analytics

## dbt Model Flow

```text
Bronze External Tables
          │
          ▼
         dbt
          │
          ▼
Silver Models
          │
          ▼
Gold Dimensions
          │
          ▼
FACT_HEALTH_METRICS
          │
          ▼
Gold Business Marts
```

---

# 🔄 Apache Airflow – Pipeline Orchestration

The project is designed to support automated pipeline orchestration using **Apache Airflow / Databricks workflows**.

## Pipeline Tasks

The documented pipeline flow consists of:

```text
Task 1
Bronze ingestion

      ↓

Task 2
Silver transformation using dbt

      ↓

Task 3
Gold aggregation and analytics

      ↓

Data Quality Validation

      ↓

Monitoring / Alerts
```

## Scheduling

The documented target schedule is:

```text
Daily Batch
02:00 UTC
```

Airflow is intended to coordinate the end-to-end pipeline execution and provide visibility into pipeline task status.

---

# ⚠ Alerts, Monitoring & Logging

The project includes alerting and monitoring capabilities for pipeline reliability.

## Slack Alerts

A Slack application named:

```text
Databricks Alerts
```

is configured using an **Incoming Webhook**.

The documented Databricks notification destination is:

```text
slack-databricks-alerts
```

The notification is configured for job failure events.

A test job with an intentionally failing SQL task was used to verify the failure-alert mechanism.

## Monitoring

Monitoring includes:

* Databricks job execution
* Databricks logs
* dbt Cloud run logs
* Pipeline failures
* Slack notifications
* Data-quality audit tables

---

# 🧾 Audits & Error Handling

The repository contains a dedicated:

```text
Audits/
```

directory.

The audit layer contains SQL models for monitoring data quality and pipeline issues.

## `data_quality_alert_summary.sql`

Aggregates data-quality flags from all five Silver models.

It generates:

* Table name
* Data-quality flag
* Issue count
* Alert severity
* Alert timestamp

Alert severity is classified as:

```text
LOW
MEDIUM
HIGH
```

based on issue counts.

---

## `duplicate_record_log.sql`

Identifies duplicate records across:

* Patient demographics
* Hospital information
* Patient diagnosis
* Laboratory results
* Patient vitals

The model records duplicate counts and relevant business keys.

---

## `etl_errors.sql`

Captures ETL-related errors including:

* NULL or empty patient IDs
* NULL or empty hospital IDs
* NULL keys after cleansing

The model also retains the source-file information for traceability.

---

## `quarantine_records.sql`

Identifies records flagged as orphan records in:

* Patient diagnosis
* Laboratory results
* Patient vitals

These records are separated for further investigation.

---

## `row_count_audit.sql`

Tracks row counts for:

* Silver patient demographics
* Silver hospital information
* Silver patient diagnosis
* Silver laboratory results
* Silver patient vitals
* Gold fact health metrics

This provides a simple mechanism for monitoring data-volume changes across the pipeline.

---

# ✅ Data Quality & Testing

The project includes automated data-quality testing using **Python, Databricks SQL, and Pytest**.

Test implementation:

```text
Test/test_data_quality.py
```

## Tests Implemented

### Fact Table Row Validation

Verifies that:

```text
fact_health_metrics
```

contains records.

---

### Fact Table Key Validation

Checks that the following keys are not NULL:

```text
patient_key
hospital_key
date_key
```

---

### Duplicate Fact Key Validation

Checks that:

```text
health_metric_key
```

does not contain duplicate records.

---

### Patient Risk Score Validation

Validates that:

```text
patient_risk_score
```

remains within the expected range.

---

### Silver Patient Deduplication

Validates that `silver_patient_demographics` does not contain duplicate:

```text
patient_id + record_date
```

records.

---

### Silver Hospital Deduplication

Validates that `silver_hospital_info` does not contain duplicate:

```text
hospital_id + record_date
```

records.

---

### Gold Mart Validation

The following eight Gold marts are checked to ensure they contain records:

```text
gold_patient_risk_summary
gold_hospital_performance_scorecard
gold_vitals_trend_analysis
gold_cost_analysis
gold_readmission_risk_distribution
gold_lifestyle_health_correlation
gold_lab_abnormality_rate
gold_monthly_quarterly_trend
```

---

# 🔐 Data Governance

The project uses **Databricks Unity Catalog** to manage healthcare data assets.

The architecture separates data into:

```text
bronze
silver
gold
```

Unity Catalog is used for:

* Table management
* External table registration
* Data governance
* Access control
* Data organization
* Lakehouse metadata management

The Bronze layer uses an **External Location and Storage Credential** to access the ADLS Gen2 Parquet data.

---

# 🔁 CI/CD – Azure DevOps

The project includes an Azure DevOps CI/CD setup.

## Repository Integration

The Databricks workspace is connected to Azure Repos using a Databricks Git folder.

The project code and notebooks can be committed and pushed through the linked Azure DevOps repository.

## Azure Pipeline

The documented pipeline includes:

* Python environment setup
* Pytest installation
* Repository validation
* Test execution
* Build artifact generation
* Artifact publishing

## Service Principal

A service principal named:

```text
Databricks-SP
```

is registered in Microsoft Entra ID to support secure non-interactive deployment access between Azure DevOps and Databricks.

---

# 📁 Repository Structure

```text
healthcare-patient-analytics/
│
├── Audits/
│   ├── data_quality_alert_summary.sql
│   ├── duplicate_record_log.sql
│   ├── etl_errors.sql
│   ├── quarantine_records.sql
│   └── row_count_audit.sql
│
├── DashBoards/
│   ├── dashboard1.png
│   ├── dashboard2.png
│   └── dash board3.png
│
├── Data/csv
│   ├── hospital_info.csv
│   ├── lab_results.csv
│   ├── patient_demographics.csv
│   ├── patient_diagnosis.csv
│   └── patient_vitals.csv
│
├── Development/
│   ├── bronze.sql
│   ├── silver.sql
│   ├── gold.sql
│   └── business aggreggations.sql
│
├── Test/
│   └── test_data_quality.py
│
├── design/
│   ├── High Level Model.png
│   ├── low level architecture.png
│   └── data_model_trimmed.pdf
│
├── Healthcare_Patient_Analytics_Premium.pptx
└── README.md
```

---

# 🎨 Architecture & Design Documentation

The repository contains architecture and data-model documentation under:

```text
design/
```

## High-Level Architecture

```text
design/High Level Model.png
```

Provides the overall healthcare pipeline architecture.

## Low-Level Architecture

```text
design/low level architecture.png
```

Provides the detailed pipeline and component-level architecture.

## Data Model

```text
design/data_model_trimmed.pdf
```

Contains the project's analytical data model.

---

# 👨‍💻 My Role

### Role: Data Engineer

My responsibilities in the project included:

* Worked on the end-to-end healthcare data engineering pipeline.
* Worked with Azure Data Factory for source-data ingestion.
* Worked with Azure Data Lake Storage Gen2.
* Supported CSV-to-Parquet data processing.
* Created and worked with Bronze external tables in Databricks.
* Developed Silver-layer transformations using dbt.
* Developed Gold-layer analytical models using dbt.
* Implemented healthcare business transformations.
* Worked with patient risk-score calculations.
* Worked with readmission-risk analysis.
* Implemented data-quality validation.
* Worked with duplicate detection and cleansing.
* Worked with audit and error-handling models.
* Developed and validated healthcare analytical marts.
* Performed testing using Python and Pytest.
* Worked with Databricks Unity Catalog.
* Supported dashboard-ready Gold datasets.
* Worked with pipeline monitoring and Slack failure alerts.
* Supported Azure DevOps repository and CI/CD processes.

---

# 📈 Key Outcomes

The project provides:

* End-to-end healthcare data processing.
* Azure-based Lakehouse architecture.
* Bronze, Silver, and Gold data layers.
* Centralized healthcare data processing.
* Clean and standardized healthcare datasets.
* Deduplicated patient and hospital records.
* Patient risk scoring.
* Readmission-risk categorization.
* Hospital performance analytics.
* Healthcare cost analytics.
* Laboratory abnormality analysis.
* Patient vitals trend analysis.
* Lifestyle-health correlation analysis.
* Monthly and quarterly healthcare trends.
* Automated data-quality testing.
* Data-quality audit and error-handling models.
* Dashboard-ready analytical datasets.
* Pipeline monitoring and failure notifications.
* Azure DevOps CI/CD support.

---

# 🔮 Future Enhancements

Potential future improvements include:

* Implement fully automated Airflow DAG deployment.
* Add incremental processing for large healthcare datasets.
* Add advanced patient-risk prediction models.
* Introduce machine-learning-based readmission prediction.
* Implement real-time healthcare monitoring.
* Expand data-quality rules.
* Add centralized pipeline observability.
* Implement automated CI/CD deployment of dbt models.
* Add more advanced healthcare dashboards.
* Introduce historical tracking for changing hospital and patient attributes.

---

# 📌 Conclusion

The **Healthcare Patient Analytics Pipeline** demonstrates an end-to-end modern Data Engineering solution for transforming healthcare data into reliable and analytics-ready datasets.

The project combines:

**Azure Data Factory → ADLS Gen2 → Databricks → Delta Lake → Unity Catalog → dbt → Gold Analytics → Testing → Monitoring**

to create a structured healthcare analytics platform.

The implementation follows **Medallion Architecture**, separating raw ingestion, data cleansing, and business analytics into Bronze, Silver, and Gold layers.

The Gold layer provides a star-schema-based analytical foundation through:

```text
DIM_PATIENT
DIM_HOSPITAL
DIM_DATE
DIM_OBSERVATION
FACT_HEALTH_METRICS
```

and supports eight business-focused analytical marts covering patient risk, hospital performance, vitals, costs, readmission risk, lifestyle-health relationships, laboratory abnormalities, and monthly/quarterly trends.

The project also incorporates **data-quality audits, automated Pytest validation, Slack failure notifications, Unity Catalog governance, and Azure DevOps CI/CD**, demonstrating the key components of a production-oriented healthcare Data Engineering pipeline.

---

## 👥 Project Team

* Harshini Maddi
* Shanmuga Sundaram
* Rakesh Akurathi
* Saritha
* Srinadh

**Built as part of the Revature Readiness Program Capstone Project.**
