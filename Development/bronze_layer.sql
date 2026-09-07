```sql
-- ============================================================
-- HEALTHCARE PATIENT ANALYTICS PIPELINE
-- BRONZE LAYER
-- ============================================================
-- Platform : Azure Databricks
-- Storage  : ADLS Gen2
-- Format   : Parquet
-- Catalog  : databricks_healthcare_ws_7405614041790072
-- Schema   : bronze
--
-- Purpose:
--   Register raw healthcare Parquet datasets as external
--   Bronze tables using Databricks Unity Catalog.
--
-- Source:
--   Azure Data Factory → ADLS Gen2 → Parquet
-- ============================================================


-- ============================================================
-- 1. BRONZE SCHEMA
-- ============================================================

CREATE SCHEMA IF NOT EXISTS
databricks_healthcare_ws_7405614041790072.bronze
COMMENT 'Raw healthcare data layer containing source Parquet datasets';


-- ============================================================
-- 2. HOSPITAL INFORMATION
-- ============================================================

CREATE TABLE IF NOT EXISTS
databricks_healthcare_ws_7405614041790072.bronze.hospital_info
USING PARQUET
LOCATION
'abfss://parquetcontainer@adlsgen2healthanalytics.dfs.core.windows.net/hospital_info.parquet'
COMMENT 'Bronze external table for hospital information';


-- ============================================================
-- 3. LABORATORY RESULTS
-- ============================================================

CREATE TABLE IF NOT EXISTS
databricks_healthcare_ws_7405614041790072.bronze.lab_results
USING PARQUET
LOCATION
'abfss://parquetcontainer@adlsgen2healthanalytics.dfs.core.windows.net/lab_results.parquet'
COMMENT 'Bronze external table for laboratory test results';


-- ============================================================
-- 4. PATIENT DEMOGRAPHICS
-- ============================================================

CREATE TABLE IF NOT EXISTS
databricks_healthcare_ws_7405614041790072.bronze.patient_demographics
USING PARQUET
LOCATION
'abfss://parquetcontainer@adlsgen2healthanalytics.dfs.core.windows.net/patient_demographics.parquet'
COMMENT 'Bronze external table for patient demographic information';


-- ============================================================
-- 5. PATIENT DIAGNOSIS
-- ============================================================

CREATE TABLE IF NOT EXISTS
databricks_healthcare_ws_7405614041790072.bronze.patient_diagnosis
USING PARQUET
LOCATION
'abfss://parquetcontainer@adlsgen2healthanalytics.dfs.core.windows.net/patient_diagnosis.parquet'
COMMENT 'Bronze external table for patient diagnosis records';


-- ============================================================
-- 6. PATIENT VITALS
-- ============================================================

CREATE TABLE IF NOT EXISTS
databricks_healthcare_ws_7405614041790072.bronze.patient_vitals
USING PARQUET
LOCATION
'abfss://parquetcontainer@adlsgen2healthanalytics.dfs.core.windows.net/patient_vitals.parquet'
COMMENT 'Bronze external table for patient vital measurements';


-- ============================================================
-- 7. BRONZE TABLE INVENTORY
-- ============================================================

SHOW TABLES IN
databricks_healthcare_ws_7405614041790072.bronze;


-- ============================================================
-- 8. RECORD COUNT VALIDATION
-- ============================================================

SELECT
    'hospital_info' AS source_table,
    COUNT(*) AS record_count
FROM databricks_healthcare_ws_7405614041790072.bronze.hospital_info

UNION ALL

SELECT
    'lab_results',
    COUNT(*)
FROM databricks_healthcare_ws_7405614041790072.bronze.lab_results

UNION ALL

SELECT
    'patient_demographics',
    COUNT(*)
FROM databricks_healthcare_ws_7405614041790072.bronze.patient_demographics

UNION ALL

SELECT
    'patient_diagnosis',
    COUNT(*)
FROM databricks_healthcare_ws_7405614041790072.bronze.patient_diagnosis

UNION ALL

SELECT
    'patient_vitals',
    COUNT(*)
FROM databricks_healthcare_ws_7405614041790072.bronze.patient_vitals;


-- ============================================================
-- 9. SCHEMA VALIDATION
-- ============================================================

DESCRIBE TABLE
databricks_healthcare_ws_7405614041790072.bronze.hospital_info;

DESCRIBE TABLE
databricks_healthcare_ws_7405614041790072.bronze.lab_results;

DESCRIBE TABLE
databricks_healthcare_ws_7405614041790072.bronze.patient_demographics;

DESCRIBE TABLE
databricks_healthcare_ws_7405614041790072.bronze.patient_diagnosis;

DESCRIBE TABLE
databricks_healthcare_ws_7405614041790072.bronze.patient_vitals;


-- ============================================================
-- 10. SAMPLE DATA VALIDATION
-- ============================================================

SELECT *
FROM databricks_healthcare_ws_7405614041790072.bronze.hospital_info
LIMIT 5;

SELECT *
FROM databricks_healthcare_ws_7405614041790072.bronze.lab_results
LIMIT 5;

SELECT *
FROM databricks_healthcare_ws_7405614041790072.bronze.patient_demographics
LIMIT 5;

SELECT *
FROM databricks_healthcare_ws_7405614041790072.bronze.patient_diagnosis
LIMIT 5;

SELECT *
FROM databricks_healthcare_ws_7405614041790072.bronze.patient_vitals
LIMIT 5;


-- ============================================================
-- END OF BRONZE LAYER
-- ============================================================
```
