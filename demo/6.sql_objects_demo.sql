-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Learning Objectives
-- MAGIC * Spark SQL Documentation
-- MAGIC * Create Database Demo
-- MAGIC * Data tab in UI
-- MAGIC * SHOW command
-- MAGIC * DESCRIBE command
-- MAGIC * Find the current database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW databases;

-- COMMAND ----------

DESCRIBE database demo;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

SHOW TABLES in demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Learning Objectives
-- MAGIC * Create Managed Tables in Python
-- MAGIC * Create Managed Tables using SQL
-- MAGIC * Effects of dropping a Managed table
-- MAGIC * Describe Table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create using Python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------

DESC EXTENDED race_results_python;

-- COMMAND ----------

SELECT *
  FROM demo.race_results_python
  WHERE race_year = 2020;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create using SQL

-- COMMAND ----------

CREATE TABLE race_results_sql
AS
SELECT * 
  FROM demo.race_results_python
  WHERE race_year = 2020;

-- COMMAND ----------

DESC EXTENDED race_results_sql;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Drop the Table and check that is has been wiped out from Hive Metastore and DBFS

-- COMMAND ----------

DROP TABLE demo.race_results_sql;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Learning Objectives
-- MAGIC * Create external table using Python
-- MAGIC * Create external table using SQL
-- MAGIC * Effect of dropping an external tble

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_py

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(race_year INT,
race_name STRING,
race_date TIMESTAMP,
circuit_location STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT,
race_time STRING,
points FLOAT,
position INT,
created_date TIMESTAMP
)
USING parquet
LOCATION "/mnt/raseelformula1dl/presentation/race_results_ext_sql"

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py WHERE race_year = 2020;

-- COMMAND ----------

SELECT COUNT(1) FROM demo.race_results_ext_sql;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Views on Table
-- MAGIC 
-- MAGIC #### Learning Objective
-- MAGIC * Create Temp View
-- MAGIC * Create Global Temp View
-- MAGIC * Create Permanent View

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Temp Views using SQL

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS
SELECT * 
  FROM race_results_python
  WHERE race_year = 2020;

-- COMMAND ----------

SELECT * FROM v_race_results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Global Temp Views using SQL

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS
SELECT * 
  FROM race_results_python
  WHERE race_year = 2012;

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Permanent View

-- COMMAND ----------

CREATE OR REPLACE VIEW pv_race_results
AS
SELECT * 
  FROM race_results_python
  WHERE race_year = 2000;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT * FROM demo.pv_race_results;

-- COMMAND ----------

