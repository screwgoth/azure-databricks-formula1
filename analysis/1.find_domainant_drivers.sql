-- Databricks notebook source
SELECT *
  FROM f1_presentation.calculated_race_results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### All time

-- COMMAND ----------

SELECT driver_name,
    COUNT(1) AS total_races,
    SUM(calulated_points) AS total_points,
    AVG(calulated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
  GROUP BY driver_name
  HAVING COUNT(1) >= 50
  ORDER BY avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Last decade - 2011 t0 2020

-- COMMAND ----------

SELECT driver_name,
    COUNT(1) AS total_races,
    SUM(calulated_points) AS total_points,
    AVG(calulated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
  WHERE race_year BETWEEN 2011 and 2020
  GROUP BY driver_name
  HAVING COUNT(1) >= 50
  ORDER BY avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Previous Decade

-- COMMAND ----------

SELECT driver_name,
    COUNT(1) AS total_races,
    SUM(calulated_points) AS total_points,
    AVG(calulated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
  WHERE race_year BETWEEN 2001 and 2010
  GROUP BY driver_name
  HAVING COUNT(1) >= 50
  ORDER BY avg_points DESC