-- Databricks notebook source
SELECT *
  FROM f1_presentation.calculated_race_results;

-- COMMAND ----------

-- MAGIC %md #### All Time

-- COMMAND ----------

SELECT team_name,
    COUNT(1) AS total_races,
    SUM(calulated_points) AS total_points,
    AVG(calulated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
  GROUP BY team_name
  HAVING COUNT(1) >= 100
  ORDER BY avg_points DESC

-- COMMAND ----------

-- MAGIC %md #### Last Decade (2011 to 2020)

-- COMMAND ----------

SELECT team_name,
    COUNT(1) AS total_races,
    SUM(calulated_points) AS total_points,
    AVG(calulated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
  WHERE race_year BETWEEN 2011 AND 2020 
  GROUP BY team_name
  HAVING COUNT(1) >= 100
  ORDER BY avg_points DESC

-- COMMAND ----------

-- MAGIC %md #### Previous Decade (2001 to 2010)

-- COMMAND ----------

SELECT team_name,
    COUNT(1) AS total_races,
    SUM(calulated_points) AS total_points,
    AVG(calulated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
  WHERE race_year BETWEEN 2001 AND 2010
  GROUP BY team_name
  HAVING COUNT(1) >= 100
  ORDER BY avg_points DESC

-- COMMAND ----------

