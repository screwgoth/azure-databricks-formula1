-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Calculate Points based on position : If you are 1st, 10 points, If you are 10th 1 point no points after that
-- MAGIC * This is because in earlier years points were given differently, i.e., 1st position was awarded only 10 points
-- MAGIC * We also filter out anyone who has reached after 10th position

-- COMMAND ----------

CREATE TABLE f1_presentation.calculated_race_results
USING parquet
AS
SELECT races.race_year, 
    constructors.name AS team_name,
    drivers.name AS driver_name,
    results.position,
    results.points,
    11 - results.position as calulated_points
  FROM results
  JOIN drivers ON (results.driver_id = drivers.driver_id)
  JOIN constructors ON (results.constructor_id = constructors.constructor_id)
  JOIN races ON (results.race_id = races.race_id)
WHERE results.position <= 10

-- COMMAND ----------

SELECT * FROM f1_presentation.calculated_race_results;

-- COMMAND ----------

