-- Databricks notebook source
USE f1_presentation;

-- COMMAND ----------

DESC driver_standings;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2018
AS
SELECT race_year, driver_name, team, total_points, wins, rank
  FROM driver_standings
  WHERE race_year = 2018;

-- COMMAND ----------

SELECT * FROM v_driver_standings_2018;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2020
AS
SELECT race_year, driver_name, team, total_points, wins, rank
  FROM driver_standings
  WHERE race_year = 2020;

-- COMMAND ----------

SELECT * FROM v_driver_standings_2020;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Outer Join

-- COMMAND ----------

SELECT *
  FROM v_driver_standings_2018 d_2018
  JOIN v_driver_standings_2020 d_2020
    ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Left Join

-- COMMAND ----------

SELECT *
  FROM v_driver_standings_2018 d_2018
  LEFT JOIN v_driver_standings_2020 d_2020
    ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### RIGHT OUTER JOIN

-- COMMAND ----------

SELECT *
  FROM v_driver_standings_2018 d_2018
  RIGHT JOIN v_driver_standings_2020 d_2020
    ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### FULL OUTER JOIN

-- COMMAND ----------

SELECT *
  FROM v_driver_standings_2018 d_2018
  FULL JOIN v_driver_standings_2020 d_2020
    ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SEMI JOIN (Inner Join with only data from left table)

-- COMMAND ----------

SELECT *
  FROM v_driver_standings_2018 d_2018
  SEMI JOIN v_driver_standings_2020 d_2020
    ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### ANTI JOIN (Opposite of SEMI JOIN)

-- COMMAND ----------

SELECT *
  FROM v_driver_standings_2018 d_2018
  ANTI JOIN v_driver_standings_2020 d_2020
    ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CROSS JOIN

-- COMMAND ----------

SELECT *
  FROM v_driver_standings_2018 d_2018
  CROSS JOIN v_driver_standings_2020 d_2020