# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Dataframes using SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ### Local Temp View
# MAGIC #### Objectives
# MAGIC * Create temp views on Dataframe
# MAGIC * Access the view from SQL cell
# MAGIC * Access the view from Python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

#race_results_df.createTempView("v_race_results")
race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### SQL Cell

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1)
# MAGIC FROM v_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

# MAGIC %md
# MAGIC #### Python cell
# MAGIC Use when you want to use Dataframe result, or when you want to use Python variables in the query

# COMMAND ----------

p_race_year = 2020

# COMMAND ----------

race_results_2019_df = spark.sql(f"SELECT * FROM v_race_results WHERE race_year = {p_race_year}")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Global Temp View
# MAGIC #### Objectives
# MAGIC * Create global temp views on Dataframe
# MAGIC * Access the view from SQL cell
# MAGIC * Access the view from Python cell
# MAGIC * Access the view from another Notebook

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC The Global Temp view gets created in another DB called `global_temp`

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES in global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC   FROM global_temp.gv_race_results

# COMMAND ----------

spark.sql("SELECT * FROM global_temp.gv_race_results").show()