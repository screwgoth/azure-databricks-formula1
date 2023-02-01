# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Global Temp View

# COMMAND ----------

# MAGIC %md
# MAGIC #### SQL Cell

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES in global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC   FROM global_temp.gv_race_results

# COMMAND ----------

spark.sql("SELECT * FROM global_temp.gv_race_results").show()