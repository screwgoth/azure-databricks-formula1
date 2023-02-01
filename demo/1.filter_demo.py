# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter Demo

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### SQL Way

# COMMAND ----------

races_filtered_df = races_df.filter("race_year = 2019 and round <= 5")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Python Way

# COMMAND ----------

races_filtered_df = races_df.filter((races_df["race_year"] == 2020) & (races_df["round"] <= 5))

# COMMAND ----------

display(races_filtered_df)

# COMMAND ----------

