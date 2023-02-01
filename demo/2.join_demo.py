# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Spark Join Transformation Demo

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
.filter("circuit_id < 70") \
.withColumnRenamed("name", "circuit_name")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019") \
.withColumnRenamed("name", "race_name")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inner Join and Selecting columns
# MAGIC Show all the Dataframes that match the condition (i.e., matching circuit_id).

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Left Outer Join
# MAGIC Show all the Dataframes from the Left Dataframe. Where the condition does not match, show `null` for the Right Dataframe

# COMMAND ----------

# Left Outer Join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Right Outer Join
# MAGIC Show all the Dataframes from the Right Dataframe. Where the condition does not match, show `null` for the Left Dataframe

# COMMAND ----------

# Right Outer Join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Full Outer Join
# MAGIC Show all the entries from both the Dataframes. Where the conditions don't match show `null`

# COMMAND ----------

# Right Outer Join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Semi Join
# MAGIC Similar to Inner Joins, but only displays columns from the Left Dataframe

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Anti Join
# MAGIC Opposite of Semi Join.
# MAGIC In this example we use `races_df` as the Left Dataframe

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, circuits_df.circuit_id == races_df.circuit_id, "anti")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cross Join

# COMMAND ----------

race_circuits_df = races_df.crossJoin(circuits_df)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df.count()

# COMMAND ----------

int(races_df.count() * circuits_df.count())

# COMMAND ----------

