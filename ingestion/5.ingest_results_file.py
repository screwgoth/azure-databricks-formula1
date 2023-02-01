# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Results from JSON file

# COMMAND ----------

# MAGIC %md #### Passing Parameters to notebooks

# COMMAND ----------

dbutils.widgets.text("param_data_source", "")
v_data_source = dbutils.widgets.get("param_data_source")

# COMMAND ----------

# MAGIC %md #### Include External Notebooks

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the data csv file using Spark Dataframe Reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), False),
    StructField("constructorId", IntegerType(), False),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("points", IntegerType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", FloatType(), True),
    StructField("statusId", IntegerType(), True)
])

# COMMAND ----------

resutls_df = spark.read \
    .schema(results_schema) \
    .json(f"{raw_folder_path}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename the Columns and insert Ingestion Date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

results_renamed_df = resutls_df.withColumnRenamed("resultId", "result_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("positionText", "position_text") \
    .withColumnRenamed("positionOrder", "position_order") \
    .withColumnRenamed("fastestLap", "fastest_lap") \
    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Drop the unrequired Column

# COMMAND ----------

results_dropped_df = results_renamed_df.drop(results_renamed_df["statusId"])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4a - Add new Ingestion date column

# COMMAND ----------

results_final_df = add_ingestion_data(results_dropped_df)

# COMMAND ----------

# MAGIC %md #### Step 4b - Adding Data Source Column

# COMMAND ----------

from pyspark.sql.functions import lit

results_final_df = results_final_df.withColumn("source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write final data to Data Lake as a Parquet file and Managed Table

# COMMAND ----------

#results_final_df.write.parquet(f"{processed_folder_path}/results", mode="overwrite")
results_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/results"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.results;

# COMMAND ----------

dbutils.notebook.exit("Success")