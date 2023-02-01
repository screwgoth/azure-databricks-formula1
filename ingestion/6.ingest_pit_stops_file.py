# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Pit Stops from multi-line JSON file

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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

pit_stops_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), False),
    StructField("stop", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pit_stops_df = spark.read \
    .option("multiLine", True) \
    .schema(pit_stops_schema) \
    .json(f"{raw_folder_path}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename the Columns and insert Ingestion Date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pit_stops_renamed_df = pit_stops_df.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3a - Add new Ingestion date column

# COMMAND ----------

pit_stops_final_df = add_ingestion_data(pit_stops_renamed_df)

# COMMAND ----------

# MAGIC %md #### Step 3b - Adding Data Source Column

# COMMAND ----------

from pyspark.sql.functions import lit

pit_stops_final_df = pit_stops_final_df.withColumn("source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write final data to Data Lake as a Parquet file and Managed Table

# COMMAND ----------

#pit_stops_final_df.write.parquet(f"{processed_folder_path}/pit_stops", mode="overwrite")
pit_stops_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/pit_stops"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.pit_stops;

# COMMAND ----------

dbutils.notebook.exit("Success")