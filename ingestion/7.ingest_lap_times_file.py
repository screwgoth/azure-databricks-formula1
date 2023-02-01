# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Lap Times from multiple CSV file

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
# MAGIC #### Step 1 - Read the data from multiple csv file using regex or folder name

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructType, StructField, DoubleType
lap_times_schema = StructType(fields=[
                             StructField("raceId", IntegerType(), False),
                             StructField("driverId", IntegerType(), True),
                             StructField("lap", IntegerType(), True),
                             StructField("position", IntegerType(), True),
                             StructField("time", StringType(), True),
                             StructField("milliseconds", IntegerType(), True)
                             ])

# COMMAND ----------

lap_times_df = spark.read \
    .schema(lap_times_schema) \
    .csv(f"{raw_folder_path}/lap_times/lap_times_split_*.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

lap_times_renamed_df = lap_times_df.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4a - Add new Ingestion date column

# COMMAND ----------

lap_times_final_df = add_ingestion_data(lap_times_renamed_df)

# COMMAND ----------

# MAGIC %md #### Step 4b - Adding Data Source Column

# COMMAND ----------

from pyspark.sql.functions import lit

lap_times_final_df = lap_times_final_df.withColumn("source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write final data to Data Lake as a Parquet file and Managed Table

# COMMAND ----------

#lap_times_final_df.write.parquet(f"{processed_folder_path}/lap_times", mode="overwrite")
lap_times_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/lap_times"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.lap_times;

# COMMAND ----------

dbutils.notebook.exit("Success")