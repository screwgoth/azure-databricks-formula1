# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Qualifying from multiple JSON file

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
# MAGIC #### Step 1 - Read the data from multiple json file using regex or folder name

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructType, StructField, DoubleType
qualifying_schema = StructType(fields=[
                             StructField("qualifyId", IntegerType(), False),
                             StructField("raceId", IntegerType(), True),
                             StructField("driverId", IntegerType(), True),
                             StructField("constructorId", IntegerType(), True),    
                             StructField("number", IntegerType(), True),
                             StructField("position", IntegerType(), True),
                             StructField("q1", StringType(), True),
                             StructField("q2", StringType(), True),
                             StructField("q3", StringType(), True),
                             ])

# COMMAND ----------

qualifying_df = spark.read \
    .schema(qualifying_schema) \
    .option("multiLine", True) \
    .json(f"{raw_folder_path}/qualifying/qualifying_split_*.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

qualifying_renamed_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4a - Add new Ingestion date column

# COMMAND ----------

qualifying_final_df = add_ingestion_data(qualifying_renamed_df)

# COMMAND ----------

# MAGIC %md #### Step 4b - Adding Data Source Column

# COMMAND ----------

from pyspark.sql.functions import lit

qualifying_final_df = qualifying_final_df.withColumn("source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write final data to Data Lake as a Parquet file and Managed Table

# COMMAND ----------

#qualifying_final_df.write.parquet(f"{processed_folder_path}/qualifying", mode="overwrite")
qualifying_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/qualifying"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.qualifying;

# COMMAND ----------

dbutils.notebook.exit("Success")