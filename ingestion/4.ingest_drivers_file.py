# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Drivers from JSON file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Passing Parameters to the Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the data csv file using Spark Dataframe Reader

# COMMAND ----------

dbutils.widgets.text("param_data_source", "")
v_data_source = dbutils.widgets.get("param_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Include External Notebooks

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

name_schema = StructType(fields=[
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC #### _Note_ : Nested schemas

# COMMAND ----------

drivers_schema = StructType(fields=[
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read \
    .schema(drivers_schema) \
    .json(f"{raw_folder_path}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename the Columns, insert Ingestion Date and new concated name column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, lit, col

# COMMAND ----------

drivers_renamed_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("driverRef", "driver_ref") \
    .withColumn("name", concat(col("name.forename"), lit(' '), col("name.surname")))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Drop the unrequired Column

# COMMAND ----------

drivers_dropped_df = drivers_renamed_df.drop(drivers_renamed_df["url"])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4a - Add new ingestion column

# COMMAND ----------

drivers_final_df = add_ingestion_data(drivers_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4b - Add data source column

# COMMAND ----------

from pyspark.sql.functions import lit

drivers_final_df = drivers_final_df.withColumn("source", lit(v_data_source))

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write final data to Data Lake as a Parquet file and Managed Table

# COMMAND ----------

#drivers_final_df.write.parquet(f"{processed_folder_path}/drivers", mode="overwrite")
drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/drivers"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit("Success")