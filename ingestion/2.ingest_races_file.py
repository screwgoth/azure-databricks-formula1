# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Races from CSV file

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

from pyspark.sql.types import IntegerType, StringType, StructType, StructField
races_schema = StructType(fields=[
                             StructField("raceId", IntegerType(), False),
                             StructField("year", IntegerType(), True),
                             StructField("round", IntegerType(), True),
                             StructField("circuitId", IntegerType(), False),
                             StructField("name", StringType(), True),
                             StructField("date", StringType(), True),
                             StructField("time", StringType(), True),
                             StructField("url", StringType(), True),
                             ])

# COMMAND ----------

races_df = spark.read \
    .option("header",True) \
    .schema(races_schema) \
    .csv(f"{raw_folder_path}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Select the Required Columns

# COMMAND ----------

from pyspark.sql.functions import col

races_selected_df = races_df.select(col("raceId"),col("year"),col("round"),col("circuitId"),col("name"),col("date"),col("time"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename the Columns

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("year", "race_year") \
    .withColumnRenamed("circuitId", "circuit_id")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Combine the 2 date and time columns to a single timestamp column

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat, lit
races_combined_df = races_renamed_df.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), "yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5a - Add new Ingestion date column

# COMMAND ----------

races_final_df = add_ingestion_data(races_combined_df)

# COMMAND ----------

# MAGIC %md #### Step 5b - Adding Data Source Column

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

races_final_df = races_combined_df.withColumn("source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6 - Write final data to Data Lake as a Parquet file and Managed Tabled

# COMMAND ----------

#races_final_df.write.parquet(f"{processed_folder_path}/races", mode="overwrite")
races_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/races"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races;

# COMMAND ----------

dbutils.notebook.exit("Success")