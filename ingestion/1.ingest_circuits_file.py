# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Circuits from CSV file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Passing Parameters to Notebooks

# COMMAND ----------

dbutils.widgets.text("param_data_source", "")
v_data_source = dbutils.widgets.get("param_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Including external Notebooks

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the data csv file using Spark Dataframe Reader

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructType, StructField, DoubleType
circuits_schema = StructType(fields=[
                             StructField("circuitId", IntegerType(), False),
                             StructField("circuitRef", StringType(), True),
                             StructField("name", StringType(), True),
                             StructField("location", StringType(), True),
                             StructField("country", StringType(), True),
                             StructField("lat", DoubleType(), True),
                             StructField("long", DoubleType(), True),
                             StructField("alt", IntegerType(), True),
                             StructField("url", StringType(), True),
                             ])

# COMMAND ----------

circuits_df = spark.read \
    .option("header",True) \
    .schema(circuits_schema) \
    .csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Select the Required Columns

# COMMAND ----------

from pyspark.sql.functions import col

circuits_selected_df = circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("long"),col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename the Columns

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("long", "longitude") \
    .withColumnRenamed("alt", "altitude")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4a - Add new Ingestion date column

# COMMAND ----------

circuits_final_df = add_ingestion_data(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4b - Adding Data source column

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn("source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write final data to Data Lake as a Parquet file AND Managed Table

# COMMAND ----------

#circuits_final_df.write.parquet(f"{processed_folder_path}/circuits", mode="overwrite")
circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/circuits"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits;

# COMMAND ----------

dbutils.notebook.exit("Success")