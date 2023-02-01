# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Constructors from JSON file

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

# MAGIC %md
# MAGIC #### _Note_ - Schema is constructed using DDL-formatted string just for understanding. Usually, it should be done the way it is done in Circuits and Races ingest files

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read \
    .schema(constructors_schema) \
    .json(f"{raw_folder_path}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Drop the unrequired Column

# COMMAND ----------

from pyspark.sql.functions import col

constructors_dropped_df = constructors_df.drop(constructors_df["url"])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename the Columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
constructors_renamed_df = constructors_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("constructorRef", "constructor_ref")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4a - Add new Ingestion date column

# COMMAND ----------

constructors_final_df = add_ingestion_data(constructors_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4b - Adding Data source column

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

constructors_final_df = constructors_final_df.withColumn("source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write final data to Data Lake as a Parquet file and Managed table

# COMMAND ----------

#constructors_final_df.write.parquet(f"{processed_folder_path}/constructors", mode="overwrite")
constructors_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/constructors"))


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors;

# COMMAND ----------

dbutils.notebook.exit("Success")