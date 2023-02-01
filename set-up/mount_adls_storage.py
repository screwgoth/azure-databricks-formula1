# Databricks notebook source
# MAGIC %md
# MAGIC # Mounting Azure Data Lake Storage

# COMMAND ----------

storage_account_name = "raseelformula1dl"
client_id = dbutils.secrets.get(scope="formula-dl-scope", key="client-id")
tenant_id = dbutils.secrets.get(scope="formula-dl-scope", key="tenant-id")
client_secret = dbutils.secrets.get(scope="formula-dl-scope", key="client-secret")

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": f"{client_id}",
    "fs.azure.account.oauth2.client.secret": f"{client_secret}",
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# COMMAND ----------

def mount_adls(container_name):
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)

# COMMAND ----------

dbutils.fs.unmount("/mnt/raseelformula1dl/raw")
dbutils.fs.unmount("/mnt/raseelformula1dl/processed")

# COMMAND ----------

dbutils.fs.unmount("/mnt/raseelformula1dl/presentation")

# COMMAND ----------

mount_adls("raw")
mount_adls("processed")

# COMMAND ----------

mount_adls("presentation")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.ls("/mnt/raseelformula1dl/presentation")

# COMMAND ----------

