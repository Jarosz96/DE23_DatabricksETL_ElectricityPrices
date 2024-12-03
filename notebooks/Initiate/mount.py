# Databricks notebook source
# Define variables
container = ""              # INSERT NAME OF CANTAINER HERE
storage_account_name = ""   # INSERT_YOUR_ACCOUNT NAME HERE
storage_account_key = ""    # INSER_YOUR_KEY_HERE
mount_name = "data_lake"

# COMMAND ----------

# Mount Azure Data Lake Storage
try:
    dbutils.fs.mount(
        source=f"wasbs://{container}@{storage_account_name}.blob.core.windows.net",
        mount_point=f"/mnt/{mount_name}",
        extra_configs={f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_key}
    )
except Exception as e:
    print("Data Lake is already mounted or encountered an error:", e)