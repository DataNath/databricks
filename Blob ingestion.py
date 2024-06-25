# Databricks notebook source
# Set variables

blob_account_name = "nathanblobstorage"
blob_container_name = "inputfiles"
blob_relative_path = "epd_2023.parquet"
blob_sas_token = dbutils.secrets.get("BlobCreds","sas_token")

# COMMAND ----------

# Create path and set spark config

wasbs_path = f"wasbs://{blob_container_name}@{blob_account_name}.blob.core.windows.net/{blob_relative_path}"
spark.conf.set(f"fs.azure.sas."+blob_container_name+"."+blob_account_name+".blob.core.windows.net", blob_sas_token)
print('Remote blob path: ' + wasbs_path)

# COMMAND ----------

# Read in parquet file from blob storage and create temp view

df = spark.read.parquet(wasbs_path)
df.createOrReplaceTempView('test_view')


# COMMAND ----------

# Switch language and run simple test query vs temp view

%sql
WITH CTE AS (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY YEAR_MONTH ORDER BY 1=1) AS LINE,
    YEAR_MONTH AS DATE,
    ROUND(ACTUAL_COST :: DOUBLE,2) AS COST
  FROM test_view)

SELECT
  DATE,
  COST
FROM CTE
WHERE LINE <= 3
