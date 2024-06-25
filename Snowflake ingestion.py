# Databricks notebook source
# Dictionary options method

user = dbutils.secrets.get("SnowflakeCreds","User")
pw = dbutils.secrets.get("SnowflakeCreds","Pass")
url = dbutils.secrets.get("SnowflakeCreds","Url")

options = {
    "sfUrl": url,
    "sfUser": user,
    "sfPassword": pw,
    "sfDatabase": "TIL_DATASCHOOL",
    "sfSchema": "DS29",
    "sfWarehoues": "DATASCHOOL_WH"
}

df = spark.read \
    .format("snowflake") \
    .options(**options) \
    .option("dbtable", "NP_CHRONODATA") \
    .load()

display(df)

# COMMAND ----------

# Standard options method

snowflake_table = (spark.read
    .format("snowflake")
    .option("host",dbutils.secrets.get("SnowflakeCreds","Url"))
    .option("user",dbutils.secrets.get("SnowflakeCreds","User"))
    .option("password",dbutils.secrets.get("SnowflakeCreds","Pass"))
    .option("sfWarehouse","DATASCHOOL_WH")
    .option("database","TIL_DATASCHOOL")
    .option("schema","DS29")
    .option("dbtable","NP_CHRONODATA")
    .load()
)

display(snowflake_table)

if not spark.catalog.tableExists('chrono_data'):
    df.createTempView('chrono_data')
else:
    pass


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Simple language switch and test query vs temp view
# MAGIC
# MAGIC SELECT *
# MAGIC FROM chrono_data
# MAGIC LIMIT 10
