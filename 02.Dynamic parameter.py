# Databricks notebook source
# MAGIC %md
# MAGIC https://docs.databricks.com/aws/en/notebooks/widgets

# COMMAND ----------

dbutils.widgets.text("file_name", "")

# COMMAND ----------

para_filename = dbutils.widgets.get("file_name")

# COMMAND ----------

para_filename

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Read**

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
  .option("cloudFiles.format", "parquet")\
  .option("cloudFiles.schemaLocation", f"abfss://bronze@strpro09.dfs.core.windows.net/checkpoint_{para_filename}")\
    .load(f"abfss://raw@strpro09.dfs.core.windows.net/{para_filename}")

# COMMAND ----------

# MAGIC %md
# MAGIC *Data write*

# COMMAND ----------

df.writeStream.format("parquet")\
    .outputMode("append")\
    .option("checkpointLocation", f"abfss://bronze@strpro09.dfs.core.windows.net/checkpoint_{para_filename}")\
    .option("path",f"abfss://bronze@strpro09.dfs.core.windows.net/{para_filename}")\
    .trigger(once=True)\
    .start()
  

# COMMAND ----------

df = spark.read.format("parquet").load(f"abfss://bronze@strpro09.dfs.core.windows.net/{para_filename}")
display(df)