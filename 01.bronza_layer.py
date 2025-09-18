# Databricks notebook source
df = spark.read.format("parquet").load("abfss://raw@strpro09.dfs.core.windows.net/orders")
df.display()

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
  .option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation","abfss://bronze@strpro09.dfs.core.windows.net/checkpoint_orders")\
      .load("abfss://raw@strpro09.dfs.core.windows.net/orders")
display(df)

# COMMAND ----------

df.writeStream.format("parquet") \
  .outputMode("append") \
  .option("path", "abfss://bronze@strpro09.dfs.core.windows.net/orders") \
  .option("checkpointLocation", "abfss://bronze@strpro09.dfs.core.windows.net/checkpoint_orders") \
  .start()

# COMMAND ----------

df = spark.read.format("parquet").load("abfss://bronze@strpro09.dfs.core.windows.net/orders")
df.count()