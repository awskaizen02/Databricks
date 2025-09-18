# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.read.format("parquet")\
    .load("abfss://bronze@strpro09.dfs.core.windows.net/orders")

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.withColumnRenamed("_rescued_data","rescued_data")

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.drop("rescued_data")
display(df)

# COMMAND ----------

df = df.withColumn("order_date",to_timestamp(col('order_date')))
df.display()

# COMMAND ----------

df = df.withColumn("year",year(col('order_date')))
df.display()

# COMMAND ----------

df = df.withColumn("dense_rank",dense_rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
df.display()


# COMMAND ----------

df1 = df.withColumn("rank",rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
df1.display()


# COMMAND ----------

df1 = df.withColumn("row",row_number().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
df1.display()

# COMMAND ----------

class windows:
    def dense_rank(self,df):
       df_dense_rank = df.withColumn("dense_rank",dense_rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
       return df_dense_rank
        
    

    def rank(self,df):
        df_rank = df.withColumn("rank",rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
        return df_rank
    
    def row(self,df):
        df_row = df.withColumn("row",row_number().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
        
        return df_row

# COMMAND ----------

df_new = df

# COMMAND ----------

df_new.display()

# COMMAND ----------

obj = windows()

# COMMAND ----------

df_result = obj.dense_rank(df_new)
df_result.display()
df_result = obj.rank(df_new)
df_result.display()
df_result = obj.row(df_new)
df_result.display()

# COMMAND ----------

df.write.format("delta").mode("append").save("abfss://silver@strpro09.dfs.core.windows.net/orders")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS elt_demo.silver.order_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@strpro09.dfs.core.windows.net/orders'