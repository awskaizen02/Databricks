# Databricks notebook source
datasets = [
    {
        "file_name": "orders"
    },
    {
        "file_name": "customers"
    },
    {
        "file_name": "product"
    },
    {
        "file_name": "region"
    },
    {
        "file_name": "titanic"
    },
    {
        "file_name": "Iris"
    }
]

# COMMAND ----------

dbutils.jobs.taskValues.set("output_datasets", datasets)