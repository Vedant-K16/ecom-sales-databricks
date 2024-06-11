# Databricks notebook source
# This cell if for debug purposes
# %run ../shared/functions

# COMMAND ----------
# MAGIC %run ./agg_and_enrich_functions

# COMMAND ----------
from pyspark.sql.functions import *

orders_clean = spark.read.table("orders_clean")
customers_clean = spark.read.table("customer_clean")
products_clean = spark.read.table("products_clean")

profit_df = profit_aggregation(orders_clean,customers_clean,products_clean)

create_table(
    df=profit_df,
    table_format="delta",
    table_name="profit",
    mode="overwrite"
)
