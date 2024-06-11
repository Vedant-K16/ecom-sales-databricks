# Databricks notebook source
# This cell if for debug purposes
# %run ../shared/functions

# COMMAND ----------
# MAGIC %run ./agg_and_enrich_functions

# COMMAND ----------


from pyspark.sql.functions import *

customers_clean = spark.read.table("customer_clean")
products_clean = spark.read.table("products_clean").selectExpr("product_id","category","`sub-category`").distinct()
orders_clean = spark.read.table("orders_clean")

# order table enriched with customer and product info and called as sales

sales_df = sales_enrichment(orders_clean,customers_clean,products_clean)
create_table(
    df=sales_df,
    table_format="delta",
    mode="overwrite",
    table_name="sales",
    overWriteSchema = "true"
)
