# Databricks notebook source
# This cell if for debug purposes
# %run ../shared/functions.py

# COMMAND ----------

from pyspark.sql.functions import *

orders_clean = spark.read.table("orders_clean")
customers_clean = spark.read.table("customer_clean")
products_clean = spark.read.table("products_clean")

# profit enriched table
profit = (orders_clean.alias("order")
                   .join(customers_clean.alias("customer"),["Customer_id"])
                   .join(products_clean.alias("product"),["Product_ID"])
                   .groupBy(
                               "customer.customer_name",
                               "product.category",
                               "product.`Sub-Category`",
                               year(col("order.order_date")).alias("order_year")
                               )
                   .agg(
                       round(sum("order.profit"),2).alias("profit")
                   )
                   )

create_table(
    df=profit,
    table_format="delta",
    table_name="profit",
    mode="overwrite"
)
