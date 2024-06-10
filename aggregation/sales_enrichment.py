# Databricks notebook source
# This cell if for debug purposes
# %run ../shared/functions.py

# COMMAND ----------

from pyspark.sql.functions import *

customers_clean = spark.read.table("customer_clean")
products_clean = spark.read.table("products_clean").selectExpr("product_id","category","`sub-category`").distinct()
orders_clean = spark.read.table("orders_clean")

# order table enriched with customer and product info and called as sales

sales = (orders_clean.withColumn("profit",bround(col("profit"),2)).alias("order")
                   .join(customers_clean.alias("customer"),["Customer_id"])
                   .join(products_clean.alias("product"),["Product_ID"])
                   .selectExpr(
                               "order.order_id",
                               "order.order_date",
                               "order.discount",
                               "order.price",
                               "order.profit",
                               "order.quantity",
                               "order.ship_date",
                               "customer.customer_name",
                               "customer.country",
                               "product.category product_category",
                               "product.`Sub-Category` product_sub_category",
                               )
                   )
create_table(
    df=sales,
    table_format="delta",
    mode="overwrite",
    table_name="sales",
    overWriteSchema = "true"
)