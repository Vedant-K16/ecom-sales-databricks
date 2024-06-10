# Databricks notebook source
# This input is for debug purpose for this notebook
# %run ../shared/functions

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import re

# orders_clean table
orders_raw = spark.read.table("orders_raw")
orders_clean = orders_raw.withColumn("Order_Date",date_format(to_date(col("Order_Date"), "d/M/yyyy"), "yyyy-MM-dd"))
create_table(
    df = orders_clean,
    table_format= "delta",
    mode = "overwrite",
    table_name="orders_clean"
)

# products_clean table
products_raw = spark.read.table("products_raw")
products_clean = products_raw.withColumn("Product_Name",regexp_replace(col("Product_Name"),r'^"|"$',''))
create_table(
    df = products_clean,
    table_format= "delta",
    mode = "overwrite",
    table_name="products_clean"
)

# customer_clean table
customers_raw = spark.read.table("customers_raw")
customer_clean = (customers_raw.withColumn("phone",clean_phone_number_udf(col("phone")))
                               .withColumn("Customer_Name",regexp_replace(col("Customer_Name"),r'[^\w\s]|[\d]','')))
create_table(
    df = customer_clean,
    table_format= "delta",
    mode = "overwrite",
    table_name="customer_clean"
)
