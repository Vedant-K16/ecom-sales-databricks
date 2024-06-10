# Databricks notebook source
# This input is for debug purpose for this notebook
# %run ../shared/functions

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import re

# creating order_raw table
orders_raw = read_file(
    file_format = 'json',
    read_options = {"multiline":"true"},
    file_path = 'dbfs:/FileStore/tables/Order.json'
)
# column name cleanup
orders_raw_with_clean_cols = clean_column_name(orders_raw)
create_table(
    df = orders_raw_with_clean_cols,
    table_format = "delta",
    mode = "overwrite",
    table_name = "orders_raw"
)

# creating products_raw table
products_raw = read_file(
    file_format = 'csv',
    read_options = {"inferSchema":"true","header":"true"},
    file_path = 'dbfs:/FileStore/tables/Product.csv'
)
# column name cleanup
products_raw_with_clean_cols = clean_column_name(products_raw)
create_table(
    df = products_raw_with_clean_cols,
    table_format = "delta",
    mode = "overwrite",
    table_name = "products_raw"
)

# creating customers_raw table
customers_raw = read_file(
    file_format = 'com.crealytics.spark.excel',
    read_options = {"inferSchema":"true","header":"true"},
    file_path = 'dbfs:/FileStore/tables/Customer.xlsx'
)
# column name cleanup
customers_raw_with_clean_cols = clean_column_name(customers_raw)
create_table(
    df = customers_raw_with_clean_cols,
    table_format = "delta",
    mode = "overwrite",
    table_name = "customers_raw"
)
