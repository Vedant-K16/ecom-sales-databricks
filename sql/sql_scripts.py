# Databricks notebook source
# 1. Profit by Year
spark.sql("select order_year year,round(sum(profit),2)profit_by_year from profit group by all").display()

# COMMAND ----------

# 2.Profit by Year + Product Category
spark.sql("select category,order_year year,round(sum(profit),2)profit_by_category_per_year from profit group by all").display()

# COMMAND ----------

# 3.Profit by Customer
spark.sql("select customer_name,round(sum(profit),2)profit_per_customer from profit group by all").display()

# COMMAND ----------

# 4.Profit by Customer + Year
spark.sql("select customer_name,order_year year,round(sum(profit),2)profit_by_customer_per_year from profit group by all").display()