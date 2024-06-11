# Databricks notebook source
from pyspark.sql.functions import *

def profit_aggregation(orders_clean,customers_clean,products_clean):
    # profit aggregated table
    profit_df = (orders_clean.alias("order")
                    .join(customers_clean.alias("customer"),expr("order.Customer_id=customer.customer_id"))
                    .join(products_clean.alias("product"),expr("order.Product_ID=product.Product_ID"))
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
    return profit_df

def sales_enrichment(orders_clean,customers_clean,products_clean):
    sales_df = (orders_clean.withColumn("profit",bround(col("profit"),2)).alias("order")
                    .join(customers_clean.alias("customer"),expr("order.Customer_id=customer.Customer_id"))
                    .join(products_clean.alias("product"),expr("order.Product_ID=product.product_id"))
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
    return sales_df
