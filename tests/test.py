# Databricks notebook source
# MAGIC %run ../shared/functions
# COMMAND ----------
# MAGIC %run ../aggregations/agg_and_enrich_functions
# COMMAND ----------

import unittest
import re

class DataFrameTest():
    """
    This is to generate dummy data and compare the dataframe schema and data
    """
    def __init__(self,actual_df,expected_df):
        self.actual_df = actual_df
        self.expected_df = expected_df
    def compare_schema(self):
        if self.actual_df.schema == self.expected_df.schema:
            return True
        else:
            return False
    def compare_data(self):
        if self.actual_df.collect()==self.expected_df.collect():
            return True
        else:
            return False
    @staticmethod
    def generate_dummy_data():
        order_data = [("cid1","2016-07-07","oid1","pid3",27.435345,0.3,"192.22",5,"2016/07/09"),
                ("cid2","2019-05-03","oid2","pid2",-27.23454,0.3,"1922.22",5,"2019/07/09"),
                ("cid2","2020-03-03","oid3","pid1",127.543,0.3,"19222.22",5,"2020/07/09"),]
        order_schema = "customer_id string,order_date string,order_id string,product_id string,profit double,discount double,price string,quantity long,ship_date string"
        test_order_df = spark.createDataFrame(order_data,order_schema)

        product_data = [("pid1","test_category","test_sub_category"),
                        ("pid2","test_category2","test_sub_categor2")]
        product_schema = "product_id string, category string,`Sub-Category` string"
        test_product_df = spark.createDataFrame(product_data,product_schema)

        customer_data =[("cid1","test customer","India"),
                        ("cid2","test customer2","USA")]
        customer_schema = "customer_id string, customer_name string,country string"
        test_customer_df = spark.createDataFrame(customer_data,customer_schema)

        return {"test_order_df":test_order_df,"test_product_df":test_product_df,"test_customer_df":test_customer_df}

class TestHelpers(unittest.TestCase):
    """
    This is a helper class to test various functions.
    This will help to ensure a smooth transition for TDD and develope a good code
    """
    def test_clean_column_name(self):
        """
        This function is to test clean_column_name.
        You will have to pass in a dummy dataframe to set your expectation and to consider test scenario
        args: 
            1. dataframe.schema : actual
            2. dataframe.schema : expected
        """
        actual_schema = ["col;int","col string"]
        expected_schema = ["col_int","col_string"]
        actual_df = spark.createDataFrame([(1,"test")],actual_schema)
        expected_df = spark.createDataFrame([(1,"test")],expected_schema)
        result = clean_column_name(actual_df).schema
        self.assertEqual(result,expected_df.schema)
    def test_clean_phone_number(self):
        """
        This function will test and ensure if correct phone numbers are used in the table
        args : 
            1. clean_phone_number(str) : actual
            2. str : expected
        """
        self.assertEqual(clean_phone_number('#ERROR!'),None)
        self.assertEqual(clean_phone_number('+1fhldksfll'),None)
        self.assertEqual(clean_phone_number('98765432'),None)
        self.assertEqual(clean_phone_number('421.580.0902x9815'),'421-580-0902x9815')
    def test_create_table(self):
        """
        This function test the various table creation scenarios with different modes
        args :
            -- actual --
            1. df
            2. mode
            3. table_name
            4. table_format
            5. Optional[overWriteSchema]
            -- expected
            1. boolean value 
                a. True for successful creation
                b. False for failure checks
        """
        schema = ["col_int int,col_string string"]
        df = spark.createDataFrame([(1,"test",)],schema[0])
        mode = ["overwrite","append","update"]
        table_name = ["my_test_table1","mytest_table2","my_test_table3"]
        table_format = "delta"
        overWriteSchema = "true"
        self.assertEqual(create_table(df = df,
                                      mode = mode[0],
                                      table_name = table_name[0],
                                      table_format = table_format,
                                      overWriteSchema=overWriteSchema),True)
        spark.sql(f"drop table if exists {table_name[0]}")
        self.assertEqual(create_table(df = df,
                                      mode = mode[1],
                                      table_name = table_name[1],
                                      table_format = table_format,
                                      overWriteSchema = overWriteSchema),True)
        spark.sql(f"drop table if exists {table_name[1]}")
        self.assertEqual(create_table(df = df,
                                      mode = mode[2],
                                      table_name = table_name[2],
                                      table_format = table_format,
                                      overWriteSchema=overWriteSchema),False)
        spark.sql(f"drop table if exists {table_name[2]}")
    def test_profit_aggregation(self):
        """
        This method is to check if the profit aggregations are coming up as expected or not
        """
        dummy_data = DataFrameTest.generate_dummy_data()
        actual_df = profit_aggregation(dummy_data.get("test_order_df"),dummy_data.get("test_customer_df"),dummy_data.get("test_product_df"))

        expected_data = [("test customer2","test_category","test_sub_category",2020,127.54),
                        ("test customer2","test_category2","test_sub_categor2",2019,-27.23)]
        expected_schema = "customer_name string,category string,`Sub-Category`string,order_year int,profit double"
        expected_df = spark.createDataFrame(expected_data,expected_schema)

        is_schema_same = DataFrameTest(actual_df,expected_df).compare_schema()
        is_data_same = DataFrameTest(actual_df,expected_df).compare_data()
        self.assertEqual(is_schema_same,True)
        self.assertEqual(is_data_same,True)

    def test_sales_enrichment(self):
        """
        This method is to verify the sales enrichments
        """
        dummy_data = DataFrameTest.generate_dummy_data()
        actual_df = sales_enrichment(dummy_data.get("test_order_df"),dummy_data.get("test_customer_df"),dummy_data.get("test_product_df"))

        expected_data = [("oid3","2020-03-03",0.3,"19222.22",127.54,5,"2020/07/09","test customer2","USA","test_category","test_sub_category"),
                 ("oid2","2019-05-03",0.3,"1922.22",-27.23,5,"2019/07/09","test customer2","USA","test_category2","test_sub_categor2")]
        expected_schema = "order_id string,order_date string,discount double,price string,profit double,quantity long,ship_date string,customer_name string,country string,product_category string,product_sub_category string"
        expected_df = spark.createDataFrame(expected_data,expected_schema)

        is_schema_same = DataFrameTest(actual_df,expected_df).compare_schema()
        is_data_same = DataFrameTest(actual_df,expected_df).compare_data()
        self.assertEqual(is_schema_same,True)
        self.assertEqual(is_data_same,True)
        
r = unittest.main(argv=[''], verbosity=2, exit=False)
assert r.result.wasSuccessful(), '::Test Failed::'
