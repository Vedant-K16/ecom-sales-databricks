# Databricks notebook source
# MAGIC %run ../shared/functions.py

# COMMAND ----------

import unittest
import re
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
    
r = unittest.main(argv=[''], verbosity=2, exit=False)
assert r.result.wasSuccessful(), '::Test Failed::'