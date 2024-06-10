# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import re

def intiliase_spark_session(SparkSession,appName,config):
    """
    This provides a centralised access to capture/create a SparkSession,
    which can be used in multiple notebooks of an application
    args:
        1. SparkSession
        2. appName:str
        3. config:dict
    """
    print(f"Starting the app {appName} with {config}")
    spark_builder = SparkSession.builder.appName(appName)
    for k,v in config.items():
        spark_builder=spark_builder.config(k,v)
    spark = spark_builder.getOrCreate()
    return spark

# read source file to get a pyspark dataframe
def read_file(file_format,read_options,file_path):
    """
    In order to read different file formats and also from various locations, 
    this function is created.
    Args:
        1. file_format:str currently supports only three
            a. csv : csv
            b. json : json
            c. xlsx : com.crealytics:spark-excel_2.12:0.13.5
            Other file formats can be supported in future use cases
    """
    try:
        if file_format in {'csv','json','com.crealytics:spark-excel_2.12:0.13.5'}:
            print(f"reading a {file_format} file from {file_path} with readoptions as {read_options}")
            return spark.read.format(file_format).options(**read_options).load(file_path)
        else:
            raise(f"File format {file_format} is currently not supported")
    except Exception as e:
        print(f"Error function read_file : {e}")


# cleaning the column names of the source dataframe
def clean_column_name(df):
    """
    This fucntions ensures that the column names for a dataframe (raw) clean and in snake_case 
    which is good from database perspective
    Args:
        1. df : dataframe
    """
    for col_name in df.columns:
        new_col_name = re.sub(r'[ ,;{}()\n\t=]', '_', col_name)
        df = df.withColumnRenamed(col_name,new_col_name.lower())
    return df

# create table from the given dataframe
def create_table(df,table_name,table_format="delta",mode="overwrite",overWriteSchema ="false"):
    """
    This is a handy function to create a table based on the inpurt dataframe with desired modes
    Args:
        1. df
        2. table_format :str >> currently supporting "delta" format only
        3. table_name: str
        4. mode : default : overwrite
        5. overWriteSchema : default : false
    """
    print(f"creating {table_format} table {table_name} with mode as {mode}")
    try:
        if table_format == "delta":
            df.write.format(table_format).mode(mode).option("overWriteSchema",overWriteSchema).saveAsTable(table_name)
            return spark.catalog.tableExists(table_name)
        else:
            raise("Table_format {table_format} is currently not supported")
    except Exception as e:
        print(f"{table_name} create failed with error:{e}")
        return False

def clean_phone_number(phone_number):
    """
    This function is to ensure phone numbers are following a specific format.
    This one is used as a udf and registered to spark in this same script
    Args : 
        1. phone_number
    """
    if phone_number is None or re.search(r'#ERROR!', phone_number):
        return None
    
    # Remove non-numeric characters except 'x' for extensions
    cleaned = re.sub(r'[^0-9x]', '', phone_number)
    
    # Split extension if exists
    parts = cleaned.split('x')
    main_number = parts[0]
    
    if len(main_number) < 10:
        return None  # Invalid phone number
    
    main_number = main_number[-10:]  # Take the last 10 digits
    
    formatted_number = f'{main_number[:3]}-{main_number[3:6]}-{main_number[6:]}'
    
    if len(parts) > 1 and parts[1].isdigit():
        extension = parts[1]
        return f'{formatted_number}x{extension}'
    
    return formatted_number
clean_phone_number_udf = udf(clean_phone_number, StringType())

intiliase_spark_session(SparkSession,
                        appName = "ecom-sales",
                        config = {'spark.jars.packages':"com.crealytics:spark-excel_2.12:0.13.5"}
                        )