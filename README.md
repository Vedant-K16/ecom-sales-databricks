# ecom-sales-databricks
## Introduction
This repository contributes towards data processing of a ecom-sales-data and analysis

## Prerequisites
Before running the analysis, you'll need to ensure the following prerequisites are met:
- You have a databricks cluster setup to run the notebook.
- You have to install below libraries:
    * maven - com.crealytics:spark-excel_2.12:0.13.5

## How to run
Please follow below path
  1. Make sure you have all the prerequistes
  2. run raw/raw_data.py
  3. run enriched/clean_data.py
  4. run the scripts inside aggregations
  5. use the scripts in sql_scripts folder to perform desired analysis 
