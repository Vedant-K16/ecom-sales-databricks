# ecom-sales-databricks
## Introduction
- This repository contributes towards data processing of a ecom-sales-data and analysis
- This tool is built using a databricks community version and developed with a TDD approach

## Prerequisites
Before running the analysis, you'll need to ensure the following prerequisites are met:
- You have a databricks cluster setup to run the notebook.
- You have to install below libraries:
    * maven - com.crealytics:spark-excel_2.12:0.13.5

## How to run
Please follow below path
  1. Make sure you have all the prerequistes
  2. run run/run.py

## Some Thoughts
- This notebook (run.run.py) is starting point for the e-com sales data analysis project.
- I have attched all the notebooks in sequence in order to generate a desired output
- Source files are uploaded to a Filestore/tables location (dbfs:/FileStore/tables).
- You will also be able to get the source data in sources folder of a git repo https://github.com/Vedant-K16/ecom-sales-databricks
- In order to support TDD, you can check for tests folder and can be run as standalone thing.

1. "These are not implemented from databricks workflow perpective but doable after dbx installation and few enhancement"
2. Also, at the production level, you can run below notebooks (except functions.py) individually per workflow with associated cluster.
      ```
         - Workflow 1. data_ingest_raw
            - notebook_path: ../raw/raw_tables.py
         - Workflow 2. data_ingest_clean
            - notebook_path: ../clean/clean_tables.py
         - Workflow 3. data_aggregations
            - Task 1: sales_enrichement
                 -notebook_path: ../aggregations/sales_enrichement.py
            - Task 2: profit_aggregations
                - notebook_path: ../aggregations/profit_aggregations.py```
4. We can use dbx and yml templeting to control the databricks job/workflow config and pass the notebook path as suggested
5. Also we can run tests/test.py as an workflow before running the actual data workflows to ensure the tests are passed.
   ```
      - Workflow test
           - notebook_path : ../test/test.py
   ```
