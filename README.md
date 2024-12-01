# safe-assignment-himanshu-sharma
Repository for sharing Safe Assignment

Name: Himanshu Sharma

## Files Information
	1. safe_transfers_dune_query.sql 
		-- Query for extracting data from Dune.
		-- Adds Vertical and Protocol at Transaction level.

	2. utility_functions.py
		-- Provides shared utility functions.
		-- Shared utilities for reading and saving data in CSV/Parquet formats and Manages configurations (e.g., API keys).

	3. dune__extract_transactions.py
		-- Data Extractions: Script to extract data from Dune API 
		-- Saves raw_data.parquet in the Output folder.

	4. dune__transform_transactions.py
		-- Script to Transform extracted raw data.
		-- Saves following summarized data as parquet files
			5a. tvp_by_week_vertical_and_safe.parquet: Weekly TVP & Transactions data by vertical and Safe.
   				-- This covers assignment Task 2.b.ii. and 2.b.iii. (The total number of transactions aggregated per Safe per week and Outgoing TVP in USD aggregated per Safe per week. )
			5b. unique_safes_by_week_vertical.parquet: Unique Safes per vertical per week.
   				-- This covers assignment Task 2.b.i (Unique Safes)
			5c. tvp_by_week_protocol_and_safe.parquet: Weekly TVP & Transactions data by protocol and Safe.
   				-- This covers assignment Task 2.c.ii. and 2.c.iii. (The total number of transactions aggregated per Safe per week and Outgoing TVP in USD aggregated per Safe per week. )
			5d. unique_safes_by_week_protocol.parquet: Unique Safes per protocol per week.
   				-- This covers assignment Task 2.c.i (Unique Safes)

	5. dune__analyze_transactions.py
		-- Script uses transformed data and prints following analysis:
			6a. Top 5 verticals by transaction volume and count.
			6b. Top 5 protocols by transaction volume and count.
		-- Results are not saved anywhere, but only printed in output.
		-- An image of the result is made available in files (Top_K_Analysis_Results.png)

	6. dune__transactions_master_script.py
		-- Orchestrates the ETL process by sequentially running the Extract, Transform, and Analyze scripts.

	7. Top_K_Analysis_Result.png 
		-- Analysis Result Image for reference.


## Prerequsites
	1. Python 3.9 or later
	2. Dependencies: Ensure you include packages like pandas, pyarrow, and dune-client
	3. config.json file
		-- In the same directory, create config.json file as:
		{
    	"DUNE_API_KEY": "your_api_key_here"
		}


## How to Execute
	1. Copy Past the files in a local directory.
	2. Make sure config.json file is created and present in same directory.
	3. Run dune__transactions_master_script.py (If required individual scripts can be executed as well.)
		-- This will save all results in an Output folder in the same directory.
		-- Output will include 
			raw_data.parquet
			tvp_by_week_vertical_and_safe.parquet
			unique_safes_by_week_vertical.parquet
			tvp_by_week_protocol_and_safe.parquet
			unique_safes_by_week_protocol.parquet


## Methodology followed
	1. Separation of Stages
		-- Scripts are separated based on their functioning: Extraction, Transformation, and Analysis

	2. Modularity
		-- Common functions are centralized into utility_functions.py

	3. Scalability
		-- Data is stored in Parquet format with partitioning, enabling efficient queries and future scalability.

	4. Comments
		-- For documentation and explanations

	5. Logging & Error Handling
		-- Detailed logging tracks script execution, and exceptions are logged for debugging.

	6. Parameterization wherever possible 

 	7. SQL Code Optimization
  		-- Only built limited / required fields
    		-- Ensured Aliasing in code for better debugging in future.
      		-- Structured and formatted code.


## Process Enhancement Recommendations (These are based on assumptions)
	1. Orchestration
		-- Use of Airflow / Dagster DAGs to orchestrate workflow.

	2. ETL Control Master
		-- Introduce an ETL control table to track incremental updates.
		-- ETL Control table to maintain metadata of load properies of all tables
		-- This will store:
			table_name | load_strategy (Incremental / Full Load) | increment_column | max_increment_columns | dune_query_id

	3. Incremental Strategy
		-- Transactions data can be pulled in an incremental manner based on etl_control_master table based on block_number.
		-- This can reduce data volume greatly and speed up the pipeline.
		-- Reconciliation checks to be employed for incrementally loaded tables.

	4. Use of Parquet File format

	5. Incremental data in GCS
		-- Store Parquet files in Google Cloud Storage (GCS) for cost efficiency and scalability.

	6. BigQuery Optimization
		-- Use partitioning and clustering for analytical queries.

	7. dbt core
		-- dbt for further transformations
		-- Use of dbt tests for data quality
		-- dbt documentations
		-- version control

	8. Use of Git
