# Script Name: utility_functions.py
# To Run: python utility_functions.py
# Last Modified By: Himanshu_Sharma
# Last Modified At: 11/30/2024

# --------------- Start of Import Libraries -------------------- #

import os
import logging
import pandas as pd
import json

# --------------- End of Import Libraries -------------------- #
# --------------- Start of Variables ------------------------- #

# Enabling Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Constants
config_file = "config.json"

# Create Output folder if it doesn't exist
script_dir = os.path.dirname(os.path.abspath(__file__))
output_folder = os.path.join(script_dir, "Output")
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# --------------- End of Variables -------------------- #
# --------------- Start of Functions ------------------ #

def load_config():
    """Load configuration from the config.json file."""
    try:
        with open(config_file) as file:
            return json.load(file)
    except FileNotFoundError:
        logging.error(f"Config file not found: {config_file}")
        raise

def save_results_to_csv(dataframe, filename):
    """Save the query results to a CSV file."""
    try:
        output_path = os.path.join(os.path.dirname(__file__), 'Output', filename)
        logging.info(f"Saving results as csv at {output_path}")
        dataframe.to_csv(output_path, index=False, mode='w')
        logging.info(f"Results saved to csv: {output_path}.")
    except Exception as e:
        logging.error(f"Error saving results: {e}")
        raise

def save_results_to_parquet(dataframe, filename, partition_cols=None):
    """Save the query results to a Parquet file with partitioning."""
    try:
        output_path = os.path.join(os.path.dirname(__file__), 'Output', filename)
        logging.info(f"Saving results as Parquet file at {output_path}")
        dataframe.to_parquet(output_path, engine='pyarrow', compression='snappy', partition_cols=partition_cols)
        logging.info(f"Results saved to Parquet: {output_path}")
    except Exception as e:
        logging.error(f"Error saving results to Parquet: {e}")
        raise

def read_from_csv(filename):
    """Reads a CSV file from the same directory as the script."""
    try:
        file_path = os.path.join(os.path.dirname(__file__), 'Output', filename)
        logging.info(f"Reading data from input file at {file_path}")
        df = pd.read_csv(file_path)
        logging.info(f"Successfully read data from input csv file at {file_path}")
        return df
    except Exception as e:
        print(f"Error reading CSV: {e}")
        raise

def read_from_parquet(filename):
    """Read data from a Parquet file."""
    try:
        file_path = os.path.join(os.path.dirname(__file__), 'Output', filename)
        df = pd.read_parquet(file_path)
        logging.info(f"Successfully read data from input parquet file at {file_path}")
        return df
    except Exception as e:
        print(f"Error reading parquet: {e}")
        raise