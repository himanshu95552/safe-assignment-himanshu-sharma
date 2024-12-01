# Script Name                   : dune__extract_transactions.py
# To Run                        : python dune__extract_transactions.py
# Last Modified By              : Himanshu_Sharma
# Last Modified At              : 11/30/2024
# Last Observed Duration        : 440 Sec

# --------------- Start of Import Libraries -------------------- #

import logging
import pandas as pd
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from utility_functions import load_config, save_results_to_parquet

# --------------- End of Import Libraries -------------------- #
# --------------- Start of Variables ------------------------- #

# Enabling Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Constants
config_file = "config.json"
query_id = 4349530

# --------------- End of Variables -------------------- #
# --------------- Start of Functions ------------------ #

def execute_query_and_get_result(query_id):
    """ Execute the query and get the result"""

    config = load_config()
    api_key = config.get("DUNE_API_KEY")
    if not api_key:
        raise ValueError("API Key not found in config.json.")
    
    dune = DuneClient(api_key=api_key, base_url="https://api.dune.com", request_timeout=300)

    query = QueryBase(query_id)
    try:
        logging.info(f"Executing query {query_id}")
        raw_data = dune.run_query_dataframe(query=query, ping_frequency=10, performance="large" )
        logging.info("Query executed successfully.")
        return raw_data
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        raise

# --------------- End of Functions --------------------------- #
# --------------- Start of Execution Script ------------------ #

if __name__ == "__main__":
    raw_data = execute_query_and_get_result(query_id)
    save_results_to_parquet(raw_data, 'raw_data.parquet' )

# --------------- End of Execution Script ------------------ #