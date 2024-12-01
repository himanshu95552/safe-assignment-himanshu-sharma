# Script Name                   : dune__transform_transactions.py
# To Run                        : python dune__transform_transactions.py
# Last Modified By              : Himanshu_Sharma
# Last Modified At              : 11/30/2024
# Last Observed Duration        : 140 Sec

# --------------- Start of Import Libraries -------------------- #

import logging
import pandas as pd
from utility_functions import save_results_to_parquet, read_from_parquet

# --------------- End of Import Libraries -------------------- #
# --------------- Start of Variables ------------------------- #

# Enabling Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Constants
raw_data = "raw_data.parquet"

# --------------- End of Variables -------------------- #
# --------------- Start of Functions ------------------ #

def transform_and_load_data(raw_data):
    """Transform raw data to generate various summaries and save them as Parquet files."""
    try:
        raw_data['block_date'] = pd.to_datetime(raw_data['block_date'])
        raw_data['week'] = raw_data['block_date'].dt.to_period('W').apply(lambda r: r.start_time)
        # Summarize TVP & Transactions - aggregated per Safe per vertical per week
        tvp_by_week_vertical_and_safe = (
            raw_data.groupby(['week', 'vertical', 'safe_sender'])
            .agg(total_transactions=('tx_hash', 'count'), outgoing_tvp_usd=('amount_usd', 'sum'))
            .reset_index() )
        # Save as Parquet file
        save_results_to_parquet(tvp_by_week_vertical_and_safe, 'tvp_by_week_vertical_and_safe.parquet', partition_cols=['week'])

        # Summarize Unique Safes by week and vertical
        unique_safes_by_week_vertical = (
            tvp_by_week_vertical_and_safe.groupby(['week', 'vertical'])
            .agg(unique_safes=('safe_sender', 'nunique'))
            .reset_index() )
        # Save as Parquet file
        save_results_to_parquet(unique_safes_by_week_vertical, 'unique_safes_by_week_vertical.parquet', partition_cols=['week'])

        # TVP & Transactions - aggregated per Safe per protocol per week
        tvp_by_week_protocol_and_safe = (
            raw_data.groupby(['week', 'protocol', 'safe_sender'])
            .agg(total_transactions=('tx_hash', 'count'), outgoing_tvp_usd=('amount_usd', 'sum'))
            .reset_index() )
        # Save as Parquet file
        save_results_to_parquet(tvp_by_week_protocol_and_safe, 'tvp_by_week_protocol_and_safe.parquet', partition_cols=['week'])

        # Summarize Unique Safes by week and protocol
        unique_safes_by_week_protocol = (
            tvp_by_week_protocol_and_safe.groupby(['week', 'protocol'])
            .agg(unique_safes=('safe_sender', 'nunique'))
            .reset_index() )
        # Save as Parquet file
        save_results_to_parquet(unique_safes_by_week_protocol, 'unique_safes_by_week_protocol.parquet', partition_cols=['week'])

    except Exception as e:
        logging.error(f"Error during transformation: {e}")
        raise

# --------------- End of Functions --------------------------- #
# --------------- Start of Execution Script ------------------ #

if __name__ == "__main__":
    raw_data = read_from_parquet(raw_data)
    transform_and_load_data(raw_data)

# --------------- End of Execution Script ------------------ #