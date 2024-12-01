# Script Name                   : dune__analyze_transactions.py
# To Run                        : python dune__analyze_transactions.py
# Last Modified By              : Himanshu_Sharma
# Last Modified At              : 11/30/2024
# Last Observed Duration        : 3 Sec

# --------------- Start of Import Libraries -------------------- #

import logging
from utility_functions import read_from_parquet

# --------------- End of Import Libraries -------------------- #
# --------------- Start of Variables ------------------------- #

# Enabling Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Constants
vertical_data = "tvp_by_week_vertical_and_safe.parquet"
protocol_data = "tvp_by_week_protocol_and_safe.parquet"
k = 5

# --------------- End of Variables -------------------- #
# --------------- Start of Functions ------------------ #

def top_k_verticals_analysis(df, k):
    # Analysis for TVP verticals based on highest transaction volume and count
    top_k_verticals_by_volume = df.groupby('vertical').agg(
        total_tvp=('outgoing_tvp_usd', 'sum')
    ).sort_values(by='total_tvp', ascending=False).head(k)
    
    top_k_verticals_by_count = df.groupby('vertical').agg(
        total_transactions=('total_transactions', 'sum')
    ).sort_values(by='total_transactions', ascending=False).head(k)

    return top_k_verticals_by_volume, top_k_verticals_by_count

def top_k_protocols_analysis(df, k):
    # Analysis for TVP protocols based on highest transaction volume and count
    top_k_protocols_by_volume = df.groupby('protocol').agg(
        total_tvp=('outgoing_tvp_usd', 'sum')
    ).sort_values(by='total_tvp', ascending=False).head(k)

    top_k_protocols_by_count = df.groupby('protocol').agg(
        total_transactions=('total_transactions', 'sum')
    ).sort_values(by='total_transactions', ascending=False).head(k)

    return top_k_protocols_by_volume, top_k_protocols_by_count

# --------------- End of Functions --------------------------- #
# --------------- Start of Execution Script ------------------ #

if __name__ == "__main__":

    vertical_df = read_from_parquet(vertical_data)
    verticals_by_volume, verticals_by_count = top_k_verticals_analysis(vertical_df, k)

    protocol_df = read_from_parquet(protocol_data)
    protocols_by_volume, protocols_by_count = top_k_protocols_analysis(protocol_df, k)

    # Print the results
    print(f"Top {k} Verticals by TVP:")
    print(verticals_by_volume)
    print(f"\nTop {k} Verticals by Transaction Count:")
    print(verticals_by_count)
    print(f"\nTop {k} Protocols by TVP:")
    print(protocols_by_volume)
    print(f"\nTop {k} Protocols by Transaction Count:")
    print(protocols_by_count)

# --------------- End of Execution Script ------------------ #