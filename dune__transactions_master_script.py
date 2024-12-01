# Script Name                   : dune__transactions_master_script.py
# To Run                        : python dune__transactions_master_script.py
# Last Modified By              : Himanshu_Sharma
# Last Modified At              : 11/30/2024
# Last Observed Duration        : 600 Sec

# --------------- Start of Import Libraries -------------------- #

import logging
import subprocess

# --------------- End of Import Libraries -------------------- #
# --------------- Start of Variables ------------------------- #

# Enabling Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# --------------- End of Variables -------------------- #
# --------------- Start of Functions ------------------ #

def run_script(script_name):
    """Run a script and log its output."""
    try:
        logging.info(f"Running script: {script_name}")
        result = subprocess.run(["python", script_name], capture_output=True, text=True)
        
        # Log the stdout and stderr from the script
        logging.info(f"Script Output:\n{result.stdout}")
        logging.error(f"Script Errors:\n{result.stderr}")

        if result.returncode == 0:
            logging.info(f"Script {script_name} ran successfully.")
        else:
            logging.error(f"Error running {script_name}: {result.stderr}")
            raise Exception(f"Error running {script_name}")

    except Exception as e:
        logging.error(f"Error: {e}")
        raise

# --------------- End of Functions --------------------------- #
# --------------- Start of Execution Script ------------------ #

if __name__ == "__main__":    
    # Run Extract, Transform, and Analyze scripts in sequence
    try:
        run_script('dune__extract_transactions.py')
        run_script('dune__transform_transactions.py')
        run_script('dune__analyze_transactions.py')
    except Exception as e:
        logging.error("Process failed.")

# --------------- End of Execution Script ------------------ #