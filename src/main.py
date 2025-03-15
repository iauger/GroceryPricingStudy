import os
import sys
import pandas as pd

# Set up directory paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Get `src/acquisition/`
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "data"))  # Navigate to `src/data/`
PROJECT_ROOT = os.path.dirname(BASE_DIR)
sys.path.append(PROJECT_ROOT)

from acquisition.tracking import load_location_tracker, update_log, update_tracker
from acquisition.fetch_product import fetch_products_in_batches
from acquisition.kroger_api import get_kroger_product_compact_token
from acquisition.data_processing import filter_products, save_to_csv

# # Set up directory paths
# BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Get `src/` directory
# DATA_DIR = os.path.join(BASE_DIR, "data")

# Define file paths
PRODUCTS_FILE = os.path.join(DATA_DIR, "kroger_product_data.csv")
PRODUCT_API_LOG = os.path.join(DATA_DIR, "product_api_log.csv")

print(f"PRODUCTS_FILE: {PRODUCTS_FILE}")
print(f"PRODUCT_API_LOG: {PRODUCT_API_LOG}")

def main(batch_size=10):
    """Orchestrates the full Kroger data pipeline."""
    
    print("\n**Starting Kroger Data Pipeline**\n")

    # Step 1: Ensure API Token is Valid
    token = get_kroger_product_compact_token()
    if not token:
        print("Failed to retrieve API token. Exiting...")
        return
    
    # Step 2: Initialize & Update Logs
    print("Loading tracker & checking logs...")
    load_location_tracker()
    update_log()  # Mark locations that need updates

    # Step 3: Fetch & Process Product Data
    print("Fetching product data...")
    updated_locations = fetch_products_in_batches(batch_size=batch_size)

    # Step 4: Apply Updates After Fetching
    print("Updating tracker after batch fetch...")
    for location_id in updated_locations:
        update_tracker(location_id)  # Ensure updates are applied after processing

    # Step 5: Load & Filter Data
    print("Processing and filtering product data...")
    if os.path.exists(PRODUCTS_FILE):
        df = pd.read_csv(PRODUCTS_FILE)
        filtered_df = filter_products(df)
        save_to_csv(filtered_df, PRODUCTS_FILE)
        print(f"Processed and saved filtered product data to {PRODUCTS_FILE}")
    else:
        print("No product data file found! Skipping filtering step.")
    
    print("\n**Pipeline Execution Complete!**")

if __name__ == "__main__":
    main(batch_size=1)  # Adjust batch size as needed
