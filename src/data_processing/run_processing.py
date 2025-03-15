import os
import subprocess

def run_script(script_name):
    """Runs a Python script from the src/data_processing/ directory."""
    try:
        subprocess.run(["python", script_name], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_name}: {e}")
        exit(1)

if __name__ == "__main__":
    print("Starting Data Processing Pipeline...")
    
    # Step 1: Run Cleaning Scripts
    print("Cleaning census data...")
    run_script("census_cleaning.py")
    
    print("Cleaning product data...")
    run_script("product_cleaning.py")
    
    print("Cleaning location data...")
    run_script("location_cleaning.py")
    
    # Step 2: Merge Data
    print("Merging data sources...")
    run_script("merge_final.py")
    
    # Step 3: Check if Final Dataset Exists
    DATA_FILE = "../data/final_dataset.csv"
    if os.path.exists(DATA_FILE):
        print(f"Final dataset generated successfully: {DATA_FILE}")
    else:
        print("Final dataset not found! Check for errors in the merge process.")
