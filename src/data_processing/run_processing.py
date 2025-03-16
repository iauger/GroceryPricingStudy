import os
import subprocess

# ✅ Get the correct path for scripts
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Get directory of run_processing.py
DATA_PROCESSING_DIR = os.path.join(BASE_DIR)  # Ensure correct path

def run_script(script_name):
    """Runs a Python script from the src/data_processing/ directory."""
    script_path = os.path.join(DATA_PROCESSING_DIR, script_name)  # Ensure full path

    if not os.path.exists(script_path):  # Check if script exists before running
        print(f"Error: {script_name} not found at {script_path}")
        exit(1)

    try:
        subprocess.run(["python", script_path], check=True)  # Run with absolute path
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
    run_script("zip_merge.py")
    
    # Step 3: Check if Final Dataset Exists
    DATA_FILE = os.path.join(BASE_DIR, "..", "data", "final_dataset.csv")
    if os.path.exists(DATA_FILE):
        print(f"Final dataset generated successfully: {DATA_FILE}")
    else:
        print("Final dataset not found! Check for errors in the merge process.")
