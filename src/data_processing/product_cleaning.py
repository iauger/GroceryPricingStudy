import pandas as pd
import os
import re

# Set up directory paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Get `src/acquisition/`
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "data"))  # Navigate to `src/data/`

# Define file paths
PRODUCTS_FILE = os.path.join(DATA_DIR, "kroger_product_data.csv")
CLEANED_PRODUCTS_FILE = os.path.join(DATA_DIR, "cleaned_product_data.csv")

# Define data types
dtype_dict = {
    "Product ID": str,
    "UPC": str,
    "Location ID": str,
    "ZIP Code": str,
    "Census Tract": str,
    "Division Number": str,
    "Store Number": str
}

def clean_product_data():
    """Loads, cleans, and standardizes product data."""
    if not os.path.exists(PRODUCTS_FILE):
        print("Product data file not found!")
        return None

    df = pd.read_csv(PRODUCTS_FILE, dtype=dtype_dict)
    
    if df.empty:
        print("Warning: Product data is empty! Skipping processing.")
        return None
        
    # Function to pad Location IDs to 8 digits
    def pad_location_id(location_id):
        return location_id.zfill(8)  # Ensures length is always 8 by adding leading zeros

    # Apply function to correct the Location ID length
    df["Location ID"] = df["Location ID"].apply(pad_location_id)

    # Standardize category names using regex to avoid misclassification
    def classify_product(description):
        desc = description.lower()
        if "egg" in desc:
            return "Egg"
        elif "bread" in desc:
            return "Bread"
        else:
            return "Other"

    df["Product Category"] = df["Description"].apply(classify_product)
    
    # Extract Quantity and UOM from Size element
    def process_size(df):
        def extract_quantity(size):
            match = re.search(r'([\d\.]+)', str(size))
            return float(match.group(1)) if match else -1 
        
        def extract_uom(size):
            match = re.search(r'[a-zA-Z]+.*$', str(size))
            return match.group(0).strip() if match else "unit"

        # Apply extraction functions
        df["Quantity"] = df["Size"].apply(extract_quantity)
        df["UOM"] = df["Size"].apply(extract_uom)

        return df

    # Apply function to product dataset
    df = process_size(df)
    
    print("Shape of data after size processing: ")
    print(df.shape)
    
    # Normalize prices, ensuring non-numeric values are handled
    df["Regular Price"] = pd.to_numeric(df["Regular Price"], errors="coerce").fillna(0)
    df["Promo Price"] = pd.to_numeric(df["Promo Price"], errors="coerce").fillna(0)
    df["Price per Unit"] = df["Regular Price"] / df["Quantity"]
    df["Promo Price per Unit"] = df["Promo Price"] / df["Quantity"]
    
    print("Shape of data after price normalization: ")
    print(df.shape)

    inactive_items_mask = (df["Stock Level"].str.lower() == "unknown") & \
                          (df["Regular Price"] == 0) & \
                          (df["Promo Price"] == 0)

    df = df.loc[~inactive_items_mask].copy()
    
    print("Shape of data after inactive processing: ")
    print(df.shape)
    
    unique_product_location_count = df[["Location ID", "Product ID"]].drop_duplicates().shape[0]
    print(f"Total unique product-location combinations: {unique_product_location_count}")

    product_location_counts = df.groupby(["Location ID", "Product ID"]).size().reset_index(name="count")
    print(product_location_counts["count"].describe())  # Summary of how often products repeat per location
        
    df = df.drop_duplicates(subset=["Product ID", "Location ID", "Date Retrieved"])
    
    print("Shape of data after duplicate removal: ")
    print(df.shape)

    # Save cleaned data
    df.to_csv(CLEANED_PRODUCTS_FILE, index=False)
    print(f"Cleaned product data saved to {CLEANED_PRODUCTS_FILE}")
    return df

if __name__ == "__main__":
    clean_product_data()
