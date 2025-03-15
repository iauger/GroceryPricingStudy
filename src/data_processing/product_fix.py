import pandas as pd
import os

# Set up directory paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Get `src/data_processing/`
DATA_DIR = os.path.join(BASE_DIR, "../data")  # Navigate to `src/data/`

# Define file paths
PRODUCTS_FILE = os.path.join(DATA_DIR, "kroger_product_data.csv")
CLEANED_PRODUCTS_FILE = os.path.join(DATA_DIR, "cleaned_product_data.csv")

dtype_dict = {
    "Product ID": str,
    "UPC": str,
    "Location ID": str,
    "ZIP Code": str,
    "Census Tract": str,
    "Division Number": str,
    "Store Number": str
}

df = pd.read_csv(PRODUCTS_FILE, dtype=dtype_dict)

# ✅ Extract unique Location ID lengths
df["ID_Length"] = df["Location ID"].apply(lambda x: len(str(x)))
length_counts = df["ID_Length"].value_counts()

# ✅ Display results
print("Unique Location ID Lengths:\n", length_counts)