import re
import pandas as pd
import os
from collections import Counter 
import geopandas as gpd

# Set up directory paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Get `src/data_processing/`
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "data"))

# Define file paths
SHAPEFILE = os.path.join(DATA_DIR, "2020_ZCTA", "tl_2020_us_zcta520.shp")
CLEANED_PRODUCTS_FILE = os.path.join(DATA_DIR, "cleaned_product_data.csv")
CLEANED_LOCATIONS_FILE = os.path.join(DATA_DIR, "cleaned_location_data.csv")
PROCESSED_CENSUS_FILE = os.path.join(DATA_DIR, "processed_census_data.csv")
FINAL_DATASET = os.path.join(DATA_DIR, "final_dataset.csv")

# Define data types
dtype_dict = {
    "Location ID": str,
    "ZIP Code": str,
    "Division Number": str,
    "Store Number": str,
    "Latitude": float,
    "Longitude": float,
    "Product ID": str,
    "UPC": str
}

# Load census data and ensure data types are properly set
if not os.path.exists(PROCESSED_CENSUS_FILE):
    print("Census data file not found!")
else:
    census_df = pd.read_csv(PROCESSED_CENSUS_FILE, dtype=dtype_dict)

# Ensure ZIP is a string for merging
census_df["ZIP Code"] = census_df["ZIP Code"].astype(str)

# Ensure all numeric columns are correctly formatted
census_df.set_index("ZIP Code", inplace=True)
census_df = census_df.astype(float)  # Convert everything else to float
census_df.reset_index(inplace=True)

# Load ZIP boundaries shapefile
if os.path.exists(SHAPEFILE):
    zip_boundaries = gpd.read_file(SHAPEFILE)
    zip_boundaries["ZIP Code"] = zip_boundaries["ZCTA5CE20"].astype(str)
    
    # Merge census data with shapefile data
    census_df = census_df.merge(zip_boundaries, on="ZIP Code", how="left").copy()
    census_df = gpd.GeoDataFrame(census_df, geometry="geometry")
else:
    print("Shapefile not found, skipping ZIP boundary integration.")
    
if not os.path.exists(CLEANED_LOCATIONS_FILE):
    print("Location data file not found!")
else:
    location_df = pd.read_csv(CLEANED_LOCATIONS_FILE, dtype=dtype_dict)

# Store the frequency and name of chains in each ZIP Code
def count_store_chains(chain_list):
    return dict(Counter(chain_list))

zip_location_summary = location_df.groupby("ZIP Code").agg(
    Store_Count=("Location ID", "count"),  # Number of stores per ZIP
    Store_Chain_Distribution=("Chain Name", lambda x: count_store_chains(x)),
    Avg_Latitude=("Latitude", "mean"),  # Average latitude
    Avg_Longitude=("Longitude", "mean"),  # Average longitude
    ).reset_index()

# Merge enhanced census data with aggregated location data
zip_location_summary = zip_location_summary.merge(
    census_df, 
    on="ZIP Code", 
    how="left").copy()

if not os.path.exists(CLEANED_PRODUCTS_FILE):
    print("Product data file not found!")
else:
    product_df = pd.read_csv(CLEANED_PRODUCTS_FILE, dtype=dtype_dict)

# Ensure date is in datetime format
product_df.loc[:, "Date Retrieved"] = pd.to_datetime(product_df["Date Retrieved"])

# Group by Product ID & Location ID
product_grouped_df = product_df.groupby(["Product ID", "Location ID", "Quantity", "UOM", "Brand", "Product Category", "Description"]).agg(
    Most_Recent_Date=("Date Retrieved", "max"),
    Most_Recent_Price=("Price per Unit", lambda x: x.iloc[-1] if not x.empty else None),  # Handle empty groups safely
    Avg_Price=("Price per Unit", "mean"),
    Promo_Price_Avg=("Promo Price per Unit", lambda x: x[x > 0].mean()),  # Avg promo price (ignoring zeros)
    Price_Volatility=("Price per Unit", "std"),  # Std deviation of price
    Promo_Observations=("Promo Price", lambda x: (x > 0).sum()),  # Count promo instances
    Total_Observations=("Date Retrieved", "count")  # Count total entries
).reset_index()

# Fill missing values (NaNs) with 0 where applicable
product_grouped_df.fillna({"Price_Volatility": 0, "Promo_Price_Avg": 0, "Most_Recent_Price": 0}, inplace=True)

# Calculate % of runs containing promo
product_grouped_df["Promo_Frequency"] = (
    product_grouped_df["Promo_Observations"] / product_grouped_df["Total_Observations"]
).fillna(0).round(2)

# Tokenize words in Description
product_grouped_df["Description List"] = product_grouped_df["Description"].str.lower().str.split()

# Aggregate price & stock availability per category per store
store_product_summary = product_grouped_df.groupby(["Location ID", "Product Category", "UOM"]).agg(
    Product_Count=("Product ID", "count"),
    Stock_Observations=("Total_Observations", "max"),
    Avg_Price=("Avg_Price", "mean"),
    Min_Price=("Avg_Price", "min"),
    Max_Price=("Avg_Price", "max"),
    Median_Price=("Avg_Price", "median"),
    Price_Volatility=("Price_Volatility", "mean"),
    Promo_Frequency=("Promo_Frequency", "mean")    
).reset_index()

def clean_keyword(word):
    word = re.sub(r"[^a-zA-Z]", "", word).lower()  # Remove non-alphabet characters & lowercase
    if word in ["egg", "eggs", "bread"]:  # Exclude these words
        return None
    return word

# Create a list of all words found  in all descriptions per product category
def keyword_list(description_lists):    
    category_list = []
    for word_list in description_lists:
        cleaned_words = [clean_keyword(word) for word in word_list if clean_keyword(word)]
        category_list.extend(cleaned_words)  # Use extend() instead of appending lists
        
    return category_list if category_list else ["other"]


# Compute keyword frequency for each location-category pair
keyword_summary = product_grouped_df.groupby(["Location ID", "Product Category"])["Description List"].apply(list).apply(keyword_list).reset_index()
keyword_summary.rename(columns={"Description List": "Keyword_Lists"}, inplace=True)

store_product_summary = store_product_summary.merge(keyword_summary, on=["Location ID", "Product Category"], how="left")

# Extract relevant columns: Location ID and ZIP Code
loc_zip_df = location_df[["Location ID", "ZIP Code"]].copy()

# Merge ZIP Code information into the product dataset
store_product_summary = store_product_summary.merge(
    location_df[["Location ID", "ZIP Code"]], on="Location ID", how="left")

# Group location product data by ZIP Code
zip_product_summary = store_product_summary.groupby(["ZIP Code", "Product Category"]).agg(
    Avg_Price=("Avg_Price", "mean"),
    Min_Price=("Min_Price", "min"),
    Max_Price=("Max_Price", "max"),
    Median_Price=("Median_Price", "mean"),
    Price_Volatility=("Price_Volatility", "mean"),
    Promo_Frequency=("Promo_Frequency", "mean"),
    Avg_Product_Count=("Product_Count", "mean") 
).reset_index()

# Process all  keyword lists into a dictionary containing  words and frequencies
# Initialize Empty List for Aggregation
zip_keyword_freq_list = []

# Iterate Over Each Row in zip_product_summary
for _, row in zip_product_summary.iterrows():
    zip_code = row["ZIP Code"]
    category = row["Product Category"]
    
    # Filter product_zip_df for matching ZIP & Category
    matching_rows = store_product_summary[
        (store_product_summary["ZIP Code"] == zip_code) & 
        (store_product_summary["Product Category"] == category)
    ]
    
    # Flatten all keyword lists from matching rows
    all_keywords = []
    for keyword_list in matching_rows["Keyword_Lists"]:
        all_keywords.extend(keyword_list)  # Merge into single list
    
    # Compute word frequency
    keyword_frequency = dict(Counter(all_keywords))  # Convert to dictionary
    
    # Append to list
    zip_keyword_freq_list.append({
        "ZIP Code": zip_code,
        "Product Category": category,
        "ZIP_Keyword_Frequency": keyword_frequency  # Store dictionary
    })

# Convert into DataFrame
zip_keyword_summary = pd.DataFrame(zip_keyword_freq_list)

zip_product_complete = zip_product_summary.merge(
    zip_keyword_summary, 
    on=["ZIP Code", "Product Category"], 
    how="left"
)

# Fill missing keyword frequencies with empty dictionaries
zip_product_complete["ZIP_Keyword_Frequency"] = zip_product_complete["ZIP_Keyword_Frequency"].apply(lambda x: x if isinstance(x, dict) else {})

# Merge finalized Product data with merged Location and Census data
zip_product_location_complete = zip_location_summary.merge(
    zip_product_complete,
    on="ZIP Code",
    how="left")

# Remove rows with any null values
zip_product_location_filtered = zip_product_location_complete.dropna()

# Convert to GeoDataFrame for mapping functionality
zip_product_location_final = gpd.GeoDataFrame(zip_product_location_filtered)

# # Save final dataset
zip_product_location_final.to_csv(FINAL_DATASET, index=False)
print(f"Final dataset saved to {FINAL_DATASET}")

