import pandas as pd
import os

# Set up directory paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Get `src/data_processing/`
DATA_DIR = os.path.join(BASE_DIR, "../data")  # Navigate to `src/data/`

# Define file paths
CENSUS_FILE = os.path.join(DATA_DIR, "cleaned_census_data.csv")
PROCESSED_CENSUS_FILE = os.path.join(DATA_DIR, "processed_census_data.csv")

def process_census_data():
    """Cleans, normalizes, and integrates ZIP Code boundary data with census data."""
    if not os.path.exists(CENSUS_FILE):
        print("Census data file not found!")
        return None
    
    # Load census data
    census_df = pd.read_csv(CENSUS_FILE, dtype={"ZIP Code": str})
    
    # Normalize percentage-based fields
    census_df["Poverty Rate (%)"] = census_df["Poverty Count"] / census_df["Total Population"]
    census_df["SNAP Participation (%)"] = census_df["SNAP Households"] / census_df["Total Population"]
    census_df["White Population (%)"] = census_df["White Population"] / census_df["Total Population"]
    census_df["Black Population (%)"] = census_df["Black Population"] / census_df["Total Population"]
    census_df["American Indian Population (%)"] = census_df["American Indian Population"] / census_df["Total Population"]
    census_df["Asian Population (%)"] = census_df["Asian Population"] / census_df["Total Population"]
    census_df["Other Race Population (%)"] = (census_df["Other Race Population"] + census_df["Pacific Islander Population"]) / census_df["Total Population"]
    census_df["Two or More Races (%)"] = census_df["Two or More Races Population"] / census_df["Total Population"]
    census_df["High School Graduate (%)"] = census_df["High School Graduate"] / census_df["Total Population"]
    census_df["Bachelor's Degree (%)"] = census_df["Bachelor's Degree"] / census_df["Total Population"]
    census_df["Master's Degree (%)"] = census_df["Master's Degree"] / census_df["Total Population"]
    census_df["Doctorate Degree (%)"] = census_df["Doctorate Degree"] / census_df["Total Population"]
    
    # Drop original count-based columns
    drop_cols = [
        "Poverty Count", "SNAP Households",
        "White Population", "Black Population", "American Indian Population", "Pacific Islander Population",
        "Asian Population", "Other Race Population", "Two or More Races Population",
        "High School Graduate", "Bachelor's Degree", "Master's Degree", "Doctorate Degree"
    ]
    census_df.drop(columns=drop_cols, inplace=True)
    
    # Fill NaN values (from divide by zero) with 0
    census_df.fillna(0, inplace=True)
    
    # Ensure ZIP is a string for merging
    census_df["ZIP Code"] = census_df["ZIP Code"].astype(str)
    
    # Ensure all numeric columns are correctly formatted
    census_df.set_index("ZIP Code", inplace=True)
    census_df = census_df.astype(float)  # Convert everything else to float
    census_df.reset_index(inplace=True)
    
    # Save processed census data
    census_df.to_csv(PROCESSED_CENSUS_FILE, index=False)
    print(f"Processed census data saved to {PROCESSED_CENSUS_FILE}")
    return census_df

if __name__ == "__main__":
    process_census_data()
