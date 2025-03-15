import pandas as pd
import os
import googlemaps
import time

# Set up directory paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Get `src/data_processing/`
DATA_DIR = os.path.join(BASE_DIR, "../data")  # Navigate to `src/data/`

# Define file paths
LOCATIONS_FILE = os.path.join(DATA_DIR, "kroger_locations.csv")
CLEANED_LOCATIONS_FILE = os.path.join(DATA_DIR, "cleaned_location_data.csv")

# # Load Google Maps API Key from environment
# GMAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
# if not GMAPS_API_KEY:
#     raise ValueError("Google Maps API Key is missing! Set it as an environment variable.")

GOOGLE_API_KEY = "AIzaSyDRBpE236p2NPURBmuWEIZ1FlhYjb04dxk" 
gmaps = googlemaps.Client(key=GOOGLE_API_KEY)

# Define data types
dtype_dict = {
    "Location ID": str,
    "ZIP Code": str,
    "Division Number": str,
    "Store Number": str,
    "Latitude": float,
    "Longitude": float
}

def fetch_geocode(address):
    """Fetches lat/lon and ZIP code for an address with retry logic."""
    attempts = 3  # Retry up to 3 times
    for attempt in range(attempts):
        try:
            result = gmaps.geocode(address)
            if result:
                location = result[0]['geometry']['location']
                zip_code = None
                for component in result[0]['address_components']:
                    if 'postal_code' in component['types']:
                        zip_code = component['long_name']
                        break
                return location['lat'], location['lng'], zip_code
        except Exception as e:
            print(f"Attempt {attempt + 1}: Geocoding failed for {address}: {e}")
            time.sleep(1) 
    return None, None, None

def geocode_store_locations():
    """Geocodes missing locations and updates ZIP codes in the cleaned dataset."""
    if not os.path.exists(LOCATIONS_FILE):
        print("Location data file not found!")
        return None

    locations_df = pd.read_csv(LOCATIONS_FILE, dtype=dtype_dict)
    locations_df["Full Address"] = locations_df.apply(lambda row: f"{row['Address']}, {row['City']}, {row['State']}, USA", axis=1)
    
    if os.path.exists(CLEANED_LOCATIONS_FILE):
        cleaned_df = pd.read_csv(CLEANED_LOCATIONS_FILE, dtype=dtype_dict)
        processed_ids = set(cleaned_df["Location ID"].astype(str))
        new_locations = locations_df[~locations_df["Location ID"].astype(str).isin(processed_ids)]
    else:
        cleaned_df = locations_df.copy()
        cleaned_df[["Latitude", "Longitude"]] = None
    
    if new_locations.empty:
        print("All locations already processed. Returning existing cleaned file.")
        return cleaned_df
    
    for index, row in new_locations.iterrows():
        address = row["Full Address"]
        lat, lon, zip_code = fetch_geocode(address)
        new_locations.at[index, "Latitude"] = lat
        new_locations.at[index, "Longitude"] = lon
        new_locations.at[index, "ZIP Code"] = zip_code  # Replacing existing ZIP Code values
        time.sleep(1)  # Prevent rate limiting
    
    updated_df = pd.concat([cleaned_df, new_locations], ignore_index=True)
    updated_df.to_csv(CLEANED_LOCATIONS_FILE, index=False)
    print(f"Cleaned location data saved to {CLEANED_LOCATIONS_FILE}")
    return updated_df

if __name__ == "__main__":
    geocode_store_locations()
