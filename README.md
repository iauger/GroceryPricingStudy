# Grocery Pricing Study

## 📌 Project Overview
This repository contains a structured pipeline for collecting, cleaning, and analyzing grocery pricing data from Kroger stores. The pipeline integrates data fetched from Kroger Location API, Kroger Product API and ACS data from the Census Bureau. This guide provides an overview of each script, the execution order, and how to run the complete pipeline.

## 🔍 Data Sources
The project integrates multiple datasets:
- **Kroger Product API** – Retrieves real-time grocery product pricing.
- **Census Data (2023)** – Provides ZIP-level demographics and economic indicators.
- **Store Location Data** – Captures geographic distribution of stores.
- **Natural Earth & ZCTA Data** – Enables geospatial mapping.

## 📂 Folder Structure
```
term_project/
│── src/                   # Source code for data processing
│   ├── acquisition/       # Scripts for retrieving external data
│   ├── data_processing/   # Scripts for cleaning and merging data
│   ├── data/              # Folder for holding final_dataset.csv
│── notebooks/             # Jupyter notebooks for data acquisition and exploratory analysis
│── .gitignore             # Ensures raw data files are not committed
│── README.md              # Project documentation
```

## 🛤️ Script Execution Steps
- Data Acquisition scripts can be executed through main.py.
   - These scripts will call the Product API and store results.  
- Data Cleaing scripts can be executed through run_processing.py.
   - These scripts will clean, aggregate  and merge each individual  dataset (Location, Product, Census).
- Location and Census  datasets can be generated within the Location API and Census Acquisition Notebook.
   - This notebook can be refactored to emulate  the above workflows in the future.

### 1️⃣ Data Acquisition (`src/acquisition/`)
- **`kroger_api.py`** – Handles authentication and API requests for product and location data.
- **`fetch_product.py`** – Queries the Kroger API for grocery product data.
- **`data_processing.py`** – Responsible for processing data retrieved from the Kroger API. .
- **`tracking.py`** – Logs API calls to track data retrieval progress.

### 2️⃣ Data Cleaning (`src/data_processing/`)
- **`census_cleaning.py`** – Processes and normalizes census data for ZIP-level demographic analysis.
- **`location_cleaning.py`** – Cleans and standardizes store location data.
- **`product_cleaning.py`** – Standardizes product descriptions, categories, and price fields.
- **`zip_merge.py`** – Merges location and census data to create a unified dataset.

### 3️⃣ Pipeline Execution (`src/`)
- **`main.py`** – Orchestrates the full acquisition pipeline by executing the above scripts in sequence.
  - Leverages GitHub  workflow actions for automation.
  - **NOTE**: Pipeline has been suspended due to data size issues. Data warehousing will need to migrate to BigQuery to support larger storage capacity.
- **`run_processing.py`** – Runs the full data processing workflow, producing the final dataset.

## 📓 Jupyter Notebooks
The `notebooks/` folder contains Jupyter notebooks used for two purposes:
  1) Outline and explain automated  workflows
  2) Act as standalone sources for data acquisition or analysis

### Location API and Census Acquisition Notebook
- **`location_&_census_acquisition.ipynb`** – Functional notebook  that was used for  the initial construction of raw Census and Location datasets.

### Data Acquisition Notebook
- **`grocery_data_acquisition.ipynb`** – Documents the full pipeline for Product Data Acqusition that was refactored to modular py files.

### Data Processing Notebooks
- **`grocery_data_processing.ipynb`** – Documents the full pipeline for Cleaning, Aggregating and Constructing a final dataset that was refactored to modular py files.

### Data Visualization Notebooks
- **`grocery_data_visualization.ipynb`** – Utilizes the final  dataset for initial EDA and Data Visualization.

## ⚠️ Challenges & Limitations
### Location Data Corrections
- Initial data contained incorrect ZIP code assignments, leading to misassigned store locations.
- Solution: Google Maps API was used to validate ZIP codes, latitude, and longitude.
### Data Pipeline Automation
- Initially developed in Jupyter Notebooks, later refactored into modular Python scripts.
- Integrated with GitHub Actions for automated daily updates.
### Current Limitation: Pipeline is temporarily suspended due to data size constraints, requiring a migration to BigQuery for better scalability.
- Missing Product Data
- Some locations lacked product data, leading to gaps in the dataset.
- Locations with incomplete data were excluded from the final analysis.
### Dataset Size & Completeness
- The dataset does not yet cover all Kroger locations.
- Expanding coverage will enhance pricing accuracy and product availability tracking.

---
