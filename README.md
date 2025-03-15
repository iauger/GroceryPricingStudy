# Grocery Pricing Study

## 📌 Project Overview
This repository contains a structured pipeline for collecting, cleaning, and analyzing grocery pricing data. The pipeline integrates data from multiple sources, including store locations, product prices, and demographic indicators. This guide provides an overview of each script, the execution order, and how to run the complete pipeline.

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
│── notebooks/             # Jupyter notebooks for exploratory analysis
│── git_data/              # Contains final processed dataset (not tracked in Git)
│── .gitignore             # Ensures raw data files are not committed
│── README.md              # Project documentation
```

## 🛤️ Pipeline Execution Order
To execute the pipeline in the correct sequence, run the following scripts in order:

### **1️⃣ Data Acquisition (`src/acquisition/`)**
- **`kroger_api.py`** – Handles authentication and API requests for product and location data.
- **`fetch_product.py`** – Queries the Kroger API for grocery product data.
- **`data_processing.py`** – Responsible for processing data retrieved from the Kroger API. .
- **`tracking.py`** – Logs API calls to track data retrieval progress.

### **2️⃣ Data Cleaning (`src/data_processing/`)**
- **`census_cleaning.py`** – Processes and normalizes census data for ZIP-level demographic analysis.
- **`location_cleaning.py`** – Cleans and standardizes store location data.
- **`product_cleaning.py`** – Standardizes product descriptions, categories, and price fields.
- **`zip_merge.py`** – Merges location and census data to create a unified dataset.

### **3️⃣ Pipeline Execution (`src/`)**
- **`main.py`** – Orchestrates the full acquisition pipeline by executing the above scripts in sequence.
  - Leverages GitHub  workflow actions for automation.   
- **`run_processing.py`** – Runs the full data processing workflow, producing the final dataset.

## 📓 Jupyter Notebooks
The `notebooks/` folder contains Jupyter notebooks used for two purposes:
  1) Outline and explain automated  workflows
  2) Act as standalone sources for data acquisition or analysis

### ** Location API and Census Acquisition Notebook**
- **`location_&_census_acquisition.ipynb`** – Functional notebook  that was used for  the initial construction of raw Census and Location datasets.

### ** Data Acquisition Notebook**
- **`grocery_data_acquisition.ipynb`** – Documents the full pipeline for Product Data Acqusition that was refactored to modular py files.

### ** Data Processing Notebooks**
- **`grocery_data_processing.ipynb`** – Documents the full pipeline for Cleaning, Aggregating and Constructing a final dataset that was refactored to modular py files.

### ** Data Visualization Notebooks**
- **`grocery_data_visualization.ipynb`** – Utilizes the final  dataset for initial EDA and Data Visualization.

## 🛠 How to Use This Repository
1. **Clone the repository:**
   ```bash
   git clone https://github.com/iauger/GroceryPricingStudy.git
   ```
2. **Ensure dependencies are installed (if `requirements.txt` is available):**
   ```bash
   pip install -r requirements.txt
   ```
3. **Run the pipeline:**
   ```bash
   python src/main.py
   ```
4. **Explore Jupyter notebooks in `notebooks/` for additional data visualization and correlation analysis.**
---
