#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ERA5 Hourly Data Downloader, Extractor, and Statistics Generator
=================================================================

Overview:
---------
This script works with ERA5 reanalysis data from ECMWF. It can either download hourly 
ERA5 data via the CDS API and process the resulting GRIB files, or it can solely extract 
data from existing GRIB files. The extraction process reads GRIB files, selects a set of 
pre-defined meteorological and oceanographic variables, and then saves the combined data 
into a CSV file for analysis.

Key Features:
-------------
- **Dual Mode Operation**:
  - **Option 1 (Download & Process)**: Downloads ERA5 data via the CDS API (in monthly chunks) 
    and then extracts specified variables from the resulting GRIB files.
  - **Option 2 (Extract Only)**: Skips downloading and processes GRIB files already available 
    locally. This mode processes files in parallel and displays progress via a progress bar.
- **Selected Variable Extraction**: Extracts key parameters including:
    - swh  : Significant height of combined wind waves and swell
    - mwd  : Mean wave direction
    - pp1d : Peak wave period
    - wind : 10 metre wind speed
    - dwi  : 10 metre wind direction
- **Robust Error Handling & Retry Mechanism**: Uses retries with exponential back-off for downloads.
- **Detailed Logging**: Logs every major step and potential issues to aid in debugging.
- **Performance Metrics**: Computes overall processing time along with average times per month and per year.

Usage:
------
1. When running the script, you are prompted to choose an operation mode:
   - Option 1: Download data from the CDS API and process GRIB files.
   - Option 2: Only extract data from existing GRIB files.
2. The script then performs the selected operation and outputs performance statistics.

Dependencies:
-------------
- Python 3.x
- Libraries:
    - `cdsapi`
    - `pygrib`
    - `pandas`
    - `numpy`
    - `tqdm`
    - `logging`
- ECCODES: Required for `pygrib`.  
  Installation via Conda:
      conda install -c conda-forge eccodes
      conda install -c conda-forge cdsapi
      conda install -c conda-forge pygrib
      conda install -c conda-forge tqdm

ECMWF Data Information:
-----------------------
- Website: https://www.ecmwf.int
- ERA5 reanalysis dataset: https://www.ecmwf.int/en/forecasts/dataset/ecmwf-reanalysis-v5
- Parameter reference: https://codes.ecmwf.int/grib/param-db/

Script Structure:
-----------------
1. **Configuration Section**:
   Sets geographical coordinates, time range, variable codes, directory paths, and API parameters.
2. **Utility Functions**:
   - `initialize_cds_client()`: Initializes the CDS API client.
   - `download_monthly_data()`: Downloads ERA5 data for a given month with retry logic.
   - `process_grib_file_df()`: Opens a GRIB file, extracts relevant data into a pandas DataFrame, and returns it.
3. **Main Execution Routine (`main()`)**:
   Prompts for the operation mode, then either downloads & processes data sequentially (Option 1) 
   or processes existing GRIB files in parallel (Option 2). Overall performance is logged.
"""

import cdsapi
import os
import sys
import time
import calendar
import pandas as pd
import numpy as np
from tqdm import tqdm
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed, TimeoutError
import multiprocessing

# Use "spawn" start method to help with issues related to C libraries (like ECCODES/pygrib)
if __name__ == '__main__':
    multiprocessing.set_start_method("spawn", force=True)

# ----------------------------- Configuration -----------------------------
# Target location: Leixões Costeira, Porto/Portugal
LONGITUDE = -8.983333
LATITUDE = 41.31666

# Years to be processed
START_YEAR = 1940
START_YEAR = 2025
YEARS = list(range(START_YEAR, END_YEAR + 1))

# Variables to extract (mapping of short names to parameter IDs)
VARIABLES = {
    'swh':  '140229',  # Significant height of combined wind waves and swell
    'mwd':  '140230',  # Mean wave direction
    'pp1d': '140231',  # Peak wave period
    'wind': '140245',  # 10 metre wind speed
    'dwi':  '140249'   # 10 metre wind direction
}

# Directories for GRIB files and output CSV
DATA_DIR = 'grib'
RESULTS_DIR = 'results'
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)

# Bounding box buffer (degrees)
BUFFER = 0.25
NORTH = LATITUDE + BUFFER
SOUTH = LATITUDE - BUFFER
EAST = LONGITUDE + BUFFER
WEST = LONGITUDE - BUFFER
AREA = [NORTH, WEST, SOUTH, EAST]

# Grid resolution for data extraction
GRID = [0.25, 0.25]

# API request delay and retry configuration
REQUEST_DELAY = 60  # seconds
MAX_RETRIES = 3

# Logging configuration
LOG_FILE = 'download_era5_data.log'
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ----------------------------- Functions -----------------------------
def initialize_cds_client():
    """
    Initialize and return the CDS API client.
    """
    try:
        client = cdsapi.Client()
        logging.info("CDS API client initialized successfully.")
        return client
    except Exception as e:
        logging.error(f"Failed to initialize CDS API client. Error: {e}")
        sys.exit(1)

def download_monthly_data(client, year, month, variable_list, area, grid, output_dir):
    """
    Download ERA5 monthly data for a specific year and month.
    Returns the file path if successful, else None.
    """
    file_name = f"ERA5_{year}_{month:02d}.grib"
    file_path = os.path.join(output_dir, file_name)
    
    if os.path.exists(file_path):
        logging.info(f"Data for {year}-{month:02d} exists. Skipping download.")
        return file_path

    days_in_month = calendar.monthrange(year, month)[1]
    days = [f"{d:02d}" for d in range(1, days_in_month + 1)]

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logging.info(f"Attempt {attempt}: Downloading data for {year}-{month:02d}...")
            client.retrieve(
                'reanalysis-era5-single-levels',
                {
                    'product_type': 'reanalysis',
                    'format': 'grib',
                    'variable': variable_list,
                    'year': str(year),
                    'month': f"{month:02d}",
                    'day': days,
                    'time': [f"{h:02d}:00" for h in range(24)],
                    'area': area,
                    'grid': grid,
                },
                file_path
            )
            logging.info(f"Successfully downloaded data for {year}-{month:02d}.")
            return file_path
        except Exception as e:
            logging.warning(f"Attempt {attempt}: Failed for {year}-{month:02d}. Error: {e}")
            if attempt < MAX_RETRIES:
                wait_time = REQUEST_DELAY * attempt
                logging.info(f"Retrying after {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logging.error(f"All {MAX_RETRIES} attempts failed for {year}-{month:02d}.")
                return None

def process_grib_file_df(file_path):
    """
    Process a GRIB file to extract selected data and return a pandas DataFrame.
    """
    try:
        import pygrib  # Import locally for each process
    except Exception as e:
        logging.error(f"Failed to import pygrib in process_grib_file_df: {e}")
        return None

    try:
        grbs = pygrib.open(file_path)
    except Exception as e:
        logging.error(f"Failed to open {file_path}. Error: {e}")
        return None

    data_records = {}
    for grb in grbs:
        try:
            short_name = grb.shortName
            valid_time = grb.validDate.strftime('%Y-%m-%d %H:%M:%S')
            if short_name in VARIABLES:
                data_array, lats, lons = grb.data()
                closest_lat_index = (np.abs(lats - LATITUDE)).argmin()
                closest_lon_index = (np.abs(lons - LONGITUDE)).argmin()
                value = data_array[closest_lat_index, closest_lon_index]
                if valid_time not in data_records:
                    data_records[valid_time] = {}
                data_records[valid_time][short_name] = value
        except Exception as e:
            logging.warning(f"Error processing a GRIB message in {file_path}: {e}")
            continue
    grbs.close()

    if not data_records:
        logging.warning(f"No data extracted from {file_path}.")
        return None

    records = []
    for dt, vars_data in data_records.items():
        row = {'datetime': dt}
        for var in VARIABLES.keys():
            row[var] = vars_data.get(var, None)
        records.append(row)
    return pd.DataFrame(records)

# ----------------------------- Main Execution -----------------------------
def main():
    """
    Main function for data retrieval and processing.
    """
    user_option = input(
        "SELECT YOUR OPTION:\n"
        "1) Download ERA5 data from CDS API and process GRIB files;\n"
        "2) Only extract data from existing GRIB files (accelerated processing).\n"
        "Choose (1 or 2): "
    )
    
    if user_option not in ['1', '2']:
        print("Invalid option selected. Exiting.")
        return

    overall_start_time = time.time()
    OUTPUT_CSV = os.path.join(RESULTS_DIR, 'download_era5_data.csv')

    if user_option == '2':
        # Option 2: Process existing GRIB files in parallel
        if os.path.exists(OUTPUT_CSV):
            os.remove(OUTPUT_CSV)
            logging.info(f"Deleted existing CSV file at {OUTPUT_CSV}.")

        grib_files = sorted([f for f in os.listdir(DATA_DIR) if f.endswith('.grib')])
        dataframes = []
        timeout_per_file = 120  # seconds

        with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
            # Submit all tasks
            futures = {executor.submit(process_grib_file_df, os.path.join(DATA_DIR, file)): file for file in grib_files}
            # Wrap the as_completed iterator with tqdm for a progress bar.
            for future in tqdm(as_completed(futures), total=len(futures), desc="Processing GRIB files"):
                file = futures[future]
                try:
                    df = future.result(timeout=timeout_per_file)
                    if df is not None and not df.empty:
                        dataframes.append(df)
                        logging.info(f"Processed data from {file}.")
                    else:
                        logging.error(f"No data extracted from {file}.")
                except TimeoutError:
                    logging.error(f"Processing {file} timed out.")
                    future.cancel()
                except Exception as exc:
                    logging.error(f"Exception while processing {file}: {exc}")

        if dataframes:
            final_df = pd.concat(dataframes, ignore_index=True)
            final_df.to_csv(OUTPUT_CSV, index=False)
            print("Data processing completed (Option 2).")
        else:
            print("No data was extracted from any GRIB file.")
    else:
        # Option 1: Download data and process each GRIB file sequentially
        client = initialize_cds_client()
        variable_list = list(VARIABLES.values())
        total_requests = len(YEARS) * 12
        pbar = tqdm(total=total_requests, desc="Downloading ERA5 Data")
        
        for year in YEARS:
            for month in range(1, 13):
                file_path = download_monthly_data(
                    client, year, month, variable_list, AREA, GRID, DATA_DIR
                )
                if file_path:
                    df = process_grib_file_df(file_path)
                    if df is not None and not df.empty:
                        write_header = not os.path.exists(OUTPUT_CSV)
                        df.to_csv(OUTPUT_CSV, mode='a', header=write_header, index=False)
                        logging.info(f"Processed data for {year}-{month:02d}.")
                    else:
                        logging.error(f"Failed to process data for {year}-{month:02d}.")
                else:
                    logging.error(f"Skipping {year}-{month:02d} due to download failure.")
                pbar.update(1)
                time.sleep(REQUEST_DELAY)
        pbar.close()

    overall_end_time = time.time()
    total_time = overall_end_time - overall_start_time
    total_months = (len(YEARS) * 12) if user_option == '1' else len(os.listdir(DATA_DIR))
    avg_time_per_month = total_time / total_months
    avg_time_per_year = total_time / len(YEARS) if user_option == '1' else total_time

    logging.info("Data processing completed.")
    logging.info(f"Total time: {total_time:.2f} seconds")
    logging.info(f"Average time per month: {avg_time_per_month:.2f} seconds")
    logging.info(f"Average time per year: {avg_time_per_year:.2f} seconds")

    print("Data processing completed.")
    print(f"Total time: {total_time:.2f} seconds")
    print(f"Average time per month: {avg_time_per_month:.2f} seconds")
    print(f"Average time per year: {avg_time_per_year:.2f} seconds")

if __name__ == "__main__":
    main()
