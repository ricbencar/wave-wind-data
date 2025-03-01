#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ERA5 Hourly Data Downloader and Extractor
=================================================================

Overview:
---------
This script works with ERA5 reanalysis data from ECMWF. It can either download hourly 
ERA5 data via the CDS API and process the resulting GRIB files, or it can solely extract 
data from existing GRIB files. The extraction process reads GRIB files, selects a set of 
pre-defined meteorological and oceanographic variables using IDW (Inverse Distance Weighting)
to interpolate data to an exact point, and then saves the combined data into a CSV file 
for analysis.

Key Features:
-------------
- **Dual Mode Operation**:
  - **Option 1 (Download & Process)**: Downloads ERA5 data via the CDS API (in monthly chunks) 
    and then extracts specified variables from the resulting GRIB files. In this update the 
    download phase is decoupled from the extraction phase so that once files are present, 
    they are processed concurrently.
  - **Option 2 (Extract Only)**: Skips downloading and processes all GRIB files already available 
    locally (ignoring the START_YEAR and END_YEAR range), using parallel processing with a progress bar.
- **Selected Variable Extraction with IDW Interpolation**: The script extracts key parameters:
    - swh  : Significant height of combined wind waves and swell
    - mwd  : Mean wave direction
    - pp   : Peak wave period
    - u10  : 10m u-component of wind
    - v10  : 10m v-component of wind
  The extraction uses IDW to interpolate the data to the exact provided coordinates.
- **Robust Error Handling & Retry Mechanism:** Downloads are retried (with delay) up to a maximum number of attempts.
- **Detailed Logging:** Logs every major step and potential issues.
- **Sorted Output:** The final CSV file is sorted by the datetime column.
- **Performance Metrics:** Overall processing time is logged.
- **Missing File Warning:** After processing, the script checks the entire years range specified by the user and warns if any monthly GRIB files are missing.

Usage:
------
1. At runtime you are prompted to choose:
   - Option 1: Download data from the CDS API and process GRIB files.
   - Option 2: Only extract data from existing GRIB files.
2. The script then executes the selected mode and outputs performance statistics, including warnings for any missing monthly GRIB files.

Dependencies:
-------------
- Python 3.x
- Libraries:
    - cdsapi
    - pygrib
    - pandas
    - numpy
    - tqdm
    - logging
- ECCODES is required for pygrib.

ECMWF Data Information:
-----------------------
- Website: https://www.ecmwf.int
- ERA5 reanalysis dataset: https://www.ecmwf.int/en/forecasts/dataset/ecmwf-reanalysis-v5
- Parameter reference: https://codes.ecmwf.int/grib/param-db/
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

# Use "spawn" start method for better compatibility with C libraries (e.g., ECCODES/pygrib)
if __name__ == '__main__':
    multiprocessing.set_start_method("spawn", force=True)

# ----------------------------- Configuration -----------------------------
# Target location: LEIXOES OCEANIC BUOY, Porto/Portugal
LONGITUDE = -9.581666670
LATITUDE = 41.14833299

# Process years (used in Option 1)
START_YEAR = 1940
END_YEAR = 2025
YEARS = list(range(START_YEAR, END_YEAR + 1))

# Official ERA5 variable names for CDS API requests.
VARIABLES = {
    'swh': 'significant_height_of_combined_wind_waves_and_swell',
    'mwd': 'mean_wave_direction',
    'pp':  'peak_wave_period',
    'u10': '10m_u_component_of_wind',
    'v10': '10m_v_component_of_wind'
}

# Directories for GRIB files and output CSV
DATA_DIR = 'grib'
RESULTS_DIR = 'results'
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)

# Define bounding box (degrees) for the target area.
BUFFER = 0.25
NORTH = LATITUDE + BUFFER
SOUTH = LATITUDE - BUFFER
EAST = LONGITUDE + BUFFER
WEST = LONGITUDE - BUFFER
AREA = [NORTH, WEST, SOUTH, EAST]

# Grid resolution for extraction
GRID = [0.25, 0.25]

# Delay and retry configuration
REQUEST_DELAY = 60  # seconds
MAX_RETRIES = 3

# Logging configuration
LOG_FILE = 'download_era5_data.log'
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ----------------------------- Utility Functions -----------------------------
def warn_missing_files(years, data_dir):
    """
    Check for missing monthly GRIB files in the specified years range and warn the user.
    """
    missing = []
    for year in years:
        for month in range(1, 13):
            file_name = f"ERA5_{year}_{month:02d}.grib"
            file_path = os.path.join(data_dir, file_name)
            if not os.path.exists(file_path):
                missing.append(file_name)
    if missing:
        warning_msg = "Warning: The following monthly GRIB files are missing:\n" + "\n".join(missing)
        print(warning_msg)
        logging.warning(warning_msg)
    else:
        info_msg = "All monthly GRIB files in the specified years range exist."
        print(info_msg)
        logging.info(info_msg)

def initialize_cds_client():
    """Initialize and return the CDS API client."""
    try:
        client = cdsapi.Client()
        logging.info("CDS API client initialized successfully.")
        return client
    except Exception as e:
        logging.error(f"Failed to initialize CDS API client. Error: {e}")
        sys.exit(1)

def download_monthly_data(client, year, month, variable_list, area, grid, output_dir):
    """
    Download ERA5 monthly data for a given year and month.
    Returns a tuple (file_path, downloaded) where downloaded is True if a new download was performed.
    If the file exists, the download is skipped.
    """
    file_name = f"ERA5_{year}_{month:02d}.grib"
    file_path = os.path.join(output_dir, file_name)
    
    if os.path.exists(file_path):
        logging.info(f"Data for {year}-{month:02d} exists. Skipping download.")
        return file_path, False

    days_in_month = calendar.monthrange(year, month)[1]
    days = [f"{d:02d}" for d in range(1, days_in_month + 1)]
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logging.info(f"Attempt {attempt}: Downloading data for {year}-{month:02d}...")
            # Pass year and month as lists per CDS API requirements.
            client.retrieve(
                'reanalysis-era5-single-levels',
                {
                    'product_type': 'reanalysis',
                    'format': 'grib',
                    'variable': variable_list,
                    'year': [str(year)],
                    'month': [f"{month:02d}"],
                    'day': days,
                    'time': [f"{h:02d}:00" for h in range(24)],
                    'area': area,
                    'grid': grid,
                },
                file_path
            )
            logging.info(f"Successfully downloaded data for {year}-{month:02d}.")
            return file_path, True
        except Exception as e:
            logging.warning(f"Attempt {attempt}: Failed for {year}-{month:02d}. Error: {e}")
            if attempt < MAX_RETRIES:
                wait_time = REQUEST_DELAY * attempt
                logging.info(f"Retrying after {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logging.error(f"All {MAX_RETRIES} attempts failed for {year}-{month:02d}.")
                return None, False

def process_grib_file_df(file_path):
    """
    Process a GRIB file to extract selected data and return a pandas DataFrame.
    Uses IDW interpolation to estimate the value at the target coordinate.
    """
    try:
        import pygrib  # Local import for each process
    except Exception as e:
        logging.error(f"Failed to import pygrib in process_grib_file_df: {e}")
        return None

    try:
        grbs = pygrib.open(file_path)
    except Exception as e:
        logging.error(f"Failed to open {file_path}. Error: {e}")
        return None

    # Map GRIB message short names to our keys.
    GRIB_KEY_MAP = {
        'swh': 'swh',
        'mwd': 'mwd',
        'pp':  'pp',
        'u10': 'u10',
        'v10': 'v10'
    }
    data_records = {}
    for grb in grbs:
        try:
            valid_time = grb.validDate.strftime('%Y-%m-%d %H:%M:%S')
            var_key = None
            if grb.shortName in GRIB_KEY_MAP:
                var_key = GRIB_KEY_MAP[grb.shortName]
            else:
                logging.info(f"Skipping GRIB message with shortName '{grb.shortName}' in {os.path.basename(file_path)}.")
                continue

            data_array, lats, lons = grb.data()
            dist = np.sqrt((lats - LATITUDE)**2 + (lons - LONGITUDE)**2)
            if np.any(dist < 1e-6):
                value = data_array.flat[dist.argmin()]
            else:
                p = 2  # IDW power
                weights = 1.0 / (dist**p)
                value = np.sum(weights * data_array) / np.sum(weights)
            if valid_time not in data_records:
                data_records[valid_time] = {}
            data_records[valid_time][var_key] = value
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
        for key in GRIB_KEY_MAP.values():
            row[key] = vars_data.get(key, None)
        records.append(row)
    return pd.DataFrame(records)

# ----------------------------- Main Execution -----------------------------
def main():
    """
    Main function to orchestrate data retrieval and processing.
    For Option 1, the download (or verification) phase is sequential; then GRIB extraction
    is performed in parallel.
    After processing, the script warns the user if any monthly GRIB files in the specified
    years range are missing.
    """
    user_option = input(
        "SELECT YOUR OPTION:\n"
        "1) Download ERA5 data from CDS API and process GRIB files;\n"
        "2) Only extract data from existing GRIB files.\n"
        "Choose (1 or 2): "
    )
    if user_option not in ['1', '2']:
        print("Invalid option selected. Exiting.")
        return

    overall_start_time = time.time()
    OUTPUT_CSV = os.path.join(RESULTS_DIR, 'download_era5_data.csv')

    if user_option == '2':
        # Option 2: Process all existing GRIB files in parallel.
        if os.path.exists(OUTPUT_CSV):
            os.remove(OUTPUT_CSV)
            logging.info(f"Deleted existing CSV file at {OUTPUT_CSV}.")
        grib_files = sorted([os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith('.grib')])
        dataframes = []
        timeout_per_file = 120  # seconds
        with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = {executor.submit(process_grib_file_df, file): file for file in grib_files}
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
            final_df["datetime"] = pd.to_datetime(final_df["datetime"])
            final_df.sort_values(by="datetime", inplace=True)
            final_df.to_csv(OUTPUT_CSV, index=False)
            print("Data processing completed (Option 2).")
        else:
            print("No data was extracted from any GRIB file.")
    else:
        # Option 1: Download (or verify) files sequentially, then process in parallel.
        client = initialize_cds_client()
        variable_list = list(VARIABLES.values())
        total_requests = len(YEARS) * 12
        pbar = tqdm(total=total_requests, desc="Downloading ERA5 Data")
        grib_files_to_process = []
        for year in YEARS:
            for month in range(1, 13):
                file_path, downloaded = download_monthly_data(client, year, month, variable_list, AREA, GRID, DATA_DIR)
                if file_path:
                    grib_files_to_process.append(file_path)
                pbar.update(1)
                if downloaded:
                    time.sleep(REQUEST_DELAY)
        pbar.close()
        # Process all collected GRIB files in parallel.
        dataframes = []
        timeout_per_file = 120  # seconds
        with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = {executor.submit(process_grib_file_df, file): file for file in grib_files_to_process}
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
            final_df["datetime"] = pd.to_datetime(final_df["datetime"])
            final_df.sort_values(by="datetime", inplace=True)
            final_df.to_csv(OUTPUT_CSV, index=False)
            logging.info("CSV file sorted by datetime column.")
        else:
            logging.error("No data was extracted in Option 1.")
            print("No data was extracted in Option 1.")
    
    # After processing, warn the user if any monthly GRIB files are missing.
    warn_missing_files(YEARS, DATA_DIR)
    
    overall_end_time = time.time()
    total_time = overall_end_time - overall_start_time
    logging.info("Data processing completed.")
    logging.info(f"Total time: {total_time:.2f} seconds")
    print("Data processing completed.")
    print(f"Total time: {total_time:.2f} seconds")

if __name__ == "__main__":
    main()
