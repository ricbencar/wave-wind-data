#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ERA5 Hourly Data Downloader and Extractor
=================================================================

Overview:
---------
This script is designed to work with ERA5 reanalysis data from ECMWF using both the CDS API
and the MARS (Meteorological Archive and Retrieval System). MARS is ECMWF’s archive retrieval
system that enables users to request data using a strictly defined syntax. Detailed information 
on MARS request syntax and best practices can be found in the official MARS User Documentation:
https://confluence.ecmwf.int/display/UDOC/MARS+user+documentation

The script supports two operational modes:
    1. Download & Process:
         - Downloads ERA5 data in monthly chunks from the CDS API (using MARS syntax rules).
         - As each GRIB file is downloaded (or confirmed to exist), it is immediately processed with pygrib.
         - For each GRIB message, the script first attempts to match by the GRIB message short name
           (which is assumed to be the same as the internal variable name). If not found, it falls back
           to comparing a dot‑separated Parameter ID (constructed from the GRIB attributes) with the expected value.
         - Extracted variables are interpolated via Inverse Distance Weighting (IDW) at the target coordinate.
         - All results are combined into a CSV file, sorted by datetime.
    2. Extract Only:
         - Skips the download phase and directly processes all available GRIB files locally.
         - Uses parallel processing with progress monitoring to efficiently extract data.
         - In addition to matching GRIB messages by their short names, it also extracts data using param IDs when available.

Dependencies include Python 3.x and libraries such as cdsapi, pygrib, pandas, numpy, tqdm, and logging.
Note: ECCODES is required for proper functioning of pygrib.

ECMWF Data Information:
-----------------------
- Website: https://www.ecmwf.int
- ERA5 reanalysis dataset: https://www.ecmwf.int/en/forecasts/dataset/ecmwf-reanalysis-v5
- Parameter reference: https://codes.ecmwf.int/grib/param-db/

For more detailed information on CDS API and MARS request syntax, please refer to:
https://confluence.ecmwf.int/display/UDOC/MARS+user+documentation
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

# Use the "spawn" start method for better compatibility with C libraries (e.g., ECCODES/pygrib)
if __name__ == '__main__':
    multiprocessing.set_start_method("spawn", force=True)

# ----------------------------- Unified Variable Mapping -----------------------------
# The VARIABLES dictionary now maps the internal variable name (which is also the expected GRIB short name)
# to the official ERA5 param ID (as a 6-digit string).
VARIABLES = {
    'swh':  '140229',   # Significant height of combined wind waves and swell
    'mwd':  '140230',   # Mean wave direction
    'pp1d': '140231',   # Peak wave period
    'wind': '140245',   # 10 metre wind speed
    'dwi':  '140249'    # 10 metre wind direction
}

# Derive the list of param IDs for the CDS API/MARS request.
PARAM_IDS = list(VARIABLES.values())

def format_param_id(param_id_str):
    """
    Convert a 6-digit parameter id string (e.g., '140229') to the dot-separated format (e.g., '229.140').
    """
    if len(param_id_str) != 6:
        return param_id_str
    return f"{int(param_id_str[3:]):03d}.{int(param_id_str[:3])}"

# ----------------------------- Configuration -----------------------------
# Target location: LEIXOES OCEANIC BUOY, Porto/Portugal
LONGITUDE = -9.581666670
LATITUDE = 41.14833299

# Process years (used in Option 1). Note: ERA5 standard reanalysis is typically available from ~1950/1959 onwards.
START_YEAR = 1940
END_YEAR = 2025
YEARS = list(range(START_YEAR, END_YEAR + 1))

# Directories for GRIB files and output CSV.
DATA_DIR = 'grib'
RESULTS_DIR = 'results'
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)

# Define the bounding box (degrees) for the target area.
# Format: [North, West, South, East] as required by the MARS request.
BUFFER = 0.25
NORTH = LATITUDE + BUFFER
SOUTH = LATITUDE - BUFFER
EAST = LONGITUDE + BUFFER
WEST = LONGITUDE - BUFFER
AREA = [NORTH, WEST, SOUTH, EAST]

# Grid resolution for extraction.
GRID = [0.25, 0.25]

# Delay and retry configuration for the CDS API request.
REQUEST_DELAY = 120  # seconds delay between requests (to avoid overloading the server)
MAX_RETRIES = 5

# Logging configuration.
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

def download_monthly_data(client, year, month, area, grid, output_dir):
    """
    Download ERA5 monthly data for a given year and month using param IDs.
    """
    file_name = f"ERA5_{year}_{month:02d}.grib"
    file_path = os.path.join(output_dir, file_name)
    
    if os.path.exists(file_path):
        logging.info(f"Data for {year}-{month:02d} exists. Skipping download.")
        return file_path, False

    days_in_month = calendar.monthrange(year, month)[1]
    days = [f"{d:02d}" for d in range(1, days_in_month + 1)]
    
    request_dict = {
        'product_type': 'reanalysis',
        'format': 'grib',
        'param': PARAM_IDS,
        'year': [str(year)],
        'month': [f"{month:02d}"],
        'day': days,
        'time': [f"{h:02d}:00" for h in range(24)],
        'area': area,  # Format: [North, West, South, East]
        'grid': grid,
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logging.info(f"Attempt {attempt}: Downloading data for {year}-{month:02d}...")
            client.retrieve('reanalysis-era5-single-levels', request_dict, file_path)
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

    data_records = {}
    
    for grb in grbs:
        try:
            valid_time = grb.validDate.strftime('%Y-%m-%d %H:%M:%S')
            var_key = None
            # First, try to match using the GRIB message short name.
            if grb.shortName in VARIABLES:
                var_key = grb.shortName
            else:
                # Fallback: try matching using the dot-separated Parameter ID.
                try:
                    param_num = grb.parameterNumber  # typically an integer, e.g., 229
                    table2 = getattr(grb, 'table2Version', None)  # e.g., 140
                except Exception:
                    param_num = None
                    table2 = None
                if param_num is not None and table2 is not None:
                    grb_param_id = f"{param_num}.{table2}"
                    for key, expected_id in VARIABLES.items():
                        if grb_param_id == format_param_id(expected_id):
                            var_key = key
                            break
            if var_key is None:
                continue

            data_array, lats, lons = grb.data()
            dist = np.sqrt((lats - LATITUDE)**2 + (lons - LONGITUDE)**2)
            if np.any(dist < 1e-6):
                value = data_array.flat[dist.argmin()]
            else:
                p = 2  # IDW power factor.
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
        for key in VARIABLES.keys():
            row[key] = vars_data.get(key, None)
        records.append(row)

    return pd.DataFrame(records)

# ----------------------------- Main Execution -----------------------------
def main():
    """
    Main function to orchestrate data retrieval and processing.
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
        if os.path.exists(OUTPUT_CSV):
            os.remove(OUTPUT_CSV)
            logging.info(f"Deleted existing CSV file at {OUTPUT_CSV}.")
        grib_files = sorted([os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith('.grib')])
        dataframes = []
        timeout_per_file = 120  # seconds per file processing timeout.
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
        client = initialize_cds_client()
        total_requests = len(YEARS) * 12
        pbar = tqdm(total=total_requests, desc="Downloading and Processing ERA5 Data")
        dataframes = []
        with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = []
            for year in YEARS:
                for month in range(1, 13):
                    file_path, downloaded = download_monthly_data(client, year, month, AREA, GRID, DATA_DIR)
                    if file_path:
                        future = executor.submit(process_grib_file_df, file_path)
                        futures.append((future, file_path))
                    pbar.update(1)
                    if downloaded:
                        time.sleep(REQUEST_DELAY)
            pbar.close()
            for future, file in tqdm(futures, desc="Processing GRIB files"):
                try:
                    df = future.result(timeout=120)
                    if df is not None and not df.empty:
                        dataframes.append(df)
                        logging.info(f"Processed data from {file}.")
                    else:
                        logging.error(f"No data extracted from {file}.")
                except TimeoutError:
                    logging.error(f"Processing {file} timed out.")
                except Exception as exc:
                    logging.error(f"Exception while processing {file}: {exc}")

        if os.path.exists(OUTPUT_CSV):
            os.remove(OUTPUT_CSV)
            logging.info(f"Deleted existing CSV file at {OUTPUT_CSV}.")
        if dataframes:
            final_df = pd.concat(dataframes, ignore_index=True)
            final_df["datetime"] = pd.to_datetime(final_df["datetime"])
            final_df.sort_values(by="datetime", inplace=True)
            final_df.to_csv(OUTPUT_CSV, index=False)
            logging.info("CSV file sorted by datetime column.")
        else:
            logging.error("No data was extracted in Option 1.")
            print("No data was extracted in Option 1.")

    warn_missing_files(YEARS, DATA_DIR)
    
    overall_end_time = time.time()
    total_time = overall_end_time - overall_start_time
    logging.info("Data processing completed.")
    logging.info(f"Total time: {total_time:.2f} seconds")
    print("Data processing completed.")
    print(f"Total time: {total_time:.2f} seconds")


if __name__ == "__main__":
    main()
