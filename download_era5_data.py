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
         - Processes the resulting GRIB files to extract a pre-defined set of meteorological 
           and oceanographic variables using Inverse Distance Weighting (IDW) for interpolation.
         - Saves the combined data into a CSV file, sorted by datetime.
    2. Extract Only:
         - Skips the download phase and directly processes all available GRIB files locally.
         - Uses parallel processing with progress monitoring to efficiently extract data.
         - In addition to matching GRIB messages by their short names, it also extracts data 
           using param IDs when available.

Detailed Functionality:
------------------------
1. CDS API and MARS Requests:
   - The request dictionary is built following the strict syntax required by the MARS system.
     For example, keys such as 'product_type', 'format', 'param', 'year', 'month', 'day', 'time', 
     'area', and 'grid' must be provided in the correct format.
   - This script builds the request using only official ERA5 param IDs (as strings) to avoid ambiguity.
   - The 'area' key is specified as [North, West, South, East] and 'time' values are provided in "HH:00:00" format.
   - For further details, refer to:
     https://confluence.ecmwf.int/display/UDOC/MARS+user+documentation

2. Unified Variable Mapping:
   - A unified dictionary called `VARIABLES` contains both the official ERA5 param ID and the expected 
     GRIB short name for each variable.
   - From this mapping, a list of param IDs (`PARAM_IDS`) is derived for the CDS API request and a 
     GRIB message key mapping (`GRIB_KEY_MAP`) is created to map the GRIB message short names to internal keys.

3. GRIB File Processing:
   - GRIB files are processed using the pygrib library.
   - For each GRIB message, the script first attempts to match the short name to our internal keys.
   - If the short name is not found, it falls back to comparing the GRIB message’s parameter number 
     (if available) to the expected param IDs.
   - The script uses Inverse Distance Weighting (IDW) interpolation to estimate the value at the target coordinate.
   - Extracted data from all GRIB files are combined into a pandas DataFrame, sorted by datetime, and exported as a CSV file.

4. Robust Error Handling and Logging:
   - Detailed logging records each major step and any encountered issues.
   - The download process is retried multiple times with increasing delay intervals if failures occur.
   - After processing, the script checks for missing monthly GRIB files and issues warnings accordingly.

Usage:
------
When executed, the user is prompted to choose between:
    - Option 1: Download ERA5 data via the CDS API (using MARS syntax) and process the downloaded GRIB files.
    - Option 2: Only process existing GRIB files in the data directory.
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
# The VARIABLES dictionary defines each variable to be processed.
# Each key represents the internal variable name and its value is a dictionary with:
#   - 'param_id': The official ERA5 param ID used in the CDS/MARS request.
#   - 'grib_short': The expected GRIB message short name in the downloaded files.
# This unified mapping reduces redundancy and improves maintainability.
VARIABLES = {
    'swh':  {'param_id': '140229', 'grib_short': 'swh'},   # Significant height of combined wind waves and swell
    'mwd':  {'param_id': '140230', 'grib_short': 'mwd'},    # Mean wave direction
    'pp1d': {'param_id': '140231', 'grib_short': 'pp1d'},   # Peak wave period
    'wind': {'param_id': '140245', 'grib_short': 'wind'},   # 10 metre wind speed
    'dwi':  {'param_id': '140249', 'grib_short': 'dwi'}     # 10 metre wind direction
}

# Derive the list of param IDs for the CDS API/MARS request.
PARAM_IDS = [v['param_id'] for v in VARIABLES.values()]

# Derive the mapping for GRIB processing.
# This maps the GRIB message short name (as provided in the downloaded files)
# to our internal variable key.
GRIB_KEY_MAP = {v['grib_short']: key for key, v in VARIABLES.items()}

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

    This function scans the data directory for expected GRIB files (one per month per year)
    and logs any files that are missing. This helps in ensuring data completeness.
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

    The CDS API client allows the script to interface with the ECMWF CDS/MARS system to request data.
    For more details on the CDS API and its configuration, refer to the MARS User Documentation:
    https://confluence.ecmwf.int/display/UDOC/MARS+user+documentation
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

    The request dictionary is built following the strict syntax required by the MARS system,
    as documented in the official MARS User Documentation:
    https://confluence.ecmwf.int/display/UDOC/MARS+user+documentation
    Key elements include:
        - 'product_type': Typically set to 'reanalysis'.
        - 'format': The desired output format, here 'grib'.
        - 'param': List of parameter IDs (using PARAM_IDS derived from VARIABLES).
        - 'year', 'month', 'day', 'time': Date and time specifications (with 'time' in HH:00:00 format).
        - 'area': The geographical bounding box defined as [North, West, South, East].
        - 'grid': The resolution of the grid.

    If the data file already exists locally, the download is skipped.

    Parameters:
        client (cdsapi.Client): The CDS API client instance.
        year (int): Year for which data is requested.
        month (int): Month for which data is requested.
        area (list): Geographical bounding box.
        grid (list): Grid resolution.
        output_dir (str): Directory to store the downloaded GRIB file.

    Returns:
        tuple: (file_path, downloaded) where downloaded is True if a new download occurred.
    """
    file_name = f"ERA5_{year}_{month:02d}.grib"
    file_path = os.path.join(output_dir, file_name)
    
    if os.path.exists(file_path):
        logging.info(f"Data for {year}-{month:02d} exists. Skipping download.")
        return file_path, False

    days_in_month = calendar.monthrange(year, month)[1]
    days = [f"{d:02d}" for d in range(1, days_in_month + 1)]
    
    # Build the request dictionary using only the 'param' key with our unified param IDs.
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

    This function opens a GRIB file using the pygrib library, iterates through each GRIB message,
    and uses Inverse Distance Weighting (IDW) interpolation to estimate the value at the target coordinate.
    If an exact grid point match is found, that value is used directly.
    
    The GRIB messages are first filtered by checking if their short name matches our expected keys.
    If not, the function falls back to checking the message's parameter number (if available) against
    our expected ERA5 param IDs. This allows Option 2 to extract data using param IDs as well.

    Parameters:
        file_path (str): The path to the GRIB file to be processed.

    Returns:
        pandas.DataFrame or None: DataFrame containing the extracted data (with a 'datetime' column),
        or None if no data was extracted.
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
            # First, try to match using GRIB message short name.
            if grb.shortName in GRIB_KEY_MAP:
                var_key = GRIB_KEY_MAP[grb.shortName]
            else:
                # Fallback: try matching using parameter number (param id).
                try:
                    param_num = grb.parameterNumber  # Typically an integer.
                except AttributeError:
                    param_num = None
                if param_num is not None:
                    for key, value in VARIABLES.items():
                        if int(value['param_id']) == param_num:
                            var_key = key
                            break
            # Skip this message if no matching variable is found.
            if var_key is None:
                continue

            data_array, lats, lons = grb.data()
            dist = np.sqrt((lats - LATITUDE)**2 + (lons - LONGITUDE)**2)
            # Apply IDW interpolation.
            if np.any(dist < 1e-6):
                # Use exact grid point value if available.
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

    This function prompts the user to choose between two operational modes:
        1. Download ERA5 data via the CDS API (using MARS syntax) and then process the GRIB files.
        2. Only process existing GRIB files in the data directory.

    After processing, the script checks for any missing GRIB files in the specified years range,
    logs performance metrics, and saves the output CSV sorted by datetime.

    For detailed information on the CDS API and MARS request syntax, please refer to:
    https://confluence.ecmwf.int/display/UDOC/MARS+user+documentation
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
        # Option 1: Download (or verify) files sequentially, then process in parallel.
        client = initialize_cds_client()
        total_requests = len(YEARS) * 12
        pbar = tqdm(total=total_requests, desc="Downloading ERA5 Data")
        grib_files_to_process = []
        for year in YEARS:
            for month in range(1, 13):
                file_path, downloaded = download_monthly_data(client, year, month, AREA, GRID, DATA_DIR)
                if file_path:
                    grib_files_to_process.append(file_path)
                pbar.update(1)
                if downloaded:
                    time.sleep(REQUEST_DELAY)
        pbar.close()

        if os.path.exists(OUTPUT_CSV):
            os.remove(OUTPUT_CSV)
            logging.info(f"Deleted existing CSV file at {OUTPUT_CSV}.")
        dataframes = []
        timeout_per_file = 120  # seconds per file processing timeout.
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
