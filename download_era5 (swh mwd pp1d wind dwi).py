#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ERA5 Hourly Data Downloader and Statistics Generator

This script downloads hourly ERA5 reanalysis data for a specified location (Horta, Faial)
using the Climate Data Store (CDS) API. Data is downloaded on a monthly basis (to comply with
API rate limits) and then processed to extract select meteorological and oceanographic variables.
The processed data is appended to a CSV file for further analysis.

Key Features:
-------------
- **Hourly Data Retrieval**: Downloads data at an hourly resolution.
- **Selected Variables**: Downloads specific variables.
    - swh  : Significant height of combined wind waves and swell
    - mwd  : Mean wave direction
    - pp1d : Peak wave period
    - wind : 10 metre wind speed
    - dwi  : 10 metre wind direction
- **Monthly Downloads**: Retrieves data in monthly chunks to help manage API limitations.
- **Retry Mechanism**: Implements retries (with exponential back-off) in case a download fails.
- **Time Statistics**: Computes and reports the total processing time, as well as the average time
  per month and per year.
- **Logging**: Detailed logs are maintained for every download attempt and processing step.

Requirements:
-------------
- Python 3.x
- Libraries:
    - `cdsapi`
    - `pygrib`
    - `pandas`
    - `tqdm`
    - `logging`
- ECCODES: Required for `pygrib`. Install via Conda:

    conda install -c conda-forge eccodes
    conda install -c conda-forge cdsapi
    conda install -c conda-forge pygrib
    conda install -c conda-forge tqdm

ECMWF Data Info:
----------------
- More info: https://www.ecmwf.int
- ERA5 reanalysis dataset: https://www.ecmwf.int/en/forecasts/dataset/ecmwf-reanalysis-v5
- Parameter info: https://codes.ecmwf.int/grib/param-db/
"""

import cdsapi
import os
import time
import pygrib
import pandas as pd
from tqdm import tqdm
import logging

# ----------------------------- Configuration -----------------------------

# Location Coordinates (Leixões Costeira, Porto/Portugal)
LONGITUDE = -8.983333
LATITUDE = +41.31666

# Years to process
START_YEAR = 1940
END_YEAR = 2025
YEARS = list(range(START_YEAR, END_YEAR + 1))

# Variables to Retrieve (Short Names mapped to Numerical Parameter IDs)
VARIABLES = {
    'swh':  '140229',  # Significant height of combined wind waves and swell
    'mwd':  '140230',  # Mean wave direction
    'pp1d': '140231',  # Peak wave period
    'wind': '140245',  # 10 metre wind speed
    'dwi':  '140249'   # 10 metre wind direction
}

# Output directories
DATA_DIR = 'grib'
RESULTS_DIR = 'results'
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)

BUFFER = 0.25  # Degrees to define bounding box

# Define bounding box [North, West, South, East]
NORTH = LATITUDE + BUFFER
SOUTH = LATITUDE - BUFFER
EAST = LONGITUDE + BUFFER
WEST = LONGITUDE - BUFFER
AREA = [NORTH, WEST, SOUTH, EAST]

# Grid resolution
GRID = [0.25, 0.25]

# Rate-limiting and retry parameters
REQUEST_DELAY = 60  # seconds between requests (also used for retry delays)
MAX_RETRIES = 3

# Logging configuration
LOG_FILE = 'download_era5_data.log'
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# CSV output initialization
OUTPUT_CSV = os.path.join(RESULTS_DIR, 'download_era5_data.csv')
if not os.path.exists(OUTPUT_CSV):
    headers = ['datetime'] + list(VARIABLES.keys())
    df_init = pd.DataFrame(columns=headers)
    df_init.to_csv(OUTPUT_CSV, index=False)
    logging.info(f"Initialized CSV file with headers at {OUTPUT_CSV}.")

# ----------------------------- Functions -----------------------------

def initialize_cds_client():
    """Initializes and returns the CDS API client."""
    try:
        client = cdsapi.Client()
        logging.info("CDS API client initialized successfully.")
        return client
    except Exception as e:
        logging.error(f"Failed to initialize CDS API client. Error: {e}")
        exit(1)

def download_monthly_data(client, year, month, variable_list, area, grid, output_dir):
    """
    Downloads ERA5 monthly data for the given year and month.

    Parameters:
    -----------
    client : cdsapi.Client
        The CDS API client.
    year : int
        The year for which data is to be downloaded.
    month : int
        The month for which data is to be downloaded.
    variable_list : list
        List of variable codes to download.
    area : list
        Bounding box defined as [North, West, South, East].
    grid : list
        Grid resolution defined as [lat_resolution, lon_resolution].
    output_dir : str
        Directory where the downloaded file will be saved.

    Returns:
    --------
    file_path : str or None
        The path to the downloaded GRIB file, or None if download failed.
    """
    file_name = f"ERA5_{year}_{month:02d}.grib"
    file_path = os.path.join(output_dir, file_name)

    # Skip download if file already exists
    if os.path.exists(file_path):
        logging.info(f"Data for {year}-{month:02d} already exists. Skipping download.")
        return file_path

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logging.info(f"Attempt {attempt}: Downloading data for {year}-{month:02d}...")
            client.retrieve(
                'reanalysis-era5-single-levels',
                {
                    'product_type': 'reanalysis',
                    'format': 'grib',
                    'variable': variable_list,  # Numeric parameter codes
                    'year': str(year),
                    'month': f"{month:02d}",
                    'day': [f"{d:02d}" for d in range(1, 32)],
                    'time': [f"{h:02d}:00" for h in range(24)],
                    'area': area,   # [North, West, South, East]
                    'grid': grid,   # [lat_resolution, lon_resolution]
                },
                file_path
            )
            logging.info(f"Successfully downloaded data for {year}-{month:02d}.")
            return file_path
        except Exception as e:
            logging.warning(f"Attempt {attempt}: Failed to download data for {year}-{month:02d}. Error: {e}")
            if attempt < MAX_RETRIES:
                wait_time = REQUEST_DELAY * attempt
                logging.info(f"Retrying after {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logging.error(f"All {MAX_RETRIES} attempts failed for {year}-{month:02d}. Skipping.")
                return None

def process_grib_file(file_path, output_csv):
    """
    Reads a GRIB file, extracts data for the specified variables, and appends the results to a CSV file.

    Parameters:
    -----------
    file_path : str
        The path to the GRIB file.
    output_csv : str
        The path to the output CSV file.

    Returns:
    --------
    bool
        True if processing and CSV appending are successful, False otherwise.
    """
    try:
        grbs = pygrib.open(file_path)
    except Exception as e:
        logging.error(f"Failed to open GRIB file {file_path}. Error: {e}")
        return False

    # Log the available shortNames in the file
    available_shortnames = []
    try:
        for grb in grbs:
            sn = grb.shortName
            if sn not in available_shortnames:
                available_shortnames.append(sn)
        logging.info(f"Available shortNames in {file_path}: {available_shortnames}")
    except Exception as e:
        logging.error(f"Error while reading GRIB messages in {file_path}: {e}")
    finally:
        grbs.close()

    # Reopen the file for data extraction
    try:
        grbs = pygrib.open(file_path)
    except Exception as e:
        logging.error(f"Failed to reopen GRIB file {file_path}. Error: {e}")
        return False

    data_records = {}
    for grb in grbs:
        try:
            short_name = grb.shortName
            valid_time = grb.validDate.strftime('%Y-%m-%d %H:%M:%S')

            # Only process the variables in our target list
            if short_name in VARIABLES:
                var_key = short_name
            else:
                continue

            data_array, _, _ = grb.data()
            value = data_array[0, 0]  # Extract data from the single grid point

            if valid_time not in data_records:
                data_records[valid_time] = {}
            data_records[valid_time][var_key] = value

        except Exception as e:
            logging.warning(f"Error processing a GRIB message in {file_path}: {e}")
            continue

    grbs.close()

    # Convert extracted data into a DataFrame
    records = []
    for dt, vars_data in data_records.items():
        row = {'datetime': dt}
        for k in VARIABLES.keys():
            row[k] = vars_data.get(k, None)
        records.append(row)

    if not records:
        logging.warning(f"No valid data extracted from {file_path}.")
        return False

    df = pd.DataFrame(records)

    # Append data to the CSV file
    try:
        df.to_csv(output_csv, mode='a', header=False, index=False)
        logging.info(f"Appended data from {file_path} to {output_csv}.")
        return True
    except Exception as e:
        logging.error(f"Failed to append data to CSV. Error: {e}")
        return False

# ----------------------------- Main Execution -----------------------------
def main():
    """
    Main execution function:
    - Initializes the CDS client.
    - Iterates over the specified years and months to download and process data.
    - Waits between requests to comply with rate limits.
    - Computes and logs overall and average time statistics.
    """
    overall_start_time = time.time()
    client = initialize_cds_client()

    # Prepare the list of numeric parameter codes to retrieve
    variable_list = list(VARIABLES.values())

    total_requests = len(YEARS) * 12
    pbar = tqdm(total=total_requests, desc="Downloading ERA5 Data")

    for year in YEARS:
        for month in range(1, 13):
            monthly_start = time.time()

            file_path = download_monthly_data(
                client,
                year,
                month,
                variable_list,
                AREA,
                GRID,
                DATA_DIR
            )
            if file_path:
                success = process_grib_file(file_path, OUTPUT_CSV)
                if success:
                    logging.info(f"Processed and appended data for {year}-{month:02d}.")
                else:
                    logging.error(f"Failed to process data for {year}-{month:02d}.")
            else:
                logging.error(f"Skipping processing for {year}-{month:02d} due to download failure.")

            pbar.update(1)
            # Pause to comply with rate limiting
            time.sleep(REQUEST_DELAY)

    pbar.close()
    overall_end_time = time.time()

    # Compute time statistics
    total_time = overall_end_time - overall_start_time  # Total elapsed time in seconds
    total_months = len(YEARS) * 12
    avg_time_per_month = total_time / total_months
    avg_time_per_year = total_time / len(YEARS)

    # Log and print the statistics
    logging.info("Data download and processing completed.")
    logging.info(f"Total time: {total_time:.2f} seconds")
    logging.info(f"Average time per month: {avg_time_per_month:.2f} seconds")
    logging.info(f"Average time per year: {avg_time_per_year:.2f} seconds")

    print("Data download and processing completed.")
    print(f"Total time: {total_time:.2f} seconds")
    print(f"Average time per month: {avg_time_per_month:.2f} seconds")
    print(f"Average time per year: {avg_time_per_year:.2f} seconds")

if __name__ == "__main__":
    main()
