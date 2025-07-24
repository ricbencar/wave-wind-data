# ERA5 Hourly Data Downloader and Extractor

## Overview:

This script is designed to work with ERA5 reanalysis data from ECMWF using both the CDS API and the MARS (Meteorological Archive and Retrieval System). MARS is ECMWF’s archive retrieval system that enables users to request data using a strictly defined syntax. Detailed information on MARS request syntax and best practices can be found in the official [MARS User Documentation](https://confluence.ecmwf.int/display/UDOC/MARS+user+documentation).
(https://github.com/user-attachments/assets/c3d475af-8c52-497c-a51a-dc59fa92a0c7)
The script supports two operational modes:

1. **Download & Process:**

   * Downloads ERA5 data in monthly chunks from the CDS API (using MARS syntax rules).

   * Processes the resulting GRIB files to extract a pre-defined set of meteorological and oceanographic variables using Inverse Distance Weighting (IDW) for interpolation.

   * Saves the combined data into a CSV file, sorted by datetime.

2. **Extract Only:**

   * Skips the download phase and directly processes all available GRIB files locally.

   * Uses parallel processing with progress monitoring to efficiently extract data.

   * In addition to matching GRIB messages by their short names, it also extracts data using param IDs when available.

## Detailed Functionality:

1. **CDS API and MARS Requests:**

   * The request dictionary is built following the strict syntax required by the MARS system. For example, keys such as `product_type`, `format`, `param`, `year`, `month`, `day`, `time`, `area`, and `grid` must be provided in the correct format.

   * This script builds the request using only official ERA5 param IDs (as strings) to avoid ambiguity.

   * The `area` key is specified as `[North, West, South, East]` and `time` values are provided in "HH:00:00" format.

   * For further details, refer to the [MARS User Documentation](https://confluence.ecmwf.int/display/UDOC/MARS+user+documentation).

2. **Unified Variable Mapping:**

   * A unified dictionary called `VARIABLES` contains both the official ERA5 param ID and the expected GRIB short name for each variable.

   * From this mapping, a list of param IDs (`PARAM_IDS`) is derived for the CDS API request and a GRIB message key mapping (`GRIB_KEY_MAP`) is created to map the GRIB message short names to internal keys.

3. **GRIB File Processing:**

   * GRIB files are processed using the `pygrib` library.

   * For each GRIB message, the script first attempts to match the short name to our internal keys.

   * If the short name is not found, it falls back to comparing the GRIB message’s parameter number (if available) to the expected param IDs.

   * The script uses Inverse Distance Weighting (IDW) interpolation to estimate the value at the target coordinate.

   * Extracted data from all GRIB files are combined into a pandas DataFrame, sorted by datetime, and exported as a CSV file.

4. **Robust Error Handling and Logging:**

   * Detailed logging records each major step and any encountered issues.

   * The download process is retried multiple times with increasing delay intervals if failures occur.

   * After processing, the script checks for missing monthly GRIB files and issues warnings accordingly.

## Usage:

When executed, the user is prompted to choose between:

* **Option 1:** Download ERA5 data via the CDS API (using MARS syntax) and process the downloaded GRIB files.

* **Option 2:** Only process existing GRIB files in the data directory.

## Installation:

To run `download_era5 (swh mwd pp1d wind dwi).py`, you need to install Python 3.x and several libraries. Additionally, `ECCODES` is a crucial non-Python dependency for `pygrib`.

### 1. Install ECCODES:

`ECCODES` is a software package developed by ECMWF for processing WMO FM-92 GRIB, WMO FM-94 BUFR, and WMO CREX messages. It is required for `pygrib` to function correctly.

**On Ubuntu/Debian:**

```
sudo apt-get update
sudo apt-get install libeccodes-dev
```

**On CentOS/RHEL/Fedora:**

```
sudo yum install eccodes-devel
# Or for Fedora:
sudo dnf install eccodes-devel
```

**On macOS (using Homebrew):**

```
brew install eccodes
```

**On Windows:**
Installing `ECCODES` on Windows can be more involved. It's often recommended to use a Linux subsystem (WSL) or a virtual machine. If you must install directly on Windows, you might need to build it from source or find pre-compiled binaries. Refer to the official ECMWF ECCODES documentation for detailed instructions: [ECMWF ECCODES Documentation](https://www.google.com/search?q=https://confluence.ecmwf.int/display/ECC/ECCODES%2Binstallation).

### 2. Install Python Dependencies:

Once `ECCODES` is installed, you can install the Python libraries using `pip`. It's highly recommended to use a virtual environment to manage dependencies.

```
# Create a virtual environment
python3 -m venv venv

# Activate the virtual environment
# On Windows:
.\venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install the required Python packages
pip install cdsapi==1.1.0 \
            pygrib==1.2.5 \
            pandas==2.3.1 \
            numpy==2.0.2 \
            tqdm==4.66.4
```

*Note: The versions listed above are those found to be compatible with the script from the provided `requirements.txt` file. While `logging` is a standard Python library and doesn't require separate installation, it's included in the script's dependencies.*

## Compiling to an Executable with PyInstaller:

You can compile the script into a standalone executable using `PyInstaller`. This allows the script to be run on systems without Python installed, provided the necessary `ECCODES` libraries are present on the target system.

1. **Install PyInstaller:**

   ```
   pip install pyinstaller
   ```

2. **Compile the script:**
   Navigate to the directory containing `download_era5 (swh mwd pp1d wind dwi).py` in your terminal and run:

   ```
   pyinstaller -F "download_era5 (swh mwd pp1d wind dwi).py"
   ```

   * The `-F` (or `--onefile`) option bundles everything into a single executable file.

   * This will create a `dist` folder in your current directory, which will contain the executable.

**Important Note for PyInstaller and `pygrib`:**
When compiling with PyInstaller, `pygrib` often requires special handling due to its underlying C libraries (`ECCODES`). You might encounter issues related to missing shared libraries (`.so`, `.dll`, `.dylib`) at runtime.

If the executable fails to run, you may need to:

* **Manually copy ECCODES libraries:** Locate the `ECCODES` shared libraries on your system (e.g., `libeccodes.so`, `eccodes.dll`) and place them in the same directory as your PyInstaller executable, or in a location where the system can find them (e.g., `PATH` on Windows, `LD_LIBRARY_PATH` on Linux).

* **Use PyInstaller hooks:** For more complex scenarios, you might need to create a custom PyInstaller hook for `pygrib` to ensure all necessary data and binary files are included. This is an advanced topic and would involve creating a `.py` file with PyInstaller hook specifications.

## ECMWF Data Information:

* **Website:** [ECMWF](https://www.ecmwf.int)

* **ERA5 reanalysis dataset:** [ERA5 Dataset](https://www.ecmwf.int/en/forecasts/dataset/ecmwf-reanalysis-v5)

* **Parameter reference:** [Parameter Database](https://codes.ecmwf.int/grib/param-db/)

For more detailed information on CDS API and MARS request syntax, please refer to the [MARS User Documentation](https://confluence.ecmwf.int/display/UDOC/MARS+user+documentation).
