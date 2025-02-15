# ERA5 Data Downloader

This repository contains a Python script to download and process **ERA5 reanalysis data** using the **Climate Data Store (CDS) API** provided by ECMWF. The script:

- `download_era5 (swh mwd pp1d wind dwi).py` - Downloads **wave height, wave direction, peak wave period, wind speed, and wind direction**.

## 📌 Features
- Downloads hourly **ERA5 reanalysis** data.
- Retrieves selected meteorological and oceanographic variables.
- Uses a retry mechanism with **exponential back-off** to handle API failures.
- Saves processed data in **CSV format** for further analysis.
- Uses **logging** to record download and processing steps.

## 📂 Files
| File | Description |
|------|-------------|
| `download_era5 (swh mwd pp1d wind dwi).py` | Retrieves wind and ocean wave data. |
| `download_era5_data.log` | Log file storing execution details. |
| `grib/` | Folder for storing raw GRIB files. |
| `results/download_era5_data.csv` | Processed data in CSV format. |

---

## 🌊 About the ERA5 Wave Model
This dataset is derived from the **ECMWF Reanalysis v5 (ERA5) wave model**, which provides hourly estimates of essential climate variables spanning from 1940 to the present. The ERA5 wave model is a component of the ERA5 dataset, developed by the **European Centre for Medium-Range Weather Forecasts (ECMWF)**. 

### ERA5 Wave Model Highlights:
- Uses **state-of-the-art** numerical weather prediction models and data assimilation techniques.
- Provides **hourly data** at a **31 km horizontal resolution** globally.
- Includes **wind-wave interactions, swell propagation, and wave generation** mechanisms.
- Incorporates **satellite observations, buoy measurements, and reanalysis techniques** to improve accuracy.
- Supplies a comprehensive **historical dataset** for research, operational forecasting, and climate applications.

More details can be found at:
- [ERA5 Single Levels Dataset](https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels?tab=overview)
- [ECMWF ERA5 Overview](https://www.ecmwf.int/en/forecasts/dataset/ecmwf-reanalysis-v5)

---

## 🚀 Installation
### 1️⃣ Install Dependencies
Ensure you have **Python 3.x** installed. Then install the required libraries using **Conda**:

```sh
conda install -c conda-forge eccodes cdsapi pygrib pandas tqdm
```

Alternatively, use **pip**:

```sh
pip install cdsapi pygrib pandas tqdm
```

### 2️⃣ Set Up CDS API Key
1. Register for an **ECMWF account** at: [CDS Registration](https://cds.climate.copernicus.eu/user/register)
2. Obtain your **API key** from: [CDS API](https://cds.climate.copernicus.eu/api-how-to)
3. Create a `.cdsapirc` file in your home directory (`~/.cdsapirc` on Linux/Mac, `C:\Users\YourName\.cdsapirc` on Windows):

```ini
url: https://cds.climate.copernicus.eu/api/v2
key: YOUR-USER-ID:YOUR-API-KEY
verify: 1
```

---

## 📌 Usage
### Run the Script
Use the following command to run the script:

```sh
python "download_era5 (swh mwd pp1d wind dwi).py"
```

### 🎯 Configurable Parameters
The script retrieves data for **Leixões Costeira, Porto (Portugal)** with coordinates **(41.31666°N, -8.983333°W)**. You can modify these values in the script:

```python
LONGITUDE = -8.983333
LATITUDE = +41.31666
```

It downloads data from **1940 to 2025**. To change the time range, update:

```python
START_YEAR = 1940
END_YEAR = 2025
```

### 📜 Variables Retrieved
| Variable | Short Name | Description |
|----------|-----------|-------------|
| `swh` | 140229 | Significant height of combined wind waves and swell |
| `mwd` | 140230 | Mean wave direction |
| `pp1d` | 140231 | Peak wave period |
| `wind` | 140245 | 10m wind speed |
| `dwi` | 140249 | 10m wind direction |

---

## 📂 Data Storage
The downloaded data is stored in:
- **GRIB files** in the `grib/` folder.
- **Processed CSV data** in `results/download_era5_data.csv`.

A sample CSV row looks like:

```csv
datetime,swh,mwd,pp1d,wind,dwi
1940-01-01 00:00:00,2.5,280,8.0,5.2,220
```

---

## 🔗 References
- [CDS API Installation](https://confluence.ecmwf.int/display/CKB/How+to+install+and+use+CDS+API+on+Windows)
- [CDS Documentation](https://confluence.ecmwf.int/display/CKB/Climate+Data+Store+%28CDS%29+documentation)
- [ERA5 Single Levels Dataset](https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels?tab=overview)
- [ECMWF ERA5 Overview](https://www.ecmwf.int/en/forecasts/dataset/ecmwf-reanalysis-v5)
- [Parameter Info](https://codes.ecmwf.int/grib/param-db/)

---
