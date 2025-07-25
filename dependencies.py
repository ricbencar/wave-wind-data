# -*- coding: utf-8 -*-
"""
This script checks for the presence of essential Python libraries
required to run 'download_era5 (swh mwd pp1d wind dwi).py'.
It also lists the installed version number for each library.
It provides guidance on installing missing libraries and
notes specific requirements like ECCODES for pygrib.
"""

import sys
import os
import importlib.metadata # Used to retrieve installed package versions

def check_library(library_name, pypi_name=None, extra_info=None):
    """
    Attempts to import a library, gets its installed version, and prints its status.
    Args:
        library_name (str): The name of the library to import (e.g., 'pandas').
        pypi_name (str, optional): The name of the package on PyPI if different
                                   from library_name (e.g., 'cdsapi'),
                                   or includes a suggested version for installation.
        extra_info (str, optional): Additional information or troubleshooting
                                    tips for the library.
    """
    # Extract package name from pypi_name if it includes a version (e.g., 'cdsapi==0.7.6')
    actual_pypi_name = pypi_name.split('==')[0] if pypi_name else library_name
    install_suggestion = pypi_name if pypi_name else library_name # Use the full string for pip install

    try:
        # Attempt to import the library
        __import__(library_name)
        
        # Try to get the installed version
        installed_version = "N/A" # Default if version can't be found or for built-in modules
        try:
            # Use the actual package name for importlib.metadata
            installed_version = importlib.metadata.version(actual_pypi_name)
        except importlib.metadata.PackageNotFoundError:
            # This can happen for some built-in modules or if metadata is missing
            pass
        except Exception as e:
            # Catch any other unexpected errors during version retrieval
            installed_version = f"Error: {e}"

        print(f"✅ '{library_name}' is installed. Version: {installed_version}")

    except ImportError:
        print(f"❌ '{library_name}' is NOT installed.")
        print(f"   Please install it using pip: pip install {install_suggestion}")
        if extra_info:
            print(f"   Note: {extra_info}")
    except Exception as e:
        print(f"⚠️  An unexpected error occurred while checking '{library_name}': {e}")
        print(f"   Details: {e}")

def main():
    """
    Main function to run all library checks.
    """
    print("\n" + "="*60)
    print("--- Checking Essential Python Libraries and Versions ---".center(60))
    print("This script will verify if the necessary libraries for".center(60))
    print("'download_era5 (swh mwd pp1d wind dwi).py' are installed and list their versions.".center(60))
    print("="*60 + "\n")

    # List of libraries to check: (library_name, pypi_name_for_install_and_version_check, extra_info)
    # The pypi_name_for_install_and_version_check should include '==' if a specific version
    # is suggested for installation, but the check will only report the installed version.
    libraries_to_check = [
        # Core dependencies from the original script
        ("cdsapi", "cdsapi==1.1.0", "Ensure your ~/.cdsapirc file is configured correctly for CDS API access."),
        ("pygrib", "pygrib==2.1.6", "pygrib requires ECCODES to be installed and configured on your system (e.g., via apt, brew, or source). Refer to the pygrib documentation for details. If you encounter 'numpy.dtype size changed' errors, ensure pygrib and numpy are compatible versions."),
        ("pandas", "pandas==2.3.1", None),
        ("numpy", "numpy==2.3.2", "If you encounter 'numpy.dtype size changed' errors, ensure pygrib is compatible with your NumPy version."),
        ("tqdm", "tqdm==4.67.1", None),

        # New packages to check
        ("fpdf", "fpdf==1.7.2", "Used for generating PDF documents."),
        ("matplotlib", "matplotlib==3.10.3", "Used for plotting and visualization."),
        ("scipy", "scipy==1.16.0", "Scientific computing library, often used with numpy."),
        ("windrose", "windrose==1.9.2", "Specialized library for plotting windrose diagrams."),

        # Standard Python libraries (no pypi_name or version needed)
        ("logging", None, "This is a standard Python library and should be available by default."),
        ("concurrent.futures", None, "This is a standard Python library for parallelism."),
        ("multiprocessing", None, "This is a standard Python library for parallelism."),
        ("calendar", None, "This is a standard Python library for calendar-related functions."),
        ("sys", None, "This is a standard Python library for system-specific parameters and functions."),
        ("os", None, "This is a standard Python library for interacting with the operating system."),
        ("time", None, "This is a standard Python library for time-related functions."),
    ]

    for lib, pypi, info in libraries_to_check:
        check_library(lib, pypi, info)
    
    print("\n" + "="*60)
    print("Library Check Complete.".center(60))
    print("If any '❌' or '⚠️' symbols appeared, please address the issues.".center(60))
    print("Remember to activate your virtual environment (venv or conda) before installing/reinstalling packages.".center(60))
    print("="*60 + "\n")

if __name__ == "__main__":
    # It's good practice to set the multiprocessing start method early,
    # especially for applications using pygrib which can have C-library interactions.
    try:
        import multiprocessing
        # 'spawn' is generally safer for cross-platform compatibility and C extensions
        multiprocessing.set_start_method("spawn", force=True)
    except RuntimeError:
        # This can happen if set_start_method is called more than once
        # or if the context is already set.
        pass
    except ImportError:
        print("Warning: 'multiprocessing' library not found. Parallel processing features might be unavailable.")

    main()
