# -*- coding: utf-8 -*-
"""
Created on Tue Jun 18 14:26:02 2024

@author: willm
"""

from glob import glob
import xarray as xr
import os
from datetime import datetime as dt
import harmonise

KNOWN_GATE_LENGTH = 50
AGGREGATION_INTERVAL = "10min"
# L1 data availability < DATA_AVAILABILITY_SUSPECT_WARN_THRESHOLD? flag warn
DATA_AVAILABILITY_SUSPECT_WARN_THRESHOLD = 75
# L1 data availability < DATA_AVAILABILITY_SUSPECT_REMOVED_THRESHOLD? flag err and remove
DATA_AVAILABILITY_SUSPECT_REMOVED_THRESHOLD = 10

INPUT_FILENAME_GSUB = "wlscerea_1a_windLz1Lb87M10mn-HR_v02_*"
INPUT_FILE_DT = "wlscerea_1a_windLz1Lb87M10mn-HR_v02_%Y%m%d_%H%M%S_1440.nc"
PRODUCT_NAME = "wls70"
SYSTEM_SERIAL = "10"
PRODUCT_LEVEL = 2
__version__ = 1.15


def wls70_flag_suspect_retrieval_warn_and_removed(dat):
    """


    Parameters
    ----------
    dat : xarray.core.dataset.Dataset
        wlscerea_1a_windLz1Lb87M10mn-HR_v02 data product with u and v components.
        Flag the suspect retrieval that need removing, remove them in the u
        and v components, then return the dataset with the removed data and
        the new flag. Retrieval is suspect (removed) if data availability 
        is less than DATA_AVAILABILITY_SUSPECT_WARN_THRESHOLD 
        (DATA_AVAILABILITY_SUSPECT_REMOVED_THRESHOLD) percent

    Returns
    -------
    dat : xarray.core.dataset.Dataset
        the dataset updated with this QC step.

    """

    suspect_retrieval_warn = (
        dat.data_availability <= DATA_AVAILABILITY_SUSPECT_WARN_THRESHOLD) & \
        (dat.data_availability >= DATA_AVAILABILITY_SUSPECT_REMOVED_THRESHOLD)
    suspect_retrieval_warn.rename("flag_suspect_retrieval_warn")
    dat["flag_suspect_retrieval_warn"] = suspect_retrieval_warn

    data_availavility_suspect_removed = dat.data_availability < \
        DATA_AVAILABILITY_SUSPECT_REMOVED_THRESHOLD
    suspect_elevated_retrievals = (
        dat.w < -2.5) & (dat.u < 1) & (dat.range > 750)
    suspect_retrieval_removed = data_availavility_suspect_removed | \
        suspect_elevated_retrievals
    suspect_retrieval_removed.rename("flag_suspect_retrieval_removed")
    dat["flag_suspect_retrieval_removed"] = suspect_retrieval_removed
    dat["u"] = dat["u"].where(~suspect_retrieval_removed)
    dat["v"] = dat["v"].where(~suspect_retrieval_removed)

    return dat


def wls70_get_scan_elevation(dat):
    # get the scan elevation through time. It is given as angle from horizon
    # and only one value per file. Use "temp_int" variable which is a 1D
    # variable (through time)
    scan_elevation = xr.zeros_like(dat.temp_int).rename("scan_elevation")
    scan_elevation = scan_elevation + 90 - dat.scan_angle
    dat["scan_elevation"] = scan_elevation

    return dat


def prepare_harmonisation(file):
    file_date = dt.strptime(os.path.basename(file), INPUT_FILE_DT)
    dat = xr.load_dataset(file)

    dat = wls70_flag_suspect_retrieval_warn_and_removed(dat)
    dat = harmonise.flag_ws_out_of_range(dat, ws_var_name="ws")
    dat = wls70_get_scan_elevation(dat)
    dat = harmonise.select_preharmonisation_data_vars(dat)
    OUTPUT_FILE = harmonise.PRODUCT_FILENAME_TEMPLATE.format(
        product_name=PRODUCT_NAME, product_level=PRODUCT_LEVEL,
        product_version=__version__, system_serial=SYSTEM_SERIAL)
    out_file = dt.strftime(file_date, OUTPUT_FILE)
    out_dir = os.path.join(harmonise.L2_BASEDIR, out_file)
    if not os.path.exists(os.path.dirname(out_dir)):
        os.makedirs(os.path.dirname(out_dir), exist_ok=True)
    print(out_dir)

    dat.to_netcdf(out_dir, encoding=harmonise.encode_nc_compression(dat))


def main():

    files = glob(os.path.join(harmonise.L1_BASEDIR,
                              SYSTEM_SERIAL, INPUT_FILENAME_GSUB))
    for file in files:
        prepare_harmonisation(file)


if __name__ == "__main__":
    main()
