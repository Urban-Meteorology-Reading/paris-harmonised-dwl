# -*- coding: utf-8 -*-
"""
Created on Wed Jun 19 15:07:07 2024

@author: willm
"""

from glob import glob
import xarray as xr
import os
from datetime import datetime as dt
import harmonise
import numpy as np

__version__ = 1.02

# gates lower than INVALID_LOW_RANGE_GATE_M are rejected
INVALID_LOW_RANGE_GATE_M = 45
# rm values if fewer than MIN_CONSECUTIVE_RANGE_GATES of data in a row
MIN_CONSECUTIVE_RANGE_GATES = 3
# individual scan has > WIND_RMSE_VALID_WARN rmse? flag warn
WIND_RMSE_VALID_WARN = 2
# individual scan has > WIND_RMSE_VALID_WARN rmse? flag err and reject
WIND_RMSE_VALID_ERR = 3
# individual scan has < WIND_RMSE_VALID_ERR? flag err and reject
NRAYS_PC_VALID = 75
# https://core.ac.uk/download/pdf/43337194.pdf
# individual scan has mean intensity < INTENSITY_VALID_WARN ? flag warn
INTENSITY_VALID_WARN = 1.007585
# individual scan has mean intensity < INTENSITY_VALID_ERR ? flag err and reject
INTENSITY_VALID_ERR = 1.0055

L1_version = "2.14"
INPUT_FILENAME_GSUB = f"*/halo-reader_WIND_????????_*{L1_version}.nc"
PRODUCT_NAME = "streamLine_L1_to_L2"
PRODUCT_LEVEL = 2



def streamline_flag_low_signal_removed(dat):

    # mean intensity valid
    low_signal_removed = dat.wind_mean_intensity < INTENSITY_VALID_ERR
    dat["zonal_wind"] = dat.zonal_wind.where(~low_signal_removed)
    dat["meridional_wind"] = dat.meridional_wind.where(~low_signal_removed)
    low_signal_removed.rename("flag_low_signal_removed")
    dat["flag_low_signal_removed"] = low_signal_removed

    return dat


def streamline_flag_low_signal_warn(dat):

    # mean intensity valid
    low_signal_warn = (dat.wind_mean_intensity < INTENSITY_VALID_WARN) & (
        dat.wind_mean_intensity > INTENSITY_VALID_ERR)
    low_signal_warn.rename("flag_low_signal_warn")
    dat["flag_low_signal_warn"] = low_signal_warn

    return dat


def streamline_flag_suspect_retrieval_warn(dat):
    # despeckle data and warn
    despeckle_invalid = dat.zonal_wind.notnull().rolling(
        range=MIN_CONSECUTIVE_RANGE_GATES, center=True).sum()
    despeckle_invalid = ((despeckle_invalid > 0) & (
        despeckle_invalid < MIN_CONSECUTIVE_RANGE_GATES))
    # don't flag the first gates
    despeckle_invalid[:, :MIN_CONSECUTIVE_RANGE_GATES] = False
    gate_length = np.unique(np.diff(dat.range)).item()
    # disregard despeckle in first range gates. leave that to
    # INVALID_LOW_RANGE_GATE_M QC
    despeckle_invalid[:, dat.range < (
        INVALID_LOW_RANGE_GATE_M +
        (gate_length * (MIN_CONSECUTIVE_RANGE_GATES / 2)))] = False

    rmse_warn = dat.wind_rmse > WIND_RMSE_VALID_WARN

    flag_suspect_retrieval_warn = despeckle_invalid | rmse_warn
    flag_suspect_retrieval_warn.rename("flag_suspect_retrieval_warn")
    dat["flag_suspect_retrieval_warn"] = flag_suspect_retrieval_warn

    return dat


def streamline_flag_suspect_retrieval_removed(dat):
    first_gates_invalid = xr.zeros_like(dat.wind_rmse, dtype=bool)
    first_gates_invalid[:, dat.range < INVALID_LOW_RANGE_GATE_M] = True

    # pc valid
    nrays = dat.nrays.median().values
    nrays_invalid = (((dat.nrays_valid / nrays) * 100) < NRAYS_PC_VALID)
    # rmse
    rmse_invalid = dat.wind_rmse > WIND_RMSE_VALID_ERR

    # add more when needed
    flag_suspect_retrieval_removed = first_gates_invalid | nrays_invalid | \
        rmse_invalid
    flag_suspect_retrieval_removed.rename("flag_suspect_retrieval_removed")
    dat["flag_suspect_retrieval_removed"] = flag_suspect_retrieval_removed
    dat["zonal_wind"] = dat.zonal_wind.where(~flag_suspect_retrieval_removed)
    dat["meridional_wind"] = dat.meridional_wind.where(
        ~flag_suspect_retrieval_removed)

    return dat


def streamline_harmonise_varnames(dat):

    from_to_list = [
        ("zonal_wind", "u"),
        ("meridional_wind", "v"),
        ("nrays", "n_rays_in_scan"),
        ("gate_length", "raw_gate_length"),
        ("npulses", "n_pulses"),
    ]
    drop_list = [
        "zonal_wind",
        "meridional_wind",
        "nrays",
        "gate_length",
        "npulses",
    ]

    for from_to in from_to_list:
        dat[from_to[1]] = dat[from_to[0]].rename([from_to[1]])

    dat = dat.drop(drop_list)

    return dat


def main():
    files = glob(os.path.join(harmonise.L1_BASEDIR, INPUT_FILENAME_GSUB))
    for file in files:
        system_serial = os.path.basename(os.path.dirname(file))

        file_date = dt.strptime(os.path.basename(file).split("_")[2], "%Y%m%d")
        dat = xr.load_dataset(file)
        dat = streamline_flag_suspect_retrieval_removed(dat)
        dat = streamline_flag_low_signal_removed(dat)
        dat = streamline_flag_low_signal_warn(dat)
        dat = streamline_flag_suspect_retrieval_warn(dat)

        dat = streamline_harmonise_varnames(dat)
        dat = harmonise.flag_ws_out_of_range(dat)
        dat = harmonise.select_preharmonisation_data_vars(dat)
        dat.attrs = {"production_level": PRODUCT_LEVEL,
                     "production_version": __version__,
                     "production_name": PRODUCT_NAME,
                     }
        OUTPUT_FILE = harmonise.PRODUCT_FILENAME_TEMPLATE.format(
            product_name=PRODUCT_NAME, product_level=PRODUCT_LEVEL,
            product_version=__version__, system_serial=system_serial)
        out_file = dt.strftime(file_date, OUTPUT_FILE)
        out_dir = os.path.join(harmonise.L2_BASEDIR, out_file)
        if not os.path.exists(os.path.dirname(out_dir)):
            os.makedirs(os.path.dirname(out_dir))
        dat.to_netcdf(out_dir, encoding=harmonise.encode_nc_compression(dat))
        print(out_dir)


if __name__ == "__main__":
    main()
