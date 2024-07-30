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
import numpy as np

INPUT_FILENAME_GSUB = "w400s_1a_LqualairLzamIdbs_v01_*"
INPUT_FILE_DT = "w400s_1a_LqualairLzamIdbs_v01_%Y%m%d_%H%M%S_1440.nc"
SYSTEM_SERIAL = "WCS000243"
PRODUCT_NAME = "w400s_L1a"
OUTPUT_FILE = f"{PRODUCT_NAME}_%Y%m%d_%H%M%S_{SYSTEM_SERIAL}.nc"
ELEVATION_ANGLE = 75  # the elevation angle of the DBS scan
PRODUCT_LEVEL = 2
__version__ = 1.27


def gate_index_to_range(ds):
    first_gate_max_range = int(ds.range.meters_to_center_of_first_gate) + \
        (int(ds.range.meters_between_gates) / 2)
    gate_length = int(ds.range.meters_between_gates)
    range_values = (ds.gate_index * gate_length) + first_gate_max_range
    ds = ds.drop("range")
    ds = ds.assign_coords({"gate_index": range_values})
    ds = ds.rename({"gate_index": "range"})

    return ds


def w400s_apply_pre_aggregation_qc(dat, std_window="5min",
                                   fraction_above_maxws_threshold=0.01):
    """


    Parameters
    ----------
    dat : xarray.core.dataset.Dataset
        w400s_1a_LqualairLzamIdbs_v01 data product with u and v components.
        Flag the suspect retrievals that need removing, remove them in the u
        and v components, then return the dataset with the removed data and
        the new flag
    std_window: The time window for stdev calculation for suspect retrieval
        evaluation
    std_threshold: Values over std_threshold in std_window are considered
        suspect retrieval
    fraction_above_std_threshold: if std_threshold is passed more than
        std_threshold in the entire std_window, flag the entire time window
        (with std_window resolution) as flag_suspect_retrieval_removed

    Returns
    -------
    dat : xarray.core.dataset.Dataset
        the dataset updated with this QC step with internal flags (_flag_*)
        ready for interpretation by temporal aggregation steps

    """

    wind_speed_status_invalid = dat.wind_speed_status != 1
    dat["u"] = dat["u"].where(~wind_speed_status_invalid)
    dat["v"] = dat["v"].where(~wind_speed_status_invalid)
    # get the std across the time window (for each range gate)
    median_u = dat.u.resample(time=std_window).median()
    median_v = dat.v.resample(time=std_window).median()
    median_ws = (np.sqrt(median_u**2 + median_v**2))
    median_ws_threshold = median_ws > harmonise.MAX_VALID_WS
    # v_std_window = std_v > std_threshold
    # uv_std = (u_std_window | v_std_window)
    # get the fraction of range gates that are above the std threshold
    ws_f = (median_ws_threshold.sum(dim="range") / median_u.count(dim="range"))
    ws_threshold = ws_f.reindex_like(
        dat, method="nearest") > fraction_above_maxws_threshold

    suspect_retrieval_removed = wind_speed_status_invalid
    suspect_retrieval_removed[ws_threshold] = True
    dat["u"] = dat["u"].where(~suspect_retrieval_removed)
    dat["v"] = dat["v"].where(~suspect_retrieval_removed)
    dat["flag_wind_speed_status_invalid"] = wind_speed_status_invalid.rename(
        "flag_wind_speed_status_invalid")
    dat["flag_ws_threshold_invalid"] = ws_threshold.rename(
        "flag_ws_threshold_invalid")
    dat = harmonise.flag_ws_out_of_range(dat)

    return dat


def w400s_flag_suspect_retrieval_removed(dat):
    ws_threshold_invalid = dat.flag_ws_threshold_invalid_pc == 100
    flag_wind_speed_status_invalid = dat.flag_wind_speed_status_invalid_pc == 100

    suspect_retrieval_removed = (ws_threshold_invalid |
                                 flag_wind_speed_status_invalid)
    dat["u"] = dat["u"].where(~suspect_retrieval_removed)
    dat["v"] = dat["v"].where(~suspect_retrieval_removed)
    dat["flag_suspect_retrieval_removed"] = suspect_retrieval_removed
    return dat


def w400s_flag_suspect_retrieval_warn(dat):
    ws_threshold_warn = (dat.flag_ws_threshold_invalid_pc > 0) &\
        (dat.flag_ws_threshold_invalid_pc < 100)
    flag_wind_speed_status_warn = (dat.flag_wind_speed_status_invalid_pc > 0) &\
        (dat.flag_wind_speed_status_invalid_pc < 100)
    flag_ws_out_of_range_warn = (dat.flag_ws_out_of_range_pc > 0) &\
        (dat.flag_ws_out_of_range_pc < 100)

    suspect_retrieval_warn = (ws_threshold_warn |
                              flag_wind_speed_status_warn |
                              flag_ws_out_of_range_warn)
    dat["flag_suspect_retrieval_warn"] = suspect_retrieval_warn.rename(
        "flag_suspect_retrieval_warn")
    return dat


def w400s_flag_ws_out_of_range(dat):
    dat["flag_ws_out_of_range"] = dat.flag_ws_out_of_range_pc == 100
    return dat


def w400s_aggregate_time(dat, agg_res):

    agg_vars = []
    agg_vars.append(dat.u.resample(time=agg_res).mean().rename("u"))
    agg_vars.append(dat.v.resample(time=agg_res).mean().rename("v"))
    agg_vars.append(dat.elevation.resample(
        time=agg_res).min().rename("elevation_min"))
    agg_vars.append(dat.elevation.resample(
        time=agg_res).max().rename("elevation_max"))

    agg_vars.append(dat.v.resample(time=agg_res).count().rename("n_samples"))
    total_samples = xr.ones_like(dat.v).resample(
        time=agg_res).count().rename("total_samples")
    agg_vars.append(total_samples)

    for var in dat.data_vars:
        if "flag_" in var:
            flag_var = ((dat[var].resample(
                time=agg_res).sum() / total_samples) * 100).rename(var + "_pc")
            agg_vars.append(flag_var)

    return xr.merge(agg_vars)


def main():

    files = glob(os.path.join(harmonise.L1_BASEDIR,
                              SYSTEM_SERIAL, INPUT_FILENAME_GSUB))
    for file in files:
        file_date = dt.strptime(os.path.basename(file), INPUT_FILE_DT)
        dat = xr.load_dataset(file)
        dat = gate_index_to_range(dat)
        u, v = harmonise.ws_wd_to_vector(dat["horizontal_wind_speed"].values,
                                         dat["wind_direction"].values)
        dat["u"], dat["v"] = [(["time", "range"], i) for i in [u, v]]
        dat = w400s_apply_pre_aggregation_qc(dat)
        dat = w400s_aggregate_time(dat, agg_res="1min")
        dat = harmonise.range_to_height_adjust(dat, ELEVATION_ANGLE)
        dat = w400s_flag_suspect_retrieval_removed(dat)
        dat = w400s_flag_suspect_retrieval_warn(dat)
        dat = w400s_flag_ws_out_of_range(dat)
        dat = harmonise.select_preharmonisation_data_vars(dat)
        OUTPUT_FILE = harmonise.PRODUCT_FILENAME_TEMPLATE.format(
            product_name=PRODUCT_NAME, product_level=PRODUCT_LEVEL,
            product_version=__version__, system_serial=SYSTEM_SERIAL)
        out_file = dt.strftime(file_date, OUTPUT_FILE)
        out_dir = os.path.join(harmonise.L2_BASEDIR, out_file)
        if not os.path.exists(os.path.dirname(out_dir)):
            os.makedirs(os.path.dirname(out_dir), exist_ok=True)
        print(out_dir)
        dat.to_netcdf(
            out_dir, encoding=harmonise.encode_nc_compression(dat))


if __name__ == "__main__":
    main()
