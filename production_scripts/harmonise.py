# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 11:47:12 2024

@author: willm
"""
import numpy as np
import json
import xarray as xr
from vardimdefs import vardimdefs
from definitions import *

class RangeGateLengthNotIdentical(Exception):
    pass


def get_deployments(json_filename="meta/deployments-DWL.json"):
    with open(json_filename) as json_file:
        deployments = json.load(json_file)
    return deployments


def range_to_height_adjust(dat):

    elevation = np.unique(dat.elevation).item()

    height_values = dat.range * np.sin(np.deg2rad(elevation))

    raise NotImplementedError("Finish me!")


def sea_level_adjust(dat, instrument_sea_level):

    dat = dat.assign_coords(range=(dat.range + instrument_sea_level))  # HEIGHT

    return dat


def ws_wd_to_vector(ws, wd):

    u = -1 * ws * np.sin(wd * np.pi / 180)
    v = -1 * ws * np.cos(wd * np.pi / 180)

    return u, v


def vector_to_ws_wd(u, v):

    horizontal_wind_speed = np.sqrt(u**2 + v**2)
    horizontal_wind_direction = (np.arctan2(-u, -v) * 180/np.pi)
    horizontal_wind_direction[horizontal_wind_direction <= 0] += 360.

    return horizontal_wind_speed, horizontal_wind_direction


def select_preharmonisation_data_vars(dat):

    data_vars = [
        "u",
        "v",
        "number_of_rays",
        "raw_gate_length",
        "n_rays_in_scan",
        "n_pulses",
        "flag_low_signal_warn",
        "flag_low_signal_removed",
        "flag_suspect_retrieval_warn",
        "flag_suspect_retrieval_removed",
        "flag_ws_out_of_range",
    ]
    dat = dat[[var for var in dat.data_vars if var in data_vars]]

    return dat


def encode_nc_compression(dat):
    not_compressed = [
        "system_id",
    ]
    return {var: {'zlib': True, "complevel": 2}
            for var in dat.data_vars if var not in not_compressed}


def flag_ws_out_of_range(dat, ws_var_name="horizontal_wind_speed"):

    # get up-to-date mask of ws based on the masked QC vars
    ws_is_nan = np.isnan(dat["u"]) & np.isnan(dat["v"])
    ws = dat[ws_var_name].where(~ws_is_nan)

    ws_out_of_range = ws > MAX_VALID_WS
    ws_out_of_range.rename("flag_ws_out_of_range")
    dat["flag_ws_out_of_range"] = ws_out_of_range

    dat["u"] = dat["u"].where(~ws_out_of_range)
    dat["v"] = dat["v"].where(~ws_out_of_range)

    return dat


def apply_attrs(dat, level: int, vardimdefs=vardimdefs):
    attr_keys = ['standard_name', 'long_name', 'units', 'comment']

    for var in [*dat.coords, *dat.data_vars]:
        vardimdef = [
            d for d in vardimdefs if
            (d.get("name") == var) and
            (d.get("level") == level)
        ]
        if not vardimdef or len(vardimdef) > 1:
            print(f"{var} has incorrect attr definitions")
            continue
        vardimdef = vardimdef[0]
        attrs = {k: vardimdef[k] for k in attr_keys if vardimdef.get(k)}
        dat[var].attrs = attrs
    return dat


def time_resample(dat, res=600, vardimdefs=vardimdefs):
    out_list = []
    res = f"{res}s"

    n_maxsamples = xr.ones_like(dat.u).resample(time=res).count()

    for vardef in vardimdefs:
        if vardef.get("type") != "variable":
            continue
        if vardef.get("L2_name") in dat.data_vars:
            dat_var = dat[vardef["L2_name"]].resample(time=res)
            if vardef["L3_fun"] == "mean":
                dat_var = dat_var.mean()
            if vardef["L3_fun"] == "pc":
                dat_var = (dat_var.sum() / n_maxsamples) * 100
        else:
            continue
        dat_var = dat_var.rename(vardef["name"])
        out_list.append(dat_var)

    out_dat = xr.merge(out_list)
    return out_dat


def range_resample(dat, min_range, max_range, res_range):
    # if the range coordinate is not int, then there are some issues
    # so far just assume range coordinate is int or n.5, so use 0.5 res step
    range_gate_lengths = np.unique(np.diff(dat.range))
    if len(range_gate_lengths) > 1:
        raise RangeGateLengthNotIdentical
    dat = dat.sel(range=slice(0, max_range + (res_range * 2)))
    dat = dat.reindex(range=np.arange(min_range, max_range, 1),
                      method="nearest", tolerance=0.5)
    dat = dat.interpolate_na(dim="range", max_gap=res_range*2)
    dat = dat.sel(range=slice(min_range, max_range, res_range))

    return dat


def add_system_id_var(dat, system_id):

    system_id_values = np.full(
        dat.time.shape, system_id).astype("S" + str(len(system_id)))
    dat["system_id"] = xr.DataArray(
        data=system_id_values,
        coords={"time": dat.time},
        dims=["time"],
        name="system_id",
    )
    return dat
