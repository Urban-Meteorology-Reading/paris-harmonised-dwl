# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 11:47:12 2024

@author: willm
"""
import numpy as np
import json
import xarray as xr

RANGE_MIN = 0
RANGE_MAX = 5000
RANGE_RES = 25
MAX_VALID_WS = 60
WS_UNITS = "m.s^-1"
UNITLESS_UNITS = "unitless"
L1_BASEDIR = "D:/Urbisphere/sandbox/data/L1/by-serialnr/France/Paris/"
L2_BASEDIR = "D:/Urbisphere/sandbox/data/L2/by-serialnr/France/Paris/"
L3_BASEDIR = "D:/urbisphere/sandbox/data/L3/by-instrumentmodel/DWL/"
PRODUCT_FILENAME_TEMPLATE = (
    "{system_serial}/{product_name}_L{product_level}_V{product_version}_"
    "%Y%m%d_%H%M%S_{system_serial}.nc"
)


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


vardimdefs = [
    {
        "level": 3,
        "type": "variable",
        "L2_name": "u",
        "L3_fun": "mean",
        "name": "u",
        "standard_name": "eastward_wind",
        "units": WS_UNITS,
        "cell_methods": "time: mean",
        "comment": 'Averaged eastward wind component. Averaged over all valid'
        ' samples within the time aggregation interval.',
    },
    {
        "level": 3,
        "type": "variable",
        "L2_name": "v",
        "L3_fun": "mean",
        "name": "v",
        "standard_name": "northward_wind",
        "units": WS_UNITS,
        "cell_methods": "time: mean",
        "comment": 'Averaged northward wind component. Averaged over all valid'
        ' samples within the time aggregation interval.',
    },
    {
        "level": 3,
        "type": "variable",
        "L2_name": "flag_low_signal_warn",
        "L3_fun": "pc",
        "name": "flag_low_signal_warn",
        "long_name": "flag_low_signal_warn",
        "units": "%",
        "comment": (
            'The scan has a signal intensity below a threshold required for a '
            'non-suspect retrieval. Use retreival with caution.'
        ),
    },
    {
        "level": 3,
        "type": "variable",
        "L2_name": "flag_low_signal_removed",
        "L3_fun": "pc",
        "name": "flag_low_signal_removed",
        "long_name": "flag_low_signal_removed",
        "units": "%",
        "comment": (
            'The scan has a signal intensity below a threshold required for a '
            'valid retrieval. Retrieval rejected.'
        ),
    },
    {
        "level": 3,
        "type": "variable",
        "L2_name": "flag_suspect_retrieval_warn",
        "L3_fun": "pc",
        "name": "flag_suspect_retrieval_warn",
        "long_name": "flag_suspect_retrieval_warn",
        "units": "%",
        "comment": (
            'The scan is suspect based on tests unique to each system model. '
            'Use retreival with caution. Missing values indicate no test.'
        ),
    },
    {
        "level": 3,
        "type": "variable",
        "L2_name": "flag_suspect_retrieval_removed",
        "L3_fun": "pc",
        "name": "flag_suspect_retrieval_removed",
        "long_name": "flag_suspect_retrieval_removed",
        "units": "%",
        "comment": (
            'The scan is suspect based on system model specific tests. '
            'Retrieval rejected.'
        ),
    },
    {
        "level": 3,
        "type": "variable",
        "L2_name": "flag_ws_out_of_range_removed",
        "L3_fun": "pc",
        "name": "flag_ws_out_of_range",
        "long_name": "flag_ws_out_of_range",
        "units": "%",
        "comment":  (
            f'The retrieval exceed the horizontal wind speed threshold of '
            f'{MAX_VALID_WS} {WS_UNITS}. Retrieval rejected.'
        ),
    },
    {
        "level": 3,
        "type": "variable",
        "L2_name": "n_rays_in_scan",
        "L3_fun": "mean",
        "name": "n_rays_in_scan",
        "long_name": "number_of_rays_in_scan",
        "units": UNITLESS_UNITS,
        "comment":  (
            'The number of rays in a given \'scan_type\' scan.'
        ),
    },
    {
        "level": 3,
        "type": "variable",
        "L2_name": "raw_gate_length",
        "L3_fun": "mean",
        "name": "raw_gate_length",
        "long_name": "raw_gate_length",
        "units": "m",
        "comment":  (
            'The gate length of the raw data prior to L3 aggregation.'
        ),
    },
    {
        "level": 3,
        "type": "variable",
        "L2_name": "n_pulses",
        "L3_fun": "mean",
        "name": "n_pulses",
        "long_name": "number_of_pulses_in_ray",
        "units": UNITLESS_UNITS,
        "comment":  (
            'The number of pulses in a given ray. The more pulses the higher'
            'the integration time. Available for StreamLine models.'
        ),
    },
    {
        "level": 3,
        "type": "variable",
        "name": "ws",
        "standard_name": "wind_speed",
        "units": WS_UNITS,
        "comment":  (
            'Calculated from the u and v wind components.'
        ),
    },
    {
        "level": 3,
        "type": "variable",
        "name": "wd",
        "standard_name": "wind_from_direction",
        "units": WS_UNITS,
        "comment":  (
            'Calculated from the u and v wind components.'
        ),
    },
    {
        "level": 3,
        "type": "dimension",
        "name": "time",
        "standard_name": "time",
        "comment":  (
                'Label represents end of {time_window_s} s interval.'
        ),
    },
    {
        "level": 3,
        "type": "dimension",
        "name": "range",
        "long_name": "range",
        "units": "m",
        "comment":  (
                'Distance from sea level to center of range gate.'
        ),
    },
    {
        "level": 3,
        "type": "dimension",
        "name": "station_code",
        "long_name": "station_code",
        "units": "m",
        "comment":  (
                'Unique identifier for the measurement station.'
        ),
    },
    {
        "level": 3,
        "type": "variable",
        "name": "system_id",
        "long_name": "system_unique_id",
        "units": UNITLESS_UNITS,
        "comment":  ('The specific system (instrument) currently deployed '),
    },
]


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
