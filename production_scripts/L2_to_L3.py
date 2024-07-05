# -*- coding: utf-8 -*-
"""
Created on Fri Jun 21 14:03:13 2024

@author: willm
"""

import harmonise
import numpy as np
import pandas as pd
import glob
import os
import datetime as dt
import xarray as xr
import logging

# todo: expand and fill arrays to give continuous dataset filled with na where no data
# todo: add maintenance events mask (manually identified, if needed)
# todo: add misc data removal  mask (manually identified, if needed)
# todo: add system_is_deployed boolean flag
# todo: add L2 versions to L2 atts
# todo: add L3 attrs
# todo: download 2021 eiffel tower data

# todo: IMPORTANT: CONVERT RANGE TO VERTICAL HEIGHT ABOVE INSTRUMENT FOR ALL INSTRUMENT TYPES at L2 level
# todo: Figure out how to handle concurrent station deployments error for 2022-12-07 00:00:00 - 2022-12-08 00:00:00
# todo: DATA_AVAILABILITY_SUSPECT_WARN_THRESHOLD = 75 wls70 - check if that's reasonable. see e.g. Jul 12 2022. Top of BL is lost.
# todo: Jul 13 morning w400s std full profile threhsold has removed a "good" set of profiles
# todo: w400s go over "uv_std_threshold" again. why are there stripes in the flag_suspect_removal. also after oct 4 2022 w400s there is a change in scan and some of the wind proifles are filtered  - check sd filter
# todo: w400s sep 12 retrieval bad aroud midday
# todo: Sep 26 the timesteps seem off for 30 in L3 product.
# todo: Dec 7 - 8 L3 not run why
# todo: remove range data_var and check that height asl is an output 1D (time) data var


# mostly done or low prio:
# todo: fine-tune QC for StreamLine based on quicklooks
# todo: add nsamples?
# todo: check that wls70 and w400s range gates are middle of gate coord
# todo: rerun scatter plots with new asl range adjust fix (not + half range gate..)
# todo: add rain mask from dwl measurements - why?


# done:
# todo: IMPORTANT: StreamLine RAW to L1 need to account for different deployments at same date
# todo: 2024-02-09 - 2024-02-12 ARBO 204 missing: halo = read(files, product=product) inconsistent azimuths. why? rejected - bad raw data
# todo: StreamLine 30: what is going on with background correction throughout deployment? SNR thresh adjusted
# todo: the R scatter plot code looks wrong. what's happening when I change closest_range? Should expect offset but I don't get that.
# todo: 2023-09-21 sn 30 missing (ask Jeremie)
# todo: ask Jeremie for 20220819, 20220827, 20220908, 20221010, 20230608, 20230921
# todo: User5_204_20240209 - User5_204_20240212 why no read? bad scan angles - some issue with scan
# todo: 20221207 is change of scan pattern. remove first or second half of raw data
# todo: s/n 30 2022-08-08 why not processed
# todo: decide max range based on % data availability across all data. currently 4 km. go to 5 km
# todo: if not in, process PAROIS 6 point VAD for 2022 and check
# wls70: QC for zero ws and noisy wd around 1km and 2km. check u v components e.g. Jun 21 2022
# streamline 30 why VAD res changes 22nd June 2022? and to what
# todo: check PAJUSS 28th Jun 2022: std thresholds hit too much? - ok
# todo: no n rays in scan in PAJUSS or PASIRT - doesnt exist
# todo: update PAROIS deployment so that it's clear about the 60 min low vad (see other todos first to check what actually is being processed - low horizon or 6 point?)
# todo: check PAROIS 6 point vs 120 point
# todo: check what has happened with the low n point ROIS scan. Is it in? or is pre-20221207 a 6 point scan? done and downloaded
# todo: harmonise.time_resample don't create variables if they dont exist before e.g. n_pulses
# todo: npulses time dim only
# investigate PA:JUSS L1-L2 QC in more detail and do pre-average
# todo: '2023-09-14 00:00:00' harmonise issue
# todo: time attr comment: Label represents end of {time_window_s} s
# todo: fix time dim label "from"
# todo: '2023-09-13 PAARBO no longer being saved- to do with delpoyments to-from and/or RAW-L2 processing
# todo: '2023-09-18 L3: __resample_dim__ must not be empty
# todo: system_id has incorrect attr definitions
# todo: figure out what to do with 25 m interpolated wls70 when it should be 50 m
# todo: figure out compression of strings so that I can add scan type and system_id str
# todo: fix range oversampled....
# todo: 2024-02-07 PALUPD 26 missing

logger = logging.getLogger(__name__)

__version__ = 1.13
logging.basicConfig(
    filename=f"C:/Users/willm/Desktop/L2_to_L3_logs/{dt.datetime.utcnow().strftime('%Y%m%d%H%M%S')}.log",
    filemode='a',
    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
    datefmt='%H:%M:%S',
    level=logging.INFO)

logging.info(f"L2_to_L3.py program version {__version__}")

product_name = "paris_dwl_L3"
time_aggs = [60*10]  # [60*10, 60*60]
min_height = 100
max_height = 7000
res_height = 25
input_dir = harmonise.L2_BASEDIR
deployments = harmonise.get_deployments()
deployments_df = pd.json_normalize(deployments, sep="_")
stations = np.unique(deployments_df.station_code)


start_datetime_full = "2023-09-02T00:00:00"
end_datetime_full = "2023-09-12T00:00:00"
file_freq = "24h"
datetime_range = pd.date_range(
    start_datetime_full, end_datetime_full, freq=file_freq)

for i in range(0, len(datetime_range)-1):
    try:
        for time_agg in time_aggs:

            start_datetime = str(datetime_range[i])
            end_datetime = str(datetime_range[i+1])
            start_datetime_dt = dt.datetime.fromisoformat(start_datetime)
            end_datetime_dt = dt.datetime.fromisoformat(end_datetime)
            dat_list = []
            for station in stations:
                # find the deployment with matching station code and dates
                d = deployments_df[
                    (deployments_df.station_code == station) &
                    (start_datetime <= deployments_df.end_datetime) &
                    (end_datetime >= deployments_df.start_datetime)
                ]

                if d.shape[0] > 1:
                    logging.error(
                        "Figure out how to handle concurrent station deployments")
                    raise ValueError(
                        "Figure out how to handle concurrent station deployments")

                if d.shape[0] == 0:
                    # print(
                    #     f"No deployment for {station} "
                    #     f"{start_datetime} - {end_datetime}"
                    # )
                    continue

                filenames = []
                # be certain that we load all the aggregation period data
                date_from = dt.datetime.fromisoformat(
                    start_datetime) - dt.timedelta(seconds=time_agg)
                date_to = dt.datetime.fromisoformat(end_datetime)

                for date in pd.date_range(date_from, date_to):
                    date_string = date.strftime("%Y%m%d")
                    glob_str = f"*{date_string}*{d.instrument_serial.item()}*.nc"
                    files_glob = os.path.join(
                        input_dir, d.instrument_serial.item(), glob_str)
                    filenames.extend(glob.glob(files_glob))

                if len(filenames) == 0:
                    logging.info(
                        f"{station}({d.instrument_serial.item()}) "
                        f"{start_datetime_dt.strftime('%Y%m%d %H')}->"
                        f"{end_datetime_dt.strftime('%Y%m%d %H')} no files found"
                    )
                    continue

                dat = xr.open_mfdataset(filenames)
                dat = dat.sel(time=slice(start_datetime_dt, end_datetime_dt))
                if len(dat.time) == 0:
                    logging.info(
                        f"{station}({d.instrument_serial.item()}) "
                        f"{start_datetime.strip(' 00:00:00')} -> "
                        f"{end_datetime.strip(' 00:00:00')} no files found"
                    )
                    continue
                dat = harmonise.sea_level_adjust(
                    dat, d.above_sea_level_height.item())
                dat = harmonise.height_resample(
                    dat, min_height, max_height, res_height)
                dat = harmonise.time_resample(dat, time_agg)
                ws, wd = harmonise.vector_to_ws_wd(dat.u.values, dat.v.values)
                dat = dat.assign(ws=(["time", "height"], ws),
                                 wd=(["time", "height"], wd))
                # appropriate unless multiple system IDs in one file interval (maybe)
                # regardless, an exception for that is raised earlier. so I will be able
                # to check that if it happen
                dat = harmonise.add_system_id_var(
                    dat, d.instrument_serial.item())

                dat = dat.expand_dims(dim="station_code").assign_coords(
                    station_code=("station_code", [station]))
                dat = harmonise.apply_attrs(dat, level=3)
                dat.time.attrs["comment"] = dat.time.attrs["comment"].format(
                    time_window_s=time_agg)
                dat_list.append(dat.load())
            if not dat_list:
                continue

            dat_out = xr.concat(dat_list, dim="station_code")

            nc_file = "{product_name}V{version}_{start_time}_{end_time}_{time_agg}s.nc".format(
                product_name=product_name,
                start_time=start_datetime_dt.strftime("%Y%m%d%H%M"),
                end_time=end_datetime_dt.strftime("%Y%m%d%H%M"),
                time_agg=time_agg,
                version=__version__)
            nc_file_full = os.path.join(harmonise.L3_BASEDIR, nc_file)
            print(nc_file_full)
            dat_out.to_netcdf(path=nc_file_full,
                              encoding=harmonise.encode_nc_compression(dat_out))

    except Exception as e:
        logging.error(f"{e} error for {start_datetime} - {end_datetime}")
        continue
