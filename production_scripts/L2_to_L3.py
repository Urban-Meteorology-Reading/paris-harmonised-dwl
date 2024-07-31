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
# todo: Figure out how to handle concurrent station deployments error for 2022-12-07 00:00:00 - 2022-12-08 00:00:00
# todo: 20230903 to check this: w400s go over "uv_std_threshold" again. why are there stripes in the flag_suspect_removal. also after oct 4 2022 w400s there is a change in scan and some of the wind pofiles are filtered - check sd filter
# todo: w400s sep 12 retrieval bad aroud midday
# todo: StreamLine scan elevation check is np.close to 75. in compute_wind() add kwarg for manual elevation.

# todo: Sep 26 the timesteps seem off for 30 in L3 product.
# todo: Dec 7 - 8 L3 not run why?
# todo: Aug 8 and 9 2023 not processed L3 why? 16:06:06,288 root ERROR Resulting object does not have monotonic global indexes along dimension height error for 2023-08-08 00:00:00 - 2023-08-09 00:00:00
# s/n 30 ['D:/Urbisphere/sandbox/data/L2/by-serialnr/France/Paris/30\\streamLine_L2_V1.12_20230807_000000_30.nc', 'D:/Urbisphere/sandbox/data/L2/by-serialnr/France/Paris/30\\streamLine_L2_V1.12_20230808_000000_30.nc']


# todo low prio
# todo: add L2 versions to L2 atts
# todo: add system_is_deployed boolean flag
# todo: add L3 attrs

# todo done
# todo: DATA_AVAILABILITY_SUSPECT_WARN_THRESHOLD = 75 wls70 - check if that's reasonable. see e.g. Jul 12 2022. Top of BL is lost.

# todo: Jul 13 morning w400s std full profile threhsold has removed a "good" set of profiles
# todo: w400s Jun 22 2022 bad retrieval

logger = logging.getLogger(__name__)

__version__ = 1.3
l2_versions = {
    "StreamLine": "V1.12",
    "WLS70": "V1.21",
    "w400s": "V1.28",
}
logging.basicConfig(
    filename=f"C:/Users/willm/Desktop/L2_to_L3_logs/{dt.datetime.utcnow().strftime('%Y%m%d%H%M%S')}.log",
    filemode='a',
    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
    datefmt='%H:%M:%S',
    level=logging.INFO)

logging.info(f"L2_to_L3.py program version {__version__}")

product_name = "paris_dwl_L3"
time_aggs = [60*10]
input_dir = harmonise.L2_BASEDIR
deployments = harmonise.get_deployments()
deployments_df = pd.json_normalize(deployments, sep="_")
station_codes = np.unique(deployments_df.station_code)
stations = harmonise.get_stations()
stations_df = pd.json_normalize(stations, sep="_").rename(
    columns={"station_code": "station"}).set_index("station")


start_datetime_full = "2022-06-23T00:00:00"
end_datetime_full = "2024-06-24T00:00:00"
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
            for station_code in station_codes:
                # find the deployment with matching station code and dates
                d = deployments_df[
                    (deployments_df.station_code == station_code) &
                    (start_datetime <= deployments_df.end_datetime) &
                    (end_datetime >= deployments_df.start_datetime)
                ]

                if d.shape[0] > 1:
                    logging.error(
                        "Figure out how to handle concurrent station deployments")
                    raise ValueError(
                        "Figure out how to handle concurrent station deployments")

                if d.shape[0] == 0:
                    logging.debug(
                        f"No deployment for {station_code} "
                        f"{start_datetime} - {end_datetime}"
                    )
                    continue

                filenames = []
                # be certain that we load all the aggregation period data
                date_from = dt.datetime.fromisoformat(
                    start_datetime) - dt.timedelta(seconds=time_agg)
                date_to = dt.datetime.fromisoformat(end_datetime)
                l2_version = l2_versions[d.instrument_type.item()]

                for date in pd.date_range(date_from, date_to):
                    date_string = date.strftime("%Y%m%d")
                    glob_str = f"*{l2_version}_{date_string}*{d.instrument_serial.item()}*.nc"
                    files_glob = os.path.join(
                        input_dir, d.instrument_serial.item(), glob_str)
                    filenames.extend(glob.glob(files_glob))

                if len(filenames) == 0:
                    logging.info(
                        f"{station_code}({d.instrument_serial.item()}) "
                        f"{start_datetime_dt.strftime('%Y%m%d %H')}->"
                        f"{end_datetime_dt.strftime('%Y%m%d %H')} no files found"
                    )
                    continue
                dat = xr.open_mfdataset(filenames)
                dat = dat.sel(time=slice(start_datetime_dt, end_datetime_dt))
                if len(dat.time) == 0:
                    logging.info(
                        f"{station_code}({d.instrument_serial.item()}) "
                        f"{start_datetime.strip(' 00:00:00')} -> "
                        f"{end_datetime.strip(' 00:00:00')} no files found"
                    )
                    continue
                dat = harmonise.sea_level_adjust(
                    dat, d.above_sea_level_m.item())
                dat = harmonise.z_resample(
                    dat, harmonise.MIN_ALTITUDE, harmonise.MAX_ALTITUDE,
                    harmonise.RES_ALTITUDE)
                dat = harmonise.time_resample(dat, time_agg)
                ws, wd = harmonise.vector_to_ws_wd(dat.u.values, dat.v.values)
                dat = dat.assign(ws=(["time", "altitude"], ws),
                                 wd=(["time", "altitude"], wd))
                # inappropriate if multiple system IDs in one file interval
                # regardless, an exception for that is raised earlier
                dat = harmonise.add_system_id_var(dat, d.instrument_serial.item())
                dat = dat.expand_dims(dim="station").assign_coords(
                    station=("station", [station_code]))
                # dat = harmonise.apply_attrs(dat, level=3)
                dat_list.append(dat.load())
            if not dat_list:
                continue

            dat_out = xr.concat(dat_list, dim="station")
            # add meta data for station dimension (var(station))
            dat_out = dat_out.merge(xr.Dataset.from_dataframe(stations_df))
            dat_out = harmonise.apply_attrs(dat_out, level=3)
            dat_out.time.attrs["comment"] = dat_out.time.attrs["comment"].format(
                time_window_s=time_agg)

            attrs = {
                "title": "Harmonised boundary layer wind profile dataset from six ground-based doppler wind lidars across Paris, France",
                "creator_name": "William Morrison (william.morrison@meteo.uni-freiburg.de, williamtjmorrison@gmail.com)",
                "creator_institution": "Environmental Meteorology, Institute of Earth and Environmental Sciences, Faculty of Environment and Natural Resources, University of Freiburg, Freiburg, 79085, Germany",
                "principal_investigator": "Andreas Christen (andreas.christen@meteo.uni-freiburg.de)",
                "metadata_doi": "ESSC_DOI",
                "processing_level": "Level 3 (L3): Raw data converted to L1 then QAQC at L2 then harmonisation at L3. Consult metadata_doi for details.",
                "processing_name": "L2_to_L3.py",
                "processing_version_L3": __version__,
                "processing_version_L2": str(l2_versions),
                "processing_url": "https://github.com/willmorrison1/paris-harmonised-dwl, https://github.com/Urban-Meteorology-Reading/paris-harmonised-dwl",
                "processing_time_utc": dt.datetime.now(tz=dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "start_time_utc": start_datetime,
                "end_time_utc": end_datetime,
                "aggregation_time_s": time_agg,
            }
            dat_out.attrs = attrs

            nc_file = "{product_name}V{version}_{start_time}_{end_time}_{time_agg}s.nc".format(
                product_name=product_name,
                start_time=start_datetime_dt.strftime("%Y%m%d%H%M"),
                end_time=end_datetime_dt.strftime("%Y%m%d%H%M"),
                time_agg=time_agg,
                version=__version__)
            nc_file_full = os.path.join(harmonise.L3_BASEDIR, nc_file)
            logging.info(nc_file_full)
            dat_out.to_netcdf(path=nc_file_full,
                              encoding=harmonise.encode_nc_compression(dat_out))

    except Exception as e:
        logging.error(f"{e} error for {start_datetime} - {end_datetime}")
        continue
