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


logger = logging.getLogger(__name__)

__version__ = 1.35
l2_versions = {
    "StreamLine": "1.16",
    "WLS70": "1.22",
    "w400s": "1.32",
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
paper_doi = "(paper in prep)"
metadata_doi = "(metadata documentation in prep)"

start_datetime_full = "2022-06-15T00:00:00"
end_datetime_full = "2024-04-02T00:00:00"
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
                if not str(dat.attrs['production_version']) == str(l2_version):
                    raise ValueError("Product version mismatch")
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

            dat_out = xr.merge(dat_list)
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
                "metadata_doi": f"paper: {paper_doi}. metadata: {metadata_doi}",
                "processing_level": "L3",
                "processing_level_description": f"Level 3 (L3): Raw observed data files are converted to L1. QAQC applied at L2. Individual files combined and harmonised at L3. Consult corresponding paper {paper_doi} for details.",
                "processing_name": "L2_to_L3.py",
                "processing_version_L3": str(__version__),
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
