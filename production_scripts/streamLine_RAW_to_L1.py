# -*- coding: utf-8 -*-
"""
Created on Tue Nov 21 08:46:52 2023

@author: willm
"""
# python .\halo-reader-write.py --startdate 2023-06-01 --enddate 2023-06-02
import argparse
from haloreader.exceptions import BackgroundCorrectionError
import glob
from pathlib import Path
from haloreader.read import read, read_bg, Product
import pandas as pd
import xarray as xr
from haloreader.variable import Variable
import datetime as dt
import json
import os
import numpy as np
import harmonise
import logging
from meta import filemeta
import fnmatch
from haloreader import __version__ as __haloreader_version__

__version__ = "2.14"

author = "William Morrison"

os.environ["OMP_NUM_THREADS"] = '7'

program_summary = (
    f"Production of L1 horizontal wind profiles from RAW .hpl StreamLine scan "
    f"files using a modified version of "
    f"https://github.com/actris-cloudnet/halo-reader version {__haloreader_version__} "
    f"with code available on https://github.com/willmorrison1/paris-harmonised-dwl"
)
# todo
# what is the expected scan elevation? reject scans if scan angle is not np.close
EXPECTED_SCAN_ELEVATION = 75


def build_compression_dict(xr_ds):
    return dict.fromkeys(
        [a for a in xr_ds.data_vars], {'zlib': True, 'complevel': 3},
    )


def select_files_by_date(file_names, file_datetime, start_date, end_date):
    """
    Select files within a given start and end date.

    Args:
        file_names (list): List of file names.
        file_datetime (str): String with time wildcards.
        start_date (datetime): Start date.
        end_date (datetime): End date.

    Returns:
        list: List of selected file names.
    """

    selected_file_names = []
    for file_name in file_names:
        try:
            file_date = dt.datetime.strptime(file_name, file_datetime)
            if start_date <= file_date <= end_date:
                selected_file_names.append(file_name)
        except ValueError:
            pass

    return selected_file_names


def to_xarray(halo):
    """


    Parameters
    ----------
    halo : halo.Halo | halo.HaloWind
        take a halo-reader class and return the data in xarray dataset format

    Returns
    -------
    halo_xr : xr.Dataset

    """

    data_vars = dict()
    coords = dict()

    for field in halo.__dataclass_fields__.keys():
        if isinstance(getattr(halo, field), Variable):
            var = getattr(halo, field)
            if not var.dimensions:
                continue
            if field in var.dimensions:
                if field in coords.keys():
                    continue
                coords[field] = (list(var.dimensions), var.data)
                continue
            data_vars[field] = (list(var.dimensions), var.data)

    # metadata variables that need a time dimension
    metadata_with_time_dim = [
        "gate_length",
        "gate_range",
        "npulses",
        "nrays",
        "resolution",
        "wavelength",
        "scantype",
        "wind_elevation",
    ]

    for field in halo.metadata.__dataclass_fields__.keys():
        if field not in metadata_with_time_dim:
            continue
        var = getattr(halo.metadata, field)
        if "data" in var.__dataclass_fields__:
            data_vars[field] = (["time"], np.repeat(
                var.data, len(halo.time.data)))

    # sneaky add elevation
    if "elevation" in halo.__dataclass_fields__.keys():
        data_vars["elevation"] = (["time"], halo.elevation.data)

    halo_xr = xr.Dataset(
        data_vars=data_vars,
        coords=coords,
    )

    halo_xr["time"] = pd.to_datetime(halo_xr["time"], unit='s')

    return halo_xr


def add_degrees(wind_direction, wind_offset):
    new_wind_direction = (wind_direction + wind_offset) % 360
    return new_wind_direction


def return_file_type(deployment, file_type_name):
    for raw_file in deployment.raw_files:
        if raw_file['type'] == file_type_name:
            return raw_file


PROGRAM_NAME = "halo-reader"
log_dir = "C:/Users/willm/Desktop/halo-reader-logs/"
logger = logging.getLogger(__name__)

# https://core.ac.uk/download/pdf/43337194.pdf
# the signal intensity for each ray within a scan is evaluated. if the
# intensity is below min_valid_intensity_threshold_wind the value is rejected
min_valid_intensity_threshold_wind = 1.005
# how many days of background data to read for any given day?
bg_n_days_ago = 21


def valid_date(s):
    """Validates the date format YYYY-MM-DD."""
    try:
        return dt.datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        msg = "Invalid date format. Please use YYYY-MM-DD (e.g., 2023-11-16)."
        raise argparse.ArgumentTypeError(msg)


parser = argparse.ArgumentParser(description="Process start and end dates.")
parser.add_argument("-s", "--startdate",
                    help="Start date in format YYYY-MM-DD",
                    type=valid_date,
                    default='2023-09-01')
parser.add_argument("-e", "--enddate",
                    help="End date in format YYYY-MM-DD",
                    type=valid_date,
                    default='2023-09-14')

args = parser.parse_args()

ARCHIVE_DIR = os.path.join(
    "D:/urbisphere/status-meteo-archive-offline/srv/meteo/archive/urbisphere/",
    "data/RAW/by-source/smurobs/by-serialnr/France/Paris/StreamLine/",
)
BASE_DIR = harmonise.L1_BASEDIR

start_date = args.startdate
end_date = args.enddate

logging.basicConfig(
    filename=f"{log_dir}/{PROGRAM_NAME}_{start_date}-{end_date}_{dt.datetime.utcnow().strftime('%Y%m%d%H%M%S')}.log",
    filemode='a',
    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
    datefmt='%H:%M:%S',
    level=logging.INFO)

logging.info(f'STARTEND{start_date} {end_date}')
logging.info(f'Command line arguments {args}')
logging.info(f"{PROGRAM_NAME} program version {__version__}")

with open("meta/deployments-DWL.json") as json_file:
    deployments = json.load(json_file)

deployments_df = pd.json_normalize(deployments, sep="_")

# hard-coded as daily files for now
dates = pd.date_range(start=start_date, end=end_date, freq="D")

product = Product.WIND

for date in dates:
    logging.info(date)
    start_date = date.to_pydatetime()
    end_date = start_date + dt.timedelta(hours=23, minutes=59, seconds=59)
    for i, deployment in deployments_df.iterrows():
        is_deployed = (
            start_date <= dt.datetime.fromisoformat(
                deployment.end_datetime)) & (
            end_date >= dt.datetime.fromisoformat(
                deployment.start_datetime))

        if not is_deployed:
            logging.debug(
                f" For {start_date} - {end_date}, deployment "
                f"{deployment.station_code} ({deployment.instrument_serial}) "
                f"{deployment.start_datetime} - {deployment.end_datetime} is "
                f"not deployed. Skip"
            )
            continue

        instrument_serial = deployment.instrument_serial
        raw_files_dir = os.path.join(ARCHIVE_DIR, instrument_serial)
        if not os.path.exists(raw_files_dir):
            continue
        os.chdir(raw_files_dir)
        all_files = os.listdir(raw_files_dir)
        do_bg_corr = not pd.isna(deployment.get("do_bg_corr"))
        if do_bg_corr:
            bg_file_datetime = "Background_%d%m%y-%H%M%S.txt"
            all_bg_files = glob.glob("Background_??????-??????.txt")
            bg_files = select_files_by_date(
                all_bg_files, bg_file_datetime, start_date -
                dt.timedelta(days=bg_n_days_ago),
                end_date)
            bg_paths = [Path(file) for file in bg_files]
            halobg = read_bg(bg_paths)
            if not halobg:
                continue

        for product in [Product.WIND]:
            file_type = return_file_type(deployment, product.value)
            if not file_type:
                logging.error(
                    f"sn {instrument_serial} on {date} has no {product.value} "
                    f"product. Skip."
                )
                continue
            file_datetime = file_type["datetime_pattern"].format(
                instrument_serial=instrument_serial)
            files = select_files_by_date(
                all_files, file_datetime, start_date, end_date)
            for pattern in filemeta.reject_files_glob_L0:
                files = [file for file in files if not fnmatch.fnmatch(
                    file, pattern)]
            files = [Path(file) for file in files]
            if not files:
                continue
            try:
                if not halobg and do_bg_corr:
                    logging.warn(
                        f"Wanted to do bg corr on {str(date)} but no bg files "
                        f"found for sn {instrument_serial}"
                    )
                halo = read(files, product=product)
            except Exception as e:
                logging.error(
                    f"Could not read from files: {files} with error {e}")
                continue
            if not halo:
                logging.error(f"Could not read from files: {files}.")
                continue
            if do_bg_corr:
                try:
                    halo.correct_background(halobg)
                except BackgroundCorrectionError as e:
                    logging.error(
                        f"{e} for instrument {instrument_serial} on {date}")
                    continue

            filename_template = "halo-reader_{product_name}_" + \
                f"{date.strftime('%Y%m%d')}_{instrument_serial}_{__version__}"

            file_name = os.path.join(
                BASE_DIR, f"{instrument_serial}/{filename_template}.nc")
            if not os.path.exists(os.path.dirname(file_name)):
                os.makedirs(os.path.dirname(file_name))
            azimuth_offset = deployment.get("options_azimuth_offset")
            if not pd.isna(azimuth_offset):
                az_offset = azimuth_offset
                halo.azimuth.data = add_degrees(
                    halo.azimuth.data, az_offset)
            if product.value == "wind":
                if not halo.is_useful_for_product(product):
                    logging.error("Wind product not useful for wind calc")
                    continue
                wind = halo.compute_wind(
                    halobg=halobg if do_bg_corr else None,
                    min_valid_intensity=min_valid_intensity_threshold_wind)
                xr_dat = to_xarray(wind)
                prod_date = dt.datetime.now(dt.timezone.utc).isoformat()
                attrs = {
                    "production_program": PROGRAM_NAME,
                    "production_version": __version__,
                    "production_date": prod_date,
                    "production_comment": program_summary,
                    "production_author": author,
                }
                xr_dat.attrs = attrs

            file_name_out = file_name.format(product_name=product.name)
            xr_dat.to_netcdf(file_name_out,
                             encoding=build_compression_dict(xr_dat))
            logging.info(f"Wrote {file_name_out} {dict(xr_dat.dims)}")
