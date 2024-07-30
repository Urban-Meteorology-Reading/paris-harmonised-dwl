# -*- coding: utf-8 -*-
"""
Created on Thu Jul  4 14:01:52 2024

@author: willm
"""
# output directory definitions
L1_BASEDIR = "D:/Urbisphere/sandbox/data/L1/by-serialnr/France/Paris/"
L2_BASEDIR = "D:/Urbisphere/sandbox/data/L2/by-serialnr/France/Paris/"
L3_BASEDIR = "D:/urbisphere/sandbox/data/L3/by-instrumentmodel/DWL/"

# some label defs
WS_UNITS = "m.s^-1"
UNITLESS_UNITS = "unitless"

# min, max and resolution of the harmonised vertical coordinates
MIN_ALTITUDE = 0
MAX_ALTITUDE = 6500
RES_ALTITUDE = 25

# maximum valid wind speed m/s for all QC
MAX_VALID_WS = 60

# template for file names
PRODUCT_FILENAME_TEMPLATE = (
    "{system_serial}/{product_name}_L{product_level}_V{product_version}_"
    "%Y%m%d_%H%M%S_{system_serial}.nc"
)
