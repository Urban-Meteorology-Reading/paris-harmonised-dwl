# -*- coding: utf-8 -*-
"""
Created on Mon Jul  1 11:25:37 2024

@author: willm
"""

reject_files_glob_L0 = [
    # the pulses per ray change on this day. the code can't handle that
    "Wind_Profile_30_20220808_1*",
    "Wind_Profile_30_20220808_2*",
    # the new deployment with updated scan. these files are 120 point. after are 12 point
    "VAD_30_20221207_0*",
    "VAD_30_20221207_10*",
    "VAD_30_20221207_11*",
    # fixed the VAD scan schedule on this day, after these times
    "User5_204_20231113_0*",
    "User5_204_20231113_1*",
    # strange scan angles in these files. The VAD scan will not process with these
    "User5_204_20240209_15*",
    "User5_204_20240209_16*",
    "User5_204_20240209_17*",
    "User5_204_20240209_18*",
    "User5_204_20240209_19*",
    "User5_204_20240209_2*",
    "User5_204_20240210*",
    "User5_204_20240211*",
    "User5_204_20240212_0*",
]

known_missing_files_glob_L0 = [
]

known_missing_data = [
    # determined from QL, assumed missing because of half-day data
    {"station_code": "PAJUSS", "from": "20221026T12:00:00", "to": "20221028T13:00:00"},


]
