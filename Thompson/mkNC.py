#! /usr/bin/env python3
#
# Create an empty NetCDF file for use on the Thompson during ARCTERX
#
# April-2023, Pat Welch, pat@mousebrains.com

from netCDF4 import Dataset
import numpy as np
import pandas as pd

def createNetCDF(fn:str, tBase:np.datetime64) -> None:
    tBase = pd.Timestamp(tBase).strftime("%Y-%m-%d %H:%M:%S")
    with Dataset(fn, "w", format="NETCDF4") as nc:
        nc.setncattr("Comment", "Generated for R/V Thompson as part of ARCTERX 2023 cruise")
        nc.createDimension("t", size=None)
        nc.createVariable("t", "i4", "t", zlib=True).setncatts(dict(
            units="seconds since " + tBase,
            calendar="proleptic_gregorian",
            ))
        nc.createVariable("lat", "f8", "t", zlib=True).setncatts(dict(
            units="Decimal degrees",
            comment="CNAV3050 latitude",
            _FillValue=np.nan,
            ))
        nc.createVariable("lon", "f8", "t", zlib=True).setncatts(dict(
            units="Decimal degrees",
            comment="CNAV3050 longitude",
            _FillValue=np.nan,
            ))
        nc.createVariable("sog", "f4", "t", zlib=True).setncatts(dict(
            units="meters/second",
            comment="CNAV3050 speed over ground",
            _FillValue=np.single(np.nan)
            ))
        nc.createVariable("cog", "f4", "t", zlib=True).setncatts(dict(
            units="degrees",
            comment="CNAV3050 Course over ground in degrees true",
            _FillValue=np.single(np.nan)
            ))
        nc.createVariable("wSpd", "f4", "t", zlib=True).setncatts(dict(
            units="meters/second",
            comment="True wind speed from bow Ultrasonic sensor",
            _FillValue=np.single(np.nan)
            ))
        nc.createVariable("wDir", "f4", "t", zlib=True).setncatts(dict(
            units="degrees",
            comment="True wind direction from bow Ultrasonic sensor",
            _FillValue=np.single(np.nan)
            ))
        nc.createVariable("par", "f4", "t", zlib=True).setncatts(dict(
            units="uE/m^2",
            comment="True wind direction from bow Ultrasonic sensor",
            _FillValue=np.single(np.nan)
            ))
        nc.createVariable("Tair", "f4", "t", zlib=True).setncatts(dict(
            units="C",
            comment="Air temperature",
            _FillValue=np.single(np.nan)
            ))
        nc.createVariable("RH", "f4", "t", zlib=True).setncatts(dict(
            units="%",
            comment="Relative Humidity",
            _FillValue=np.single(np.nan)
            ))
        nc.createVariable("Pair", "f4", "t", zlib=True).setncatts(dict(
            units="mb",
            comment="Air Pressure",
            _FillValue=np.single(np.nan)
            ))
        nc.createVariable("shortWave", "f4", "t", zlib=True).setncatts(dict(
            units="W/m^2",
            comment="Shortwave radiation",
            _FillValue=np.single(np.nan)
            ))
        nc.createVariable("longWave", "f4", "t", zlib=True).setncatts(dict(
            units="W/m^2",
            comment="Longwave radiation",
            _FillValue=np.single(np.nan)
            ))
        nc.createVariable("Tinlet", "f4", "t", zlib=True).setncatts(dict(
            units="C",
            comment="Inlet water temperature from SBE38",
            _FillValue=np.single(np.nan)
            ))
        nc.createVariable("Ttsg", "f4", "t", zlib=True).setncatts(dict(
            units="C",
            comment="Water temperature from TSG",
            _FillValue=np.single(np.nan)
            ))
        nc.createVariable("cond", "f4", "t", zlib=True).setncatts(dict(
            units="V",
            comment="Water conductivity from TSG",
            _FillValue=np.single(np.nan)
            ))
        nc.createVariable("salinity", "f4", "t", zlib=True).setncatts(dict(
            units="PSU",
            comment="Water salinity from TSG",
            _FillValue=np.single(np.nan)
            ))
        nc.createVariable("fluorometer", "u2", "t", zlib=True).setncatts(dict(
            units="counts",
            comment="Fluorometer counts",
            _FillValue=np.ushort(65535)
            ))
        nc.createVariable("flThermistor", "u2", "t", zlib=True).setncatts(dict(
            units="counts",
            comment="Fluorometer thermistor reading",
            _FillValue=np.ushort(65535)
            ))
        nc.createVariable("depthMB", "f4", "t", zlib=True).setncatts(dict(
            units="meters",
            comment="Water depth from multibeam",
            _FillValue=np.single(np.nan)
            ))
        nc.createVariable("depthKN", "f4", "t", zlib=True).setncatts(dict(
            units="meters",
            comment="Water depth from Knudsen PKEL99 3.5kHz",
            _FillValue=np.single(np.nan)
            ))
    
if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("nc", type=str, help="Output NetCDF filename")
    parser.add_argument("--tBase", type=str, default="2024-04-01 00:10:00", help="Base time for CF")
    args = parser.parse_args()

    tBase = np.datetime64(args.tBase)
    createNetCDF(args.nc, tBase)
