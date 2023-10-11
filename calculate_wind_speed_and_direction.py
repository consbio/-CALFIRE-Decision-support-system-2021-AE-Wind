import intake
from derived_variables import *

import os
import xarray as xr

intermediate_ws = r"G:\CALFIRE_Decision_support_system_2021_mike_gough\Tasks\Wind\Data\Intermediate\WRF"

# Datasets
u10_ds = xr.open_zarr('s3://cadcat/wrf/cae/ensmean/historical/1hr/u10/d03/', storage_options={'anon': True})
v10_ds = xr.open_zarr('s3://cadcat/wrf/cae/ensmean/historical/1hr/v10/d03/', storage_options={'anon': True})

# Convert Datasets to DataArrays needed by the compute_wind_mag function:
u10_da = u10_ds["u10"]
v10_da = v10_ds["v10"]

print("Computing Wind Speed...")
wind_speed = compute_wind_mag(u10_da, v10_da, name="wind_speed_derived")

print("Saving to NetCDF...")
wind_speed.to_netcdf(intermediate_ws + os.sep + 'WRF-UCLA-ENSEMBLE-historical-wind_speed-d03.nc')