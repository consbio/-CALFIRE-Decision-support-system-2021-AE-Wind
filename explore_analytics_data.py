import intake
from derived_variables import *

import os
import xarray as xr

#u10_ds = xr.open_zarr('s3://cadcat/wrf/cae/ensmean/historical/1hr/u10/d03/', storage_options={'anon': True})
#u10_ds = xr.open_zarr('s3://cadcat/wrf/ucla/cesm2/historical/1hr/u10/d03/', storage_options={'anon': True})
#u10_ds = xr.open_zarr('s3://cadcat/wrf/ucla/fgoals-g3/ssp370/1hr/u10/d03/', storage_options={'anon': True})
#u10_ds = xr.open_zarr('s3://cadcat/wrf/ucla/cesm2/historical/1hr/u10/d03/', storage_options={'anon': True})
#u10_ds = xr.open_zarr('s3://cadcat/wrf/ucla/cnrm-esm2-1/historical/1hr/u10/d03/', storage_options={'anon': True})
#u10_ds = xr.open_zarr('s3://cadcat/wrf/ucla/ec-earth3-veg/historical/1hr/u10/d03/', storage_options={'anon': True})
u10_ds = xr.open_zarr('s3://cadcat/wrf/ucla/fgoals-g3/historical/1hr/u10/d03/', storage_options={'anon': True})

print(u10_ds.coords["Lambert_Conformal"])

print(u10_ds)
#u10_da = u10_ds["u10"]
#time = xr.DataArray(u10_ds.time.values, coords=[u10_da.time.values], dims=['time'])
#time_first = max(time.resample(time='1M').max()
#time_last = time.resample(time='1M').max()

#print("First: " + str(time_first))
#print("Last: " + str(time_last))

