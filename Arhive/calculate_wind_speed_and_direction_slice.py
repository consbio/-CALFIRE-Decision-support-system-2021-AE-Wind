import intake
import rioxarray
from derived_variables import *
import arcpy
import os
import xarray as xr
from datetime import *
import pandas

start_script = datetime.now()
print("\nStart Time: " + str(start_script))

output_ws_wind_speed = r"G:\CALFIRE_Decision_support_system_2021_mike_gough\Tasks\Wind\Data\Outputs\GeoTiffs\ws"
output_ws_wind_direction = r"G:\CALFIRE_Decision_support_system_2021_mike_gough\Tasks\Wind\Data\Outputs\GeoTiffs\wd"
proj_file = r"G:\CALFIRE_Decision_support_system_2021_mike_gough\Tasks\Wind\Data\USA_Contiguous_Lambert_Conformal_Conic_UCLA.prj"

# Datasets
#u10_ds = xr.open_zarr('s3://cadcat/wrf/ucla/cesm2/historical/1hr/u10/d03/', storage_options={'anon': True})
#v10_ds = xr.open_zarr('s3://cadcat/wrf/ucla/cesm2/historical/1hr/v10/d03/', storage_options={'anon': True})
#u10_ds = xr.open_zarr('s3://cadcat/wrf/ucla/cesm2/historical/1hr/u10/d03/', storage_options={'anon': True})
#v10_ds = xr.open_zarr('s3://cadcat/wrf/ucla/cesm2/historical/1hr/v10/d03/', storage_options={'anon': True})
#u10_ds = xr.open_zarr('s3://cadcat/wrf/ucla/cesm2/ssp370/1hr/u10/d03/', storage_options={'anon': True})
#v10_ds = xr.open_zarr('s3://cadcat/wrf/ucla/cesm2/ssp370/1hr/v10/d03/', storage_options={'anon': True})

u10_ds = xr.open_zarr('s3://cadcat/wrf/ucla/era5/historical/1hr/u10/d03/', storage_options={'anon': True})
v10_ds = xr.open_zarr('s3://cadcat/wrf/ucla/era5/historical/1hr/v10/d03/', storage_options={'anon': True})

# Convert Datasets to DataArrays needed by the compute_wind_mag function:
u10_da = u10_ds["u10"]
v10_da = v10_ds["v10"]

#timeslice = '2020-08-30T12:00:00'
#timeslice = '2021-08-30T12:00:00'

# Pre-Fire
start_time = '2021-08-28T00:00:00'
end_time = '2021-08-31T23:00:00'

# Post-Fire
start_time = '2021-08-28T00:00:00'
end_time = '2021-08-31T23:00:00'

timestamps = pandas.date_range(start_time, end_time, freq='H')

timestamps_str = [str(x) for x in timestamps]

for timestamp in timestamps_str:
    print("\n" + timestamp)

    print("Getting Time Slice...")
    #u10_da_slice = u10_da.sel(time=slice('2001-01-01', '2001-01-02')).groupby('time.day').mean('time')
    #v10_da_slice = v10_da.sel(time=slice('2001-01-01', '2001-01-02')).groupby('time.day').mean('time')

    u10_da_slice = u10_da.sel(time=timestamp)
    v10_da_slice = v10_da.sel(time=timestamp)

    print("Computing Wind Speed...")
    wind_speed_slice = compute_wind_mag(u10_da_slice, v10_da_slice, name="wind_speed_derived")

    #print("Saving to NetCDF...")
    #output_netcdf = os.path.join(intermediate_ws, 'WRF-UCLA-ENSEMBLE-historical-wind_speed-d03_test_slice_hour.nc')
    #wind_speed_slice.to_netcdf(output_netcdf)

    print("Saving Wind Speed to GeoTiff...")
    output_name_ws = 'wrf_ucla_era5_wind_speed_' + timestamp.replace("-", "").replace(" ", "_").replace(":", "") + ".tif"
    output_raster_ws = os.path.join(output_ws_wind_speed, output_name_ws)
    wind_speed_slice.rio.to_raster(output_raster_ws)

    print("Defining Projection...")
    arcpy.DefineProjection_management(in_dataset=output_raster_ws, coor_system=proj_file)

    print("Computing Wind Direction ...")
    wind_direction_slice = compute_wind_dir(u10_da_slice, v10_da_slice, name="wind_direction_derived")

    print("Saving Wind Direction to GeoTiff...")
    output_name_wd = 'wrf_ucla_era5_wind_dir_' + timestamp.replace("-", "").replace(" ", "_").replace(":", "") + ".tif"
    output_raster_wd = os.path.join(output_ws_wind_direction, output_name_wd)
    wind_direction_slice.rio.to_raster(output_raster_wd)

    print("Defining Projection...")
    arcpy.DefineProjection_management(in_dataset=output_raster_wd, coor_system=proj_file)


end_script = datetime.now()
print("\nEnd Time: " + str(end_script))
print("Duration: " + str(end_script - start_script))

