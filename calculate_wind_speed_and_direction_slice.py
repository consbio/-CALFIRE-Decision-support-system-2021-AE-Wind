########################################################################################################################
# File name: calculate_wind_speed_and_direction.py
# Author: Mike Gough
# Date created: 10/11/2023
# Python Version: 3.x
# Description: Creates 3km hourly wind speed and wind direction rasters from UCLA WRF downscaled climate data
# (u10 and v10) acquired from the Analytics Engine Server (https://analytics.cal-adapt.org/data) for a given date range.
# u10 = West-East component of Wind at 10m
# v10 = North-South component of Wind at 10m
# The functions for calculating wind speed and direction from u10 and v10 are stored in the derived_variables.py script
# which was acquired from the climakitae toolkit:
# https://github.com/cal-adapt/climakitae/blob/main/climakitae/tools/derived_variables.py#L228
# This script produces three different versions of each variable (wind speed/wind direction) which are written to
# the following 3 directories:
# 1_orig: original wind speed (ws) and wind direction (wd) data calculated directly from u10 and v10.
# 2_clip: ws and wd clipped to the SSN study area
# 3_proj: ws and wd projected to UTM zone 10N (to match the CFO data)

# Duration: ~ 14 mins for each hour of data.
# Based on current data (4 days before and after each fire): ~1.87 days
# 8 days * 24 hours * 14 mins per hour = 2,688 mins = 1.87 days)
########################################################################################################################
import intake
import rioxarray
from derived_variables import *
import arcpy
import os
import xarray as xr
from datetime import *
import pandas
arcpy.env.overwriteOutput = True

start_script = datetime.now()
print("\nStart Time: " + str(start_script))

# The paths to the input data (u10 and v10 variables) in the Analytics Engine S3 Bucket.
# All available datasets are listed in the following CSV:
# https://cadcat.s3.amazonaws.com/cae-zarr.csv
u10_ds = xr.open_zarr('s3://cadcat/wrf/ucla/era5/historical/1hr/u10/d03/', storage_options={'anon': True})
v10_ds = xr.open_zarr('s3://cadcat/wrf/ucla/era5/historical/1hr/v10/d03/', storage_options={'anon': True})

# SSN study area boundary in the same CRS as the UCLA Data (which is based on a custom Lambert projection).
clip_boundary = r"G:\CALFIRE_Decision_support_system_2021_mike_gough\Tasks\Wind\Data\Inputs\Inputs.gdb\EEMS_SSN_boundary_Lambert_UCLA"

# Output folders. These must be created before hand. Subfolders will be created automatically.
output_folder = r"G:\CALFIRE_Decision_support_system_2021_mike_gough\Tasks\Wind\Data\Outputs\ucla_wrf_wind_speed_and_direction"
orig_folder = os.path.join(output_folder, "1_orig")
clip_folder = os.path.join(output_folder, "2_clip")
proj_folder = os.path.join(output_folder, "3_proj")

# Projection file defining the CRS of the original input data:
proj_file_in = r"G:\CALFIRE_Decision_support_system_2021_mike_gough\Tasks\Wind\Data\USA_Contiguous_Lambert_Conformal_Conic_UCLA.prj"

# Projection file defining the desired output CRS:
proj_file_out = r"G:\CALFIRE_Decision_support_system_2021_mike_gough\Tasks\Wind\Data\WGS_1984_UTM_Zone_10N.prj"

# Convert Datasets to DataArrays needed by the compute_wind_mag function:
u10_da = u10_ds["u10"]
v10_da = v10_ds["v10"]

# Pre-Fire Dates
start_time = '2020-08-28T00:00:00'
end_time = '2020-08-31T23:00:00'

# Post-Fire Dates
#start_time = '2021-08-28T00:00:00'
#end_time = '2021-08-31T23:00:00'

timestamps = pandas.date_range(start_time, end_time, freq='H')

timestamps_str = [str(x) for x in timestamps]

for timestamp in timestamps_str:
    print("\n" + timestamp)

    year = timestamp.split("-")[0]

    print("Getting Time Slice...")
    # Option for aggregating data if needed:
    #u10_da_slice = u10_da.sel(time=slice('2001-01-01', '2001-01-02')).groupby('time.day').mean('time')
    #v10_da_slice = v10_da.sel(time=slice('2001-01-01', '2001-01-02')).groupby('time.day').mean('time')

    u10_da_slice = u10_da.sel(time=timestamp)
    v10_da_slice = v10_da.sel(time=timestamp)

    ##### Wind Speed ###################################################################################################

    print("\nComputing Wind Speed...")
    wind_speed_slice = compute_wind_mag(u10_da_slice, v10_da_slice, name="wind_speed_derived")

    print("Establishing GeoTiff Name...")
    output_name_ws = 'ucla_wrf_era5_wind_speed_' + timestamp.replace("-", "").replace(" ", "_").replace(":", "") + ".tif"

    print("Saving Wind Speed to Temporary GeoTiff (Orig)...")
    output_folder = os.path.join(orig_folder, "ws", year)
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    orig_raster = os.path.join(output_folder, output_name_ws)
    wind_speed_slice.rio.to_raster(orig_raster)

    print("Defining Projection...")
    arcpy.DefineProjection_management(in_dataset=orig_raster, coor_system=proj_file_in)

    print("Clipping...")
    output_folder = os.path.join(clip_folder, "ws", year)
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    clip_raster = os.path.join(output_folder, output_name_ws)
    out_raster = arcpy.sa.ExtractByMask(
        in_raster=orig_raster,
        in_mask_data=clip_boundary,
        extraction_area="INSIDE",
    )
    out_raster.save(clip_raster)

    print("Projecting...")
    # Using NEAREST because wind direction needs to be NEAREST. Also not sure of the effect changing windspeed values would have.

    output_folder = os.path.join(proj_folder, "ws", year)
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    proj_raster = os.path.join(output_folder, output_name_ws)
    arcpy.management.ProjectRaster(
        in_raster=clip_raster,
        out_raster=proj_raster,
        out_coor_system=proj_file_out,
        resampling_type="NEAREST",
        cell_size="3000 3000",
        geographic_transform="",
        Registration_Point=None,
        in_coor_system=proj_file_in,
        vertical="NO_VERTICAL"
    )


    ##### Wind Direction ###############################################################################################

    print("\nComputing Wind Direction...")
    wind_direction_slice = compute_wind_dir(u10_da_slice, v10_da_slice, name="wind_direction_derived")

    print("Establishing GeoTiff Name...")
    output_name_wd = 'ucla_wrf_era5_wind_direction_' + timestamp.replace("-", "").replace(" ", "_").replace(":", "") + ".tif"

    print("Saving Wind Direction to GeoTiff (Orig)...")
    output_folder = os.path.join(orig_folder, "wd", year)
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    orig_raster = os.path.join(output_folder, output_name_wd)
    wind_direction_slice.rio.to_raster(orig_raster)

    print("Defining Projection...")
    arcpy.DefineProjection_management(in_dataset=orig_raster, coor_system=proj_file_in)

    print("Clipping...")
    output_folder = os.path.join(clip_folder, "wd", year)
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    clip_raster = os.path.join(output_folder, output_name_wd)
    out_raster = arcpy.sa.ExtractByMask(
        in_raster=orig_raster,
        in_mask_data=clip_boundary,
        extraction_area="INSIDE",
    )
    out_raster.save(clip_raster)

    print("Projecting...")
    # Using NEAREST because wind direction needs to be NEAREST. Also not sure of the effect changing winddirection values would have.

    output_folder = os.path.join(proj_folder, "wd", year)
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    proj_raster = os.path.join(output_folder, output_name_wd)
    arcpy.management.ProjectRaster(
        in_raster=clip_raster,
        out_raster=proj_raster,
        out_coor_system=proj_file_out,
        resampling_type="NEAREST",
        cell_size="3000 3000",
        geographic_transform="",
        Registration_Point=None,
        in_coor_system=proj_file_in,
        vertical="NO_VERTICAL"
    )


end_script = datetime.now()
print("\nEnd Time: " + str(end_script))
print("Duration: " + str(end_script - start_script))
