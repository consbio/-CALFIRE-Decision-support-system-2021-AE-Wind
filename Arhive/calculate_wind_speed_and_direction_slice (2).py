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

u10_ds = xr.open_zarr('s3://cadcat/wrf/ucla/era5/historical/1hr/u10/d03/', storage_options={'anon': True})
v10_ds = xr.open_zarr('s3://cadcat/wrf/ucla/era5/historical/1hr/v10/d03/', storage_options={'anon': True})

clip_boundary = r"\\loxodonta\gis\Source_Data\boundaries\region\SN_CA\CALFIRE_Decision_support_system_Study_Area_Boundary\reneonsoapanalysisupdates\EEMS_SSN_boundary_prj.shp"

output_folder = r"G:\CALFIRE_Decision_support_system_2021_mike_gough\Tasks\Wind\Data\Outputs\GeoTiffs"

tmp_folder_orig = r"G:\CALFIRE_Decision_support_system_2021_mike_gough\Tasks\Wind\Data\Intermediate\GeoTiffs_Wind_Speed_And_Direction\Orig"
tmp_folder_proj = r"G:\CALFIRE_Decision_support_system_2021_mike_gough\Tasks\Wind\Data\Intermediate\GeoTiffs_Wind_Speed_And_Direction\Proj"
tmp_folder_clip = r"G:\CALFIRE_Decision_support_system_2021_mike_gough\Tasks\Wind\Data\Intermediate\GeoTiffs_Wind_Speed_And_Direction\Clip"

proj_file_in = r"G:\CALFIRE_Decision_support_system_2021_mike_gough\Tasks\Wind\Data\USA_Contiguous_Lambert_Conformal_Conic_UCLA.prj"
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

    ####################################################################################################################

    # Output Raster Name (Wind Speed)
    output_name_ws = 'wrf_ucla_era5_wind_speed_' + timestamp.replace("-", "").replace(" ", "_").replace(":", "") + ".tif"

    print("\nComputing Wind Speed...")
    wind_speed_slice = compute_wind_mag(u10_da_slice, v10_da_slice, name="wind_speed_derived")

    print("Saving Wind Speed to Temporary GeoTiff...")
    tmp_raster_orig_ws = os.path.join(tmp_folder_orig, output_name_ws)
    wind_speed_slice.rio.to_raster(tmp_raster_orig_ws)

    print("Defining Projection...")
    arcpy.DefineProjection_management(in_dataset=tmp_raster_orig_ws, coor_system=proj_file_in)

    print("Projecting...")
    # Using NEAREST because wind direction needs to be NEAREST. Also not sure of the effect changing windspeed values would have.
    tmp_raster_proj_ws = os.path.join(tmp_folder_proj, output_name_ws)
    arcpy.management.ProjectRaster(
        in_raster=tmp_raster_orig_ws,
        out_raster=tmp_raster_proj_ws,
        out_coor_system=proj_file_out,
        resampling_type="NEAREST",
        cell_size="3000 3000",
        geographic_transform="WGS_1984_(ITRF00)_To_NAD_1983",
        Registration_Point=None,
        in_coor_system=proj_file_in,
        vertical="NO_VERTICAL"
    )

    print("Clipping...")
    output_folder_ws = os.path.join(output_folder, "ws", year)
    output_raster_ws = os.path.join(output_folder_ws, output_name_ws)
    arcpy.sa.ExtractByMask(tmp_raster_proj_ws,clip_boundary)
    out_raster = arcpy.sa.ExtractByMask(
        in_raster=tmp_raster_proj_ws,
        in_mask_data=clip_boundary,
        extraction_area="INSIDE",
    )
    out_raster.save(output_raster_ws)

    ####################################################################################################################

    # Output Raster name (Wind Direction)
    output_name_wd = 'wrf_ucla_era5_wind_dir_' + timestamp.replace("-", "").replace(" ", "_").replace(":", "") + ".tif"

    print("\nComputing Wind Direction ...")
    wind_direction_slice = compute_wind_dir(u10_da_slice, v10_da_slice, name="wind_direction_derived")

    print("Saving Wind Direction to Temporary GeoTiff...")
    tmp_raster_orig_wd = os.path.join(tmp_folder_orig, output_name_wd)
    wind_direction_slice.rio.to_raster(tmp_raster_orig_wd)

    print("Defining Projection...")
    arcpy.DefineProjection_management(in_dataset=tmp_raster_orig_wd, coor_system=proj_file_in)

    print("Projecting...")
    tmp_raster_proj_wd = os.path.join(tmp_folder_proj, output_name_wd)
    arcpy.management.ProjectRaster(
        in_raster=tmp_raster_orig_wd,
        out_raster=tmp_raster_proj_wd,
        out_coor_system=proj_file_out,
        resampling_type="NEAREST",
        cell_size="3000 3000",
        geographic_transform="WGS_1984_(ITRF00)_To_NAD_1983",
        Registration_Point=None,
        in_coor_system=proj_file_in,
        vertical="NO_VERTICAL"
    )

    print("Clipping...")
    output_folder_wd = os.path.join(output_folder, "wd", year)
    output_raster_wd = os.path.join(output_folder_wd, output_name_wd)
    arcpy.sa.ExtractByMask(tmp_raster_proj_wd,clip_boundary)
    out_raster = arcpy.sa.ExtractByMask(
        in_raster=tmp_raster_proj_wd,
        in_mask_data=clip_boundary,
        extraction_area="INSIDE",
    )
    out_raster.save(output_raster_wd)

end_script = datetime.now()
print("\nEnd Time: " + str(end_script))
print("Duration: " + str(end_script - start_script))

