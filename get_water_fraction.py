# import apache_beam as beam
from pathlib import Path
import geopandas as gpd
import pandas as pd
import ee
import geemap
import xarray as xr
from dask.diagnostics import ProgressBar

# Initialize the Earth Engine API with a specific project and high-volume API URL
ee.Initialize(
    project='byu-hydroinformatics-gee',
    opt_url=ee.data.HIGH_VOLUME_API_BASE_URL
)

# Set the name for the baseline dataset
name = 'VIIRS_test_cases'
print(name)
catchments = ee.FeatureCollection('users/ldp48/CatchmentsUS5_dissolved')
cases = pd.read_csv('/Users/ldp/Documents/FIERKEL/VIIRSTestCases.csv', header=None)

# Load the NOAA JPSS floods image collection from Earth Engine
noaa_jpss_floods = ee.ImageCollection('projects/byu-hydroinformatics-gee/assets/noaa_jpss_floods')

# Load the HUC8 ID image from Earth Engine
huc8_ids = ee.Image('projects/byu-hydroinformatics-gee/assets/NOAA_FIER/huc8Id_image')

# Function to convert an image to water fraction
def to_water_fraction(img):
    valid_mask = img.gte(99).Or(img.eq(16).Or(img.eq(17)))  # Define valid mask
    water_permanent = img.eq(99).multiply(100)  # Permanent water
    water_fraction = img.updateMask(img.gte(100)).subtract(100).unmask(0)  # Water fraction

    return (
        water_permanent.add(water_fraction)
        .updateMask(valid_mask)
        .unmask(255)
        .float()
        .set({
            'system:time_start': img.date().millis(),  # Set the time start property
        })
    )

for row in cases.iterrows():
    print(row[1][0])
    catchment_str = str(row[1][0])
    ms_aoi = catchments.filterMetadata('linkno', 'equals', int(row[1][0])).first()
    # Define the output path for the NetCDF file
    outpath = Path(f'../water_fractions/{name}/VIIRS_{catchment_str}.nc')

    # Create the output directory if it doesn't exist
    if not outpath.parent.exists():
        outpath.parent.mkdir(parents=True)

    # Map the function over the image collection to get water fractions
    water_fractions = noaa_jpss_floods.map(to_water_fraction)

    # Get the projection information from the first image in the collection
    proj = water_fractions.first().projection().getInfo()
    scale = proj['transform'][0]
    crs = proj['crs']

    # Open the dataset using xarray with Earth Engine as the engine
    ds = xr.open_dataset(
        water_fractions,
        engine='ee',
        crs=crs,
        scale=scale,
        geometry=ms_aoi.geometry(),
    )
    # Chunk the dataset and rename the variable
    ds = ds.chunk({"time": 10, "lon": 512, "lat": 512}).rename({'water_detection': 'water_fraction'})

    # Open the Huc8ID mask dataset using xarray with Earth Engine as the engine
    mask_ds = xr.open_dataset(
        ee.ImageCollection([huc8_ids]),
        engine='ee',
        crs=crs,
        scale=scale,
        geometry=ms_aoi.geometry(),
    )

    # Merge the datasets and transpose the dimensions
    out_ds = xr.merge([ds, mask_ds.huc8Id.isel(time=0)])
    out_ds = out_ds.transpose('time', 'lat', 'lon')  # .astype(np.uint8)

    # Set the attributes for the output dataset
    out_ds.attrs = {
        'huc_level': 8,
        'huc_id': name,
        'crs': crs,
        'scale': scale,
    }

    # Define the encoding settings for the NetCDF file
    encoding = {
        "water_fraction": {
            "dtype": "uint8",
            "zlib": True,
            "complevel": 3,
            "_FillValue": 255,
        },
        "huc8Id": {
            "dtype": "uint32",
            "zlib": True,
            "complevel": 9,
            "_FillValue": 0,
        },
    }

    # Use a progress bar to monitor the NetCDF file writing process
    with ProgressBar():
        out_ds.to_netcdf(outpath, encoding=encoding)