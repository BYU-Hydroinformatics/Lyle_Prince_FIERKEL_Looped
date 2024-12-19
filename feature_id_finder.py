import xarray as xr
import geopandas as gpd
import pandas as pd
from pathlib import Path
from shapely.geometry import box

name = 'VIIRS_test_cases'
option = 'geoglows'
SAR = False
water_fraction_dir = Path(f'../water_fractions/{name}/')

cases = pd.read_csv('/Users/ldp/Documents/FIERKEL/VIIRSTestCases.csv', header=None)

if option == 'nwm':
    streams = gpd.read_parquet('/Users/ldp/Downloads/nwm_bq_streams_geo.parquet')
    # streams = gpd.read_file('/Users/ldp/Downloads/nwm_flows4326.gpkg') #.to_crs('EPSG:4326')
    # Filter streams to include only those with stream order >= 6
    streams = streams.loc[streams['streamOrder'] >= 5]
    # Read the streamflow data and rename the variable to 'hydro_var'
    q = xr.open_dataset('/Users/ldp/Documents/nwm_streamflow_daily_sac.nc').rename({'streamflow': 'hydro_var'})

elif option == 'geoglows':
    # Read in the stream network streamflow data for GeoGLOWS
    streams = gpd.read_parquet('/Users/ldp/Downloads/geo_streams_complete.parquet')
    print(streams.columns)
    streams = streams.loc[streams['strmOrder'] >= 5]
    streams.rename(columns={'LINKNO': 'station_id'}, inplace=True)

else:
    raise ValueError(f'Invalid option: {option}')

for row in cases.iterrows():
    print(row[1][0])
    catchment_str = str(row[1][0])
    water_fraction_input = f'../water_fractions/{name}/VIIRS_{catchment_str}.nc'
    # Open the water fraction dataset through january 31, 2023
    water_fraction = xr.open_dataset(water_fraction_input).sel(time=slice(None, '2023-01-31')).rename(
        {'lon': 'lon', 'lat': 'lat'})

    # stop if no images are in water_fraction
    if len(water_fraction.time) == 0:
        print(f"No images in {water_fraction_input}")
    # print the number of images in water_fraction
    print(f"Number of images in {water_fraction}: {len(water_fraction.time)}")

    # Get the longitude and latitude values
    lons = water_fraction.lon.values
    lats = water_fraction.lat.values

    # Define the bounding box for the water fraction data
    xmin, xmax = lons.min(), lons.max()
    ymin, ymax = lats.min(), lats.max()
    bbox = box(xmin, ymin, xmax, ymax)

    # Create a GeoDataFrame for the bounding box
    extent = gpd.GeoDataFrame({'geometry': bbox}, index=[0], crs="EPSG:4326")

    # Select streams that intersect with the bounding box
    domain_streams = gpd.sjoin(streams, extent, predicate='intersects')
    # print(domain_streams)
    domain_streams = domain_streams.drop_duplicates(subset=['station_id']).reset_index().drop(columns='index')
    # print(domain_streams)
    assert domain_streams.station_id.is_unique
    # save the domain_streams to a txt file
    domain_streams = domain_streams[['station_id']]
    domain_streams.to_csv(f'/Users/ldp/Documents/FIERKEL/domain_streams{option}_{row[1][0]}.txt', index=False, header=False)
