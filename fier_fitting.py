import geopandas as gpd
import fierpy
import numpy as np
from shapely.geometry import box
import xarray as xr
from pathlib import Path
import pandas as pd
options = ['nwm']
img_count = [20,100,150,200,250,300,400]
for option in options:
    for count in img_count:
        count_str = str(count)
        cases = pd.read_csv('/Users/ldp/Documents/FIERKEL/VIIRSTestCases.csv', header=None)
        def filter_poor_observations(ds, variable, threshold):
            # Select the data array for the given variable
            da = ds[variable]
            # Count the number of finite values along the longitude and latitude dimensions
            x = np.isfinite(da).sum(dim=["lon", "lat"])
            # Calculate the fraction of finite values
            y = x / (da.shape[1] * da.shape[2])
            # Keep dates where the fraction of finite values is above the threshold
            keep_dates = y.where(y > threshold).dropna(dim="time").time
            # Return the dataset filtered by the selected dates
            return ds.sel(time=keep_dates)

        thresholds = [0.9999, 0.9995, 0.999, 0.995, 0.99, 0.95, 0.9]

        name = 'VIIRS_test_cases'
        SAR = False
        print(f"RUNNING {name}")

        # Define the directory containing water fraction files
        water_fraction_dir = Path(f'../water_fractions/{name}/')

        # Get a list of all NetCDF files in the water fraction directory
        water_fraction_inputs = water_fraction_dir.glob('*.nc')

        # Reads in the stream network streamflow data for National Water Model, will need to update for GEOGLOWS
        # Read the stream network data and convert to EPSG:4326 coordinate reference system
        # Filter streams to include only those with stream order >= 6 using geopandas read_file and sql

        if option == 'nwm':
            streams = gpd.read_parquet('/Users/ldp/Downloads/nwm_bq_streams_geo.parquet')
            # streams = gpd.read_file('/Users/ldp/Downloads/nwm_flows4326.gpkg') #.to_crs('EPSG:4326')
            # Filter streams to include only those with stream order >= 6
            streams = streams.loc[streams['streamOrder'] >= 5]

        elif option == 'geoglows':
            # Read in the stream network streamflow data for GeoGLOWS
            streams = gpd.read_parquet('/Users/ldp/Downloads/geo_streams_complete.parquet')
            print(streams.columns)
            streams = streams.loc[streams['strmOrder'] >= 5]
            streams.rename(columns={'LINKNO': 'station_id'}, inplace=True)

        else:
            raise ValueError(f'Invalid option: {option}')

        # Iterate over each water fraction input file (should be only one for the baseline dataset)
        for row in cases.iterrows():
            print(row[1][0], option)
            catchment_str = str(row[1][0])
            water_fraction_input = f'../water_fractions/{name}/VIIRS_{catchment_str}.nc'

            if row[1][0] == 770347759 or row[1][0] == 770235795 or row[1][0] == 710145787 or row[1][0] == 760021611:
                continue

            # Open the water fraction dataset through january 31, 2023
            water_fraction = xr.open_dataset(water_fraction_input).sel(time=slice(None, '2023-01-31'))
            # Read the streamflow data and rename the variable to 'hydro_var'
            if option == 'geoglows':
                q = xr.open_dataset(f'/Users/ldp/Documents/FIERKEL/geoglows_streamflow/{row[1][0]}.nc').rename({'streamflow':'hydro_var'})

            if option == 'nwm':
                q = xr.open_dataset(f'/Users/ldp/Documents/FIERKEL/nwm_streamflow/{row[1][0]}.nc').rename({'streamflow':'hydro_var'})

            for t in thresholds:
                water_fraction_filtered = filter_poor_observations(water_fraction, 'water_fraction', threshold=t)
                if len(water_fraction_filtered.time) > count:
                    threshold = t
                    break
            threshold_str = str(int(threshold * 10000)).zfill(5)
            #stop if no images are in water_fraction
            if len(water_fraction_filtered.time) < count:
                continue
            #print the number of images in water_fraction
            print(f"Number of images in {water_fraction_filtered}: {len(water_fraction_filtered.time)}")

            # Define the output path for the processed data
            outpath = Path(f'../reof_ds/{name}/') / f'reof_{catchment_str}_{option}_{threshold_str}_{count_str}_test.nc'

            # Create the output directory if it doesn't exist
            if not outpath.parent.exists():
                outpath.parent.mkdir(parents=True)

            # Process the file if the output file doesn't already exist
            if outpath.exists():
                continue

            # Get the longitude and latitude values
            lons = water_fraction_filtered.lon.values
            lats = water_fraction_filtered.lat.values

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

            # Select streamflow data for the selected streams
            domain_qs = []
            for i in domain_streams.station_id.values:
                try:
                    domain_qs.append(q.hydro_var.sel(feature_id=i))
                    # print(q.hydro_var.sel(feature_id=i))
                except:
                    pass
            # if domain_qs is empty, skip the catchment
            if len(domain_qs) == 0:
                continue
            domain_q = xr.concat(domain_qs, dim='feature_id').rename({'feature_id':'reachid'})
            # drop all reachids with nan values
            domain_q = domain_q.dropna(dim='reachid')
            # if option == 'nwm', check the streamflow data for each reachid and drop any reachids with values less than 0 or that are constant
            if option == 'nwm':
                #domain_q = domain_q.where(domain_q > 0).dropna(dim='reachid')
                domain_q = domain_q.where(domain_q.std(dim='time') > 0).dropna(dim='reachid')

            # if domain_q is empty, skip the catchment
            if len(domain_q.reachid) == 0:
                continue

            print('domain_q', domain_q)
            # Perform EOF analysis on the water fraction data
            eof_ds = fierpy.reof(water_fraction_filtered.water_fraction)
            print('eof_ds', eof_ds)
            # Find the hydrological modes that correspond to the EOFs
            hydro_modes = fierpy.find_hydro_mode(eof_ds, domain_q, deoutlier=True, threshold=0.6)
            print('hydro_modes', hydro_modes)
            # Combine the EOFs with the hydrological modes
            eof_hydro = fierpy.combine_eof_hydro(eof_ds, domain_q, hydro_modes)
            print('eof_hydro', eof_hydro)
            # Save the combined dataset to a NetCDF file
            print(outpath)
            eof_hydro.to_netcdf(outpath)
