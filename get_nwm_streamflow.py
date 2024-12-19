import numpy as np
import pandas as pd
import s3fs
import xarray as xr
from dask.diagnostics import ProgressBar
from pathlib import Path

cases = pd.read_csv('/Users/ldp/Documents/FIERKEL/VIIRSTestCases.csv', header=None)
option = 'geoglows'
for row in cases.iterrows():
    print(row[1][0])
    if row[1][0] == 770347759 or row[1][0] == 770347761:
        continue
    # Path to the file containing feature IDs (one ID per line)
    input_ids = f'/Users/ldp/Documents/FIERKEL/domain_streams{option}_{row[1][0]}.txt'

    # Path to the output NetCDF file
    output_file = f'/Users/ldp/Documents/FIERKEL/{option}_streamflow/{row[1][0]}.nc'
    outpath = Path(output_file)
    if outpath.exists():
        continue
    # Read feature IDs from the input file
    with open(input_ids, 'r') as f:
        feature_ids = f.readlines()

    # Clean and sort the feature IDs
    feature_ids = sorted([int(x.replace('\n', '')) for x in feature_ids])

    # S3 bucket URI and region name
    bucket_uri = 's3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/chrtout.zarr'
    region_name = 'us-east-1'

    # Initialize S3 filesystem with anonymous access
    s3 = s3fs.S3FileSystem(anon=True, client_kwargs=dict(region_name=region_name))
    s3store = s3fs.S3Map(root=bucket_uri, s3=s3, check=False)

    # Open the Zarr dataset from S3 and drop unnecessary variables
    ds = (
        xr.open_zarr(s3store)
        .drop_vars(['crs', 'order', 'latitude', 'longitude', 'elevation', 'gage_id'])
        .sel(time=slice('2012-01-01', None))
    )
    # Select the 'streamflow' variable, filter by feature IDs, and resample to daily mean
    ds = ds[['streamflow']].sel(feature_id=feature_ids).resample(time='1D').mean()

    print(ds)
    # Define chunk sizes for the output NetCDF file
    out_chunks = {'feature_id': 1, 'time': len(ds.time)}

    # Write the dataset to a NetCDF file with specified encoding options
    with ProgressBar():
        ds.to_netcdf(
            output_file,
            encoding={
                'streamflow': {
                    'dtype': 'float32',
                    'zlib': True,
                    'complevel': 3,
                    'chunksizes': (30, 1)
                }
            }
        )