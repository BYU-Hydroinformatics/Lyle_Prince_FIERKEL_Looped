import s3fs
import xarray as xr
from dask.diagnostics import ProgressBar
import pandas as pd


cases = pd.read_csv('/Users/ldp/Documents/FIERKEL/VIIRSTestCases.csv', header=None)
option = 'geoglows'
for row in cases.iterrows():
    print(row[1][0])
    if row[1][0] == 770347759:
        continue
    # Path to the file containing feature IDs (one ID per line)
    input_ids = f'/Users/ldp/Documents/FIERKEL/domain_streams{option}_{row[1][0]}.txt'

    # Path to the output NetCDF file
    output_file = f'/Users/ldp/Documents/FIERKEL/geoglows_streamflow/{row[1][0]}.nc'

    # Read feature IDs from the input file
    with open(input_ids, 'r') as f:
        feature_ids = f.readlines()

    # Clean and sort the feature IDs
    feature_ids = sorted([int(x.replace('\n', '')) for x in feature_ids])

    # S3 bucket URI and region name
    bucket_uri = 's3://geoglows-v2-retrospective/retrospective.zarr'
    region_name = 'us-west-2'

    # Initialize S3 filesystem with anonymous access
    s3 = s3fs.S3FileSystem(anon=True, client_kwargs=dict(region_name=region_name))
    s3store = s3fs.S3Map(root=bucket_uri, s3=s3, check=False)

    # Open the Zarr dataset from S3 and drop unnecessary variables
    ds = xr.open_zarr(s3store).sel(time=slice('2012-01-01', None))

    ds = ds.rename({'Qout': 'streamflow', 'rivid': 'feature_id'})
    ds = ds[['streamflow']].sel(feature_id=feature_ids)
    # Define chunk sizes for the output NetCDF file
    out_chunks = {'feature_id': 1, 'time': len(ds.time)}
    print(ds)
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