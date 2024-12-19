import pandas as pd
import xarray as xr
import fierpy
from pathlib import Path

models = pd.read_csv('/Users/ldp/PycharmProjects/reof_ds/spatial_mode.csv')
# filter files with spatial_mode == True and unique == TRUE and error == 3
models = models.loc[(models['spatial_mode'] == True) & (models['Unique'] == True) & (models['Error'] == 3)]
print(models)
# for every file in models['file'], open the file and sythesize
for row in models.iterrows():
    file = row[1]['file']
    file_str = file.split('.')[0]
    # check if output file exists
    outpath = Path(f'/Users/ldp/PycharmProjects/reof_ds/VIIRS_test_cases/syn_models/syn_{file_str}_min.nc')
    if outpath.exists():
        continue
    print(file)
    eof_hydro = xr.open_dataset(f'/Users/ldp/PycharmProjects/reof_ds/VIIRS_test_cases/{file}')
    # get the file name without the extension as a str
    option = row[1]['Option']
    catchment = str(int(row[1]['Catchment']))
    # open the streamflow data
    q_full = xr.open_dataset(f'/Users/ldp/Documents/FIERKEL/{option}_streamflow/{catchment}.nc')
    q_full = q_full.sel(feature_id=eof_hydro.reachid)
    print(q_full)
    # select time of max value of streamflow from any reach id
    # select the reach id with the higher mean streamflow
    # Calculate the mean streamflow for each feature_id
    mean_streamflow = q_full['streamflow'].mean(dim='time')

    # Find the index of the maximum mean streamflow
    max_index = mean_streamflow.argmax()

    # Get the corresponding feature_id
    max_feature_id = mean_streamflow.coords['feature_id'][max_index].values.item()
    # select the feature_id with the max mean streamflow
    q_red  = q_full.where(q_full.feature_id == max_feature_id, drop=True)
    # Drop duplicate feature_ids with the same mean streamflow
    # select first indexed mode
    q_red = q_red.isel(mode=0)

    max_time = q_red.time[q_red.streamflow.argmax()]
    max_time_str = str(max_time.values).split('T')[0]
    max_time_str = max_time_str[:7]
    max_time_start = max_time_str + '-01'
    max_time_end = max_time_str + '-28'

    min_time = q_red.time[q_red.streamflow.argmin()]
    min_time_str = str(min_time.values).split('T')[0]
    min_time_str = min_time_str[:7]
    min_time_start = min_time_str + '-01'
    min_time_end = min_time_str + '-28'
    # filter q_full for the entire month of max value
    q_full_max = q_full.sel(time=slice(max_time_start, max_time_end))
    # filter q_full for the entire month of min value
    q_full_min = q_full.sel(time=slice(min_time_start, min_time_end))

    # synthesize
    syn_stack_max = fierpy.synthesize(eof_hydro, q_full_max.streamflow)
    syn_stack_max.to_netcdf(f'/Users/ldp/PycharmProjects/reof_ds/VIIRS_test_cases/syn_models/syn_{file_str}_max.nc')
    syn_stack_min = fierpy.synthesize(eof_hydro, q_full_min.streamflow)
    syn_stack_min.to_netcdf(f'/Users/ldp/PycharmProjects/reof_ds/VIIRS_test_cases/syn_models/syn_{file_str}_min.nc')
    print(f'{file_str} synthesized')