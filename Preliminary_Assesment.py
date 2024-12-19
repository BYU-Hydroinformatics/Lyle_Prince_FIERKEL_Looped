import pandas as pd
import os
import xarray as xr

# for every file in /Users/ldp/PycharmProjects/reof_ds/VIIRS_test_cases check if the spatial_mode exists
reof_dir = '/Users/ldp/PycharmProjects/reof_ds/VIIRS_test_cases'
reof_files = pd.Series(os.listdir(reof_dir))
output = pd.DataFrame(columns=['file', 'spatial_mode'])
for reof_file in reof_files:
    reof = xr.open_dataset(os.path.join(reof_dir,reof_file))
    if len(reof.spatial_modes.mode) == 0:
        output = output._append({'file': reof_file, 'spatial_mode': False}, ignore_index=True)
    else:
        output = output._append({'file': reof_file, 'spatial_mode': True}, ignore_index=True)
output.to_csv('/Users/ldp/PycharmProjects/reof_ds/spatial_mode.csv')