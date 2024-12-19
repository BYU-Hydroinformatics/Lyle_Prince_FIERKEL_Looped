import xarray as xr
import pandas as pd
from matplotlib import pyplot as plt

files = pd.read_csv('/Users/ldp/PycharmProjects/reof_ds/spatial_mode.csv')
# filter files with spatial_mode == True and unique == TRUE
files = files.loc[(files['spatial_mode'] == True) & (files['Unique'] == True)]
# for every file in files, open the file and plot each spatial mode
for file in files['file']:
    reof = xr.open_dataset(f'/Users/ldp/PycharmProjects/reof_ds/VIIRS_test_cases/{file}')
    # get the file name without the extension as a str
    file_str = file.split('.')[0]
    for mode in reof.spatial_modes.mode:
        reof.spatial_modes.sel(mode=mode).plot()
        #add file name and mode to the plot
        mode_str = str(mode.values)
        plt.title(f'{file_str} mode {mode_str}')

        #get the spatial mode number as a str

        # save the plot to file
        plt.savefig(f'/Users/ldp/PycharmProjects/reof_ds/VIIRS_test_cases/spatial_modes/{file_str}_mode_{mode_str}.png')
        plt.show()