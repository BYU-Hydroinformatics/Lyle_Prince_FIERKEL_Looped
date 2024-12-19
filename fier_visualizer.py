import xarray as xr
from matplotlib import pyplot as plt
import os
# for every file in /Users/ldp/PycharmProjects/reof_ds/VIIRS_test_cases/syn_models open the file and plot the syn_stack on a scale of 0-100

for file in os.listdir('/Users/ldp/PycharmProjects/reof_ds/VIIRS_test_cases/syn_models/'):
    print(file)
    syn_stack = xr.open_dataset(os.path.join('/Users/ldp/PycharmProjects/reof_ds/VIIRS_test_cases/syn_models',file))
    # get the file name without the extension as a str
    file_str = file.split('.')[0]
    # for every time in syn_stack, plot the syn_stack on a scale of 0-100
    for time in syn_stack.time:
        time_str = str(time.values).split('T')[0]
        syn_stack.sel(time=time).synthesized.plot(cmap='Blues', vmin=0, vmax=100)
        plt.title(f'{file_str} {time}')
        # save the plot to file
        plt.savefig(f'/Users/ldp/PycharmProjects/reof_ds/VIIRS_test_cases/syn_maps/syn_{file_str}_{time_str}.png')
        plt.show()
