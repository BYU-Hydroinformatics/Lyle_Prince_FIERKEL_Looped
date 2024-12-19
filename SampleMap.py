import xarray as xr
from matplotlib import pyplot as plt
import os
# for every file in /Users/ldp/PycharmProjects/reof_ds/VIIRS_test_cases/syn_models open the file and plot the syn_stack on a scale of 0-100

file = 'syn_reof_760601832_nwm_09995_100_test_max.nc'
print(file)
syn_stack = xr.open_dataset(os.path.join('/Users/ldp/PycharmProjects/reof_ds/VIIRS_test_cases/syn_models',file))
# get the file name without the extension as a str
file_str = file.split('.')[0]
# for every time in syn_stack, plot the syn_stack on a scale of 0-100
time = '2019-06-06'
#rename sythesized to water fraction
syn_stack = syn_stack.rename_vars({'synthesized': 'WaterFraction'})

syn_stack.sel(time=time).WaterFraction.plot(cmap='Blues', vmin=0, vmax=100, cbar_kwargs={'label': 'Water Fraction'})
#change legend name from synthesized to water fraction



plt.title(f'Water Fraction Map for Group 760601832 on {time}')
#change legend nme to water fraction
plt.ylabel('Latitude')
plt.xlabel('Longitude')

# save the plot to file
plt.savefig(f'/Users/ldp/PycharmProjects/reof_ds/VIIRS_test_cases/syn_maps/syn_{file_str}_{time}.png')
plt.show()