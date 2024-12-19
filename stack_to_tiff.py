import rasterio
import xarray as xr
import os
root_output_folder = '/Users/ldp/Downloads'
file = 'syn_reof_760601832_nwm_09995_100_test_max.nc'
syn_stack = xr.open_dataset(f'/Users/ldp/PycharmProjects/reof_ds/VIIRS_test_cases/syn_models/{file}')
flow_array = syn_stack.sel(time="2019-06-06")
flow_array = flow_array.to_array()
#save flow_array to tiff





nx = flow_array.sizes['lon']
ny = flow_array.sizes['lat']
xmin, ymin, xmax, ymax = [flow_array.lon.values.min(), flow_array.lat.values.min(), flow_array.lon.values.max(), flow_array.lat.values.max()]
xres = (xmax - xmin) / float(nx)
yres = (ymax - ymin) / float(ny)
geotransform = rasterio.transform.from_origin(xmin, ymax, xres, yres)
with rasterio.open(os.path.join(root_output_folder,'Miss_WaterMap.tif'), 'w',
                   driver='GTiff',
                   height=ny,
                   width=nx,
                   count=1,
                   dtype='uint8',
                   crs='EPSG:4326',
                   transform=geotransform, ) as dst_ds:
    dst_ds.write(flow_array, 1)