
import geopandas as gpd
import pandas as pd
import ee
import geemap


# Initialize the Earth Engine API with a specific project and high-volume API URL
ee.Initialize(
    project='byu-hydroinformatics-gee',
    opt_url=ee.data.HIGH_VOLUME_API_BASE_URL
)

# Set the name for the baseline dataset
name = 'VIIRS_test_cases'
print(name)
catchments = gpd.read_file('/Users/ldp/Downloads/VIIRS_Count_Results/VIIRS_count_25new.shp')
cases = pd.read_csv('/Users/ldp/Documents/FIERKEL/VIIRSTestCases.csv', header=None)

for row in cases.iterrows():
    print(row[1][0])
    catchment_str = str(row[1][0])
    asset_id = f'projects/ldp48/assets/{catchment_str}'
    #if asset doesn't exist, create it
    try:
        # Attempt to get asset information
        ee.data.getInfo(asset_id)
        exists = True  # Asset exists if no exception is raised
    except ee.EEException:
        exists = False  # Asset doesn't exist or an error occurred
        print(f"Asset {asset_id} does not exist. Creating it...")
        catchment = catchments.loc[catchments['linkno'] == row[1][0]]
        print(catchment)
        fc = geemap.geopandas_to_ee(catchment)

        task = ee.batch.Export.table.toAsset(
            collection=fc,
            description=catchment_str,
            assetId=asset_id
        )
        task.start()