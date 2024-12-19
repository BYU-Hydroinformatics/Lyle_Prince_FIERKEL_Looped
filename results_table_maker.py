import pandas as pd
import geopandas as gpd

cases = pd.read_csv('/Users/ldp/Documents/FIERKEL/VIIRSTestCases.csv', header=None)
catchments = gpd.read_file('/Users/ldp/Downloads/VIIRS_Count_Results/VIIRS_count_25new.shp')
spatial_modes = pd.read_csv('/Users/ldp/PycharmProjects/reof_ds/spatial_mode.csv')
df_out = pd.DataFrame(columns=['Catchment', 'mean', 'nwm_result', 'geoglows_result'])
for case in cases[0]:
    catchment = catchments.loc[catchments['linkno'] == case]
    mean = catchment['mean'].values[0]
    print(case , mean)
    # find the rows in spatial_modes that have the contain the catchment and select the max Error
    spatial_mode = spatial_modes.loc[spatial_modes['Catchment'] == int(case)]
    nwm_result = spatial_mode.loc[spatial_mode['Option'] == 'nwm']['Error'].max()
    geoglows_result = spatial_mode.loc[spatial_mode['Option'] == 'geoglows']['Error'].max()
    df_out = df_out._append({'Catchment': case, 'mean': mean, 'nwm_result': nwm_result, 'geoglows_result': geoglows_result}, ignore_index=True)

df_out.to_csv('/Users/ldp/PycharmProjects/reof_ds/results_summary.csv')
