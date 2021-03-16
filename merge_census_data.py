from helper_functions import write_to_log
import geopandas as gpd
import pandas as pd
from data_constants import *
import numpy as np

def get_var_from_overlay(df_shp1, df_shp2, varsToGet):
    '''
    Moves variables from df_shp2 to df_shp1 based on a spatial join, e.g. grabbing information from census geographies
    :param df_shp1:
    :param df_shp2:
    :param varsToGet:
    :param dfToConvert: specifies which df's crs to convert, in case df is much larger than the other
    :return:
    '''
    # makes a unique ID for df_shp1 which will later be deleted, in case it intersects multiple in df_shp2
    df_shp1['__ID__'] = range(df_shp1.shape[0])

    # dropping nonetype rows from shp files
    write_to_log("Droppping {} and {} rows from df_shp1 and df_shp2 whose geometries are None".format(sum(df_shp1.geometry.type.isnull()),sum(df_shp2.geometry.type.isnull())),2, doPrint=True)
    df_shp1 = df_shp1[ ~df_shp1.geometry.type.isnull()]
    df_shp2 = df_shp2[ ~df_shp2.geometry.type.isnull()]

    if (df_shp1.crs is not default_crs):
        write_to_log(f"df_shp1 crs  is {str(df_shp1.crs)}, assigning {default_crs}",2, doPrint=True)
        df_shp2 = df_shp2.to_crs(default_crs)
        df_shp1.crs = default_crs

    if (df_shp2.crs is not default_crs):
        write_to_log(f"df_shp1 crs  is {str(df_shp2.crs)}, assigning {default_crs}",2, doPrint=True)
        df_shp2 = df_shp2.to_crs(default_crs)
        df_shp1.crs = default_crs

    df_shp1 = df_shp1.drop(columns = varsToGet,errors = "ignore")
    df_shp1_overlay = gpd.sjoin(df_shp1, df_shp2[varsToGet+['geometry']], how = "left", op='intersects')
    write_to_log("{} rows in df_shp1 intersected a shape in df_shp2, {} intersected multiple shapes, {} intersected no shapes".format(
        str(len(df_shp1_overlay['__ID__'].drop_duplicates())),
        str(len(df_shp1_overlay['__ID__'][df_shp1_overlay['__ID__'].duplicated()].drop_duplicates())),
        str(df_shp1.shape[0]- len(df_shp1_overlay['__ID__'].drop_duplicates()))
    ),1, doPrint=True)
    print(df_shp1_overlay['GEOID10'].isna().sum() /df_shp1_overlay['GEOID10'].shape[0], "percent GEOID10 that is NA" )
    df_shp1_overlay = df_shp1_overlay.drop_duplicates(subset= ['__ID__']).drop(columns = ['__ID__'])
    return(df_shp1_overlay)

def merge_GEOID(df, census_shp):
    # convert dataframe to geodataframe to do overlay
    df['index'] = np.arange(len(df))
    df_overlay = df[(df["long_from_address"].notnull()) & (df["lat_from_address"].notnull())]
    df_overlay = gpd.GeoDataFrame(df_overlay, geometry=gpd.points_from_xy(df_overlay.long_from_address, df_overlay.lat_from_address))
    # get vars by overlaying parcel shapefiles onto blk shapefiles
    varsToGet = ['GEOID10']
    # convert back to pandas dataframe when done
    df_overlay = pd.DataFrame(get_var_from_overlay(df_overlay, census_shp, varsToGet=varsToGet))

    # get bg, tract from overlay
    # substring GEOID10 to get block and tract
    df_overlay['Blk_ID_10'] = '_' + df_overlay['GEOID10']
    df_overlay['BG_ID_10'] = df_overlay['GEOID10'].str.slice(start=0, stop = 12).astype(str)
    df_overlay['CT_ID_10'] = df_overlay['GEOID10'].str.slice(start=0, stop = 11).astype(str)
    df = pd.merge(df, df_overlay[['index', 'Blk_ID_10', 'BG_ID_10', 'CT_ID_10']], how="left", on="index").drop(columns="index")

    return df

def merge_census_tract_data(df, census_df, panel=False, base_year=2010):
    census_df = census_df.rename(columns = {'GEOID10': 'CT_ID_10'})
    # merge census data onto df
    if panel is not False:
        merge_vars = ['CT_ID_10', 'year']
    else:
        merge_vars = ['CT_ID_10']
        census_df = census_df[census_df['year']==base_year]
    df = pd.merge(df, census_df, how='left', on=merge_vars)
    return df

if __name__ == "__main__":
    data_dict = make_data_dict(use_seagate=True)

    # # read in  business data
    # sd_bus = pd.read_csv(data_dict['final']['sd']['business location'] + 'business_locations.csv', dtype = {'CT_ID_10': str})
    # # read in census data
    # ca_shp = gpd.read_file(census_data_dict['ca bg shp'])
    # census_tracts = pd.read_csv(census_data_dict['census tract data'], dtype = {'GEOID10': str})
    # census_tracts['GEOID10'] = census_tracts['GEOID10'].str.pad(width = 11, side="left", fillchar = '0')
    # print(census_tracts['GEOID10'].str.len().value_counts())
    #
    # # overlay GEOID
    # sd_bus = merge_GEOID(sd_bus, ca_shp)
    # print(sd_bus['CT_ID_10'].str.len().value_counts())
    # sd_bus = merge_census_tract_data(sd_bus, census_tracts, panel=True)
    # print(sum(sd_bus['pop'].isna()))
    # sd_bus.to_csv(data_dict['final']['sd']['business location'] + '/business_location_panel.csv', index = False)
    # read in  business data
    # philly_bus = (pd.read_csv(data_dict['final']['philly']['business location'] + 'business_locations.csv').
    #               rename(columns = {"lat": "lat_from_address", "long": "long_from_address"})
    #               )
    # # read in census data
    # pa_shp = gpd.read_file(census_data_dict['pa bg shp'])
    # census_tracts = pd.read_csv(census_data_dict['census tract data'], dtype={'GEOID10': str})
    # census_tracts['GEOID10'] = census_tracts['GEOID10'].str.pad(width=11, side="left", fillchar='0')
    # print(census_tracts['GEOID10'].str.len().value_counts())
    #
    # # overlay GEOID
    # philly_bus = merge_GEOID(philly_bus, pa_shp)
    # print(philly_bus['CT_ID_10'].str.len().value_counts())
    # philly_bus = merge_census_tract_data(philly_bus, census_tracts, panel=True)
    # print(sum(philly_bus['pop'].isna()))
    # philly_bus.to_csv(data_dict['final']['philly']['business location'] + '/business_location_panel.csv', index=False)

    # chicago_bus = (pd.read_csv(data_dict['final']['chicago']['business location'] + '/business_locations.csv'))
    # print(chicago_bus['lat_from_address'].isna().sum() / chicago_bus.shape[0])
    # chicago_bus['long_from_address'] = -1*chicago_bus['long_from_address']
    # # read in census data
    # il_shp = gpd.read_file(census_data_dict['il bg shp'])
    # census_tracts = pd.read_csv(census_data_dict['census tract data'], dtype={'GEOID10': str})
    # census_tracts['GEOID10'] = census_tracts['GEOID10'].str.pad(width=11, side="left", fillchar='0')
    #
    # # overlay GEOID
    # chicago_bus = merge_GEOID(chicago_bus, il_shp)
    # chicago_bus = merge_census_tract_data(chicago_bus, census_tracts, panel=True)
    # print(sum(chicago_bus['pop'].isna()))
    # chicago_bus.to_csv(data_dict['final']['chicago']['business location'] + '/business_location_panel.csv', index=False)
    # baton_rouge_bus = (pd.read_csv(data_dict['final']['baton_rouge']['business location'] + '/business_locations.csv'))
    # print(baton_rouge_bus['lat'].isna().sum() / baton_rouge_bus.shape[0])
    # print(baton_rouge_bus['lat_from_address'].isna().sum() / baton_rouge_bus.shape[0])
    # baton_rouge_bus['long_from_address'] = -1*baton_rouge_bus['long_from_address']
    # # read in census data
    # la_shp = gpd.read_file(census_data_dict['la bg shp'])
    # census_tracts = pd.read_csv(census_data_dict['census tract data'], dtype={'GEOID10': str})
    # census_tracts['GEOID10'] = census_tracts['GEOID10'].str.pad(width=11, side="left", fillchar='0')
    #
    # # overlay GEOID
    # baton_rouge_bus = merge_GEOID(baton_rouge_bus, la_shp)
    # baton_rouge_bus = merge_census_tract_data(baton_rouge_bus, census_tracts, panel=True)
    # print(sum(baton_rouge_bus['pop'].isna()))
    # baton_rouge_bus.to_csv(data_dict['final']['baton_rouge']['business location'] + '/business_location_panel.csv', index=False)
    stl_bus = (pd.read_csv(data_dict['final']['stl']['business location'] + '/business_locations.csv'))
    stl_bus['long_from_address'] = -1*(stl_bus['long_from_address'])

    # read in census data
    mo_shp = gpd.read_file(census_data_dict['mo bg shp'])
    census_tracts = pd.read_csv(census_data_dict['census tract data'], dtype={'GEOID10': str})
    census_tracts['GEOID10'] = census_tracts['GEOID10'].str.pad(width=11, side="left", fillchar='0')

    # overlay GEOID
    stl_bus = merge_GEOID(stl_bus, mo_shp)
    stl_bus = merge_census_tract_data(stl_bus, census_tracts, panel=True)
    stl_bus.to_csv(data_dict['final']['stl']['business location'] + '/business_location_panel.csv', index=False)

