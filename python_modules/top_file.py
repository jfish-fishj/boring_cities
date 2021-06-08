import pandas as pd
import geopandas as gpd
from data_constants import make_data_dict, census_data_dict, dataPrefix
from clean_address_data import merge_sac_parcel_id, clean_sac_add
from clean_business_data import (
    clean_sac_bus, clean_sd_bus,
    clean_la_bus, clean_sf_bus, 
    clean_seattle_bus, clean_long_beach_bus,
     clean_stl_bus,clean_baton_rouge_bus, clean_chicago_bus)
from merge_address_data import (merge_addresses, misc_sf_cleaning, misc_chi_cleaning,
misc_seattle_cleaning, misc_baton_rouge_cleaning)
from make_business_vars import make_business_panel, make_business_vars_wrapper, make_location_vars, make_business_panel_wrapper
from merge_census_data import merge_census_tract_data, merge_GEOID
from helper_functions import business_loc_to_sql
import sqlite3
import gc
# data dict
data_dict = make_data_dict(use_seagate=False)


## SACREMENTO
def run_sac():
    sac_bus = pd.read_csv(data_dict['intermediate']['sac']['business_location'] + 'business_location.csv')
    sac_bus = clean_sac_bus()
    sac_add = pd.read_csv(data_dict['raw']['sac']['parcel'] + 'Address.csv')
    sac_xwalk = pd.read_csv(data_dict['raw']['sac']['parcel'] + 'Address_parcel_xwalk.csv')
    sac_add = merge_sac_parcel_id(sac_add=sac_add, xwalk=sac_xwalk)
    sac_add= clean_sac_add(sac_add)
    sac_add.to_csv(data_dict['intermediate']['sac']['parcel'] + 'addresses_concat.csv',
     index=False)

    sac_bus = merge_addresses(
            sac_bus, sac_add, fuzzy=True, nearest_n1=True,
            fuzzy_threshold=90, n1_threshold=5,
            add_merge_cols=['parsed_addr_n1', 'parsed_addr_sn', 'parsed_addr_ss'],
            bus_merge_cols=['primary_cleaned_addr_n1', 'primary_cleaned_addr_sn', 'primary_cleaned_addr_ss'],
            cols_to_merge=['lat', 'long', 'parcelID', 'parsed_city', 'parsed_addr_zip']
                                  )
    sac_bus.to_csv(data_dict['intermediate']['sac']['business_location'] + '/business_location_addresses_merged.csv', 
    index=False)
    sac_bus = pd.read_csv(data_dict['intermediate']['sac']['business_location'] + '/business_location_addresses_merged.csv')
    sac_bus = make_business_vars_wrapper(sac_bus, n_cores=4)
    sac_bus.to_csv(data_dict['final']['sac']['business_location'] + '/business_location_flat.csv', index=False)
    sac_bus = pd.read_csv(data_dict['final']['sac']['business_location'] + '/business_location_flat.csv')
    # read in census data
    ca_shp = gpd.read_file(census_data_dict['ca bg shp'])
    con = sqlite3.connect(dataPrefix + "/data/census.db")
    census_tracts = pd.read_sql_query("SELECT GEOID10 from acs", con)
    census_tracts['GEOID10'] = census_tracts['GEOID10'].str.pad(width = 11, side="left", fillchar = '0')
    print(census_tracts['GEOID10'].str.len().value_counts())
    sac_bus = merge_GEOID(sac_bus, ca_shp, reset=True)
    business_loc_to_sql(sac_bus, table="business_locations_flat", mode="replace")
    # sac_bus = pd.read_csv(data_dict['final']['sac']['business_location'] + '/business_location_flat.csv')
    #sac_bus = pd.read_csv(data_dict['final']['sac']['business_location'] + '/business_location_panel.csv')
    sac_bus = make_business_panel_wrapper(sac_bus, n_cores=4)
    sac_bus = make_location_vars(sac_bus)


    sac_bus.to_csv(data_dict['final']['sac']['business_location'] + '/business_location_panel.csv', index=False)
    business_loc_to_sql(sac_bus, table="business_locations_panel", mode="replace")
    print(sac_bus['CT_ID_10'].str.len().value_counts())
    ###sac_bus = merge_census_tract_data(sac_bus, census_tracts, panel=True)
    print(sum(sac_bus['pop'].isna()))
    sac_bus.to_csv(data_dict['final']['sac']['business_location'] + '/business_location_panel_census_merge.csv', index = False)
    sac_bus = None
    sac_add = None
    gc.collect()


# SAN DIEGO
def run_sd():
    sd_bus = clean_sd_bus()
    sd_add = pd.read_csv(data_dict['intermediate']['sd']['parcel'] + 'addresses.csv')

    sd_bus = merge_addresses(
            sd_bus, sd_add, fuzzy=True, nearest_n1=True,
            fuzzy_threshold=90, n1_threshold=5,
            add_merge_cols=['parsed_addr_n1', 'parsed_addr_sn', 'parsed_addr_ss'],
            bus_merge_cols=['primary_cleaned_addr_n1', 'primary_cleaned_addr_sn', 'primary_cleaned_addr_ss'],
            cols_to_merge=['lat', 'long', 'parcelID', 'parsed_city', 'parsed_addr_zip']
                                )
    sd_bus.to_csv(data_dict['intermediate']['sd']['business_location'] + '/business_location_addresses_merged.csv', 
    index=False)
    sd_bus = pd.read_csv('/home/jfish/project_data/boring_cities/data/intermediate/sd/business_location/business_location_addresses_merged.csv')
    # read in census data
    ca_shp = gpd.read_file(census_data_dict['ca bg shp'])
    con = sqlite3.connect(dataPrefix + "/data/census.db")
    census_tracts = pd.read_sql_query("SELECT GEOID10 from acs", con)
    census_tracts['GEOID10'] = census_tracts['GEOID10'].str.pad(width = 11, side="left", fillchar = '0')
    # print(census_tracts['GEOID10'].str.len().value_counts())
    sd_bus = merge_GEOID(sd_bus, ca_shp, reset=True)
    business_loc_to_sql(sd_bus, table="business_locations_flat", mode="append")
    # con = sqlite3.connect(dataPrefix + "/data/business_locations.db")
    # sd_bus = pd.read_sql_query("SELECT * from business_locations_flat", con)
    sd_bus = make_business_panel_wrapper(sd_bus, n_cores=4)
    sd_bus = make_location_vars(sd_bus)

    sd_bus.to_csv(data_dict['final']['sd']['business_location'] + '/business_location_panel.csv', index=False)
    # con = sqlite3.connect(dataPrefix + "/data/business_locations.db")
    # panel = pd.read_sql_query("SELECT * from business_locations_panel WHERE ", con)
    business_loc_to_sql(sd_bus, table="business_locations_panel", mode="append")


# LOS ANGELES
"""
Only runs up through exporting to flat because of issues with memory...
"""
def run_la():
    # la_bus = clean_la_bus()
    # # la_bus = pd.read_csv(data_dict['intermediate']['la']['business_location'] + '/business_location.csv')
    # la_add = pd.read_csv(data_dict['intermediate']['la']['parcel'] + 'addresses.csv')

    # la_bus = merge_addresses(
    #         la_bus, la_add, fuzzy=True, nearest_n1=True,
    #         fuzzy_threshold=90, n1_threshold=5,
    #         add_merge_cols=['parsed_addr_n1', 'parsed_addr_sn', 'parsed_addr_ss'],
    #         bus_merge_cols=['primary_cleaned_addr_n1', 'primary_cleaned_addr_sn', 'primary_cleaned_addr_ss'],
    #         cols_to_merge=['lat', 'long', 'parcelID', 'parsed_city', 'parsed_addr_zip']
    #                             )
    # la_bus.to_csv(data_dict['intermediate']['la']['business_location'] + '/business_location_addresses_merged.csv', 
    # index=False)
    # # la_bus = pd.read_csv('/home/jfish/project_data/boring_cities/data/intermediate/la/business_location/business_location_addresses_merged.csv')
    # # read in census data
    # ca_shp = gpd.read_file(census_data_dict['ca bg shp'])
    # con = sqlite3.connect(dataPrefix + "/data/census.db")
    # census_tracts = pd.read_sql_query("SELECT GEOID10 from acs", con)
    # census_tracts['GEOID10'] = census_tracts['GEOID10'].str.pad(width = 11, side="left", fillchar = '0')
    # # print(census_tracts['GEOID10'].str.len().value_counts())
    # la_bus = merge_GEOID(la_bus, ca_shp, reset=True)
    # business_loc_to_sql(la_bus, table="business_locations_flat", mode="append")
    con = sqlite3.connect(dataPrefix + "/data/business_locations.db")
    la_bus = (pd.read_sql_query('SELECT * from business_locations_flat WHERE source = "los angeles"', con).
    reset_index())
    la_bus = la_bus.drop(columns=[col for col in la_bus.columns if "mail" in col])
    print(f"read in la bus with {la_bus.shape[0]} rows")
    # la_bus.to_csv(data_dict['final']['la']['business_location'] + '/business_location.csv', index=False)
    print("turning la bus into panel")
    # la_bus = make_business_panel_wrapper(la_bus, n_cores=1)
    la_bus = make_business_panel(la_bus)
    # print("finished panel, making business vars")
    # la_bus = make_location_vars(la_bus)
    print("exporting data")

    la_bus.to_csv(data_dict['final']['la']['business_location'] + '/business_location_panel.csv', index=False)
    # con = sqlite3.connect(dataPrefix + "/data/business_locations.db")
    # panel = pd.read_sql_query("SELECT * from business_locations_panel WHERE ", con)
    # business_loc_to_sql(la_bus, table="business_locations_panel", mode="append")


# SAN FRANCISCO
def run_sf():
    sf_bus = clean_sf_bus()
    sf_bus = misc_sf_cleaning(sf_bus)
    sf_add = pd.read_csv(data_dict['intermediate']['sf']['parcel'] + 'addresses.csv')

    sf_bus = merge_addresses(
            sf_bus, sf_add, fuzzy=True, nearest_n1=True,
            fuzzy_threshold=90, n1_threshold=5,
            add_merge_cols=['parsed_addr_n1', 'parsed_addr_sn', 'parsed_addr_ss'],
            bus_merge_cols=['primary_cleaned_addr_n1', 'primary_cleaned_addr_sn', 'primary_cleaned_addr_ss'],
            cols_to_merge=['lat', 'long', 'parcelID', 'parsed_city', 'parsed_addr_zip']
                                )
    sf_bus.to_csv(data_dict['intermediate']['sf']['business_location'] + '/business_location_addresses_merged.csv', 
    index=False)
    # sf_bus = pd.read_csv('/home/jfish/project_data/boring_cities/data/intermediate/sf/business_location/business_location_addresses_merged.csv')
    # read in census data
    ca_shp = gpd.read_file(census_data_dict['ca bg shp'])
    con = sqlite3.connect(dataPrefix + "/data/census.db")
    census_tracts = pd.read_sql_query("SELECT GEOID10 from acs", con)
    census_tracts['GEOID10'] = census_tracts['GEOID10'].str.pad(width = 11, side="left", fillchar = '0')
    # print(census_tracts['GEOID10'].str.len().value_counts())
    sf_bus = merge_GEOID(sf_bus, ca_shp, reset=True)
    business_loc_to_sql(sf_bus, table="business_locations_flat", mode="append")
    # con = sqlite3.connect(dataPrefix + "/data/business_locations.db")
    # sf_bus = pd.read_sql_query("SELECT * from business_locations_flat", con)
    sf_bus = make_business_panel_wrapper(sf_bus, n_cores=4)
    sf_bus = make_location_vars(sf_bus)

    sf_bus.to_csv(data_dict['final']['sf']['business_location'] + '/business_location_panel.csv', index=False)
    # con = sqlite3.connect(dataPrefix + "/data/business_locations.db")
    # panel = pd.read_sql_query("SELECT * from business_locations_panel WHERE ", con)
    business_loc_to_sql(sf_bus, table="business_locations_panel", mode="append")


# SEATTLE
def run_seattle():
    # seattle_bus = clean_seattle_bus()
    # seattle_bus = misc_seattle_cleaning(seattle_bus)
    # seattle_add = pd.read_csv(data_dict['intermediate']['seattle']['parcel'] + 'addresses.csv')

    # seattle_bus = merge_addresses(
    #         seattle_bus, seattle_add, fuzzy=True, nearest_n1=True,
    #         fuzzy_threshold=90, n1_threshold=5,
    #         add_merge_cols=['parsed_addr_n1', 'parsed_addr_sn', 'parsed_addr_ss'],
    #         bus_merge_cols=['primary_cleaned_addr_n1', 'primary_cleaned_addr_sn', 'primary_cleaned_addr_ss'],
    #         cols_to_merge=['lat', 'long', 'parcelID', 'parsed_city', 'parsed_addr_zip']
    #                             )
    # seattle_bus.to_csv(data_dict['intermediate']['seattle']['business_location'] + '/business_location_addresses_merged.csv', 
    # index=False)
    seattle_bus = pd.read_csv('/home/jfish/project_data/boring_cities/data/intermediate/seattle/business_location/business_location_addresses_merged.csv')
    if seattle_bus.shape[0] > 300000:
        raise ValueError
    print(seattle_bus.shape)
    # read in census data
    wa_shp = gpd.read_file(census_data_dict['wa bg shp'])
    con = sqlite3.connect(dataPrefix + "/data/census.db")
    census_tracts = pd.read_sql_query("SELECT GEOID10 from acs", con)
    census_tracts['GEOID10'] = census_tracts['GEOID10'].str.pad(width = 11, side="left", fillchar = '0')

    # print(census_tracts['GEOID10'].str.len().value_counts())
    seattle_bus = merge_GEOID(seattle_bus, wa_shp, reset=True)
    business_loc_to_sql(seattle_bus, table="business_locations_flat", mode="append")
    #con = sqlite3.connect(dataPrefix + "/data/business_locations.db")
    #seattle_bus = pd.read_sql_query("SELECT * from business_locations_flat WHERE source = 'seattle'", con)
    seattle_bus = make_business_panel_wrapper(seattle_bus, n_cores=4)
    seattle_bus = make_location_vars(seattle_bus)

    seattle_bus.to_csv(data_dict['final']['seattle']['business_location'] + '/business_location_panel.csv', index=False)
    # con = sqlite3.connect(dataPrefix + "/data/business_locations.db")
    # panel = pd.read_sql_query("SELECT * from business_locations_panel WHERE ", con)
    business_loc_to_sql(seattle_bus, table="business_locations_panel", mode="append")


# stl
def run_stl():
    stl_bus = clean_stl_bus()
    stl_add = pd.read_csv(data_dict['intermediate']['stl']['parcel'] + 'addresses.csv')

    stl_bus = merge_addresses(
            stl_bus, stl_add, fuzzy=True, nearest_n1=True,
            fuzzy_threshold=90, n1_threshold=5,
            add_merge_cols=['parsed_addr_n1', 'parsed_addr_sn', 'parsed_addr_ss'],
            bus_merge_cols=['primary_cleaned_addr_n1', 'primary_cleaned_addr_sn', 'primary_cleaned_addr_ss'],
            cols_to_merge=['lat', 'long', 'parcelID', 'parsed_city', 'parsed_addr_zip']
                                )
    stl_bus.to_csv(data_dict['intermediate']['stl']['business_location'] + '/business_location_addresses_merged.csv', 
    index=False)
    stl_bus = pd.read_csv('/home/jfish/project_data/boring_cities/data/intermediate/stl/business_location/business_location_addresses_merged.csv')
    stl_bus["long_from_address"] = stl_bus["long_from_address"]
    # read in census data
    mo_shp = gpd.read_file(census_data_dict['mo bg shp'])
    con = sqlite3.connect(dataPrefix + "/data/census.db")
    census_tracts = pd.read_sql_query("SELECT GEOID10 from acs", con)
    census_tracts['GEOID10'] = census_tracts['GEOID10'].str.pad(width = 11, side="left", fillchar = '0')
    # print(census_tracts['GEOID10'].str.len().value_counts())
    stl_bus = merge_GEOID(stl_bus, mo_shp, reset=True)
    business_loc_to_sql(stl_bus, table="business_locations_flat", mode="append")
    # con = sqlite3.connect(dataPrefix + "/data/business_locations.db")
    # stl_bus = pd.read_sql_query("SELECT * from business_locations_flat", con)
    stl_bus = make_business_panel_wrapper(stl_bus, n_cores=4)
    stl_bus = make_location_vars(stl_bus)

    stl_bus.to_csv(data_dict['final']['stl']['business_location'] + '/business_location_panel.csv', index=False)
    # con = sqlite3.connect(dataPrefix + "/data/business_locations.db")
    # panel = pd.read_sql_query("SELECT * from business_locations_panel WHERE ", con)
    business_loc_to_sql(stl_bus, table="business_locations_panel", mode="append")


# BATON ROUGE
def run_baton_rouge():
    baton_rouge_bus = clean_baton_rouge_bus()
    baton_rouge_add = pd.read_csv(data_dict['intermediate']['baton_rouge']['parcel'] + 'addresses.csv')
    print(baton_rouge_add.columns)
    print(baton_rouge_bus.shape[0], '!!!!')
    baton_rouge_bus  = misc_baton_rouge_cleaning(bus_df=baton_rouge_bus, 
    add_df=baton_rouge_add)

    baton_rouge_bus = merge_addresses(
            baton_rouge_bus, baton_rouge_add, fuzzy=True, nearest_n1=True,
            fuzzy_threshold=90, n1_threshold=5,
            add_merge_cols=['parsed_addr_n1', 'parsed_addr_sn', 'parsed_addr_ss'],
            bus_merge_cols=['primary_cleaned_addr_n1', 'primary_cleaned_addr_sn', 'primary_cleaned_addr_ss'],
            cols_to_merge=['lat', 'long', 'parcelID', 'parsed_city', 'parsed_addr_zip']
                                )
    baton_rouge_bus.to_csv(data_dict['intermediate']['baton_rouge']['business_location'] + '/business_location_addresses_merged.csv', 
    index=False)
    # baton_rouge_bus = pd.read_csv('/home/jfish/project_data/boring_cities/data/intermediate/baton_rouge/business_location/business_location_addresses_merged.csv')
    # if baton_rouge_bus.shape[0] > 300000:
    #     raise ValueError
    # print(baton_rouge_bus.shape)
    # read in census data
    la_shp = gpd.read_file(census_data_dict['la bg shp'])
    con = sqlite3.connect(dataPrefix + "/data/census.db")
    census_tracts = pd.read_sql_query("SELECT GEOID10 from acs", con)
    census_tracts['GEOID10'] = census_tracts['GEOID10'].str.pad(width = 11, side="left", fillchar = '0')

    # print(census_tracts['GEOID10'].str.len().value_counts())
    baton_rouge_bus = merge_GEOID(baton_rouge_bus, la_shp, reset=True)
    business_loc_to_sql(baton_rouge_bus, table="business_locations_flat", mode="append")
    #con = sqlite3.connect(dataPrefix + "/data/business_locations.db")
    #baton_rouge_bus = pd.read_sql_query("SELECT * from business_locations_flat WHERE source = 'baton_rouge'", con)
    baton_rouge_bus = make_business_panel_wrapper(baton_rouge_bus, n_cores=4)
    baton_rouge_bus = make_location_vars(baton_rouge_bus)

    baton_rouge_bus.to_csv(data_dict['final']['baton_rouge']['business_location'] + '/business_location_panel.csv', index=False)
    # con = sqlite3.connect(dataPrefix + "/data/business_locations.db")
    # panel = pd.read_sql_query("SELECT * from business_locations_panel WHERE ", con)
    business_loc_to_sql(baton_rouge_bus, table="business_locations_panel", mode="append")


# CHICAGO
def run_chicago():
    print("running chicago")
    chicago_bus = clean_chicago_bus()
    print("cleaned businesses")
    chicago_bus = pd.read_csv(data_dict['intermediate']['chicago']['business_location'] + '/business_location.csv')
    chicago_add = pd.read_csv(data_dict['intermediate']['chicago']['parcel'] + 'addresses.csv')
    chicago_add, chicago_bus  = misc_chi_cleaning(bus_df=chicago_bus, 
    add_df=chicago_add)
    print("misc add")

    chicago_bus = merge_addresses(
            chicago_bus, chicago_add, fuzzy=True, nearest_n1=True,
            fuzzy_threshold=90, n1_threshold=5,
            add_merge_cols=['parsed_addr_n1', 'parsed_addr_sn', 'parsed_addr_ss'],
            bus_merge_cols=['primary_cleaned_addr_n1', 'primary_cleaned_addr_sn', 'primary_cleaned_addr_ss'],
            cols_to_merge=['lat', 'long', 'parcelID', 'parsed_city', 'parsed_addr_zip']
                                )
    chicago_bus.to_csv(data_dict['intermediate']['chicago']['business_location'] + '/business_location_addresses_merged.csv', 
    index=False)
    print("merged addresses")
    # chicago_bus = pd.read_csv('/home/jfish/project_data/boring_cities/data/intermediate/chicago/business_location/business_location_addresses_merged.csv')
    # if chicago_bus.shape[0] > 300000:
    #     raise ValueError
    # print(chicago_bus.shape)
    # read in census data
    il_shp = gpd.read_file(census_data_dict['il bg shp'])
    con = sqlite3.connect(dataPrefix + "/data/census.db")
    census_tracts = pd.read_sql_query("SELECT GEOID10 from acs", con)
    census_tracts['GEOID10'] = census_tracts['GEOID10'].str.pad(width = 11, side="left", fillchar = '0')

    # print(census_tracts['GEOID10'].str.len().value_counts())
    chicago_bus['long_from_address'] = chicago_bus['long_from_address'].abs()
    chicago_bus['long'] = chicago_bus['long'].abs()
    chicago_bus = merge_GEOID(chicago_bus, il_shp, reset=True)
    business_loc_to_sql(chicago_bus, table="business_locations_flat", mode="append")
    print("exported to int")
    #con = sqlite3.connect(dataPrefix + "/data/business_locations.db")
    #chicago_bus = pd.read_sql_query("SELECT * from business_locations_flat WHERE source = 'chicago'", con)
    chicago_bus = make_business_panel_wrapper(chicago_bus, n_cores=4)
    chicago_bus = chicago_bus.drop_duplicates(subset=['location_id','year'])
    chicago_bus = make_location_vars(chicago_bus)

    chicago_bus.to_csv(data_dict['final']['chicago']['business_location'] + '/business_location_panel.csv', index=False)
    # con = sqlite3.connect(dataPrefix + "/data/business_locations.db")
    # panel = pd.read_sql_query("SELECT * from business_locations_panel WHERE ", con)
    business_loc_to_sql(chicago_bus, table="business_locations_panel", mode="append")


# long_beach
def run_long_beach():
    print("running long_beach")
    long_beach_bus = clean_long_beach_bus()
    # long_beach_bus = pd.read_csv(data_dict['intermediate']['long_beach']['business_location'] + '/business_location.csv')
    #df = pd.read_csv('/home/jfish/project_data/boring_cities/data/intermediate/long_beach/business_location/business_location.csv')
    print(long_beach_bus.shape[0])
    long_beach_add = pd.read_csv(data_dict['intermediate']['la']['parcel'] + 'addresses.csv')
    # long_beach_bus,long_beach_add  = misc_chi_cleaning(bus_df=long_beach_bus, 
    # add_df=long_beach_add)
    print("misc add")

    long_beach_bus = merge_addresses(
            long_beach_bus, long_beach_add, fuzzy=True, nearest_n1=True,
            fuzzy_threshold=90, n1_threshold=5,
            add_merge_cols=['parsed_addr_n1', 'parsed_addr_sn', 'parsed_addr_ss'],
            bus_merge_cols=['primary_cleaned_addr_n1', 'primary_cleaned_addr_sn', 'primary_cleaned_addr_ss'],
            cols_to_merge=['lat', 'long', 'parcelID', 'parsed_city', 'parsed_addr_zip']
                                )
    print(long_beach_bus.shape[0])
    long_beach_bus.to_csv(data_dict['intermediate']['long_beach']['business_location'] + '/business_location_addresses_merged.csv', 
    index=False)
    print("merged addresses")
    # long_beach_bus = pd.read_csv('/home/jfish/project_data/boring_cities/data/intermediate/long_beach/business_location/business_location_addresses_merged.csv')
    print(len(long_beach_bus))
    print(long_beach_bus.shape)
    if long_beach_bus.shape[0] > 300000:
        raise ValueError
    # print(long_beach_bus.shape)
    # read in census data
    ca_shp = gpd.read_file(census_data_dict['ca bg shp'])
    con = sqlite3.connect(dataPrefix + "/data/census.db")
    census_tracts = pd.read_sql_query("SELECT GEOID10 from acs", con)
    census_tracts['GEOID10'] = census_tracts['GEOID10'].str.pad(width = 11, side="left", fillchar = '0')

    # print(census_tracts['GEOID10'].str.len().value_counts())
    long_beach_bus = merge_GEOID(long_beach_bus, ca_shp, reset=True)
    business_loc_to_sql(long_beach_bus, table="business_locations_flat", mode="append")
    print("exported to int")
    #con = sqlite3.connect(dataPrefix + "/data/business_locations.db")
    #long_beach_bus = pd.read_sql_query("SELECT * from business_locations_flat WHERE source = 'long_beach'", con)
    long_beach_bus = make_business_panel_wrapper(long_beach_bus, n_cores=4)
    long_beach_bus = long_beach_bus.drop_duplicates(subset=['location_id','year'])
    long_beach_bus = make_location_vars(long_beach_bus)

    long_beach_bus.to_csv(data_dict['final']['long_beach']['business_location'] + '/business_location_panel.csv', index=False)
    # con = sqlite3.connect(dataPrefix + "/data/business_locations.db")
    # panel = pd.read_sql_query("SELECT * from business_locations_panel WHERE ", con)
    business_loc_to_sql(long_beach_bus, table="business_locations_panel", mode="append")


if __name__ == "__main__":
    # run_la()
    # run_sf()
    # run_seattle()
    # run_stl()
    # run_baton_rouge()
    print("running?")
    run_sac()
    run_sd()
    #run_chicago()
    run_long_beach()