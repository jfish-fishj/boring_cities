import pandas as pd
import geopandas as gpd
from data_constants import make_data_dict, census_data_dict, dataPrefix
from clean_address_data import merge_sac_parcel_id, clean_sac_add
from clean_business_data import clean_sac_bus, clean_sd_bus, clean_la_bus, clean_sf_bus, clean_seattle_bus
from merge_address_data import merge_addresses, misc_sf_cleaning, misc_seattle_cleaning
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
    seattle_bus = clean_seattle_bus()
    seattle_bus = misc_seattle_cleaning(seattle_bus)
    seattle_add = pd.read_csv(data_dict['intermediate']['seattle']['parcel'] + 'addresses.csv')

    seattle_bus = merge_addresses(
            seattle_bus, seattle_add, fuzzy=True, nearest_n1=True,
            fuzzy_threshold=90, n1_threshold=5,
            add_merge_cols=['parsed_addr_n1', 'parsed_addr_sn', 'parsed_addr_ss'],
            bus_merge_cols=['primary_cleaned_addr_n1', 'primary_cleaned_addr_sn', 'primary_cleaned_addr_ss'],
            cols_to_merge=['lat', 'long', 'parcelID', 'parsed_city', 'parsed_addr_zip']
                                )
    seattle_bus.to_csv(data_dict['intermediate']['seattle']['business_location'] + '/business_location_addresses_merged.csv', 
    index=False)
    # seattle_bus = pd.read_csv('/home/jfish/project_data/boring_cities/data/intermediate/seattle/business_location/business_location_addresses_merged.csv')
    # read in census data
    ca_shp = gpd.read_file(census_data_dict['ca bg shp'])
    con = sqlite3.connect(dataPrefix + "/data/census.db")
    census_tracts = pd.read_sql_query("SELECT GEOID10 from acs", con)
    census_tracts['GEOID10'] = census_tracts['GEOID10'].str.pad(width = 11, side="left", fillchar = '0')
    # print(census_tracts['GEOID10'].str.len().value_counts())
    seattle_bus = merge_GEOID(seattle_bus, ca_shp, reset=True)
    business_loc_to_sql(seattle_bus, table="business_locations_flat", mode="append")
    # con = sqlite3.connect(dataPrefix + "/data/business_locations.db")
    # seattle_bus = pd.read_sql_query("SELECT * from business_locations_flat", con)
    seattle_bus = make_business_panel_wrapper(seattle_bus, n_cores=4)
    seattle_bus = make_location_vars(seattle_bus)

    seattle_bus.to_csv(data_dict['final']['seattle']['business_location'] + '/business_location_panel.csv', index=False)
    # con = sqlite3.connect(dataPrefix + "/data/business_locations.db")
    # panel = pd.read_sql_query("SELECT * from business_locations_panel WHERE ", con)
    business_loc_to_sql(seattle_bus, table="business_locations_panel", mode="append")

if __name__ == "__main__":
    run_la()
    # run_sf()