import pandas as pd
import numpy as np
import geopandas as gpd
from helper_functions import write_to_log, make_year_var, WTL_TIME, add_subset_address_cols, interpolate_polygon
from data_constants import make_data_dict, filePrefix, name_parser_files, default_crs
from name_parsing import parse_and_clean_name, classify_name, clean_name, combine_names
from address_parsing import clean_parse_address
from pathos.multiprocessing import ProcessingPool as Pool
from helper_functions import make_panel
import re
import os
import janitor

# classify & clean name columns+ clean & parse primary and mailing addresses
# function that runs code in parallel
def parallelize_dataframe(df, func, n_cores=4):
    df_split = np.array_split(df, n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    # have to include this to prevent leakage and allow multiple parallel function calls
    pool.terminate()
    pool.restart()
    return df

# wrapper function to run each city in parallel
def clean_parse_parallel(df):
    df = clean_parse_address(
        dataframe=df, address_col='address_fa',st_name="address_sn", st_sfx="address_ss",
        st_d="address_sd", unit='address_u', st_num='address_n1',
        country='address_country', state='address_state', st_num2 ='address_n2',city='address_city',
        zipcode='address_zip', prefix2='parsed_', prefix1='cleaned_'
    )
    return df

def clean_chi_add1(df):
    chicago_rename_dict = {
        'ADDRNOCOM': 'address_n1',
        'STNAMEPRD': 'address_sd',
        'STNAME': 'address_sn',
        'STNAMEPOT': 'address_ss',
        'PLACENAME': 'address_city',
        'ZIP5': 'address_zip',
        'CMPADDABRV': 'address_fa',
        'PIN': 'parcelID',
        'XPOSITION': 'long',
        'YPOSITION': 'lat'
    }
    df.rename(columns=chicago_rename_dict, inplace=True)
    df = add_subset_address_cols(df)
    df = parallelize_dataframe(df=df, func=clean_parse_parallel, n_cores=2)
    return df

def clean_chi_add2(df):
    chicago_rename_dict = {
        'property_address':'address_fa',
        'property_city': 'address_city',
        'property_zip': 'address_zip',
        'pin': 'parcelID',
        'latitude': 'lat',
        'longitude': 'long'
    }
    df.rename(columns=chicago_rename_dict, inplace=True)
    df = add_subset_address_cols(df)
    df = parallelize_dataframe(df=df, func=clean_parse_parallel, n_cores=4)
    return df


def clean_stl_add(df):
    df = df.rename(
        columns = {
             "STREETNAME": "address_sn", "STREETTYPE": "address_ss", "PREDIR": "address_sd", "ZIP_CODE": "address_zip"
        }
    )
    df['index'] = np.arange(df.shape[0])
    df = df.to_crs(default_crs)
    df.crs = default_crs
    bounds = df.bounds
    df['address_city'] = 'saint louis'
    df['latitude_min'] = bounds["miny"]
    df['latitude_max'] = bounds["maxy"]
    df['longitude_min'] = bounds["minx"]
    df['longitude_max'] = bounds["maxx"]
    df['direction'] = np.where(
        ((df['FROMLEFT'] < df['TOLEFT']) & (df['FROMRIGHT'] < df['TORIGHT'])),
        "NE",
        np.where(
            ((df['FROMLEFT'] < df['TOLEFT']) & (df['FROMRIGHT'] > df['TORIGHT'])),
            "NW",
            np.where(
                ((df['FROMLEFT'] > df['TOLEFT']) & (df['FROMRIGHT'] < df['TORIGHT'])),
                "SE",
                np.where(
                    ((df['FROMLEFT'] > df['TOLEFT']) & (df['FROMRIGHT'] > df['TORIGHT'])),
                    "SW",
                    "SW"

                )
            )
        )
    )
    df_r = df[[col for col in df.columns if not bool(re.search("LEFT", col))]]
    df_r['address_n1'] = np.where(
        df_r['FROMRIGHT'] > df_r['TORIGHT'],
        df_r['TORIGHT'],
        df_r['FROMRIGHT']
    )
    df_r['address_n2'] = np.where(
        df_r['TORIGHT'] > df_r['FROMRIGHT'],
        df_r['TORIGHT'],
        df_r['FROMRIGHT']
    )
    df_l = df[[col for col in df.columns if not bool(re.search("RIGHT", col))]]
    df_l['address_n1'] = np.where(
        df_l['FROMLEFT'] > df_l['TOLEFT'],
        df_l['TOLEFT'],
        df_l['FROMLEFT']
    )
    df_l['address_n2'] = np.where(
        df_l['TOLEFT'] > df_l['FROMLEFT'],
        df_l['TOLEFT'],
        df_l['FROMLEFT']
    )
    df = pd.concat([df_r, df_l])
    df = df[~((df['address_n1'] <= 0) & (df['address_n1'] <= 0))]
    df = make_panel(df,start_year="address_n1", end_year="address_n2", current_year=df['address_n2'],
                    evens_and_odds=True ).rename(columns = {'year': 'address_n1'})
    # interpolate lat long

    df = interpolate_polygon(df, "index", "direction")
    df['lat'] = df['lat_interpolated']
    df['long'] = df["long_interpolated"]

    df = add_subset_address_cols(df)
    df = parallelize_dataframe(df=df, func=clean_parse_parallel, n_cores=2)
    return df

def clean_la_add(df):
    la_rename_dict = {
        'AIN': 'parcelID',
        'UnitName': 'address_u',
        'Number': 'address_n1',
        'PostDir': 'address_ss',
        'PreDirAbbr':   'address_sd',
        'ZipCode': 'address_zip',
        'LegalComm': 'address_city',
    }
    df.rename(columns=la_rename_dict, inplace=True)
    combine_names(df, name_cols=['PreType', 'StArticle', 'StreetName'], newCol="address_sn")
    df = df.to_crs(default_crs)
    df.crs = default_crs
    df['long'] = df.geometry.centroid.x
    df['lat'] = df.geometry.centroid.y
    df = add_subset_address_cols(df)
    df = parallelize_dataframe(df=df, func=clean_parse_parallel, n_cores=2)
    return df

def clean_sd_add(df):
    sd_rename_dict = {
        'parcelid': 'parcelID', # this could also be apn
        'addrunit': 'address_u',
        'addrnmbr': 'address_n1',
        'addrpdir':'address_sd',
        'addrname': 'address_sn',
        'addrsfx': 'address_ss',
        'addrzip': 'address_zip',
        'community': 'address_city',
        'PIN': 'parcelID',
    }
    df.rename(columns=sd_rename_dict, inplace=True)
    df = df.to_crs(default_crs)
    df.crs = default_crs
    df['long'] = df.geometry.centroid.x
    df['lat'] = df.geometry.centroid.y
    df = add_subset_address_cols(df)
    df = parallelize_dataframe(df=df, func=clean_parse_parallel, n_cores=2)
    return df

def clean_sf_add(df):
    sf_rename_dict = {
        "Parcel Number": 'parcelID',
        'Unit Number': 'address_u',
        'Address Number': 'address_n1',
        'Street Name': 'address_sn',
        'Street Type':   'address_ss',
        'ZIP Code': 'address_zip',
        'Address': 'address_fa',
        'PIN': 'parcelID',
        'Longitude': 'long',
        'Latitude': 'lat'
    }
    df.rename(columns=sf_rename_dict, inplace=True)
    df['address_city'] = "San Francisco"
    df = add_subset_address_cols(df)
    df = parallelize_dataframe(df=df, func=clean_parse_parallel, n_cores=2)
    return df

def clean_seattle_add(df):
    seattle_rename_dict = {
        'PIN': 'parcelID',
        'UNIT_NUM': 'address_u',
        'ADDR_NUM': 'address_n1',
        'ADDR_SN': 'address_sn',
        'ADDR_ST': 'address_ss',
        'ADDR_SD':   'address_sd',
        'ZIP5': 'address_zip',
        'CTYNAME': 'address_city',
        'ADDR_FULL': 'address_fa',
        'LON': 'long',
        'LAT': 'lat'
    }
    df.rename(columns=seattle_rename_dict, inplace=True)
    df = add_subset_address_cols(df)
    df = parallelize_dataframe(df=df, func=clean_parse_parallel, n_cores=2)
    return df

def clean_baton_rouge_add(df):
    baton_rouge_rename_dict = {
        'ADDRNOCOM': 'address_n1',
        'ASTREET PREFIX DIRECTION': 'address_sd',
        'STREET NAME': 'address_sn',
        'STREET SUFFIX TYPE': 'address_ss',
        'CITY': 'address_city',
        'ZIP': 'address_zip',
        'FULL ADDRESS': 'address_fa'
    }
    df.rename(columns=baton_rouge_rename_dict, inplace=True)
    lat_long = df['GEOLOCATION'].str.extract('([0-9\.]+),([0-9\.]+)')
    df['lat'] = lat_long.iloc[:,0]
    df['long'] = lat_long.iloc[:,1]
    df = add_subset_address_cols(df)
    df = parallelize_dataframe(df=df, func=clean_parse_parallel, n_cores=4)
    return df

def concat_chi_add(df1, df2):
    df1 = df1.append(df2).drop_duplicates(subset = [
        'parcelID',
        "parsed_addr_n1",
        "parsed_addr_sn",
        "parsed_addr_ss",
        "parsed_city"

    ])
    return df1


if __name__ == "__main__":
    data_dict = make_data_dict(use_seagate=True)
    stl_add = gpd.read_file(data_dict['raw']['stl']['parcel'] + 'streets/tgr_str_cl.shp')
    stl_add = clean_stl_add(stl_add)
    stl_add.to_csv(data_dict['intermediate']['stl']['parcel'] + 'addresses.csv', index=False)
    # baton_rouge_add = pd.read_csv(
    #     data_dict['raw']['baton_rouge']['parcel'] + 'addresses_Property_Information_ebrp.csv')
    # baton_rouge_add = clean_baton_rouge_add(baton_rouge_add)
    # baton_rouge_add.to_csv(data_dict['intermediate']['baton_rouge']['parcel'] + 'addresses.csv', index=False)
    # chicago_add1 = pd.read_csv(data_dict['raw']['chicago']['parcel'] + 'Cook_County_Assessor_s_Property_Locations.csv')
    # chicago_add2 = pd.read_csv(data_dict['raw']['chicago']['parcel'] + 'Address_Points_cook_county.csv')
    # la_add = gpd.read_file(data_dict['raw']['la']['parcel'] + 'la_addresspoints.gdb')
    #sd_add = gpd.read_file(data_dict['raw']['sd']['parcel'] + 'addrapn_datasd_san_diego/addrapn_datasd.shp')
    # sf_add = pd.read_csv(
    #     data_dict['raw']['sf']['parcel'] + 'Addresses_with_Units_-_Enterprise_Addressing_System_san_francisco.csv')
    # seattle_add = gpd.read_file(data_dict['raw']['seattle']['parcel'] +
    #                           'Parcels_for_King_County_with_Address_with_Property_Information___parcel_address_area-shp/'
    #                           'Parcels_for_King_County_with_Address_with_Property_Information___parcel_address_area.shp')
    #
    # # clean_baton_rouge_add(baton_rouge_add).to_csv(data_dict['intermediate']['baton_rouge']['parcel'] + 'addresses.csv', index=False)
    # clean_chi_add2(chicago_add1).to_csv(data_dict['intermediate']['chicago']['parcel'] + 'addresses_from_parcels.csv', index=False)
    # clean_chi_add1(chicago_add2).to_csv(data_dict['intermediate']['chicago']['parcel'] + 'addresses_from_points.csv', index=False)
    # # clean_la_add(la_add).to_csv(data_dict['intermediate']['la']['parcel'] + 'addresses.csv', index=False)
    # # clean_sf_add(sf_add).to_csv(data_dict['intermediate']['sf']['parcel'] + 'addresses.csv', index=False)
    # #clean_sd_add(sd_add).to_csv(data_dict['intermediate']['sd']['parcel'] + 'addresses.csv', index=False)
    # # clean_seattle_add(seattle_add).to_csv(data_dict['intermediate']['seattle']['parcel'] + 'addresses.csv', index=False)
    # chi1 = pd.read_csv(data_dict['intermediate']['chicago']['parcel'] + 'addresses_from_parcels.csv', dtype={"parsed_addr_n1": str})
    # chi2 = pd.read_csv(data_dict['intermediate']['chicago']['parcel'] + 'addresses_from_points.csv', dtype={"parsed_addr_n1": str})
    # concat_chi_add(chi1,chi2).to_csv(data_dict['intermediate']['chicago']['parcel'] + 'addresses_concat.csv', index=False)
    pass
