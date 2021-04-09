import pandas as pd
import numpy as np
import geopandas as gpd
from python_modules.helper_functions import add_subset_address_cols, interpolate_polygon
from python_modules.helper_files.data_constants import default_crs
from python_modules.name_parsing import combine_names
from python_modules.helper_files.address_parsing import clean_parse_address
from python_modules.helper_functions import make_panel
from pathos.multiprocessing import ProcessingPool as Pool
import re
import fiona


# function for reading in corrupted gdb files. really only relevant for LA CAMS data
def readShp_nrow(path, numRows):
    fiona_obj = fiona.open(str(path))
    toReturn = gpd.GeoDataFrame.from_features(fiona_obj[0:numRows])
    toReturn.crs = fiona_obj.crs
    return (toReturn)


# classify & clean name columns+ clean & parse primary and mailing addresses
# function that runs code in parallel
def parallelize_dataframe(df:pd.DataFrame, func, n_cores=4) -> pd.DataFrame:
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
def clean_parse_parallel(df:pd.DataFrame) -> pd.DataFrame:
    df = clean_parse_address(
        dataframe=df, address_col='address_fa',st_name="address_sn", st_sfx="address_ss",
        st_d="address_sd", unit='address_u', st_num='address_n1',
        country='address_country', state='address_state', st_num2 ='address_n2',city='address_city',
        zipcode='address_zip', prefix2='parsed_', prefix1='cleaned_'
    )
    return df


# ADDRESS CLEANING FUNCTIONS #
# takes an address df (geopandas or pandas), stanardizes and cleans it and returns a standardized pandas dataframe
# these functions get address dataframes to be in standardized formats (renamed columns, added variables, etc)
# such that the dataframe can be passed to clean_parse_parallel and exported
# see address cols in data constants for full list of necessary columns needed for clean_parse_parallel
# ill note if there is anything special with the function, but otherwise assume that it follows a standard flow of
# 1. rename columns -> add columns -> subset to only needed columns -> clean_parse_parrallel -> return

# chicago cleaning functions:
# chicago address files come in two seperate files that together represent a full set of addresses in cook county

# clean chi_add_points cleans a points file that represents centroid points for cook county parcel polygons
def clean_chi_add_points(df):
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


# basically the same as address points, but these are for parcel polygons (lat long are centroid points, so it is
# basically equivalent, these just have some addresses not in the other df and vice versa
def clean_chi_add_parcels(df):
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


def concat_chi_add(df1, df2):
    df1 = df1.append(df2).drop_duplicates(subset = [
        'parcelID',
        "parsed_addr_n1",
        "parsed_addr_sn",
        "parsed_addr_ss",
        "parsed_city"

    ])
    return df1


# saint louis is a little strange because they provide parcel polygons for entire streets
# eg main st 100-900. This is fine for small streets as its not problematic to take centroid polygons, but
# it becomes an issue for larger streets. For larger streets I take a best guess on which way the street runs and
# linearly interpolate lat long between the bottom and top range of the address span
# so if main st 100-900 runs nw that means it has its smallest numbers in the south east and increases going north west

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
        'PostType': 'address_ss',
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


def merge_sac_addresses():
    add = pd.read_csv(data_dict)


def clean_sf_add(df):
    sf_rename_dict = {
        "Parcel Number": 'parcelID',
        'Unit Number': 'address_u',
        'Address Number': 'address_n1',
        'Street Name': 'address_sn',
        'Street Type':   'address_ss',
        'ZIP Code': 'address_zip',
        'Address': 'address_fa',
        #'PIN': 'parcelID',
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


def clean_orlando_add(df):
    orlando_rename_dict = {
        'OFFICIAL_P': 'parcelID',
       "COMPLETE_A": 'address_fa',
        "ADDRESS__1": 'address_n1',
        "ADDRESS__2": "address_n2",
        "BASENAME": "address_sn",
        "POST_TYPE":"address_ss",
        "POST_DIREC":   "address_sd",
        "MUNICIPAL_": 'address_city',
        "ZIPCODE": "address_zip",
        "LATITUDE": "lat",
        "LONGITUDE": "long",
    }
    df.rename(columns=orlando_rename_dict, inplace=True)
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


def merge_sac_parcel_id(sac_add = pd.DataFrame,  xwalk = pd.DataFrame):
    return pd.merge(
        sac_add, 
        xwalk[xwalk['Parcel Number'].notna()][["Address_ID", "Parcel_Number"]].drop_duplicates(), 
        left_on = "Address_ID", right_on = "Address_ID", how = "left"
        )



def clean_sac_add(df):
    sac_rename_dict = {
        'APN': 'parcelID',
        "Address_Number": 'address_n1',
        "Street_Name": "address_sn",
        "Street_Suffix":"address_ss",
        "Pre_Directiona;":   "address_sd",
        "Postal_City": 'address_city',
        "Zip_Code": "address_zip",
        "Latititde_Y": "lat",
        "Longitude_X": "long",
    }
    df.rename(columns=sac_rename_dict, inplace=True)
    df = add_subset_address_cols(df)
    df = parallelize_dataframe(df=df, func=clean_parse_parallel, n_cores=2)
    return df



# used to reclean data in the event that you dont want to read in a shapefile
# mostly uses because its faster to read in a csv than a shp
def clean_int_addresses(df):
    df = add_subset_address_cols(df)
    df = parallelize_dataframe(df=df, func=clean_parse_parallel, n_cores=2)
    return df


if __name__ == "__main__":
    print("hello")
#     data_dict = make_data_dict(use_seagate=False)
    # stl_add = gpd.read_file(data_dict['raw']['stl']['parcel'] + 'streets/tgr_str_cl.shp')
    # stl_add = clean_stl_add(stl_add)
    # stl_add.to_csv(data_dict['intermediate']['stl']['parcel'] + 'addresses.csv', index=False)
    # baton_rouge_add = pd.read_csv(
    #     data_dict['raw']['baton_rouge']['parcel'] + 'addresses_Property_Information_ebrp.csv')
    # baton_rouge_add = clean_baton_rouge_add(baton_rouge_add)
    # baton_rouge_add.to_csv(data_dict['intermediate']['baton_rouge']['parcel'] + 'addresses.csv', index=False)
    # chicago_add1 = pd.read_csv(data_dict['raw']['chicago']['parcel'] + 'Cook_County_Assessor_s_Property_Locations.csv')
    # chicago_add2 = pd.read_csv(data_dict['raw']['chicago']['parcel'] + 'Address_Points_cook_county.csv')

    # orlando_add = gpd.read_file(data_dict['raw']['orlando']['parcel'] + "Address Points/ADDRESS_POINT.shp")
    # clean_orlando_add(orlando_add).to_csv(data_dict['intermediate']['orlando']['parcel'] + 'addresses.csv', index=False)

    # la_add = gpd.read_file("/Users/JoeFish/Desktop/la_addresspoints.gdb", nrows = 100)
    # la_add = pd.read_csv(data_dict['intermediate']['la']['parcel'] + 'addresses.csv')
    # file is corrupted so we have to read it in this way...
    # print(la_add.head())
    #sd_add = gpd.read_file(data_dict['raw']['sd']['parcel'] + 'addrapn_datasd_san_diego/addrapn_datasd.shp')
    # sf_add = pd.read_csv(
    #     data_dict['raw']['sf']['parcel'] + 'Addresses_with_Units_-_Enterprise_Addressing_System_san_francisco.csv')
    # seattle_add = gpd.read_file(data_dict['raw']['seattle']['parcel'] +
    #                           'Addresses_in_King_County___address_point/Addresses_in_King_County___address_point.shp')
    #
    # # clean_baton_rouge_add(baton_rouge_add).to_csv(data_dict['intermediate']['baton_rouge']['parcel'] + 'addresses.csv', index=False)
    # clean_chi_add2(chicago_add1).to_csv(data_dict['intermediate']['chicago']['parcel'] + 'addresses_from_parcels.csv', index=False)
    # clean_chi_add1(chicago_add2).to_csv(data_dict['intermediate']['chicago']['parcel'] + 'addresses_from_points.csv', index=False)
    # clean_int_addresses(la_add).to_csv(data_dict['intermediate']['la']['parcel'] + 'addresses_temp.csv', index=False)
    # clean_sf_add(sf_add).to_csv(data_dict['intermediate']['sf']['parcel'] + 'addresses.csv', index=False)
    # #clean_sd_add(sd_add).to_csv(data_dict['intermediate']['sd']['parcel'] + 'addresses.csv', index=False)
    # clean_seattle_add(seattle_add).to_csv(data_dict['intermediate']['seattle']['parcel'] + 'addresses.csv', index=False)
    # chi1 = pd.read_csv(data_dict['intermediate']['chicago']['parcel'] + 'addresses_from_parcels.csv', dtype={"parsed_addr_n1": str})
    # chi2 = pd.read_csv(data_dict['intermediate']['chicago']['parcel'] + 'addresses_from_points.csv', dtype={"parsed_addr_n1": str})
    # concat_chi_add(chi1,chi2).to_csv(data_dict['intermediate']['chicago']['parcel'] + 'addresses_concat.csv', index=False)
    # sac_add = pd.read_csv(data_dict['raw']['sac']['parcel'] + 'Addresses.csv')
    # sac_xwalk = pd.read_csv(data_dict['raw']['sac']['parcel'] + 'Address_parcel_xwalk.csv')
    # sac_add = merge_sac_parcel_id(sac_add=sac_add, sac_xwalk=sac_xwalk)
    # clean_sac_add(sac_add).to_csv(data_dict['intermediate']['sac']['parcel'] + 'addresses_concat.csv', index=False)
        
    
    pass
