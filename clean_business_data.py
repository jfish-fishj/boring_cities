# file cleans business location data and exports back
import pandas as pd
import numpy as np
from helper_functions import write_to_log, make_year_var, WTL_TIME
from data_constants import make_data_dict, filePrefix, name_parser_files
from name_parsing import parse_and_clean_name, classify_name, clean_name
from address_parsing import clean_parse_address
from pathos.multiprocessing import ProcessingPool as Pool
import os


# add columns not present in one dataframe to the other
# TODO make data constant that has all desired business columns
def add_cols_from_other_df(df1, df_list):
    col_list = [col for df in df_list for col in df.columns]
    for col in col_list:
        if col not in df1.columns:
            df1[col] = np.nan
    return df1

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
    # assumes all name/address columns exist in dataframe
    # otherwise will throw an error
    # classify names into businesses/sole propreitorships
    # sole proprietorships are flagged as "person"
    # clean name columns
    df = clean_name(dataframe=df, name_column='dba_name', prefix1='cleaned_')
    df = clean_name(dataframe=df, name_column='ownership_name', prefix1='cleaned_')
    df = clean_name(dataframe=df, name_column='business_name', prefix1='cleaned_')
    df = classify_name(
        # reminder that probabilistic means names like "Uber" will be flagged as businesses
        # also means uncommon names like Matinee Apinwasree will get flagged as businesses
        dataframe=df, name_cols=['cleaned_dba_name', 'cleaned_business_name'], probalistic_classification=True,
        type_col='is_business', weight_format='[a-z\s]+\s&[a-z\s]+' # regex for law firms such as johnson & johnson
    )
    # primary address
    df = clean_parse_address(
        dataframe=df, address_col='primary_address_fa',st_name="primary_address_sn", st_sfx="primary_address_ss",
        st_d="primary_address_sd", unit='primary_address_u', st_num='primary_address_n1',
        country='primary_address_country', state='primary_address_state', st_num2 ='primary_address_n2',city='primary_address_city',
        zipcode='primary_address_zip', prefix2='primary_cleaned_', prefix1='cleaned_'
    )
    # mailing address
    df =clean_parse_address(
        dataframe=df, address_col='mail_addr_fa', city='mail_addr_city',st_name="address_sn", st_sfx="mail_address_ss",
        st_d="mail_address_sd", unit='mail_address_u',
        st_num='mail_address_n',country='mail_address_country', state='mail_address_state', st_num2 ='mail_address_n2',
        zipcode='mail_addr_zip', prefix2='mail_cleaned_', prefix1='cleaned_', raise_error_on_na=False
    )
    return df

# wrapper functions for cleaning
# san francisco
def clean_sf_bus(sf_bus):
    sf_bus = parallelize_dataframe(df=sf_bus, func=clean_parse_parallel, n_cores=4)
    sf_bus = make_year_var(df=sf_bus, date_col='location_start_date', new_col='location_start_year')
    sf_bus = make_year_var(df=sf_bus, date_col='location_end_date', new_col='location_end_year')
    sf_bus = make_year_var(df=sf_bus, date_col='business_start_date', new_col='business_start_year')
    sf_bus = make_year_var(df=sf_bus, date_col='business_end_date', new_col='business_end_year')
    sf_start_year_agg = sf_bus.groupby('location_start_year').agg(**{
        'num_businesses': ('location_id', 'count'),
        'num_sole_prop': ('is_business', lambda x: (x == 'person').sum()),
        'num_missing_naics': ('naics', lambda x: x.isna().sum()),
        'num_missing_pa': ('naics', lambda x: x.isna().sum()),
        'num_missing_ma': ('naics', lambda x: x.isna().sum()),
        'num_ended': ('location_end_year', lambda x: x.notnull().sum()),
    }
                                                                  )
    sf_start_year_agg.to_csv(filePrefix + "/qc/sf_start_year_agg.csv")
    sf_bus.to_csv(data_dict['intermediate']['sf']['business location'] + '/business_location.csv', index=False)

# los angeles
def clean_la_bus(la_bus):
    la_bus = parallelize_dataframe(df=la_bus, func=clean_parse_parallel, n_cores=4)
    # make year variables from dates
    la_bus = make_year_var(df=la_bus, date_col='location_start_date', new_col='location_start_year')
    la_bus = make_year_var(df=la_bus, date_col='location_end_date', new_col='location_end_year')

    # quality control logs
    # aggregations by starting year
    la_start_year_agg = la_bus.groupby('location_start_year').agg(**{
        'num_businesses': ('location_id', 'count'),
        'num_sole_prop': ('is_business', lambda x: (x == 'person').sum()),
        'num_missing_naics': ('naics', lambda x: x.isna().sum()),
        'num_missing_pa': ('naics', lambda x: x.isna().sum()),
        'num_missing_ma': ('naics', lambda x: x.isna().sum()),
        'num_ended': ('location_end_year', lambda x: x.notnull().sum()),
    }
                                                                  )

    la_start_year_agg.to_csv(filePrefix + "/qc/la_start_year_agg.csv")

    la_bus.to_csv(data_dict['intermediate']['la']['business location'] + '/business_location.csv', index=False)

# chicago
def clean_chicago_bus(chicago_bus):
    chicago_bus = parallelize_dataframe(df=chicago_bus, func=clean_parse_parallel, n_cores=4)
    # make year variables from dates
    chicago_bus = make_year_var(df=chicago_bus, date_col='location_start_date', new_col='location_start_year')
    chicago_bus = make_year_var(df=chicago_bus, date_col='location_end_date', new_col='location_end_year')

    # quality control logs
    # aggregations by starting year
    chi_start_year_agg = chicago_bus.groupby('location_start_year').agg(**{
        'num_businesses': ('location_id', 'count'),
        'num_sole_prop': ('is_business', lambda x: (x == 'person').sum()),
        'num_missing_naics': ('naics', lambda x: x.isna().sum()),
        'num_missing_pa': ('naics', lambda x: x.isna().sum()),
        'num_missing_ma': ('naics', lambda x: x.isna().sum()),
        'num_ended': ('location_end_year', lambda x: x.notnull().sum()),
    }
                                                                  )

    chi_start_year_agg.to_csv(filePrefix + "/qc/chi_start_year_agg.csv")

    chicago_bus.to_csv(data_dict['intermediate']['chicago']['business location'] + '/business_location.csv', index=False)

# san diego
def clean_sd_bus(sd_bus):
    sd_bus = parallelize_dataframe(df=sd_bus, func=clean_parse_parallel, n_cores=4)
    # make year variables from dates
    sd_bus = make_year_var(df=sd_bus, date_col='location_start_date', new_col='location_start_year')
    sd_bus = make_year_var(df=sd_bus, date_col='location_end_date', new_col='location_end_year')

    # quality control logs
    # aggregations by starting year
    sd_start_year_agg = sd_bus.groupby('location_start_year').agg(**{
        'num_businesses': ('location_id', 'count'),
        'num_sole_prop': ('is_business', lambda x: (x == 'person').sum()),
        'num_missing_naics': ('naics', lambda x: x.isna().sum()),
        'num_missing_pa': ('naics', lambda x: x.isna().sum()),
        'num_missing_ma': ('naics', lambda x: x.isna().sum()),
        'num_ended': ('location_end_year', lambda x: x.notnull().sum()),
    }
                                                                  )

    sd_start_year_agg.to_csv(filePrefix + "/qc/sd_start_year_agg.csv")

    sd_bus.to_csv(data_dict['intermediate']['sd']['business location'] + '/business_location.csv', index=False)


if __name__ == "__main__":
    write_to_log(f'Starting clean business data at {WTL_TIME}')
    # initialize data dict
    data_dict = make_data_dict(use_seagate=True)
    # read in raw files comment out as needed
    sf_bus = pd.read_csv(
            data_dict['raw']['sf']['business location'] + '/Registered_Business_Locations_-_San_Francisco.csv', nrows=50)
    # raw business data
    la_bus = pd.read_csv(data_dict['raw']['la']['business location'] + '/Listing_of_All_Businesses.csv', nrows=50)
    chicago_bus = pd.read_csv(data_dict['raw']['chicago']['business location'] + '/Business_Licenses.csv')
    # san diego comes split apart so read in and concat
    sd_file_list = os.listdir(data_dict['raw']['sd']['business location'] )
    sd_df_list = [pd.read_csv(data_dict['raw']['sd']['business location'] + f'{file}') for file in sd_file_list]
    sd_bus = pd.concat(sd_df_list)
    # rename columns for cleaning
    sf_rename_dict = {
        'Location Id': 'location_id',
        'Business Account Number': 'business_id',
        'Ownership Name': 'ownership_name',
        'DBA Name': 'dba_name',
        'Street Address': 'primary_address_fa',
        'City': 'primary_address_city',
        'State': 'primary_address_state',
        'Source Zipcode': 'primary_address_zip',
        'Business Start Date': 'business_start_date',
        'Business End Date': 'business_end_date',
        'Location Start Date': 'location_start_date',
        'Location End Date': 'location_end_date',
        'Mail Address': 'mail_addr_fa',
        'Mail City': 'mail_addr_city',
        'Mail Zipcode': 'mail_addr_zip',
        'Mail State': 'mail_addr_state',
        'NAICS Code': 'naics',
        'NAICS Code Description': 'naics_descr',
        # ignore all columns not in rename dict
    }
    sd_rename_dict = {
        'account_key': 'location_id',
        'business_owner_name': 'ownership_name',
        'dba_name': 'dba_name',
        'address_number': 'primary_address_n1',
        'address_road': 'primary_address_sn',
        'address_sfx': 'primary_address_ss',
        'address_city': 'primary_address_city',
        'address_state': 'primary_address_state',
        'address_zip': 'primary_address_zip',
        'suite': 'primary_address_u',
        'date_account_creation': 'location_start_date',
        'date_cert_expiration': 'location_end_date',
        'naics_code': 'naics',
        'naics_description': 'naics_descr',
        'ownership_type': 'ownership_type'
        # ignore all columns not in rename dict
    }
    chi_rename_dict = {
        'ID': 'location_id',
        'ACCOUNT NUMBER': 'business_id',
        'LEGAL NAME': 'business_name',
        'DOING BUSINESS AS': 'dba_name',
        'ADDRESS': 'primary_address_fa',
        'CITY': 'primary_address_city',
        'STATE': 'primary_address_state',
        'ZIP CODE': 'primary_address_zip',
        'LICENSE TERM START DATE': 'location_start_date',
        'LICENSE TERM EXPIRATION DATE': 'location_end_date',
        'LICENSE DESCRIPTION': 'business_type'
        # ignore all columns not in rename dict
    }
    la_rename_dict = {
        'LOCATION ACCOUNT #': 'location_id',
        'BUSINESS NAME': 'business_name',
        'DBA NAME': 'dba_name',
        'STREET ADDRESS': 'primary_address_fa',
        'CITY': 'primary_address_city',
        'ZIP CODE': 'primary_address_zip',
        'LOCATION START DATE': 'location_start_date',
        'LOCATION END DATE': 'location_end_date',
        'MAILING ADDRESS': 'mail_addr_fa',
        'MAILING CITY': 'mail_addr_city',
        'MAILING ZIP CODE': 'mail_addr_zip',
        'NAICS': 'naics',
        'PRIMARY NAICS DESCRIPTION': 'naics_descr',
        # ignore all columns not in rename dict
    }
    # rename cols
    la_bus.rename(columns=la_rename_dict, inplace=True)
    sf_bus.rename(columns=sf_rename_dict, inplace=True)
    sd_bus.rename(columns=sd_rename_dict, inplace=True)
    chicago_bus.rename(columns=chi_rename_dict, inplace=True)
    # filter columns
    la_bus = la_bus[la_rename_dict.values()]
    sf_bus = sf_bus[sf_rename_dict.values()]
    sd_bus = sd_bus[sd_rename_dict.values()]
    sf_bus = sf_bus[sf_rename_dict.values()]
    # formatted like this so it's easy to comment out
    dfs_to_add = [
        sf_bus,
        chicago_bus,
        sd_bus,
        la_bus
    ]
    # add all col names
    la_bus = add_cols_from_other_df(df1=la_bus, df_list=dfs_to_add)
    sf_bus = add_cols_from_other_df(df1=sf_bus, df_list=dfs_to_add)
    sd_bus = add_cols_from_other_df(df1=sd_bus, df_list=dfs_to_add)
    chicago_bus = add_cols_from_other_df(df1=chicago_bus, df_list=dfs_to_add)
    # cleaning functions
    clean_sd_bus(sd_bus=sd_bus, )
    clean_chicago_bus(chicago_bus=chicago_bus)