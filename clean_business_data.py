# file cleans business location data and exports back
import pandas as pd
import numpy as np
from helper_functions import write_to_log, make_year_var, WTL_TIME
from data_constants import make_data_dict, filePrefix, name_parser_files
from name_parsing import parse_and_clean_name, classify_name, clean_name
from address_parsing import clean_parse_address
from pathos.multiprocessing import ProcessingPool as Pool
write_to_log(f'Starting clean business data at {WTL_TIME}')
# initialize data dict
data_dict = make_data_dict(use_seagate=True)

# raw business data
la_bus = pd.read_csv(data_dict['raw']['la']['business location'] + '/Listing_of_All_Businesses.csv')
sf_bus = pd.read_csv(data_dict['raw']['sf']['business location'] + '/Registered_Business_Locations_-_San_Francisco.csv')

# dictionaries for renamind col names
sf_rename_dict = {
    'Location Id': 'location_id',
    'Business Account Number': 'business_id',
    'Ownership Name': 'ownership_name',
    'DBA Name': 'dba_name',
    'Street Address': 'primary_addr_fa',
    'City': 'primary_addr_city',
    'State': 'primary_addr_state',
    'Source Zipcode': 'primary_addr_zip',
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
la_rename_dict = {
    'LOCATION ACCOUNT #': 'location_id',
    'BUSINESS NAME': 'business_name',
    'DBA NAME': 'dba_name',
    'STREET ADDRESS': 'primary_addr_fa',
    'CITY': 'primary_addr_city',
    'ZIP CODE': 'primary_addr_zip',
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
sf_bus.rename(columns=sf_rename_dict, inplace=True)
la_bus.rename(columns = la_rename_dict, inplace=True)

# filter columns
sf_bus = sf_bus[sf_rename_dict.values()]
la_bus = la_bus[la_rename_dict.values()]

# add columns not present in one dataframe to the other
# TODO make data constant that has all desired business columns
def add_cols_from_other_df(df1, df2):
    for col in df2.columns:
        if col not in df1.columns:
            df1[col] = np.nan
    return df1

sf_bus = add_cols_from_other_df(df1=sf_bus, df2=la_bus)
la_bus = add_cols_from_other_df(df1=la_bus, df2=sf_bus)

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
        dataframe=df, address_col='primary_addr_fa',st_name="sn", st_sfx="ss", st_d="sd", unit='u', st_num='n',
        country='country', state='state', st_num2 ='n2',city='primary_addr_city',
        zipcode='primary_addr_zip', prefix2='primary_cleaned_', prefix1='cleaned_'
    )
    # mailing address
    df =clean_parse_address(
        dataframe=df, address_col='mail_addr_fa', city='mail_addr_city',st_name="sn", st_sfx="ss", st_d="sd", unit='u',
        st_num='n',country='country', state='state', st_num2 ='n2',
        zipcode='mail_addr_zip', prefix2='mail_cleaned_', prefix1='cleaned_', raise_error_on_na=False
    )
    return df

# run cleaning & parsing function function in parallel
# default is n cores, but change as needed for more or less speed/as computer can handle
sf_bus = parallelize_dataframe(df=sf_bus, func=clean_parse_parallel, n_cores=4)
la_bus = parallelize_dataframe(df=la_bus, func=clean_parse_parallel, n_cores=4)

# make year variables from dates
sf_bus = make_year_var(df=sf_bus, date_col='location_start_date', new_col='location_start_year')
sf_bus = make_year_var(df=sf_bus, date_col='location_end_date', new_col='location_end_year')
sf_bus = make_year_var(df=sf_bus, date_col='business_start_date', new_col='business_start_year')
sf_bus = make_year_var(df=sf_bus, date_col='business_end_date', new_col='business_end_year')
la_bus = make_year_var(df=la_bus, date_col='location_start_date', new_col='location_start_year')
la_bus = make_year_var(df=la_bus, date_col='location_end_date', new_col='location_end_year')

# quality control logs
# aggregations by starting year
la_start_year_agg = la_bus.groupby('location_start_year').agg(**{
    'num_businesses': ('location_id', 'count'),
    'num_sole_prop': ('is_business', lambda x: (x=='person').sum() ),
    'num_missing_naics':('naics', lambda x: x.isna().sum()),
    'num_missing_pa':('naics', lambda x: x.isna().sum()),
    'num_missing_ma':('naics', lambda x: x.isna().sum()),
    'num_ended':('location_end_year', lambda x: x.notnull().sum()),
}
)
sf_start_year_agg = sf_bus.groupby('location_start_year').agg(**{
    'num_businesses': ('location_id', 'count'),
    'num_sole_prop': ('is_business', lambda x: (x=='person').sum() ),
    'num_missing_naics':('naics', lambda x: x.isna().sum()),
    'num_missing_pa':('naics', lambda x: x.isna().sum()),
    'num_missing_ma':('naics', lambda x: x.isna().sum()),
    'num_ended':('location_end_year', lambda x: x.notnull().sum()),
}
)

la_start_year_agg.to_csv(filePrefix + "/qc/la_start_year_agg.csv")
sf_start_year_agg.to_csv(filePrefix + "/qc/sf_start_year_agg.csv")

la_bus.to_csv(data_dict['intermediate']['la']['business location'] + '/business_location.csv', index=False)
sf_bus.to_csv(data_dict['intermediate']['sf']['business location'] + '/business_location.csv', index=False)