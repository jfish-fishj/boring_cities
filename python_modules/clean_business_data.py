# file cleans business_location data and exports back
from multiprocessing import Value
import pandas as pd
import numpy as np
from helper_functions import *
from data_constants import make_data_dict, filePrefix, name_parser_files, business_cols, misc_data_dict
from name_parsing import classify_name, clean_name, combine_names
from address_parsing import clean_parse_address
from pathos.multiprocessing import ProcessingPool as Pool
import os
import janitor
import re
import sys

data_dict = make_data_dict(use_seagate=False)

# classify & clean name columns+ clean & parse primary and mailing addresses
# function that runs code in parallel
def parallelize_dataframe(df:pd.DataFrame, func, n_cores=4):
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
def clean_parse_parallel(df:pd.DataFrame, standardization_dict:dict = None):
    og_shape = df.shape[0]
    if standardization_dict is None:
        standardization_dict = {}
    # assumes all name/address columns exist in dataframe
    # otherwise will throw an error
    # classify names into businesses/sole propreitorships
    # sole proprietorships are flagged as "person"
    # make business columns
    # naics codes
    df = make_naics_vars(df, "naics")
    # business types
    df['business_type_standardized'] = standardize_business_type(df["business_type"],
                                                                 standardize_dict=standardization_dict)
    # clean up standardization dict w/ regex based cleaning
    df = get_business_type(df=df, naics_col="naics_descr3_standardized", business_type_col='business_type_standardized')
    # clean name columns
    df["cleaned_dba_name"] = clean_name(df['dba_name'])
    df["cleaned_ownership_name"] = clean_name(df['ownership_name'])
    df["cleaned_business_name"] = clean_name(df['business_name'])
    if og_shape != df.shape[0]:
        raise ValueError(f"Number of rows has changed from {og_shape} to {df.shape[0]}")
    df["is_business"] = classify_name(
        # reminder that probabilistic means names like "Uber" will be flagged as businesses
        # also means uncommon names like Matinee Apinwasree will get flagged as businesses
        dataframe=df[['cleaned_dba_name', 'cleaned_business_name','cleaned_ownership_name']],
        name_cols=['cleaned_dba_name', 'cleaned_business_name','cleaned_ownership_name'],
        probalistic_classification=True,
        type_col='is_business', weight_format='[a-z\s]+\s&[a-z\s]+' # regex for law firms such as johnson & johnson
    )
    if og_shape != df.shape[0]:
        raise ValueError(f"Number of rows has changed from {og_shape} to {df.shape[0]}")

    # primary address
    df = clean_parse_address(
        dataframe=df, address_col='primary_address_fa',st_name="primary_address_sn", st_sfx="primary_address_ss",
        st_d="primary_address_sd", unit='primary_address_u', st_num='primary_address_n1',
        country='primary_address_country', state='primary_address_state', st_num2 ='primary_address_n2',city='primary_address_city',
        zipcode='primary_address_zip', prefix2='primary_cleaned_', prefix1='cleaned_'
    )
    if og_shape != df.shape[0]:
        raise ValueError(f"Number of rows has changed from {og_shape} to {df.shape[0]}")

    # mailing address
    df = clean_parse_address(
        dataframe=df, address_col='mail_address_fa', city='mail_address_city',st_name="mail_address_sn", st_sfx="mail_address_ss",
        st_d="mail_address_sd", unit='mail_address_u',
        st_num='mail_address_n',country='mail_address_country', state='mail_address_state', st_num2 ='mail_address_n2',
        zipcode='mail_address_zip', prefix2='mail_cleaned_', prefix1='cleaned_', raise_error_on_na=False
    )
    if og_shape != df.shape[0]:
        raise ValueError(f"Number of rows has changed from {og_shape} to {df.shape[0]}")
    return df


# wrapper functions for all business cleaning
# unless otherwise specified or if additional notes are needed, flow is
#   read in df -> rename columns -> add missing columns -> clean_parse_parallel -> subset columns -> create qc csv
#   export df to intermediate folder

def clean_sf_bus():
    sf_bus = pd.read_csv(
        data_dict['raw']['sf']['business_location'] + '/Registered_Business_Locations_-_San_Francisco.csv')
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
        'Mail Address': 'mail_address_fa',
        'Mail City': 'mail_address_city',
        'Mail Zipcode': 'mail_address_zip',
        'Mail State': 'mail_address_state',
        'NAICS Code': 'naics',
        'NAICS Code Description': 'naics_descr',
        # ignore all columns not in rename dict
    }
    sf_bus.rename(columns=sf_rename_dict, inplace=True)
    sf_bus = sf_bus.assign(
        business_name = np.nan,
        business_type = np.nan,
        source = "san francisco"
    )
    sf_bus = parallelize_dataframe(df=sf_bus, func=clean_parse_parallel, n_cores=4)
    sf_bus['location_start_year'] = make_year_var(sf_bus['location_start_date'])
    sf_bus['location_end_year'] = make_year_var(sf_bus['location_end_date'])
    sf_bus['business_start_year'] = make_year_var(sf_bus['business_start_date'])
    sf_bus['business_end_year'] = make_year_var(sf_bus['business_end_date'])
    sf_bus = add_subset_business_cols(sf_bus)
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
    sf_bus.to_csv(data_dict['intermediate']['sf']['business_location'] + '/business_location.csv', index=False)
    return sf_bus

def clean_la_bus():
    la_rename_dict = {
        'LOCATION ACCOUNT #': 'location_id',
        'BUSINESS NAME': 'business_name',
        'DBA NAME': 'dba_name',
        'STREET ADDRESS': 'primary_address_fa',
        'CITY': 'primary_address_city',
        'ZIP CODE': 'primary_address_zip',
        'LOCATION START DATE': 'location_start_date',
        'LOCATION END DATE': 'location_end_date',
        'MAILING ADDRESS': 'mail_address_fa',
        'MAILING CITY': 'mail_address_city',
        'MAILING ZIP CODE': 'mail_address_zip',
        'NAICS': 'naics',
        'PRIMARY NAICS DESCRIPTION': 'naics_descr',
        "LOCATION": "location"
        # ignore all columns not in rename dict
    }
    la_bus =  pd.read_csv(data_dict['raw']['la']['business_location'] + '/Listing_of_All_Businesses.csv',
     usecols=list(la_rename_dict.keys()))
    la_bus = la_bus.rename(columns=la_rename_dict)
    la_bus = la_bus.assign(
        ownership_name = np.nan,
        business_type = np.nan,
        source = "los angeles"
    )
    # make year variables from dates
    la_bus['location_start_year'] = make_year_var(la_bus['location_start_date'])
    la_bus['location_end_year'] = make_year_var(la_bus['location_end_date'])
    lat_long = la_bus['location'].str.extract(r'(-[0-9\.]+)[\s,]+([0-9\.]+)')
    la_bus['lat'] = lat_long.iloc[:, 0]
    la_bus['long'] = lat_long.iloc[:, 1]

    la_bus = parallelize_dataframe(df=la_bus, func=clean_parse_parallel, n_cores=1)
    la_bus = add_subset_business_cols(la_bus)

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

    la_bus.to_csv(data_dict['intermediate']['la']['business_location'] + '/business_location.csv', index=False)
    return la_bus


def clean_chicago_bus():
    chicago_bus = pd.read_csv(data_dict['raw']['chicago']['business_location'] + '/Business_Licenses.csv')
    chi_rename_dict = {
        'ACCOUNT NUMBER': 'business_id',
        'LEGAL NAME': 'business_name',
        'DOING BUSINESS AS NAME': 'dba_name',
        'ADDRESS': 'primary_address_fa',
        'CITY': 'primary_address_city',
        'STATE': 'primary_address_state',
        'ZIP CODE': 'primary_address_zip',
        'LICENSE TERM START DATE': 'location_start_date',
        'LICENSE TERM EXPIRATION DATE': 'location_end_date',
        'BUSINESS ACTIVITY': 'business_type',
        'LONGITUDE':'long',
        'LATITUDE':'lat'
        # ignore all columns not in rename dict
    }
    chicago_bus.rename(columns=chi_rename_dict, inplace=True)
    chicago_bus = chicago_bus.assign(
        mail_address_fa = np.nan,
        source = "chicago",
        ownership_name = np.nan,
        naics = np.nan
    )
    chicago_biz_type_dict = {

    }
    # make year variables from dates
    chicago_bus['location_start_year'] = make_year_var(chicago_bus['location_start_date'])
    chicago_bus['location_end_year'] = make_year_var(chicago_bus['location_end_date'], round_down=True)
    chicago_bus['location_end_year'] =np.where(
        (chicago_bus['location_start_year'] > chicago_bus['location_end_year']),
        chicago_bus['location_start_year'],
        chicago_bus['location_end_year']
    )
    chicago_bus['business_start_year'] = chicago_bus.groupby("business_id")['location_start_year'].transform("min")
    chicago_bus['business_end_year'] = chicago_bus.groupby("business_id")['location_end_year'].transform("min")
    
    chicago_bus = parallelize_dataframe(df=chicago_bus, func=clean_parse_parallel, n_cores=4)
    
    chicago_bus ['location_id'] = chicago_bus.groupby(
        ["cleaned_dba_name","cleaned_business_name",  "primary_cleaned_fullAddress"]).ngroup()
    chicago_bus['location_id'] = chicago_bus['location_id'].astype(str) + "_chicago"
    chicago_bus = add_subset_business_cols(chicago_bus)
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

    chicago_bus.to_csv(data_dict['intermediate']['chicago']['business_location'] + '/business_location.csv', index=False)
    return chicago_bus


def clean_philly_bus():
    df = pd.read_csv(data_dict['raw']['philly']['business_location'] + 'business_licenses-2.csv')
    df = df.rename(columns={
        'address': 'primary_address_fa',
        'zip': 'primary_address_zip',
        'parcel_id_num': 'parcelID',
        'legalname': 'business_name',
        'business_name': 'dba_name',
        'business_mailing_address': 'mail_address_fa',
        'lat': 'lat',
        'lng': 'long',
        'initialissuedate': 'location_start_date',
        'expirationdate': 'location_end_date',
        'licensenum': 'location_id',
        'licensetype': 'business_type',
        'legalentitytype': 'ownership_type'
    })
    df['location_start_year'] = make_year_var(df['location_start_date'])
    df['location_end_year'] = make_year_var(df['location_end_date'])
    df['primary_address_city'] = "philadelphia"
    df = parallelize_dataframe(df=df, func=clean_parse_parallel, n_cores=2)
    # quality control logs
    # aggregations by starting year
    philly_start_year_agg = df.groupby('location_start_year').agg(**{
        'num_businesses': ('location_id', 'count'),
        'num_sole_prop': ('is_business', lambda x: (x == 'person').sum()),
        'num_missing_naics': ('naics', lambda x: x.isna().sum()),
        'num_missing_pa': ('naics', lambda x: x.isna().sum()),
        'num_missing_ma': ('naics', lambda x: x.isna().sum()),
        'num_ended': ('location_end_year', lambda x: x.notnull().sum()),
    }
                                                                   )

    philly_start_year_agg.to_csv(filePrefix + "/qc/philly_start_year_agg.csv")

    df.to_csv(data_dict['intermediate']['philly']['business_location'] + '/business_location.csv', index=False)


def clean_long_beach_bus():
    df = pd.read_csv(data_dict['raw']['long_beach']['business_location'] + 'Business_Licenses_Public_View.csv')
    og_rows = df.shape[0]
    print(df.shape[0])
    df = df.rename(columns={
        'SITELOCATION': 'primary_address_fa',
        'ZIP': 'primary_address_zip',
        'FULLNAME': 'business_name',
        'DBANAME': 'dba_name',
        'X': 'lat',
        'Y': 'long',
        'ISSDTTM': 'location_start_date',
        'MILESTONEDATE': 'location_end_date',
        'LICENSENO': 'location_id',
        'LICCATDESC': 'business_type',
        'COMPANYTYPE': 'ownership_type'
    })
    df['location_start_year'] = make_year_var(df['location_start_date'])
    df['location_end_year'] = make_year_var(df['location_end_date'])
    df['primary_address_city'] = np.where(
        df['OUTSIDECITY'] == 'No',
        "Long Beach",
        np.nan
    )
    df = df.assign(
        mail_address_fa = np.nan,
        source = "long beach",
        ownership_name = np.nan,
        naics = np.nan
    )
    df = parallelize_dataframe(df=df, func=clean_parse_parallel, n_cores=4)
    if og_rows != df.shape[0]:
        raise ValueError()
    print(df.shape[0])
    # quality control logs
    # aggregations by starting year
    long_beach_start_year_agg = df.groupby('location_start_year').agg(**{
        'num_businesses': ('location_id', 'count'),
        'num_sole_prop': ('is_business', lambda x: (x == 'person').sum()),
        'num_missing_naics': ('naics', lambda x: x.isna().sum()),
        'num_missing_pa': ('naics', lambda x: x.isna().sum()),
        'num_missing_ma': ('naics', lambda x: x.isna().sum()),
        'num_ended': ('location_end_year', lambda x: x.notnull().sum()),
    }
                                                                   )

    long_beach_start_year_agg.to_csv(filePrefix + "/qc/long_beach_start_year_agg.csv")
    print("pre return shape", df.shape[0])
    df = add_subset_business_cols(df)
    df.to_csv(data_dict['intermediate']['long_beach']['business_location'] + '/business_location.csv', index=False)
    return df


def clean_baton_rouge_bus():
    df = pd.read_csv(data_dict['raw']['baton_rouge']['business_location'] +
                                  'Businesses_Registered_with_EBR_Parish_baton_r.csv')
    df = df.rename(columns = {
        "ACCOUNT NO": 'location_id',
        "ACCOUNT NAME": 'dba_name',
        "LEGAL NAME": 'ownership_name',
        "BUSINESS OPEN DATE": 'location_start_date',
        "BUSINESS CLOSE DATE": 'location_end_date',
        "OWNERSHIP TYPE": 'ownership_type',
        # "CONTACT PERSON": "ownership_name",
        "NAICS Code": 'naics',
        "NAICS CATEGORY": 'naics_descr',
        "MAILING ADDRESS - LINE 1": 'mail_address_fa1',
        "MAILING ADDRESS - LINE 2": "mail_address_fa2",
        "MAILING ADDRESS - CITY": 'mail_address_city',
        "MAILING ADDRESS - STATE": 'mail_address_state',
        "MAILING ADDRESS - ZIP CODE": 'mail_address_zip',
        "PHYSICAL ADDRESS - LINE 1": 'primary_address_fa1',
        "PHYSICAL ADDRESS - LINE 2": 'primary_address_fa2',
        "PHYSICAL ADDRESS - CITY": 'primary_address_city',
        "PHYSICAL ADDRESS - STATE": 'primary_address_state',
        "PHYSICAL ADDRESS - ZIP CODE": 'primary_address_zip'
    })

    df = df.assign(
        source = "baton rouge",
        business_type = np.nan,
        business_name = np.nan
    )

    df['location_start_year'] = make_year_var(df['location_start_date'])
    df['location_end_year'] = make_year_var(df['location_end_date'])
    df['mail_address_fa'] = combine_names(dataframe = df[['mail_address_fa1','mail_address_fa2']],
                                        name_cols=['mail_address_fa1','mail_address_fa2'])
    df['primary_address_fa'] = combine_names(dataframe=df[['primary_address_fa1', 'primary_address_fa2']],
                                             name_cols=['primary_address_fa1', 'primary_address_fa2'])
    lat_long = df['GEOLOCATION'].str.extract(r'BATON ROUGE.+([-0-9\.]+)[\s,]+([0-9\.-]+)')
    df['lat'] = lat_long.iloc[:, 0]
    df['long'] = lat_long.iloc[:, 1]
    df = parallelize_dataframe(df=df, func=clean_parse_parallel, n_cores=4)


    # quality control logs
    # aggregations by starting year
    baton_rouge_start_year_agg = df.groupby('location_start_year').agg(**{
        'num_businesses': ('location_id', 'count'),
        'num_sole_prop': ('is_business', lambda x: (x == 'person').sum()),
        'num_missing_naics': ('naics', lambda x: x.isna().sum()),
        'num_missing_pa': ('naics', lambda x: x.isna().sum()),
        'num_missing_ma': ('naics', lambda x: x.isna().sum()),
        'num_ended': ('location_end_year', lambda x: x.notnull().sum()),
    }
                                                                   )

    baton_rouge_start_year_agg.to_csv(filePrefix + "/qc/baton_rouge_start_year_agg.csv")
    df = add_subset_business_cols(df)
    df.to_csv(data_dict['intermediate']['baton_rouge']['business_location'] + '/business_location.csv', index=False)
    return df


# files are stored in a bunch of different excel files w/ inconsistent names
def clean_stl_bus():
    df_file_list  =  [file for file in os.listdir(data_dict['raw']['stl']['business_location'] ) if bool(re.search( "[0-9]", file))]
    df_list = [pd.read_excel(data_dict['raw']['stl']['business_location'] + f'{file}').clean_names().assign(
        year = re.search("([0-9]{4})", file).group(1) ) for file in df_file_list]
    df = pd.concat(df_list)
    df = df.clean_names()
    # unite columns because stl uses inconsistant names
    df['bill_date'] = combine_names(df[['bill_date', 'billed_date']], name_cols=['bill_date', 'billed_date'])
    df['contact_name'] =  combine_names(df[['contact_e_mail', 'contact_name']], name_cols=['contact_e_mail', 'contact_name'])
    df['doc_nbr'] = combine_names(df[['doc_nbr', 'doc_nbr_', 'doc_nbr_1']],
                                    name_cols=['doc_nbr', 'doc_nbr_', 'doc_nbr_1'])
    df['date_business_started'] = combine_names(df[['date_business_started', 'date_busines_started']],
                                    name_cols=['date_business_started', 'date_busines_started'])
    df['file_date'] =  combine_names(df[['file_date', 'filed_date']],
                                     name_cols=['file_date', 'filed_date'])
    df['mail_address_fa'] = combine_names(df[['mailing_address_line_1', 'mailing_address_line_2', 'mailing_address_line_3']],
                                    name_cols=['mailing_address_line_1', 'mailing_address_line_2', 'mailing_address_line_3'] )
    df['tax_period'] = combine_names(df[['tax_period', 'tax_period_1']],
                                     name_cols=['tax_period', 'tax_period_1'])
    df['tax_year'] = combine_names(df[['tax_year', 'tax_period_1']], name_cols=['tax_year', 'tax_period_1'])
    df = df.rename(columns={
        "date_business_started": "business_start_date",
        "from_date": "location_start_date",
        "to_date": "location_end_date",
        "owner_business_hq_name": "ownership_name",
        "trade_name": "dba_name",
        "house_nbr_": "primary_address_n1",
        "property_st_name": "primary_address_sn",
        "st_dir_": "primary_address_sd",
        "st_type": "primary_address_ss",
        "zip_cd_": "primary_address_zip"
    })
    df['location_start_date'] = df['year'].astype(str) + "-01-01"
    df['location_end_date'] = (df['year'].astype(int).astype(str)) + "-01-01"
    # data are quarterly so drop duplicates on business + year
    df['location_id'] = df.groupby(
        ["dba_name", "primary_address_n1", "primary_address_sn", "primary_address_ss"]).ngroup()
    df['business_id'] = df.groupby(['dba_name']).ngroup()
    df = df.drop_duplicates(subset=["location_id", "year"])
    df = df.assign(
        source = "saint louis",
        business_type = np.nan,
        naics = np.nan
    )
    df['location_start_year'] = make_year_var(df['location_start_date'])
    df['location_end_year'] = make_year_var(df['location_end_date'])
    df['business_name'] = np.nan
    df = parallelize_dataframe(df=df, func=clean_parse_parallel, n_cores=2)
    df = add_subset_business_cols(df)
    # quality control logs
    # aggregations by starting year
    stl_start_year_agg = df.groupby('location_start_year').agg(**{
        'num_businesses': ('location_id', 'count'),
        'num_sole_prop': ('is_business', lambda x: (x == 'person').sum()),
        'num_missing_naics': ('naics', lambda x: x.isna().sum()),
        'num_missing_pa': ('naics', lambda x: x.isna().sum()),
        'num_missing_ma': ('naics', lambda x: x.isna().sum()),
        'num_ended': ('location_end_year', lambda x: x.notnull().sum()),
    }
    )

    stl_start_year_agg.to_csv(filePrefix + "/qc/stl_start_year_agg.csv")
    df.to_csv(data_dict['intermediate']['stl']['business_location'] + '/business_location.csv', index=False)
    return df


def clean_sd_bus():
    # san diego comes split apart so read in and concat
    sd_file_list = os.listdir(data_dict['raw']['sd']['business_location'])
    sd_df_list = [pd.read_csv(data_dict['raw']['sd']['business_location'] + f'{file}')
                  for file in sd_file_list]
    sd_bus = pd.concat(sd_df_list)
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
        'date_business_start': 'location_start_date',
        'date_cert_expiration': 'location_end_date',
        'naics_code': 'naics',
        'naics_description': 'naics_descr',
        'ownership_type': 'ownership_type'
        # ignore all columns not in rename dict
    }
    sd_bus = sd_bus.rename(columns=sd_rename_dict)
    # make year variables from dates
    sd_bus['location_start_year'] = make_year_var(sd_bus['location_start_date'])
    sd_bus['location_end_year'] = make_year_var(sd_bus['location_end_date'])
    # make misc variables
    sd_bus['primary_address_fa'] = combine_names(dataframe=sd_bus[['primary_address_n1', 'primary_address_sn', 'primary_address_ss']],
                                                 name_cols=['primary_address_n1', 'primary_address_sn', 'primary_address_ss'])
    sd_bus = sd_bus.assign(
        business_name = np.nan,
        business_type = np.nan,
        mail_address_fa = np.nan,
        source = "san diego"

    )

    sd_bus = parallelize_dataframe(df=sd_bus, func=clean_parse_parallel, n_cores=4)
    sd_bus = add_subset_business_cols(sd_bus)
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

    sd_bus.to_csv(data_dict['intermediate']['sd']['business_location'] + '/business_location.csv', index=False)
    return sd_bus


def clean_seattle_bus():
    df = pd.read_csv(data_dict['raw']['seattle']['business_location'] + '/2020LISTOFALLBUSINESSESPDR.csv')
    seattle_rename_dict = {
        'Customer #': 'business_id',
        'Legal Name': 'business_name',
        'Trade Name': 'dba_name',
        'open_date': 'location_start_date',
        'close_date': 'location_end_date',
        'naics_code': 'naics',
        'DESCRIPTION': 'naics_descr',
        'street_address': 'primary_address_fa',
        'city_state_zip': 'primary_address_city_state_zip'
    }
    df = df.rename(columns=seattle_rename_dict)
    # fix dates
    start_year = df['location_start_date'].str.split("/", expand = True)
    start_year['year'] = np.where(
        start_year.iloc[:,2].astype(int).isin(range(0, 22)),
        '20' + start_year.iloc[:,2],
        '19' + start_year.iloc[:,2]
    )
    end_year = df['location_end_date'].str.split("/", expand = True)
    end_year['year'] = np.where(
        end_year.iloc[:, 2].astype(np.float64).isin(range(0, 22)),
        '20' + end_year.iloc[:, 2],
        np.where(
            end_year.iloc[:, 2].astype(np.float64).isin(range(23, 99)),
            '19' + end_year.iloc[:, 2],
            np.nan

        )

    )
    df['location_start_date'] = start_year['year'] + '-' + start_year.iloc[:,0] + '-' + start_year.iloc[:,1]
    df['location_end_date'] = end_year['year'] + '-' + end_year.iloc[:,0] + '-' + end_year.iloc[:,1]

    # extra cleaning for
    df['location_start_year'] = make_year_var(df['location_start_date'])
    df['location_end_year'] = make_year_var(df['location_end_date'])
    # split city state zip into 3 columns
    address_split = df['primary_address_city_state_zip'].str.extract('(.+),\s(WA)\s([0-9]+)')
    df['primary_address_city'] = address_split[0]
    df['primary_address_state'] = address_split[1]
    df['primary_address_zip'] = address_split[2]
    # make na columns
    df = df.assign(
        ownership_name=np.nan,
        mail_address_fa=np.nan,
        source = "seattle",
        business_type = np.nan

    )
    df = parallelize_dataframe(df=df, func=clean_parse_parallel, n_cores=4)
    df['location_id'] = df.groupby(
        ["cleaned_dba_name", "cleaned_business_name", "primary_cleaned_fullAddress"]).ngroup()
    df = add_subset_business_cols(df)
    # quality control logs
    # aggregations by starting year
    seattle_start_year_agg = df.groupby('location_start_year').agg(**{
        'num_businesses': ('location_id', 'count'),
        'num_sole_prop': ('is_business', lambda x: (x == 'person').sum()),
        'num_missing_naics': ('naics', lambda x: x.isna().sum()),
        'num_missing_pa': ('naics', lambda x: x.isna().sum()),
        'num_missing_ma': ('naics', lambda x: x.isna().sum()),
        'num_ended': ('location_end_year', lambda x: x.notnull().sum()),
    }
                                                                   )
    seattle_start_year_agg.to_csv(filePrefix + "/qc/seattle_start_year_agg.csv")

    df.to_csv(data_dict['intermediate']['seattle']['business_location'] + '/business_location.csv', index=False)
    return df


def clean_orlando_bus():
    df = pd.read_csv(data_dict['raw']['orlando']['business_location'] + 'Business_Tax_Receipts_orlando.csv')
    df = df.rename(columns = {
        "Business Open Date": "business_start_date",
        "Case Number": "business_id",
        "Last Licensed Issue Date": "business_end_date",
        "Business Name": "business_name",
        "Business Owner Name": "ownership_name",
        "Business Address": "primary_address_fa",
        "Business Mailing Address": "mail_address_fa",
        "License Type": "business_type"
    }).assign(
       dba_name = np.nan,
        primary_address_city = "Orlando",
        source = "orlando"
    )
    df['location_start_date'] = df['business_start_date']
    df['location_end_date'] = df['business_end_date']
    # make year variables from dates
    df['location_start_year'] = make_year_var(df['location_start_date'])
    df['location_end_year'] = make_year_var(df['location_end_date'])
    lat_long = df['New Georeferenced Column'].str.extract(r'(-[0-9\.]+)\s([0-9\.]+)')
    df['lat'] = lat_long.iloc[:, 0]
    df['long'] = lat_long.iloc[:, 1]
    df = parallelize_dataframe(df=df, func=clean_parse_parallel, n_cores=4)
    df = add_subset_business_cols(df)
    df['location_id'] = df.groupby(
        ["cleaned_ownership_name", "cleaned_business_name", "primary_cleaned_fullAddress"]).ngroup()
    orlando_start_year_agg = df.groupby('location_start_year').agg(**{
        'num_businesses': ('location_id', 'count'),
        'num_sole_prop': ('is_business', lambda x: (x == 'person').sum()),
        'num_missing_naics': ('naics', lambda x: x.isna().sum()),
        'num_missing_pa': ('naics', lambda x: x.isna().sum()),
        'num_missing_ma': ('naics', lambda x: x.isna().sum()),
        'num_ended': ('location_end_year', lambda x: x.notnull().sum()),
    }
                                                                   )

    orlando_start_year_agg.to_csv(filePrefix + "/qc/orlando_start_year_agg.csv")

    df.to_csv(data_dict['intermediate']['orlando']['business_location'] + '/business_location.csv', index=False)


# abq is weird because the file is split up across two datasets do to weird happenings with when they converted to
# a new business system
def clean_abq_bus():
     df = pd.read_csv(data_dict['raw']['sac']['business_location'] + "cleaned_business_locations.csv")
     df = df.rename(
         columns = {
             "clean": "business_name",
             'clean_owner_name': "ownership_name",
             "NATURE_OF_BUSINESS": 'business_type',
             'SITE_ADDRESS': "primary_address_fa",
             "CITY": 'primary_address_city',
             'STATE': "primary_address_state",
             "ZIP": "primary_address_zip",
             "STREET_NUMBER": "primary_address_n1",
             "STREET_TYPE": 'primary_address_ss',
             "STREET_NAME": "primary_address_sn",
             "POST_DIRECTIONAL": "primary_address_sd"
         }
     )
     # make location start and end dates
     


def clean_sac_bus():
    df = pd.read_csv(data_dict['raw']['sac']['business_location'] + "Business_Operation_Tax_Information.csv")
    df = df.rename(
        columns = {
            "Account_Number": "location_id",
            "Business_Name":"business_name",
            "Business_Description": "business_type",
            "Business_Start_Date": "location_start_date",
            "Business_Close_Date": "location_end_date",
            "Location_Street_Number": "primary_address_n1",
            "Location_Direction": "primary_address_sd",
            "Location_Street_Name": "primary_address_sn",
            "Location_Street_Type": "primary_address_ss",
            "Location_Unit": "primary_address_u",
            "Location_City": "primary_address_city",
            "Location_State": "primary_address_state",
            "Location_Zip_code": "primary_address_zip",
            "Mail_Street_Number": "mail_address_n1",
            "Mail_Street_Direction": "mail_address_sd",
            "Mail_Street_Name": "mail_address_sn",
            "Mail_Street_Direction1": "mail_address_ss",
            "Mail_Unit": "mail_address_u",
            "Mail_City": "mail_address_city",
            "Mail_State": "mail_address_state",
            "Mail_Zip_code": "mail_address_zip"
        }
    )
    # create owner name column
    df['ownership_name'] = combine_names(df[['Principal_Owner_First_name', "Principal_Owner_Last_Name"]],
     name_cols=['Principal_Owner_First_name', "Principal_Owner_Last_Name"])
    # clean zipcode column
    df['mail_address_zip'] = df['mail_address_zip'].fillna("").astype(str).str.slice(0,5)
    df['primary_address_zip'] = df['primary_address_zip'].fillna("").astype(str).str.slice(0,5)
    df = df.assign(
        naics = np.nan,
        dba_name = np.nan,
        source = "sacramento"
    )
    df = parallelize_dataframe(df=df, func=clean_parse_parallel, n_cores=4)
    # make year variables from dates
    df['location_start_year'] = make_year_var(df['location_start_date'])
    df['location_end_year'] = make_year_var(df['location_end_date'])
    df['location_end_year'] = np.where(
        (df['location_end_year'] == 1970),
        np.nan,
        df['location_end_year']
    )

    df['business_id'] = df.groupby(["cleaned_ownership_name", "cleaned_business_name"]).ngroup()
    df['business_start_date'] = df.groupby('business_id')['location_start_date'].transform("min")
    df['business_end_date'] = df.groupby('business_id')['location_end_date'].transform("max")
    df['business_start_year'] = df.groupby('business_id')['location_start_year'].transform("min")
    df['business_end_year'] = df.groupby('business_id')['location_end_year'].transform("max")

    df = add_subset_business_cols(df)

    df_start_year_agg = df.groupby('location_start_year').agg(**{
        'num_businesses': ('location_id', 'count'),
        'num_sole_prop': ('is_business', lambda x: (x == 'person').sum()),
        'num_missing_naics': ('naics', lambda x: x.isna().sum()),
        'num_missing_pa': ('naics', lambda x: x.isna().sum()),
        'num_missing_ma': ('naics', lambda x: x.isna().sum()),
        'num_ended': ('location_end_year', lambda x: x.notnull().sum()),
    }
                                                                        )

    df_start_year_agg.to_csv(filePrefix + "/qc/df_start_year_agg.csv")

    df.to_csv(data_dict['intermediate']['sac']['business_location'] + '/business_location.csv', index=False)
    return df


if __name__ == "__main__":
    write_to_log(f'Starting clean business data at {WTL_TIME}')
    # initialize data dict
    data_dict = make_data_dict(use_seagate=False)
    # cleaning functions
    # clean_la_bus()
    # clean_sf_bus()
    # clean_sd_bus()
    # clean_chicago_bus()
    # clean_seattle_bus()
    # clean_baton_rouge_bus()
    # clean_philly_bus()
    # clean_stl_bus()
    # clean_orlando_bus()
    # clean_sac_bus()
    clean_long_beach_bus()