# file cleans business location data and exports back
import pandas as pd
import numpy as np
from helper_functions import write_to_log, make_year_var, WTL_TIME
from data_constants import make_data_dict, filePrefix, name_parser_files, business_cols
from name_parsing import parse_and_clean_name, classify_name, clean_name, combine_names
from address_parsing import clean_parse_address
from pathos.multiprocessing import ProcessingPool as Pool
import os
import janitor
import re
import sys


# add columns not present in one dataframe to the other
# TODO make data constant that has all desired business columns
def add_cols_from_other_df(df1, df_list):
    col_list = [col for df in df_list for col in df.columns]
    for col in col_list:
        if col not in df1.columns:
            df1[col] = np.nan
    return df1

def add_subset_business_cols(df):
    for col in business_cols:
        if col not in df.columns:
            df[col] = np.nan
    return df[business_cols]

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
        dataframe=df, name_cols=['cleaned_dba_name', 'cleaned_business_name','cleaned_ownership_name'], probalistic_classification=True,
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
        dataframe=df, address_col='mail_address_fa', city='mail_address_city',st_name="mail_address_sn", st_sfx="mail_address_ss",
        st_d="mail_address_sd", unit='mail_address_u',
        st_num='mail_address_n',country='mail_address_country', state='mail_address_state', st_num2 ='mail_address_n2',
        zipcode='mail_address_zip', prefix2='mail_cleaned_', prefix1='cleaned_', raise_error_on_na=False
    )
    return df

# wrapper functions for cleaning
def clean_sf_bus():
    sf_bus = pd.read_csv(
        data_dict['raw']['sf']['business location'] + '/Registered_Business_Locations_-_San_Francisco.csv')
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
        business_name = np.nan
    )
    sf_bus = parallelize_dataframe(df=sf_bus, func=clean_parse_parallel, n_cores=4)
    sf_bus = make_year_var(df=sf_bus, date_col='location_start_date', new_col='location_start_year')
    sf_bus = make_year_var(df=sf_bus, date_col='location_end_date', new_col='location_end_year')
    sf_bus = make_year_var(df=sf_bus, date_col='business_start_date', new_col='business_start_year')
    sf_bus = make_year_var(df=sf_bus, date_col='business_end_date', new_col='business_end_year')
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
    sf_bus.to_csv(data_dict['intermediate']['sf']['business location'] + '/business_location.csv', index=False)

def clean_la_bus():
    la_bus =  pd.read_csv(data_dict['raw']['la']['business location'] + '/Listing_of_All_Businesses.csv')
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
        # ignore all columns not in rename dict
    }
    la_bus.rename(columns=la_rename_dict, inplace=True)
    la_bus = la_bus.assign(
        ownership_name = np.nan
    )
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

def clean_chicago_bus():
    chicago_bus = pd.read_csv(data_dict['raw']['chicago']['business location'] + '/Business_Licenses.csv')
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
        'BUSINESS ACTIVITY': 'business_type'
        # ignore all columns not in rename dict
    }
    chicago_bus.rename(columns=chi_rename_dict, inplace=True)
    chicago_bus = chicago_bus.assign(
        mail_address_fa = np.nan
    )
    chicago_bus = parallelize_dataframe(df=chicago_bus, func=clean_parse_parallel, n_cores=4)
    # make year variables from dates
    chicago_bus = make_year_var(df=chicago_bus, date_col='location_start_date', new_col='location_start_year')
    chicago_bus = make_year_var(df=chicago_bus, date_col='location_end_date', new_col='location_end_year',
                                round_down=True)

    chicago_bus ['location_id'] = chicago_bus.groupby(
        ["cleaned_dba_name","cleaned_business_name", "primary_cleaned_address_n1",
         "primary_cleaned_address_sn", "primary_cleaned_address_ss"]).ngroup()

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

    chicago_bus.to_csv(data_dict['intermediate']['chicago']['business location'] + '/business_location.csv', index=False)

def clean_philly_bus():
    df = pd.read_csv(data_dict['raw']['philly']['business location'] + 'business_licenses-2.csv')
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
    df = make_year_var(df=df, date_col='location_start_date', new_col='location_start_year')
    df = make_year_var(df=df, date_col='location_end_date', new_col='location_end_year')
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

    df.to_csv(data_dict['intermediate']['philly']['business location'] + '/business_location.csv', index=False)

def clean_baton_rouge_bus():
    df = pd.read_csv(data_dict['raw']['baton_rouge']['business location'] +
                                  'Businesses_Registered_with_EBR_Parish_baton_r.csv', nrows = 50)
    df = df.rename(columns = {
        "ACCOUNT NO": 'location_id',
        "ACCOUNT NAME": 'dba_name',
        "LEGAL NAME": 'business_name',
        "BUSINESS OPEN DATE": 'location_start_date',
        "BUSINESS CLOSE DATE": 'location_end_date',
        "OWNERSHIP TYPE": 'ownership_type',
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

    df = make_year_var(df=df, date_col='location_start_date', new_col='location_start_year')
    df = make_year_var(df=df, date_col='location_end_date', new_col='location_end_year')
    combine_names(dataframe = df, name_cols=['mail_address_fa1','mail_address_fa2'],
                  newCol='mail_address_fa')
    combine_names(dataframe=df, name_cols=['primary_address_fa1', 'primary_address_fa2'],
                  newCol='primary_address_fa')
    df = parallelize_dataframe(df=df, func=clean_parse_parallel, n_cores=2)
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

    df.to_csv(data_dict['intermediate']['baton_rouge']['business location'] + '/business_location.csv', index=False)

def clean_stl_bus():
    df_file_list  =  [file for file in os.listdir(data_dict['raw']['df']['business location'] ) if bool(re.search( "[0-9]", file))]
    df_list = [pd.read_excel(data_dict['raw']['df']['business location'] + f'{file}').clean_names().assign(
        year = re.search("([0-9]{4})", file).group(1) ) for file in df_file_list]
    df = pd.concat(df_list)
    df = df.clean_names()
    # unite columns because stl uses inconsistant names
    combine_names(df, name_cols=['bill_date', 'billed_date'], newCol='bill_date')
    combine_names(df, name_cols=['contact_e_mail', 'contact_name'], newCol='contact_name')
    combine_names(df, name_cols=['doc_nbr', 'doc_nbr_', 'doc_nbr_1'], newCol='doc_nbr')
    combine_names(df, name_cols=['date_business_started', 'date_busines_started'], newCol='date_business_started')
    combine_names(df, name_cols=['file_date', 'filed_date'], newCol='file_date')
    combine_names(df, name_cols=['mailing_address_line_1', 'mailing_address_line_2', 'mailing_address_line_3'],
                  newCol='mail_address_fa', )
    combine_names(df, name_cols=['tax_period', 'tax_period_1'], newCol='tax_period')
    combine_names(df, name_cols=['tax_year', 'tax_period_1'], newCol='tax_year')
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
    df['location_end_date'] = (df['year'].astype(int).add(1).astype(str)) + "-01-01"
    # data are quarterly so drop duplicates on business + year
    df['location_id'] = df.groupby(
        ["dba_name", "primary_address_n1", "primary_address_sn", "primary_address_ss"]).ngroup()
    df = df.drop_duplicates(subset=["location_id", "year"])
    df = make_year_var(df=df, date_col='location_start_date', new_col='location_start_year')
    df = make_year_var(df=df, date_col='location_end_date', new_col='location_end_year')
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

    df.to_csv(data_dict['intermediate']['stl']['business location'] + '/business_location.csv', index=False)

def clean_sd_bus():
    # san diego comes split apart so read in and concat
    sd_file_list = os.listdir(data_dict['raw']['sd']['business location'])
    sd_df_list = [pd.read_csv(data_dict['raw']['sd']['business location'] + f'{file}')
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
        'date_account_creation': 'location_start_date',
        'date_cert_expiration': 'location_end_date',
        'naics_code': 'naics',
        'naics_description': 'naics_descr',
        'ownership_type': 'ownership_type'
        # ignore all columns not in rename dict
    }
    sd_bus = sd_bus.rename(columns=sd_rename_dict)
    # make year variables from dates
    sd_bus = make_year_var(df=sd_bus, date_col='location_start_date', new_col='location_start_year')
    sd_bus = make_year_var(df=sd_bus, date_col='location_end_date', new_col='location_end_year')
    # make misc variables
    combine_names(dataframe=sd_bus, name_cols=['primary_address_n1', 'primary_address_sn', 'primary_address_ss'],
                  newCol='primary_address_fa')
    sd_bus = sd_bus.assign(
        business_name = np.nan,
        mail_address_fa = np.nan

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

    sd_bus.to_csv(data_dict['intermediate']['sd']['business location'] + '/business_location.csv', index=False)
    pass

def clean_seattle_bus():
    df = pd.read_csv(data_dict['raw']['seattle']['business location'] + '/2020LISTOFALLBUSINESSESPDR.csv')
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
    df = make_year_var(df=df, date_col='location_start_date', new_col='location_start_year')
    df = make_year_var(df=df, date_col='location_end_date', new_col='location_end_year')
    # split city state zip into 3 columns
    address_split = df['primary_address_city_state_zip'].str.extract('(.+),\s(WA)\s([0-9]+)')
    df['primary_address_city'] = address_split[0]
    df['primary_address_state'] = address_split[1]
    df['primary_address_zip'] = address_split[2]
    # make na columns
    df = df.assign(
        ownership_name=np.nan,
        mail_address_fa=np.nan

    )
    df = parallelize_dataframe(df=df, func=clean_parse_parallel, n_cores=4)
    df['location_id'] = df.groupby(
        ["cleaned_dba_name", "cleaned_business_name", "primary_cleaned_addr_n1",
         "primary_cleaned_addr_sn", "primary_cleaned_addr_ss"]).ngroup()
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

    df.to_csv(data_dict['intermediate']['seattle']['business location'] + '/business_location.csv', index=False)

def clean_orlando_bus():
    df = pd.read_csv(data_dict['raw']['orlando']['business location'] + 'Business_Tax_Receipts_orlando.csv')


if __name__ == "__main__":
    write_to_log(f'Starting clean business data at {WTL_TIME}')
    # initialize data dict
    data_dict = make_data_dict(use_seagate=True)
    # cleaning functions
    # clean_la_bus()
    #clean_sf_bus()
    # clean_sd_bus()
    # clean_chicago_bus()
    clean_seattle_bus()
    # clean_baton_rouge_bus()
    # clean_philly_bus()
    # clean_stl_bus()