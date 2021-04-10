"""
Python file that contains functions for making misc business vars and for making panels out of
    id. startDate endDate type dataframes

Main function takes in cleaned business dataframe, makes misc business vars,converts business dataframe into panel and
writes business dataframe to csv
"""
import pandas as pd
import numpy as np
import re
from helper_functions import write_to_log, WTL_TIME, fuzzy_merge, get_nearest_address, make_panel
from data_constants import make_data_dict, filePrefix
from name_parsing import parse_business
from clean_address_data import parallelize_dataframe
from typing import Union


# function that determines if variable is a chain in a given year
def make_chain_var(df, name_col='business_id', time_col='year',
                   loc_col='num_locations'):
    """
    Function takes in a dataframe with a name and time column and returns columns containing:
        the number of observations with the same name and an indicator variable if the number of observations is
        greater than the threshold
    :param df: dataframe
    :param name_col: string type column of names
    :param time_col: usually is something like year, but technically can be any second variable to group on
    :param loc_col: name of num_observations column to be made
    :param loc_col: name of chain column to be made
    :return: chain col: PD.SERIES
    """
    df['index'] = df.index
    # replace empty strings w/ NAs
    df[name_col] = df[name_col].replace("", np.nan)
    df[loc_col] = df.groupby([time_col,name_col])['index'].transform('count')
    df.drop(columns='index', inplace=True)
    return df[loc_col]


def merge_addresses(bus_df, add_df, fuzzy = False,nearest_n1 = False, expand_addresses=False):
    og_cols = bus_df.columns
    add_merge_cols = [
        "parsed_addr_n1",
        "parsed_addr_sn",
        "parsed_addr_ss",
    ]
    bus_merge_cols = [
        "primary_cleaned_addr_n1",
        "primary_cleaned_addr_sn",
        "primary_cleaned_addr_ss",
    ]
    bus_df[bus_merge_cols] = bus_df[bus_merge_cols].fillna("").astype(str)
    for col in bus_merge_cols:
        bus_df[col] = bus_df[col].str.replace('\.0+', "")

    add_df[add_merge_cols] = add_df[add_merge_cols].fillna("").astype(str)
    for col in add_merge_cols:
        add_df[col] = add_df[col].str.replace('\.0+', "")

    if expand_addresses:
        add_df = make_panel(add_df, start_year="parsed_addr_n1", end_year="parsed_addr_n2",
                            current_year=add_df["parsed_addr_n1"], limit=200, evens_and_odds=True ).rename(
            columns = {"year": "parsed_addr_n1"}
        )

    bus_df = bus_df.merge(
        add_df[add_merge_cols + ["lat","long","parcelID"]].drop_duplicates(subset =add_merge_cols ), how = "left",
        left_on = bus_merge_cols,
        right_on = add_merge_cols,
        indicator = True,
        suffixes=[None, '_from_address']
    )
    bus_df['merged_from'] = np.where(bus_df["_merge"] != "left_only", "num_st_sfx", "not merged succesfully")
    # try nearest parcel matching
    if nearest_n1 is not False:
        # filter for addresses that havent been merged
        lo = bus_df[bus_df['_merge'] == "left_only"][og_cols]  # reset columns so we dont get suffixes
        nlo = bus_df[bus_df['_merge'] != "left_only"]
        lo = get_nearest_address(
            df1 = lo, df2 = add_df[add_merge_cols + ["lat","long","parcelID"]].drop_duplicates(subset =add_merge_cols ),
            left_cols=["primary_cleaned_addr_sn","primary_cleaned_addr_ss"],
            right_cols=["parsed_addr_sn", "parsed_addr_ss"],
            n1_col_left='primary_cleaned_addr_n1',
            n1_col_right='parsed_addr_n1',
            threshold=5,
            indicator=True,
            suffixes=[None, '_from_address']
        )
        lo['merged_from'] = np.where(lo["_merge"] != "left_only", "nearest_n1", "not merged succesfully")
        bus_df = pd.concat([lo, nlo])

    if fuzzy is not False:
        # filter for addresses that havent been merged
        lo = bus_df[bus_df['_merge'] == "left_only"][og_cols] # reset columns so we dont get suffixes
        nlo = bus_df[bus_df['_merge'] != "left_only"]
        lo = fuzzy_merge(
            df1 = lo, df2 = add_df,
            left_cols=['primary_cleaned_addr_n1',"primary_cleaned_addr_ss"],
            right_cols=['parsed_addr_n1', "parsed_addr_ss"],
            left_fuzzy_col = "primary_cleaned_addr_sn",
            right_fuzzy_col = "parsed_addr_sn",
            threshold=90,
            indicator=True,
            suffixes=[None, '_from_address']
        )
        lo['merged_from'] = np.where(lo["_merge"] != "left_only", "fuzzy", "not merged succesfully")
        bus_df = pd.concat([lo, nlo])

    # try fuzzy matching on street name
    return bus_df


# function that checks if business name is listed in nasdaq or nyse
def make_publically_traded_vars(df, name_col, publically_traded_col='is_publically_traded'):
    """
    Function takes dataframe with a name column and checks if that name column is contained within the NASDAQ or NYSE
    2020 listings. Lookup is regular expressions on joined string, which means the name column should be the most
    parsimonious name that still correctly identifies the company. I.e. use "Uber" vs "Uber Technologies, Inc."
        Regex will obviously miss subsidiaries, which is something to be cognisant about. I.e. google vs alphabet
    :param df: dataframe
    :param name_col: any string type column
    :param publically_traded_col: name of output column
    :return: dataframe with column saying if name column is publically listed on stock exchange
    """
    # read nasdaq as csv
    nasdaq = pd.read_csv(filePrefix + "/stock/nasdaq/nasdaqlisted.txt",delimiter='|')
    nasdaq_other = pd.read_csv(filePrefix + "/stock/nasdaq/otherlisted.txt", delimiter='|')
    # read nyse as csv
    nyse = pd.read_csv(filePrefix + "/stock/nyse/nyse-listed_csv.csv")
    nyse_other = pd.read_csv(filePrefix + "/stock/nyse/other-listed_csv.csv")

    # extract just the name column and combine accross listings. drop duplicates on name to make more efficient
    all_listings = pd.concat([
        nasdaq[["Security Name"]].rename(columns = {"Security Name": 'listed_name'}),
        nasdaq_other[["Security Name"]].rename(columns={"Security Name": 'listed_name'}),
        nyse[["Company Name"]].rename(columns={"Company Name": 'listed_name'}),
        nyse_other[["Company Name"]].rename(columns={"Company Name": 'listed_name'}),
    ]).drop_duplicates(subset = 'listed_name').dropna()
    # join to one string for faster regex lookup
    all_listings_string = ' '.join(all_listings['listed_name'])
    # function for performing regex. has boundary characters so uber does not get matched to red tuber inc. or something
    def check_string(string):
        if re.search(f"[^a-z0-9]{string}[^a-z0-9]", all_listings_string, flags=re.IGNORECASE):
            return 'is publically traded'
        else:
            return 'not publically traded'
    # drop duplicates on name column so you only try to lookup each name once
    df_dd = df.drop_duplicates(subset = name_col)[[name_col]]
    df_dd[publically_traded_col] = df_dd[name_col].apply(check_string)
    # merge dropped duplicated df back on to original dataframe
    df = pd.merge(df, df_dd, on=name_col, how='left')
    return df


def make_business_varsr(df: pd.DataFrame, n_cores:int = 4) -> pd.DataFrame:
    def wrapper(_df):
        for _var in ["cleaned_ownership_name", 'cleaned_business_name', "cleaned_dba_name"]:
            _df = _df.merge(
                parse_business(df = _df[[_var]].drop_duplicates(subset=_var),
                               business_name_col= _var, use_business_name_as_base=True),
                on = _var, how="left"
            )
            _df = _df.merge(
                make_publically_traded_vars(df = _df[[_var]].drop_duplicates(), name_col=_var,
                                            publically_traded_col=_var + "_is_publically_traded"),
                on = _var, how = "left"
            )
        _df['index'] = np.arange(_df.shape[0])
        _df = pd.merge(_df,  make_panel(_df[['index', 'location_start_year', 'location_end_year']],
                        start_year='location_start_year', end_year='location_end_year', keep_cum_count=True),
                     how='left', on='index'
                      )
        _df = _df.drop(columns = "index")
        return _df
    df = parallelize_dataframe(df, wrapper, n_cores=n_cores)
    for var in ["cleaned_ownership_name", 'cleaned_business_name', "cleaned_dba_name", "business_id"]:
        df["num_locations_" + var] = make_chain_var(df[[var, "year"]], name_col=var, time_col="year")
    return df


def make_business_vars_wrapper(city, n_cores = 4):
    df = pd.read_csv(data_dict['intermediate'][city]['business location'] + '/business_location_addresses_merged.csv')
    df['index'] = np.arange(df.shape[0])
    df = df.merge(
        make_business_vars_wrapper(df[["cleaned_ownership_name",
                                           'cleaned_business_name',
                                           "cleaned_dba_name",
                                           "index"]], n_cores),
        on="index", how="left"

    )
    df.to_csv(data_dict['final'][city]['business location'] + '/business_locations.csv', index=False)



if __name__ == "__main__":

    write_to_log(f'Starting clean business data at {WTL_TIME}')
    # initialize data dict
    data_dict = make_data_dict(use_seagate=True)
    #
    for city in [
        "sf",
        "sd",
        "sac",
        "la",
        "chi",
        "seattle",
        "baton_rouge",
        "philly",
        "orlando",
        "stl"
    ]:
        make_business_vars_wrapper(city, n_cores=4)