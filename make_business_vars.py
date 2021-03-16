"""
Python file that contains functions for making misc business vars and for making panels out of
    id. startDate endDate type dataframes

Main function takes in cleaned business dataframe, makes misc business vars,converts business dataframe into panel and
writes business dataframe to csv
"""
import pandas as pd
import numpy as np
import re
from helper_functions import write_to_log, WTL_TIME, fuzzy_merge, get_nearest_address
from data_constants import make_data_dict, filePrefix, name_parser_files
from name_parsing import parse_and_clean_name, classify_name, clean_name, parse_business
from clean_address_data import parallelize_dataframe
from address_parsing import clean_parse_address
from pathos.multiprocessing import ProcessingPool as Pool

# function that takes data w/ start and end year and turns into panel
def make_panel(df, start_year, end_year, current_year = 2020,
               keep_cum_count=False, limit=False, drop_future=True, evens_and_odds = False):
    """
    :param df: dataframe to be made into panel
    :param start_year: first year of observation
    :param end_year: last year of observation (can be missing)
    :param current_year: if last year is missing, this is used to fill in last observed date
    :param keep_cum_count: allows you to keep the running variable (eg num years observation has existed)
    :param limit: allows you to limit max number of years
    :param drop_future: allows you to drop observations with start year > current year
    :return: expanded dataframe
    """
    # drop any with no startyear
    df = df[~ df[start_year].isna()]
    if type(current_year) is pd.core.series.Series:
        current_year = current_year.dropna()
    if drop_future is True:
        df = df[df[start_year] <= current_year]
    # fill in end year with current year if missing
    df[end_year] = df[end_year].fillna(current_year)
    # make a number of years var
    df['numYears'] = df[end_year] - df[start_year] + 1
    df = df[df['numYears'] > 0]
    if limit is not False:
        df= df[df['numYears'] < limit]
    # make a row for every year
    df = df.loc[df.index.repeat(df.numYears)]
    # make the year variable
    df['one'] = 1
    df['addToYear'] = df.groupby(df.index)['one'].cumsum()
    df['year'] = df[start_year] + df['addToYear'] - 1
    if evens_and_odds is not False:
        df = df[df['addToYear'] % 2 ==1]

    if keep_cum_count is not False:
        df = df.drop(columns=[ 'one', 'addToYear', start_year, end_year])
    else:
        df = df.drop(columns = ['numYears','one','addToYear',start_year,end_year])
    return(df)


# function that determines if variable is a chain in a given year
def make_chain_var(df, name_col='business_id', time_col='year',
                   threshold=5, loc_col='num_locations', chain_col='is_chain'):
    """
    Function takes in a dataframe with a name and time column and returns columns containing:
        the number of observations with the same name and an indicator variable if the number of observations is
        greater than the threshold
    :param df: dataframe
    :param name_col: string type column of names
    :param time_col: usually is something like year, but technically can be any second variable to group on
    :param threshold: number where if greater or equal to is considered a chain
    :param loc_col: name of num_observations column to be made
    :param chain_col: name of chain column to be made
    :return: dataframe w/ chain columns added
    """
    df['index'] = df.index
    # replace empty strings w/ NAs
    df[name_col] = df[name_col].replace("", np.nan)
    df[loc_col] = df.groupby([time_col,name_col])['index'].transform('count')
    df[chain_col] = np.where(df[loc_col] > threshold, 'is_chain', 'not_chain')
    df.drop(columns='index', inplace=True)
    return df

# function that makes misc business vars like first year at location, last year at location, etc
def make_business_vars(df, id_col, time_col='year'):
    """
    Function that identifies if row is the min or max value within a given group. e.g for a panel that spans 2000-2010
    first year as business would be 1 for 2000 and 0 elsewhere, last year as business would be 1 for 2010 else 0
        *Caveat that the names are context specific. eg 2020 will be max year for most datasets, but that doesnt mean
            the business closed in 2020
    :param df: dataframe
    :param id_col: grouping column
    :param time_col: any numeric column
    :return: dataframe with columns added
    """
    df['first_year_as_business'] = df.groupby(id_col)[time_col].transform(lambda x: 1 if x ==x .min() else 0)
    df['last_year_as_business'] = df.groupby(id_col)[time_col].transform(lambda x: 1 if x == x.max() else 0)

# TODO function that gets parcel characteristics of the parcel where a business is located
def get_property_charateristics(df, city, address_cols):
    pass

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

def chi_business_vars(n_cores = 4):
    chi_bus = pd.read_csv(data_dict['intermediate']['chicago']['business location'] + '/business_location_addresses_merged.csv')
    def wrapper(chi_bus):
        chi_bus = parse_business(df=chi_bus, business_name_col='cleaned_business_name', use_business_name_as_base=True)
        chi_bus = parse_business(df=chi_bus, business_name_col='cleaned_dba_name', use_business_name_as_base=True)
        chi_bus = chi_bus[chi_bus['is_business'] == 'business']
        chi_bus = make_publically_traded_vars(chi_bus, 'cleaned_dba_name_short_name',
                                                publically_traded_col="dba_is_publicly_traded")

        chi_bus = make_publically_traded_vars(chi_bus, 'cleaned_business_name_short_name',
                                                publically_traded_col="business_name_is_publicly_traded")
        # chicago is already psuedo panel
        chi_bus = make_panel(
            df=chi_bus, start_year='location_start_year', end_year='location_end_year', keep_cum_count=True
        )
        chi_bus = make_chain_var(chi_bus)
        return chi_bus
    chi_bus = parallelize_dataframe(chi_bus, wrapper, n_cores = n_cores)
    chi_bus.to_csv(data_dict['final']['chicago']['business location'] + '/business_locations.csv', index=False)

def baton_rouge_business_vars(n_cores = 4):
    baton_rouge_bus = pd.read_csv(data_dict['intermediate']['baton_rouge']['business location'] + '/business_location_addresses_merged.csv')
    def wrapper(baton_rouge_bus):
        baton_rouge_bus = parse_business(df=baton_rouge_bus, business_name_col='cleaned_business_name', use_business_name_as_base=True)
        baton_rouge_bus = parse_business(df=baton_rouge_bus, business_name_col='cleaned_dba_name', use_business_name_as_base=True)
        baton_rouge_bus = baton_rouge_bus[baton_rouge_bus['is_business'] == 'business']
        baton_rouge_bus = make_publically_traded_vars(baton_rouge_bus, 'cleaned_dba_name_short_name',
                                                publically_traded_col="dba_is_publicly_traded")

        baton_rouge_bus = make_publically_traded_vars(baton_rouge_bus, 'cleaned_business_name_short_name',
                                                publically_traded_col="business_name_is_publicly_traded")
        baton_rouge_bus = make_panel(
            df=baton_rouge_bus, start_year='location_start_year', end_year='location_end_year', keep_cum_count=True
        )
        baton_rouge_bus = make_chain_var(baton_rouge_bus)
        return baton_rouge_bus
    baton_rouge_bus = parallelize_dataframe(baton_rouge_bus, wrapper, n_cores = n_cores)
    baton_rouge_bus.to_csv(data_dict['final']['baton_rouge']['business location'] + '/business_locations.csv', index=False)

def sd_business_vars(n_cores = 4):
    sd_bus = pd.read_csv(data_dict['intermediate']['sd']['business location'] + '/business_location_addresses_merged.csv')
    def wrapper(sd_bus):
        sd_bus = parse_business(df=sd_bus, business_name_col='cleaned_ownership_name', use_business_name_as_base=True)
        sd_bus = parse_business(df=sd_bus, business_name_col='cleaned_dba_name', use_business_name_as_base=True)
        sd_bus = sd_bus[sd_bus['is_business'] == 'business']
        sd_bus = make_publically_traded_vars(sd_bus, 'cleaned_dba_name_short_name',
                                                publically_traded_col="dba_is_publicly_traded")

        sd_bus = make_publically_traded_vars(sd_bus, 'cleaned_ownership_name_short_name',
                                                publically_traded_col="ownership_name_is_publicly_traded")
        sd_bus = make_panel(
            df=sd_bus, start_year='location_start_year', end_year='location_end_year', keep_cum_count=True
        )
        sd_bus = make_chain_var(sd_bus)
        return sd_bus
    sd_bus = parallelize_dataframe(sd_bus, wrapper, n_cores = n_cores)
    sd_bus.to_csv(data_dict['final']['sd']['business location'] + '/business_locations.csv', index=False)


def stl_business_vars(n_cores = 4):
    stl_bus = pd.read_csv(data_dict['intermediate']['stl']['business location'] + '/business_location_addresses_merged.csv')
    stl_bus['location_end_year'] = stl_bus['location_end_year'] - 1
    def wrapper(stl_bus):
        stl_bus = parse_business(df=stl_bus, business_name_col='cleaned_ownership_name', use_business_name_as_base=True)
        stl_bus = parse_business(df=stl_bus, business_name_col='cleaned_dba_name', use_business_name_as_base=True)
        # stl_bus = stl_bus[stl_bus['is_business'] == 'business']
        stl_bus = make_publically_traded_vars(stl_bus, 'cleaned_dba_name_short_name',
                                                publically_traded_col="dba_is_publicly_traded")

        stl_bus = make_publically_traded_vars(stl_bus, 'cleaned_ownership_name_short_name',
                                                publically_traded_col="ownership_name_is_publicly_traded")
        # stlis already basically panel
        stl_bus = make_panel(
            df=stl_bus, start_year='location_start_year', end_year='location_end_year', keep_cum_count=True
        )
        stl_bus = make_chain_var(stl_bus)
        return stl_bus
    stl_bus = parallelize_dataframe(stl_bus, wrapper, n_cores = n_cores)
    print(stl_bus['year'].value_counts())
    stl_bus.to_csv(data_dict['final']['stl']['business location'] + '/business_locations.csv', index=False)


if __name__ == "__main__":

    write_to_log(f'Starting clean business data at {WTL_TIME}')
    # initialize data dict
    data_dict = make_data_dict(use_seagate=True)

    # read in CLEANED business data
    # sf_bus = pd.read_csv(data_dict['intermediate']['sf']['business location'] + '/business_location.csv')
    # sf_add = pd.read_csv(data_dict['intermediate']['sf']['parcel'] + '/addresses.csv')
    # sf_bus = merge_addresses(sf_bus, sf_add)
    # sf_bus = merge_addresses(sf_bus, sf_add)
    # la_bus = pd.read_csv(data_dict['intermediate']['la']['business location'] + '/business_location.csv')
    # philly_bus = pd.read_csv(data_dict['intermediate']['philly']['business location'] + '/business_location.csv')

    # chi_bus = pd.read_csv(data_dict['intermediate']['chicago']['business location'] + '/business_location.csv')
    # seattle_bus = pd.read_csv(data_dict['intermediate']['seattle']['business location'] + '/business_location.csv')
    #
    # # add business name columns
    # sf_bus = parse_business(df=sf_bus, business_name_col='cleaned_dba_name', use_business_name_as_base =True)
    # sf_bus = parse_business(df=sf_bus, business_name_col='cleaned_ownership_name', use_business_name_as_base=True)
    #
    # la_bus = parse_business(df=la_bus, business_name_col='cleaned_business_name', use_business_name_as_base =True)
    # la_bus = parse_business(df=la_bus, business_name_col='cleaned_dba_name', use_business_name_as_base=True)

    # philly_bus = parse_business(df=philly_bus, business_name_col='cleaned_business_name', use_business_name_as_base=True)
    # philly_bus = parse_business(df=philly_bus, business_name_col='cleaned_dba_name', use_business_name_as_base=True)

    # chi_bus = parse_business(df=chi_bus, business_name_col='cleaned_business_name', use_business_name_as_base=True)
    # chi_bus = parse_business(df=chi_bus, business_name_col='cleaned_dba_name', use_business_name_as_base=True)
    #
    # seattle_bus = parse_business(df=seattle_bus, business_name_col='cleaned_business_name',
    #                              use_business_name_as_base=True)
    # seattle_bus = parse_business(df=seattle_bus, business_name_col='cleaned_dba_name', use_business_name_as_base=True)
    #
    # # filter dfs to be not businesses that I think are sole proprietorships
    # sf_bus = sf_bus[sf_bus['is_business'] == 'business']
    # la_bus = la_bus[la_bus['is_business'] == 'business']
    # seattle_bus = seattle_bus[seattle_bus['is_business'] == 'business']
    # philly_bus = philly_bus[philly_bus['is_business'] == 'business']
    # chi_bus = chi_bus[chi_bus['is_business'] == 'business']

    # make time-invarient business vars
    # sf_bus = make_publically_traded_vars(sf_bus, 'cleaned_dba_name_short_name',
    #                                      publically_traded_col="dba_is_publicly_traded")
    # sf_bus = make_publically_traded_vars(sf_bus, 'cleaned_ownership_name_short_name',
    #                                      publically_traded_col="business_name_is_publicly_traded")
    #
    # la_bus = make_publically_traded_vars(la_bus, 'cleaned_dba_name_short_name',
    #                                      publically_traded_col="dba_is_publicly_traded")
    # la_bus = make_publically_traded_vars(la_bus, 'cleaned_business_name_short_name',
    #                                      publically_traded_col="business_name_is_publicly_traded")
    #
    # seattle_bus = make_publically_traded_vars(seattle_bus, 'cleaned_dba_name_short_name',
    #                                             publically_traded_col="dba_is_publicly_traded")
    #
    # seattle_bus = make_publically_traded_vars(seattle_bus, 'cleaned_business_name_short_name',
    #                                             publically_traded_col="business_name_is_publicly_traded")

    # philly_bus = make_publically_traded_vars(philly_bus, 'cleaned_dba_name_short_name',
    #                                      publically_traded_col="dba_is_publicly_traded")
    #
    # philly_bus = make_publically_traded_vars(philly_bus, 'cleaned_business_name_short_name',
    #                                      publically_traded_col="business_name_is_publicly_traded")

    # chi_bus = make_publically_traded_vars(chi_bus, 'cleaned_dba_name_short_name',
    #                                         publically_traded_col="dba_is_publicly_traded")
    #
    # chi_bus = make_publically_traded_vars(chi_bus, 'cleaned_business_name_short_name',
    #                                         publically_traded_col="business_name_is_publicly_traded")

    # make panel
    # sf_bus = make_panel(
    #     df=sf_bus, start_year='location_start_year', end_year='location_end_year', keep_cum_count=True
    # )
    #
    # la_bus = make_panel(
    #     df=la_bus, start_year='location_start_year', end_year='location_end_year', keep_cum_count=True
    # )
    # # chicago is already psuedo panel
    # chi_bus = make_panel(
    #     df=chi_bus, start_year='location_start_year', end_year='location_end_year', keep_cum_count=True
    # )

    # philly_bus = make_panel(
    #     df=philly_bus, start_year='location_start_year', end_year='location_end_year', keep_cum_count=True
    # )

    # seattle_bus = make_panel(
    #     df=seattle_bus, start_year='location_start_year', end_year='location_end_year', keep_cum_count=True
    # )
    #
    # sf_bus = make_chain_var(sf_bus)
    # seattle_bus = make_chain_var(seattle_bus)
    # chi = make_chain_var(chi_bus)
    # la_bus = make_chain_var(la_bus, name_col='cleaned_business_name', chain_col="num_locations_business_name")
    # la_bus = make_chain_var(la_bus, name_col='cleaned_dba_name', chain_col="num_locations_dba_name")
    # philly_bus = make_chain_var(philly_bus, name_col='cleaned_business_name', chain_col="num_locations_business_name")
    # philly_bus = make_chain_var(philly_bus, name_col='cleaned_dba_name', chain_col="num_locations_dba_name")
    #
    # sf_bus.to_csv(data_dict['final']['sf']['business location'] + '/business_locations.csv', index=False)
    # la_bus.to_csv(data_dict['final']['la']['business location'] + '/business_locations.csv', index=False)
    # philly_bus.to_csv(data_dict['final']['philly']['business location'] + '/business_locations.csv', index=False)
    # chi_bus.to_csv(data_dict['final']['chicago']['business location'] + '/business_locations.csv', index=False)
    # seattle_bus.to_csv(data_dict['final']['seattle']['business location'] + '/business_locations.csv', index=False)
    # chi_business_vars(4)
    # baton_rouge_business_vars(4)
    sd_business_vars(4)
    # stl_business_vars(4)