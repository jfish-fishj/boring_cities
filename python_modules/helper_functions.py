# python file for useful functions
import inspect
import warnings
import os
import pandas as pd
from datetime import datetime
import re
from fuzzywuzzy import fuzz
import data_constants
import numpy as np
from typing import Union
WTL_TIME = datetime.today().strftime('%Y-%m-%d')


# generic logging function,
# text is what you want to log, tabs lets you format logging, doPrint prints to console, warn warns, loc based tabbing
# guesses how to format text
def write_to_log(text, tabs=0, doPrint=True, warn=False,loc_based_tabbing = False):
    """
    
    :param text: text to write to log
    :param tabs:  how many tabs
    :param doPrint: print text to console in addition to log?
    :param warn: raise warning in addition to log?
    :param loc_based_tabbing: guesses how many tabs
    :return: 
    """
    text = str(text)
    # for i in inspect.stack():
    #     print("STACK: {}".format(str(i[3])))

    if loc_based_tabbing:
        tabs = len(inspect.stack())

    if not os.path.exists((data_constants.filePrefix + "/logs/{}".format(WTL_TIME))):
        os.mkdir((data_constants.filePrefix + "/logs/{}".format(WTL_TIME)))

    f = open((data_constants.filePrefix + '/logs/{}/__OVERALL_LOG__.txt'.format(WTL_TIME)), "a+")

    text = text.rjust(len(text) + tabs * 3)
    if tabs == 0:
        text = "\n" + text

    # writing
    f.write(text  + "\n")
    if doPrint:
        print(text)
    if warn:
        warnings.warn(text)
        w = open((data_constants.filePrefix + '/logs/{}/__Warnings.txt'.format(WTL_TIME)),"a+")
        w.write(text + "\n")


# function for taking a polygon and interpolating it into a series of coordinates
# so if a polygon goes from (0,0) to (10,10) with eleven splits in between it will return a dataframe with
# lat long coordinates of (0,0), (1,1), ..., (10,10)
def interpolate_polygon(df:pd.DataFrame, id_col:str, direction:str):
    # idea of function is that if i new which way the streets run then i am able to tell how increases in street numbers
    # map to coordinates
    # so a nw street has its smallest numbers in the south east and increases going north west
    # so mininum lat / longs will occue where the numbers are lowest
    df['n1_min'] = df.groupby(id_col)['address_n1'].transform("min")
    df['n1_max'] = df.groupby(id_col)['address_n1'].transform("max")
    # latitude is north/south, longitude is east/west
    df_nw = df[df[direction] == "NW"]
    df_nw['lat_interpolated'] = np.where(
        (df_nw['address_n1'] == df_nw['n1_min']),
        df_nw['latitude_min'],
        np.where(
            (df_nw['address_n1'] == df_nw['n1_max']),
            df_nw['latitude_max'],
            np.nan
        )
    )
    df_nw['long_interpolated'] = np.where(
        (df_nw['address_n1'] == df_nw['n1_min']),
        df_nw['longitude_min'],
        np.where(
            (df_nw['address_n1'] == df_nw['n1_max']),
            df_nw['longitude_max'],
            np.nan
        )
    )
    df_ne = df[df[direction] == "NE"]
    df_ne['lat_interpolated'] = np.where(
        (df_ne['address_n1'] == df_ne['n1_min']),
        df_ne['latitude_min'],
        np.where(
            (df_ne['address_n1'] == df_ne['n1_max']),
            df_ne['latitude_max'],
            np.nan
        )
    )
    df_ne['long_interpolated'] = np.where(
        (df_ne['address_n1'] == df_ne['n1_min']),
        df_ne['longitude_max'],
        np.where(
            (df_ne['address_n1'] == df_ne['n1_max']),
            df_ne['longitude_min'],
            np.nan
        )
    )
    df_sw = df[df[direction] == "SW"]
    df_sw['lat_interpolated'] = np.where(
        (df_sw['address_n1'] == df_sw['n1_min']),
        df_sw['latitude_max'],
        np.where(
            (df_sw['address_n1'] == df_sw['n1_max']),
            df_sw['latitude_min'],
            np.nan
        )
    )
    df_sw['long_interpolated'] = np.where(
        (df_sw['address_n1'] == df_sw['n1_min']),
        df_sw['longitude_min'],
        np.where(
            (df_sw['address_n1'] == df_sw['n1_max']),
            df_sw['longitude_max'],
            np.nan
        )
    )
    df_se = df[df[direction] == "SE"]
    df_se['lat_interpolated'] = np.where(
        (df_se['address_n1'] == df_se['n1_min']),
        df_se['latitude_max'],
        np.where(
            (df_se['address_n1'] == df_se['n1_max']),
            df_se['latitude_min'],
            np.nan
        )
    )
    df_se['long_interpolated'] = np.where(
        (df_se['address_n1'] == df_se['n1_min']),
        df_se['longitude_max'],
        np.where(
            (df_se['address_n1'] == df_se['n1_max']),
            df_se['longitude_min'],
            np.nan
        )
    )
    df = pd.concat([df_nw, df_ne, df_sw, df_se])

    df['lat_interpolated'] = df.groupby(id_col)['lat_interpolated'].transform(pd.DataFrame.interpolate)
    df['long_interpolated'] = df.groupby(id_col)['long_interpolated'].transform(pd.DataFrame.interpolate)

    return df


# function that takes creates a panel based on a start column and an end column
# so a row with start = 1 and end = 10 gets turned into 10 rows begining at 1 and ending at 10
def make_panel(df:pd.DataFrame, start_year:str, end_year:str, current_year:Union[int, pd.Series] = 2020,
               keep_cum_count=False, limit=False, drop_future=True, evens_and_odds = False):
    """
    :param df: dataframe to be made into panel
    :param start_year: first year of observation
    :param end_year: last year of observation (can be missing)
    :param current_year: if last year is missing, this is used to fill in last observed date
    :param keep_cum_count: allows you to keep the running variable (eg num years observation has existed)
    :param limit: allows you to limit max number of years
    :param drop_future: allows you to drop observations with start year > current year
    :param evens_and_odds: instead of creating an index between 1 -> 5, for example it evens and odds creates 1, 3, 5
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
    return df


# generic function for cleaning columns, replaced with janitor package
def clean_column_names(col_list:list):
    def clean_column(string):
        string = str.lower(string) # make lowercase
        string = re.sub(pattern='\s',string=string, repl='_') # replace spaces w/ underscores
        string = re.sub(pattern='[^a-zA-Z0-9]',string=string,repl="") # remove non-alphanumeric characters
        return string
    new_col_list = [clean_column(x) for x in col_list]
    return new_col_list


# takes a string-date column with any format that has a yyyy and returns the year as an float
# round down returns the year minus 1
def make_year_var(date_col, round_down = False):
    date_col = date_col.astype(str).str.extract('([0-9]{4})').astype(float)
    if round_down is not False:
        date_col = date_col.astype(float) -1
    return date_col


# takes a dataframe and returns that dataframe with just the address_cols from data constants
# missing columns are set as NA
def add_subset_address_cols(df:pd.DataFrame) -> pd.DataFrame:
    for col in data_constants.address_cols:
        if col not in df.columns:
            df[col] = np.nan
    df = df[data_constants.address_cols]
    return df


# returns dataframe w/ only business cols from data constants, if columns are missing assigns NA
def add_subset_business_cols(df:pd.DataFrame):
    for col in data_constants.business_cols:
        if col not in df.columns:
            df[col] = np.nan
    return df[data_constants.business_cols]


# make naics vars (uses 2017 NAICS codes)
# helper function for seeing how much granulairity is contained within an naics col
def get_naics_depth(naics_col:pd.Series) -> pd.Series:
    return naics_col.fillna("").astype(str).str.replace("[0\D]+", "").str.len()


def subset_naics_col(naics_col:pd.Series, depth:int) -> pd.Series:
    col = naics_col.fillna("").astype(str).str.replace("[0\D]+", "").str.slice(0, depth)
    col =pd.Series(np.where(
        col.str.len() < depth, np.nan, col
    ))
    return col


def get_naics_descr(naics_col:pd.Series) -> pd.Series:
    naics = (pd.read_csv(data_constants.misc_data_dict['naics'],
                        usecols=["2017 NAICS US   Code","2017 NAICS US Title"],
                        dtype={
                            "2017 NAICS US Title": str,
                            "2017 NAICS US   Code": str,
                        }))
    naics["2017 NAICS US   Code"] = naics["2017 NAICS US   Code"].str.strip()
    naics = dict(naics.values)

    # there are 7 rows where there is a - ie.e 44-45 = retail trade
    naics["44"] = "Retail Trade"
    naics["45"] = "Retail Trade"
    naics["48"] = "Transportation and Warehousing"
    naics["49"] = "Transportation and Warehousing"
    naics["31"] = "Manufacturing"
    naics["32"] = "Manufacturing"
    naics["33"] = "Manufacturing"
    return naics_col.fillna("").astype(str).str.replace("[0\D]+", "").map(naics)


# function takes df and naics column and returns that dataframe with the following columns:
# naics_descr_2 : two digit naics code description
def make_naics_vars(df:pd.DataFrame, naics_col:str = "naics") -> pd.DataFrame:
    # min, max depth are not needed as of now
    # min_depth = get_naics_depth(df[naics_col]).max()
    # max_depth = get_naics_depth(df[naics_col]).min()
    df['naics_descr_2'] = get_naics_descr(
        subset_naics_col(df[naics_col], 2)
    )
    df['naics_descr_3'] = get_naics_descr(
        subset_naics_col(df[naics_col], 3)
    )
    df['naics_descr2_standardized'] = naics_to_business_type_coarse(df['naics_descr_2'],
                                                                    xwalk = data_constants.naics_two_digit_business_type_coarse_xwalk)
    df['naics_descr3_standardized'] = naics_to_business_type_coarse(df['naics_descr_3'],
                                                                    xwalk = data_constants.naics_three_digit_business_type_coarse_xwalk)
    return df


def naics_to_business_type_coarse(naics:pd.Series, xwalk:dict = data_constants.naics_two_digit_business_type_coarse_xwalk) -> pd.Series:
    return naics.map(xwalk)


def standardize_business_type(business_type:pd.Series, standardize_dict:dict) -> pd.Series:
    return business_type.map(standardize_dict)


def guess_business_type_from_name(name:pd.Series) -> np.ndarray:
    return np.where(
        name.str.contains("manufact|agric|utilit|\soil\s|PG&E|\selectric\s|chemical|auto|\btire", flags = re.IGNORECASE),
        "industrial",
        np.where(
            name.str.contains("construction|brick", flags = re.IGNORECASE),
        )
    )


def get_business_type(df:pd.DataFrame, naics_col:str = "naics_descr3_standardized",
                      business_type_col:str = "business_type_standardized"):
    # start from a standardized business type column
    # where the business type is unknown, or other, fill in from an naics column crosswalked to standardized values
    # where the business type is still unknown or other, try to fill in w/ the business names TODO IMPLEMENT THIS
    df['business_type_imputed'] = np.where(
        df[business_type_col].isin([x for x in data_constants.business_types_coarse if x not in
                                    ["other", "unknown", "not a business"]]),
        df[business_type_col],
        np.where(
            df[naics_col].isin([x for x in data_constants.business_types_coarse if x not in
                                    ["other", "unknown", "not a business"]]),
            df[naics_col],
            np.nan
        )
    )
    return df


# function for doing left fuzzy merges between two datasets where you want to exact match on a set of columns
# and do a fuzzy match on an additional column.
# df1 and df2 are pandas dataframes (function does a left join w/ df1)
# right fuzzy col is the name of the column in df2 that you want to fuzzy join on
# left fuzzy col is the name of the column in df1 that you want to fuzzy join on
# left cols and right cols are the columns in df1 and df2 that you want to exact match on
# indicator specifies if you want the indicator column _merged to be in the dataframe
# suffixes are a list of desired suffixes to be added to the merged columns if they appear in both dataframes
# returns a merged dataframe
def fuzzy_merge(df1:pd.DataFrame, df2:pd.DataFrame, left_fuzzy_col:str,
                right_fuzzy_col:str, left_cols:list, right_cols:list, threshold:int, indicator = True,
                suffixes=None) -> pd.DataFrame:
    if suffixes is None:
        suffixes = [None, '_from_address']
    if threshold == "from column":
        threshold = df1['threshold']
    if "_merge" in df1.columns:
        df1.drop(columns = ["_merge"], inplace = True)
    df1_copy = df1.copy(deep=True)
    df1 = df1[~df1.duplicated(subset=[left_fuzzy_col] + left_cols, keep='first')][[left_fuzzy_col] + left_cols]
    df2 = df2[~df2.duplicated(subset=[right_fuzzy_col] + right_cols, keep='first')]
    df1['index'] = np.arange(df1.shape[0])
    df_m = pd.merge(df1, df2, left_on=left_cols, right_on=right_cols, how="left")
    # make difference columns
    df_m[left_fuzzy_col] = df_m[left_fuzzy_col].astype(str)
    df_m[right_fuzzy_col] = df_m[right_fuzzy_col].astype(str)
    df_m['similarity'] = df_m.apply(lambda x: fuzz.ratio(x[left_fuzzy_col], x[right_fuzzy_col]), axis=1)
    df_m['max_similarity'] = df_m.groupby('index')['similarity'].transform('max')
    # filter where difference is minimum & below threshold
    # equivalent to saying grab closest string if that string is close enough
    # if there are two equivalent strings get the first one
    df_m = df_m[ (df_m['similarity'] >= threshold) & (df_m['max_similarity'] == df_m['similarity']) ].drop_duplicates(subset='index')
    df1 = df1.merge(df_m.drop(columns=[left_fuzzy_col, 'similarity', 'max_similarity'] + left_cols),
                    on='index', how = 'left', indicator=indicator)
    df1.drop(columns='index', inplace=True)
    df1 = pd.merge(left=df1_copy, right=df1, on=[left_fuzzy_col] + left_cols, how='left', suffixes = suffixes)
    return df1


# equivalent to doing a fuzzy merge on a numeric column. similar to doing a rolling join but with absolute distance
def get_nearest_address(df1:pd.DataFrame,df2:pd.DataFrame, n1_col_left:str, n1_col_right:str, right_cols:list,
                        left_cols:list, threshold=5, indicator=True,suffixes=None) -> pd.DataFrame:
    if suffixes is None:
        suffixes = [None, '_from_address']
    if "_merge" in df1.columns:
        df1.drop(columns = ["_merge"], inplace = True)
    if threshold == "from column":
        threshold = df1['threshold']
    # drop duplicates on address cols to avoid gigantic merges
    # keep copy of df1 to merge back
    df1_copy = df1.copy(deep=True)
    df1 = df1[~df1.duplicated(subset= [n1_col_left] + left_cols, keep='first')][[n1_col_left] + left_cols]
    df2 = df2[~df2.duplicated(subset = [n1_col_right] + right_cols, keep='first')]
    df1['index'] = np.arange(df1.shape[0])
    df_m = pd.merge(df1, df2,left_on = left_cols, right_on=right_cols, how= "left")
    # make difference columns
    # this is way too much effort to convert a column to numeric...
    if pd.api.types.is_string_dtype(df_m[n1_col_left]):
        df_m[n1_col_left] = df_m[n1_col_left].str.replace('\.0+', '')
        df_m[n1_col_left] = df_m[n1_col_left].str.replace('\D', '')
        df_m[n1_col_left] = df_m[n1_col_left].replace("", np.nan, regex=False)
        df_m[n1_col_left] = pd.to_numeric(df_m[n1_col_left], errors='coerce')
        df_m[n1_col_left] = df_m[n1_col_left].astype('Int64')
    if pd.api.types.is_string_dtype(df_m[n1_col_right]):
        df_m[n1_col_right] = df_m[n1_col_right].str.replace('\.0+', '')
        df_m[n1_col_right] = df_m[n1_col_right].str.replace('\D', '')
        df_m[n1_col_right] = df_m[n1_col_right].replace("", np.nan, regex=False)
        df_m[n1_col_right] = pd.to_numeric(df_m[n1_col_right], errors='coerce')
        df_m[n1_col_right] = df_m[n1_col_right].astype('Int64')
    df_m['difference'] = abs(df_m[n1_col_left] - df_m[n1_col_right]).fillna(1000000) # fill na w/ really big number
    df_m['min_difference'] = df_m.groupby('index')['difference'].transform('min')
    # filter where difference is minimum & below threshold
    # equivalent to saying grab closest address if that address is close enough
    # if there are two equivalent min distances get the first one
    df_m = df_m[(df_m['difference'] <= threshold) & (df_m['min_difference'] == df_m['difference'])].drop_duplicates(
        subset='index')
    df1 = df1.merge(df_m.drop(columns=[n1_col_left, 'difference', 'min_difference'] + left_cols),
                    on='index', how='left', indicator=indicator)
    df1.drop(columns = 'index', inplace = True)
    df1 = pd.merge(left=df1_copy, right=df1,on=[n1_col_left] + left_cols, how='left' , suffixes=suffixes)
    return df1

