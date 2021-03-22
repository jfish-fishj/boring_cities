# python file for useful functions
import inspect
import warnings
import os
import pandas as pd
from datetime import datetime
import re
from fuzzywuzzy import fuzz
from data_constants import filePrefix, address_cols
import numpy as np
WTL_TIME = datetime.today().strftime('%Y-%m-%d')

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

    if not os.path.exists((filePrefix + "/Logs/{}".format(WTL_TIME))):
        os.mkdir((filePrefix + "/Logs/{}".format(WTL_TIME)))

    f = open((filePrefix + '/Logs/{}/__OVERALL_LOG__.txt'.format(WTL_TIME)), "a+")

    text = text.rjust(len(text) + tabs * 3)
    if tabs == 0:
        text = "\n" + text

    # writing
    f.write(text  + "\n")
    if doPrint:
        print(text)
    if warn:
        warnings.warn(text)
        w = open((filePrefix + '/Logs/{}/__Warnings.txt'.format(WTL_TIME)),"a+")
        w.write(text + "\n")


def interpolate_polygon(df, id_col, direction):
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


def clean_column_names(col_list):
    def clean_column(string):
        string = str.lower(string) # make lowercase
        string = re.sub(pattern='\s',string=string, repl='_') # replace spaces w/ underscores
        string = re.sub(pattern='[^a-zA-Z0-9]',string=string,repl="") # remove non-alphanumeric characters
        return string
    new_col_list = [clean_column(x) for x in col_list]
    return new_col_list

def make_year_var(df, date_col, new_col, round_down=False):
    df[new_col] = df[date_col].astype(str).str.extract('([0-9]{4})')
    if round_down is not False:
        df[new_col] = df[new_col].astype(int) -1
    return(df)

def add_subset_address_cols(df):
    for col in address_cols:
        if col not in df.columns:
            df[col] = np.nan
    df = df[address_cols]
    return df

def fuzzy_merge(df1,df2, left_fuzzy_col, right_fuzzy_col, left_cols, right_cols, threshold, indicator = True,
                suffixes=None):
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


def get_nearest_address(df1,df2, n1_col_left, n1_col_right, right_cols, left_cols, threshold=5, indicator=True,
                        suffixes=None):
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

