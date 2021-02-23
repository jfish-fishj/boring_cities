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


def clean_column_names(col_list):
    def clean_column(string):
        string = str.lower(string) # make lowercase
        string = re.sub(pattern='\s',string=string, repl='_') # replace spaces w/ underscores
        string = re.sub(pattern='[^a-zA-Z0-9]',string=string,repl="") # remove non-alphanumeric characters
        return string
    new_col_list = [clean_column(x) for x in col_list]
    return new_col_list

def make_year_var(df, date_col, new_col):
    df[new_col] = df[date_col].str.extract('([0-9]{4})')
    return(df)

def add_subset_address_cols(df):
    for col in address_cols:
        if col not in df.columns:
            df[col] = np.nan
    df = df[address_cols]
    return df

def fuzzy_merge(df1,df2, left_fuzzy_col, right_fuzzy_col, left_cols, right_cols, threshold, indicator = True,
                suffixes=['', '_from_address']):
    df1['index'] = np.arange(df1.shape[0])
    df_m = pd.merge(df1, df2, left_on=left_cols, right_on=right_cols, how="left")
    # make difference columns
    df_m['difference'] = df_m.apply(lambda x: fuzz.ratio(x[left_fuzzy_col], x[right_fuzzy_col]), axis=1)
    df_m['min_difference'] = df_m.groupby('index')['difference'].transform('min')
    # filter where difference is minimum & below threshold
    # equivalent to saying grab closest address if that address is close enough
    # if there are two equivalent min distances get the first one
    df_m = df_m[(df_m['difference'] <= threshold) & (df_m['min_difference'] == df_m['difference'])].drop_duplicates(
        subset='index')
    df1 = df1.merge(df_m.drop(columns=[left_fuzzy_col, 'difference', 'min_difference'] + left_cols),
                    on='index', how='left', indicator=indicator, suffixes=suffixes)
    df1.drop(columns='index', inplace=True)
    return df1


def get_nearest_address(df1,df2, n1_col_left, n1_col_right, right_cols, left_cols, threshold=5, indicator=True,
                        suffixes=['', '_from_address']):
    if suffixes is None:
        suffixes = ['', '_from_address']
    df1['index'] = np.arange(df1.shape[0])
    df_m = pd.merge(df1, df2,left_on = left_cols, right_on=right_cols, how= "left" )
    # make difference columns
    df_m['similarity'] = abs(df_m[n1_col_left] - df_m[n1_col_right])
    df_m['max_similarity'] = df_m.groupby('index')['similarity'].transform('max')
    # filter where difference is minimum & below threshold
    # equivalent to saying grab closest address if that address is close enough
    # if there are two equivalent min distances get the first one
    df_m = df_m[ (df_m['similarity'] >= threshold) & (df_m['max_similarity'] == df_m['similarity']) ].drop_duplicates(subset='index')
    df1 = df1.merge(df_m.drop(columns=[n1_col_left, 'similarity', 'max_similarity'] + left_cols),
                    on='index', how = 'left', indicator=indicator, suffixes = suffixes)
    df1.drop(columns = 'index', inplace = True)
    return df1

