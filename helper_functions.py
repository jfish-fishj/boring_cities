# python file for useful functions
import inspect
import warnings
import os
import pandas as pd
from datetime import datetime
import re
from data_constants import filePrefix
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
