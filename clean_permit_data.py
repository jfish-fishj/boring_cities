# file cleans permit data and exports back
import pandas as pd
import numpy as np
from helper_functions import write_to_log, make_year_var, WTL_TIME
from data_constants import make_data_dict, filePrefix, name_parser_files
from name_parsing import parse_and_clean_name, classify_name, clean_name, combine_names
from address_parsing import clean_parse_address
from pathos.multiprocessing import ProcessingPool as Pool
import os

