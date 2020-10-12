# file cleans parcel data and exports back
import pandas as pd
import numpy as np
from helper_functions import write_to_log, WTL_TIME
from data_constants import make_data_dict, filePrefix, name_parser_files
from name_parsing import parse_and_clean_name, classify_name, clean_name
from address_parsing import clean_parse_address
from pathos.multiprocessing import ProcessingPool as Pool

write_to_log(f'Starting clean business data at {WTL_TIME}')
# initialize data dict
data_dict = make_data_dict(use_seagate=True)

sf_bus = data_dict['intermediate']['sf']['business location'] + '/business_location.csv'
la_bus = data_dict['intermediate']['la']['business location'] + '/business_location.csv'

