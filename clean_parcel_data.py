# file cleans parcel data and exports back
import pandas as pd
import numpy as np
from helper_functions import write_to_log, make_year_var, WTL_TIME
from data_constants import make_data_dict, filePrefix, name_parser_files
from name_parsing import parse_and_clean_name, classify_name, clean_name
from address_parsing import clean_parse_address
from pathos.multiprocessing import ProcessingPool as Pool

write_to_log(f'Starting clean business data at {WTL_TIME}')
# initialize data dict
data_dict = make_data_dict(use_seagate=True)
la_p = pd.read_csv(data_dict['intermediate']['la']['parcel'] + '/Assessor_Parcels_Data_-_2006_thru_2019.csv')
# sf parcel data
sf_p = pd.read_csv(data_dict['intermediate']['sf'])
sf_p['parcelID'] = sf_p['parcelID'].str.replace('[^\x00-\x7F]', "")
# los angeles parcel data



# TODO st louis, miami, nyc, boston, houston parcel data

