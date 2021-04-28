import sqlite3
import pandas as pd
from data_constants import make_data_dict, dataPrefix
from helper_functions import business_loc_to_sql
import os
data_dict = make_data_dict(use_seagate=False)
# print(data_dict['intermediate']['sd']['business_location'] + '/business_location_addresses_merged.csv')
# print([(data_dict['raw']['sd']['business_location'] + f'{file}')
#                   for file in os.listdir(data_dict['raw']['sd']['business_location']) if "to" in file])
# print(dataPrefix + "/data/business_locations.db")
# sac_bus = pd.read_csv(data_dict['final']['sac']['business_location'] + '/business_location_flat.csv')
# business_loc_to_sql(sac_bus, table="business_locations_flat", mode="replace")
# con = sqlite3.connect(dataPrefix + "/data/business_locations.db")
# df = pd.read_sql_query("SELECT * from business_locations_flat", con)
# print(df.shape[0])
# con.close()
print(bool(False == False))