import pandas as pd
from python_modules.data_cleaning.clean_address_data import merge_sac_parcel_id, clean_sac_add
from python_modules.helper_files.data_constants import make_data_dict

# data dict
data_dict = make_data_dict(use_seagate=False)


# SACREMENTO
sac_add = pd.read_csv(data_dict['raw']['sac']['parcel'] + 'Addresses.csv')
sac_xwalk = pd.read_csv(data_dict['raw']['sac']['parcel'] + 'Address_parcel_xwalk.csv')
sac_add = merge_sac_parcel_id(sac_add=sac_add, sac_xwalk=sac_xwalk)
clean_sac_add(sac_add).to_csv(data_dict['intermediate']['sac']['parcel'] + 'addresses_concat.csv',
 index=False)
        