import pandas as pd


# data dict
data_dict = make_data_dict(use_seagate=False)


# SACREMENTO
sac_add = pd.read_csv(data_dict['raw']['sac']['parcel'] + 'Address.csv')
sac_xwalk = pd.read_csv(data_dict['raw']['sac']['parcel'] + 'Address_parcel_xwalk.csv')
sac_add = merge_sac_parcel_id(sac_add=sac_add, xwalk=sac_xwalk)
clean_sac_add(sac_add).to_csv(data_dict['intermediate']['sac']['parcel'] + 'addresses.csv',
 index=False)
print("hello")