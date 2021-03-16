import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz
from data_constants import *
from helper_functions import fuzzy_merge, get_nearest_address
from make_business_vars import make_panel


def merge_addresses(bus_df, add_df, bus_merge_cols, add_merge_cols, cols_to_merge,
                    fuzzy = False,nearest_n1 = False, expand_addresses=False, n1_threshold=5, fuzzy_threshold=90):

    if expand_addresses is not False:
        add_df['parsed_addr_n1'] = add_df['parsed_addr_n1'].fillna("").astype(str).str.replace('\.0+', '')
        add_df['parsed_addr_n1'] = add_df['parsed_addr_n1'].str.replace('\D', '')
        add_df['parsed_addr_n1'] = add_df['parsed_addr_n1'].replace("", np.nan, regex=False)
        add_df['parsed_addr_n1'] = pd.to_numeric(add_df['parsed_addr_n1'], errors='coerce')
        add_df['parsed_addr_n1'] = add_df['parsed_addr_n1'].astype(np.float64)

        add_df['parsed_addr_n2'] = add_df['parsed_addr_n2'].fillna(add_df['parsed_addr_n1']).astype(str).str.replace('\.0+', '')
        add_df['parsed_addr_n2'] = add_df['parsed_addr_n2'].str.replace('\D', '')
        add_df['parsed_addr_n2'] = add_df['parsed_addr_n2'].replace("", np.nan, regex=False)
        add_df['parsed_addr_n2'] = pd.to_numeric(add_df['parsed_addr_n2'], errors='coerce')
        add_df['parsed_addr_n2'] = add_df['parsed_addr_n2'].astype(np.float64)

        add_df = make_panel(add_df, start_year='parsed_addr_n1', end_year='parsed_addr_n2',
                            drop_future=False, limit=200, evens_and_odds=True)
        add_df['parsed_addr_n1'] = add_df['year'].astype(int).astype('str')
        add_df = add_df.drop(columns = "year")

    og_cols = bus_df.columns

    bus_df[bus_merge_cols] = bus_df[bus_merge_cols].fillna("").astype(str)
    for col in bus_merge_cols:
        bus_df[col] = bus_df[col].str.replace('\.0+', "")

    add_df[add_merge_cols] = add_df[add_merge_cols].fillna("").astype(str)
    for col in add_merge_cols:
        add_df[col] = add_df[col].str.replace('\.0+', "")

    bus_df = bus_df.merge(
        add_df[add_merge_cols + cols_to_merge].drop_duplicates(subset =add_merge_cols ), how = "left",
        left_on = bus_merge_cols,
        right_on = add_merge_cols,
        indicator = True,
        suffixes=[None, '_from_address']
    )

    bus_df['merged_from'] = np.where(bus_df["_merge"] != "left_only", "num_st_sfx", "not merged succesfully")
    # try nearest parcel matching
    if nearest_n1 is not False:
        # filter for addresses that havent been merged
        lo = bus_df[bus_df['_merge'] == "left_only"][og_cols]  # reset columns so we dont get suffixes
        nlo = bus_df[bus_df['_merge'] != "left_only"]
        lo = get_nearest_address(
            df1 = lo, df2 = add_df[add_merge_cols + cols_to_merge].drop_duplicates(subset =add_merge_cols ),
            left_cols=["primary_cleaned_addr_sn","primary_cleaned_addr_ss"],
            right_cols=["parsed_addr_sn", "parsed_addr_ss"],
            n1_col_left='primary_cleaned_addr_n1',
            n1_col_right='parsed_addr_n1',
            threshold=n1_threshold,
            indicator=True,
            suffixes=[None, '_from_address']
        )
        lo['merged_from'] = np.where(lo["_merge"] != "left_only", "nearest_n1", "not merged succesfully")
        bus_df = pd.concat([lo, nlo])

    if fuzzy is not False:
        # filter for addresses that havent been merged
        lo = bus_df[bus_df['_merge'] == "left_only"][og_cols] # reset columns so we dont get suffixes
        nlo = bus_df[bus_df['_merge'] != "left_only"]
        lo = fuzzy_merge(
            df1 = lo, df2 = add_df[add_merge_cols + cols_to_merge].drop_duplicates(subset =add_merge_cols ),
            left_cols=['primary_cleaned_addr_n1',"primary_cleaned_addr_ss"],
            right_cols=['parsed_addr_n1', "parsed_addr_ss"],
            left_fuzzy_col = "primary_cleaned_addr_sn",
            right_fuzzy_col = "parsed_addr_sn",
            threshold=fuzzy_threshold,
            indicator=True,
            suffixes=[None, '_from_address']
        )
        lo['merged_from'] = np.where(lo["_merge"] != "left_only", "fuzzy", "not merged succesfully")
        bus_df = pd.concat([lo, nlo])
    # bus_df["long_from_address"] = abs(bus_df["long_from_address"])
    print(bus_df['merged_from'].value_counts())
    print(bus_df['parsed_city'].value_counts().head())
    # try fuzzy matching on street name
    return bus_df

def misc_sf_cleaning(df):
    pass

def misc_baton_rouge_cleaning(bus_df, add_df):
    bus_df['primary_cleaned_addr_n1'] = bus_df['primary_cleaned_addr_n1'].str.replace("[a-z]", "")
    bus_df['primary_cleaned_addr_n1'] = bus_df['primary_cleaned_addr_n1'].str.replace("^(0+)([1-9][0-9]+)", r"\g<2>")
    # bus_df['primary_cleaned_addr_ss'] = np.where(
    #     bus_df['primary_cleaned_addr_sn'].str.contains("sherwood forest",na = False, regex=True),
    #     "blvd",
    #     bus_df['primary_cleaned_addr_ss']
    #
    # )
    add_df = add_df[add_df['parsed_city'] == "baton rouge"]
    add_df['index1'] = np.arange(add_df.shape[0])

    bus_df = bus_df.merge((
        add_df.groupby(["parsed_addr_sn", "parsed_addr_ss"])['index1'].
            agg(**{"count": "count"}).reset_index().
            sort_values(['parsed_addr_sn', 'count'], ascending=False).
            drop_duplicates(subset=['parsed_addr_sn'], keep="first").
            drop(columns="count").assign(fill_col=add_df['parsed_addr_ss'])

    ), how="left", left_on='primary_cleaned_addr_sn', right_on=['parsed_addr_sn'])
    bus_df['primary_cleaned_addr_ss'] = bus_df['primary_cleaned_addr_ss'].replace("", np.nan).fillna(bus_df['fill_col'])
    # bus_df['primary_cleaned_addr_ss'] = np.where(
    #     bus_df['primary_cleaned_addr_sn'].str.contains("acadian", na=False, regex=True),
    #     "thwy",
    #     bus_df['primary_cleaned_addr_ss']
    #
    # )
    # bus_df['primary_cleaned_addr_ss'] = np.where(
    #     bus_df['primary_cleaned_addr_sn'] == "perkins",
    #     "rd",
    #     bus_df['primary_cleaned_addr_ss']
    #
    # )
    # bus_df['primary_cleaned_addr_ss'] = np.where(
    #     bus_df['primary_cleaned_addr_sn'] == "cortana",
    #     "pl",
    #     bus_df['primary_cleaned_addr_ss']
    #
    # )
    # bus_df['primary_cleaned_addr_ss'] = np.where(
    #     bus_df['primary_cleaned_addr_sn'] == "bluebonnet",
    #     "blvd",
    #     bus_df['primary_cleaned_addr_ss']
    #
    # )
    # bus_df['primary_cleaned_addr_ss'] = np.where(
    #     bus_df['primary_cleaned_addr_sn'] == "choctaw",
    #     "dr",
    #     bus_df['primary_cleaned_addr_ss']
    #
    # )
    bus_df['primary_cleaned_addr_ss'] = np.where(
        bus_df['primary_cleaned_addr_sn'] == "united",
        "blvd",
        bus_df['primary_cleaned_addr_ss']
    )

    bus_df['primary_cleaned_addr_ss'] = np.where(
        (bus_df['primary_cleaned_addr_sn'] == "sherwood forest") &  (bus_df['primary_cleaned_addr_sd'] == "n"),
        "dr",
        bus_df['primary_cleaned_addr_ss']

    )


    return bus_df


def misc_chi_cleaning(add_df, bus_df):
    add_df["parsed_addr_n1"] = add_df["parsed_addr_n1"].astype(str).str.replace("\.0", "")
    bus_df["primary_cleaned_addr_n1"] = bus_df["primary_cleaned_addr_n1"].astype(str).str.replace("\.0", "")
    # replace broadway
    bus_df['primary_cleaned_addr_ss'] = np.where(
        bus_df['primary_cleaned_addr_sn'] == "broadway", "st", bus_df['primary_cleaned_addr_ss']
    )
    # replace corner address
    bus_df['primary_cleaned_addr_sn'] = np.where(
        (bus_df['primary_cleaned_addr_sn'] == "rockwell") & (bus_df['primary_cleaned_addr_n1'].astype(str) == "2700") ,
        "85th",
        bus_df['primary_cleaned_addr_sn']
    )
    # replace large office buildings
    bus_df['primary_cleaned_addr_sn'] = np.where(
        (bus_df['primary_cleaned_addr_sn'] == "cityfront") ,
        "cityfrontz",
        bus_df['primary_cleaned_addr_sn']
    )
    bus_df['primary_cleaned_addr_ss'] = np.where(
        (bus_df['primary_cleaned_addr_sn'] == "cityfrontz") ,
        "dr",
        bus_df['primary_cleaned_addr_ss']
    )
    bus_df['primary_cleaned_addr_sn'] = np.where(
        bus_df['primary_cleaned_addr_sn'].str.contains("martin luther"),
        "martin luther king",
        bus_df['primary_cleaned_addr_sn']
    )

    # repeat for add df
    add_df['parsed_addr_sn'] = np.where(
        add_df['parsed_addr_sn'].str.contains("martin luther"),
        "martin luther king",
        add_df['parsed_addr_sn']
    )
    # add threshold column
    # bus_df['threshold'] = np.where(
    #     # large office buildings get higher threshold
    # )
    # filter add df to just chicago
    add_df = add_df[add_df['parsed_city'] == "chicago"]
    return add_df, bus_df

if __name__ == "__main__":
    data_dict = make_data_dict(use_seagate=True)
    sd_bus = pd.read_csv(data_dict['intermediate']['sd']['business location'] + '/business_location.csv')
    sd_add = pd.read_csv(data_dict['intermediate']['sd']['parcel'] + '/addresses.csv')
    sd_bus = merge_addresses(sd_bus, sd_add, fuzzy=True, nearest_n1=True,
                             add_merge_cols=['parsed_addr_n1', 'parsed_addr_sn', 'parsed_addr_ss'],
                             bus_merge_cols=['primary_cleaned_addr_n1', 'primary_cleaned_addr_sn',
                                             'primary_cleaned_addr_ss'],
                             cols_to_merge=['lat', 'long', 'parcelID', 'parsed_city', 'parsed_addr_zip']
                             )
    sd_bus.to_csv(data_dict['intermediate']['sd']['business location'] + '/business_location_addresses_merged.csv', index=False)
    # chicago_bus = pd.read_csv(data_dict['intermediate']['chicago']['business location'] + '/business_location.csv')
    # chicago_add = pd.read_csv(data_dict['intermediate']['chicago']['parcel'] + '/addresses_from_parcels.csv')
    # chicago_add, chicago_bus = misc_chi_cleaning(chicago_add, chicago_bus)
    # chicago_bus = merge_addresses(chicago_bus, chicago_add, fuzzy=True, nearest_n1=True,
    #                               add_merge_cols=['parsed_addr_n1', 'parsed_addr_sn', 'parsed_addr_ss'],
    #                               bus_merge_cols=['primary_cleaned_addr_n1', 'primary_cleaned_addr_sn',
    #                                               'primary_cleaned_addr_ss'],
    #                               cols_to_merge=['lat', 'long', 'parcelID', 'parsed_city', 'parsed_addr_zip']
    #                               )
    #
    # chicago_bus.to_csv(data_dict['intermediate']['chicago']['business location'] + '/business_location_addresses_merged.csv', index=False)
    # baton_rouge_bus = pd.read_csv(
    #     data_dict['intermediate']['baton_rouge']['business location'] + '/business_location.csv')
    # baton_rouge_add = pd.read_csv(data_dict['intermediate']['baton_rouge']['parcel'] + '/addresses.csv')
    # baton_rouge_bus = misc_baton_rouge_cleaning(baton_rouge_bus, add_df= baton_rouge_add)
    # baton_rouge_bus = merge_addresses(baton_rouge_bus, baton_rouge_add,fuzzy=True, nearest_n1=True, expand_addresses=True,
    #                                   add_merge_cols=['parsed_addr_n1', 'parsed_addr_sn', 'parsed_addr_ss'],
    #                                   bus_merge_cols=['primary_cleaned_addr_n1', 'primary_cleaned_addr_sn',
    #                                                   'primary_cleaned_addr_ss'],
    #                                   cols_to_merge=['lat', 'long', 'parcelID', 'parsed_city', 'parsed_addr_zip']
    #                                   )
    # # print(baton_rouge_bus['merged_from'].value_counts())
    # baton_rouge_bus.to_csv(data_dict['intermediate']['baton_rouge']['business location'] + '/business_location_addresses_merged.csv')
    # stl_bus = pd.read_csv(data_dict['intermediate']['stl']['business location'] + '/business_location.csv')
    # stl_add = pd.read_csv(data_dict['intermediate']['stl']['parcel'] + '/addresses.csv')
    # stl_bus = merge_addresses(stl_bus, stl_add, fuzzy=True, nearest_n1=True,
    #                           add_merge_cols = ['parsed_addr_n1', 'parsed_addr_sn', 'parsed_addr_ss'],
    #                           bus_merge_cols = ['primary_cleaned_addr_n1', 'primary_cleaned_addr_sn', 'primary_cleaned_addr_ss'],
    #                           cols_to_merge=['lat', 'long', 'parcelID', 'parsed_city', 'parsed_addr_zip']
    #                           )
    # print(stl_bus['merged_from'].value_counts())
    # stl_bus.to_csv(data_dict['intermediate']['stl']['business location'] + '/business_location_addresses_merged.csv', index=False)