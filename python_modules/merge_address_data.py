import pandas as pd
import numpy as np
from data_constants import *
from python_modules.helper_functions import fuzzy_merge, get_nearest_address
from python_modules.helper_files.make_business_vars import make_panel

# master function for left merging address data onto a business dataframe
# goal is to get lat long coordinates from an address list (and parcelID if it exists) so that we can geocode
# bus_df is the business df
# add_df is the address df
# bus_merge_cols are what columns you want to merge on w/ the business_df
# add_merge_cols are what columns you want to merge on w./ the address df
# cols to merge indicates which columns (other than the merge cols) that you want to bring onto the business df
# fuzzy indicates if you want to do a fuzzy merge on street name
# nearestn1 indicates if you want to do a fuzzy merge on street number
# n1 threshold defaults to 5 meaning 123 main st matches 128 main st but not 129 main st
# fuzzy threshold is the minimum levensthein distance for a fuzzy match to be allowable
# expand addresses converts address ranges into multiple rows e.g. 123-129 main st -> 123, 125, 127, 129 main st in
# four different rows. NOTE: this implicitly does even and odds
# returns a business dataframe w/ merged address components and an indicator column saying whether a row was merged
# and from which type of merge
def merge_addresses(bus_df:pd.DataFrame, add_df:pd.DataFrame, bus_merge_cols:list,
                    add_merge_cols:list, cols_to_merge:list,fuzzy = False,nearest_n1 = False,
                    expand_addresses=False, n1_threshold=5, fuzzy_threshold=90):

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

    # sort values on parcelID so that if there are two addresses with the same address components,
    # but one has a parcelID the code will prioritize the parcelID row when it drops duplicateds

    # the casting as str is a weird workaround and should be replaced by enforced dtypes
    # but what can happen is that pandas allows a column to have mixed types,
    # so if a parcelID has both string and float types then it throws an error when it tries to sort

    add_df = (add_df[add_merge_cols + cols_to_merge].
              assign(parcelID = lambda x:x.parcelID.astype(str).
                     replace("nan", np.nan, regex=False)).
              sort_values("parcelID", na_position="last", ascending=False).
              drop_duplicates(subset=add_merge_cols, keep="first"))

    bus_df = bus_df.merge(
        add_df, how = "left",
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
            df1 = lo, df2 = add_df,
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
            df1 = lo, df2 = add_df,
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
    print(bus_df['merged_from'].value_counts())
    print(bus_df['parsed_city'].value_counts().head())
    # try fuzzy matching on street name
    return bus_df


# misc cleaning functions do any address standardization that couldnt be done in the generic cleaning functions
# eg in SF the address data has "The embarcadero" for the embarcadero center, but no one in real life ever puts that
# as an address
def misc_sf_cleaning(df):
    df['primary_cleaned_addr_ss'] = np.where(
        df['primary_cleaned_addr_sn'] == 'broadway',
        "",
        df['primary_cleaned_addr_ss']
    )

    df['primary_cleaned_addr_sn'] = np.where(
        df['primary_cleaned_addr_sn'] == 'embarcadero',
        "the embarcadero",
        df['primary_cleaned_addr_sn']
    )

    df['primary_cleaned_addr_ss'] = np.where(
        df['primary_cleaned_addr_sn'] == 'the embarcadero',
        "",
        df['primary_cleaned_addr_ss']
    )

    return df


def misc_seattle_cleaning(df):
    df['primary_cleaned_addr_sn'] = np.where(
        (df['primary_cleaned_addr_sn'] == 'm l king jr') | (df['primary_cleaned_addr_sn'] == "mlk jr"),
        "martin luther king jr",
        df['primary_cleaned_addr_sn']
    )


    return df


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
    bus_df['primary_cleaned_addr_ss'] = np.where(
        bus_df['primary_cleaned_addr_sn'].str.contains("acadian", na=False, regex=True),
        "thwy",
        bus_df['primary_cleaned_addr_ss']

    )
    bus_df['primary_cleaned_addr_sn'] = np.where(
        bus_df['primary_cleaned_addr_sn'].str.contains("acadian", na=False, regex=True),
        "acadian",
        bus_df['primary_cleaned_addr_sn']
    )
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


def misc_la_cleaning(bus_df, add_df):
    add_df['parsed_addr_ss'] = np.where(
        add_df['parsed_addr_sn'].str.contains("ave [0-9]{1,2}"),
        "ave",
        add_df['parsed_addr_ss']
    )


    bus_df['primary_cleaned_addr_sn'] = np.where(
        (bus_df['primary_cleaned_addr_sn'].str.contains("^[nsew]$", na=False, regex=True)) &
        (bus_df['primary_cleaned_addr_ss'] == "ave"),
        bus_df['cleaned_primary_address_fa'].str.extract('(ave [0-9]{1,2})').iloc[:,0],
        bus_df['primary_cleaned_addr_sn']
    )

    bus_df['primary_cleaned_addr_sn'] = np.where(
        (bus_df['primary_cleaned_addr_sn'].str.contains("^[s]$", na=False, regex=True)) &
        (bus_df['primary_cleaned_addr_ss'] == "st"),
        "st andrews",
        bus_df['primary_cleaned_addr_sn']
    )
    bus_df['primary_cleaned_addr_ss'] = np.where(
        (bus_df['primary_cleaned_addr_sn'] == "st andrews"),
        "pl",
        bus_df['primary_cleaned_addr_ss']
    )

    bus_df['primary_cleaned_addr_ss'] = np.where(
        (bus_df['primary_cleaned_addr_sn'] == "century") & (bus_df['cleaned_primary_address_fa'].str.contains("park|pk")),
        "pk",
        bus_df['primary_cleaned_addr_ss']
    )

    return bus_df, add_df



if __name__ == "__main__":
    data_dict = make_data_dict(use_seagate=True)
    # sd_bus = pd.read_csv(data_dict['intermediate']['sd']['business location'] + '/business_location.csv')
    # sd_add = pd.read_csv(data_dict['intermediate']['sd']['parcel'] + '/addresses.csv')
    # sd_bus = merge_addresses(sd_bus, sd_add, fuzzy=True, nearest_n1=True,
    #                          add_merge_cols=['parsed_addr_n1', 'parsed_addr_sn', 'parsed_addr_ss'],
    #                          bus_merge_cols=['primary_cleaned_addr_n1', 'primary_cleaned_addr_sn',
    #                                          'primary_cleaned_addr_ss'],
    #                          cols_to_merge=['lat', 'long', 'parcelID', 'parsed_city', 'parsed_addr_zip']
    #                          )
    # sd_bus.to_csv(data_dict['intermediate']['sd']['business location'] + '/business_location_addresses_merged.csv', index=False)
    # la_bus = pd.read_csv(data_dict['intermediate']['la']['business location'] + '/business_location.csv')
    # la_add = pd.read_csv(data_dict['intermediate']['la']['parcel'] + '/addresses_temp.csv')
    # la_bus, la_add = misc_la_cleaning(la_bus, la_add)
    # la_bus = merge_addresses(la_bus, la_add, fuzzy=True, nearest_n1=True,
    #                          add_merge_cols=['parsed_addr_n1', 'parsed_addr_sn', 'parsed_addr_ss'],
    #                          bus_merge_cols=['primary_cleaned_addr_n1', 'primary_cleaned_addr_sn',
    #                                          'primary_cleaned_addr_ss'],
    #                          cols_to_merge=['lat', 'long', 'parcelID', 'parsed_city', 'parsed_addr_zip']
    #                          )
    # la_bus.to_csv(data_dict['intermediate']['la']['business location'] + '/business_location_addresses_merged.csv', index=False)
    # orlando_bus = pd.read_csv(data_dict['intermediate']['orlando']['business location'] + '/business_location.csv')
    # orlando_add = pd.read_csv(data_dict['intermediate']['orlando']['parcel'] + '/addresses.csv')
    # orlando_add = orlando_add[orlando_add['parsed_city']=="orlando"]
    # orlando_add['parsed_addr_ss'] = np.where(orlando_add['parsed_addr_sn'] == "orange blossom", "trl", orlando_add['parsed_addr_ss'])
    # orlando_bus = merge_addresses(orlando_bus, orlando_add, fuzzy=True, nearest_n1=True,
    #                          add_merge_cols=['parsed_addr_n1', 'parsed_addr_sn', 'parsed_addr_ss'],
    #                          bus_merge_cols=['primary_cleaned_addr_n1', 'primary_cleaned_addr_sn',
    #                                          'primary_cleaned_addr_ss'],
    #                          cols_to_merge=['lat', 'long', 'parcelID', 'parsed_city', 'parsed_addr_zip']
    #                          )
    # orlando_bus.to_csv(data_dict['intermediate']['orlando']['business location'] + '/business_location_addresses_merged.csv', index=False)
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
    # print(baton_rouge_bus['merged_from'].value_counts())
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
    sf_bus = pd.read_csv(data_dict['intermediate']['sf']['business location'] + '/business_location.csv')
    sf_bus = misc_sf_cleaning(sf_bus)
    sf_add = pd.read_csv(data_dict['intermediate']['sf']['parcel'] + '/addresses.csv')
    sf_bus = merge_addresses(
        sf_bus, sf_add, fuzzy=True, nearest_n1=True, fuzzy_threshold=90, n1_threshold=5,
        add_merge_cols=['parsed_addr_n1', 'parsed_addr_sn', 'parsed_addr_ss'],
        bus_merge_cols=['primary_cleaned_addr_n1', 'primary_cleaned_addr_sn', 'primary_cleaned_addr_ss'],
        cols_to_merge=['lat', 'long', 'parcelID', 'parsed_city', 'parsed_addr_zip']
                              )
    sf_bus.to_csv(data_dict['intermediate']['sf']['business location'] + '/business_location_addresses_merged.csv', index=False)
    # seattle_bus = pd.read_csv(data_dict['intermediate']['seattle']['business location'] + '/business_location.csv')
    # seattle_bus = misc_seattle_cleaning(seattle_bus)
    # seattle_add = pd.read_csv(data_dict['intermediate']['seattle']['parcel'] + '/addresses.csv')
    # seattle_bus = merge_addresses(
    #     seattle_bus, seattle_add[seattle_add['parsed_city']=='seattle'], fuzzy=True, nearest_n1=True,
    #     fuzzy_threshold=90, n1_threshold=5,
    #     add_merge_cols=['parsed_addr_n1', 'parsed_addr_sn', 'parsed_addr_ss'],
    #     bus_merge_cols=['primary_cleaned_addr_n1', 'primary_cleaned_addr_sn', 'primary_cleaned_addr_ss'],
    #     cols_to_merge=['lat', 'long', 'parcelID', 'parsed_city', 'parsed_addr_zip']
    #                           )
    # seattle_bus.to_csv(data_dict['intermediate']['seattle']['business location'] + '/business_location_addresses_merged.csv', index=False)