import pandas as pd
import os
from data_constants import bls_data_dict, misc_data_dict, make_data_dict, filePrefix
import re
import janitor
import numpy as np


# generic function for comparing business data to bls estimates
def make_qc_aggs(bls_df:pd.DataFrame, city:str, make_naics_aggs=False):
    data_dict = make_data_dict(use_seagate=True)
    bus_df = pd.read_csv(data_dict['final'][city]['business location'] + "business_locations.csv",
                         usecols=["year", "parsed_city", "parsed_addr_zip","is_business",
                                  "cleaned_business_name", "cleaned_dba_name", "primary_cleaned_fullAddress"]).\
        drop_duplicates(subset = ["cleaned_business_name", "cleaned_dba_name", "primary_cleaned_fullAddress", "year"])
    bus_df = bus_df.assign(index = np.arange(bus_df.shape[0]))
    bus_df = bus_df[bus_df['is_business'] != "person"]
    bls_df = bls_df[(bls_df['parsed_city'].isin(bus_df['parsed_city'])) &
                    (bls_df['year'].isin(bus_df['year']))]
    bls_city_agg = (
        bls_df.
            groupby(['parsed_city', 'year']).
            agg(**{"num_establishments": ('est', 'sum')}).
            reset_index()
    )

    bus_df_city_agg = (
        bus_df.
            groupby(['parsed_city', 'year']).
            agg(**{"num_establishments": ('index', 'count')}).
            reset_index()
    )
    city_agg = pd.merge(
        bls_city_agg, bus_df_city_agg, how="outer", suffixes=["_bls", "_business_loc"],
        on=["parsed_city", "year"]
                        )
    # repeat for zipcode
    bls_city_zip_agg = (
        bls_df.
            groupby(['parsed_city', 'zip', 'year']).
            agg(**{"num_establishments": ('est', 'sum')}).
            reset_index()
    )
    bus_df_city_zip_agg = (
        bus_df.
            groupby(['parsed_city',"parsed_addr_zip", 'year']).
            agg(**{"num_establishments": ('index', 'count')}).
            reset_index()
    )
    city_zip_agg = pd.merge(
        bls_city_zip_agg, bus_df_city_zip_agg, how="outer", suffixes=["_bls", "_business_loc"],
        left_on=["parsed_city", "year",'zip' ], right_on=["parsed_city", "year",'parsed_addr_zip']
    )
    if make_naics_aggs is not False:
        bls_city_naics_agg =(
            bls_df.
                groupby(['parsed_city',"naics", 'year']).
                agg(**{"num_establishments": ('est', 'sum')})
        )
        bus_df_city_naics_agg = (
            bus_df.
                groupby(['parsed_city','naics', 'year']).
                agg(**{"num_establishments": 'size'})
        )
        city_naics_agg = pd.merge(
            bls_city_naics_agg, bus_df_city_naics_agg, how="outer", suffixes=["_bls", "_business_loc"],
            on=["parsed_city", "year", "naics"]
                            )
        bls_city_zip_naics_agg = (
            bls_df.
                groupby(['parsed_city','zip' , "naics", 'year']).
                agg(**{"num_establishments": ('est', 'sum')})
        )
        bus_df_city_zip_naics_agg = (
            bus_df.
                groupby(['parsed_city',"parsed_addr_zip", 'naics', 'year']).
                agg(**{"num_establishments": 'size'})
        )
        city_zip_naics_agg = pd.merge(
            bls_city_zip_naics_agg, bus_df_city_zip_naics_agg, how="outer", suffixes=["_bls", "_business_loc"],
            left_on=["parsed_city", "year", "naics",'zip' ], right_on=["parsed_city", "year","naics",'parsed_addr_zip']
        )
        city_naics_agg.to_csv(filePrefix + f"/qc/bls_{city}_naics_agg.csv", index=False)
        city_zip_naics_agg.to_csv(filePrefix + f"/qc/bls_{city}_zip_naics_agg.csv", index=False)

    city_agg.to_csv(filePrefix + f"/qc/bls_{city}_agg.csv", index=False)

    city_zip_agg.to_csv(filePrefix + f"/qc/bls_{city}_zip_agg.csv", index=False)


if __name__ == "__main__":
    bls_df = pd.read_csv(bls_data_dict['appended zipcode'] + "filtered_appended_zip.csv")
    city_list = [
        'stl',
        'sf',
        'seattle',
        'sd',
        'chicago',
        'baton_rouge',
        'la',
        'philly'
    ]
    for city in city_list:
        print(city)
        make_qc_aggs(bls_df = bls_df, city = city)