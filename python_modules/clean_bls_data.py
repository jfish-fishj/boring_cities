import pandas as pd
import os
from data_constants import bls_data_dict, misc_data_dict, make_data_dict
import re
import janitor

def process_raw_data():
    file_list = [bls_data_dict['raw zipcode'] + file for file in os.listdir(bls_data_dict['raw zipcode'])
                 if re.search("\.txt", file) ]

    def process_df(file_path):
        df = pd.read_csv(file_path).clean_names()
        year = re.search("([0-9]{2})", file_path).group(1)
        if int(year) < 80:
            year = int("20" + year)
        else:
            year = int("19" + year)
        if "sic" in df.columns:
            df = df.rename(columns = {'sic': "naics"})
        if "n<5" in df.columns:
            df = df.rename(columns = {"n<5": "n1_4"})
        df = df[df['naics'].str.contains("^[0-9]{2}-+")]

        print(f"year is {year}, num rows is {df.shape[0]}")
        df['year'] = year
        return df
    df = pd.concat([process_df(file) for file in file_list])
    df.to_csv(bls_data_dict['appended zipcode'] + "appended_zip.csv", index = False)

def filter_bls_data(df, address_df):
    # pad zipcode to 5 digits
    df['zip'] ="_" + df['zip'].astype(str).str.pad(5, side="left", fillchar = "0")
    # merge city from address data
    df = df.merge(address_df[['parsed_city', 'parsed_addr_zip']].drop_duplicates(), how = "inner", left_on = "zip",
                  right_on = "parsed_addr_zip")
    print(df['parsed_city'].value_counts())
    print(df.groupby(['parsed_city']).agg(**{"num_zip":('zip','count' ), "num_unique_zip": ('zip', 'nunique')}))
    return df

if __name__ == "__main__":
    # process_raw_data()
    data_dict = make_data_dict(use_seagate=True)
    city_dict = {
        "stl": "",
        "sf": "^san francisco$",
        "seattle": "^seattle$",
        "sd": "^san diego$",
        "chicago" : "^chicago$",
        "baton_rouge": "^baton rouge$",
        "la": "^los angeles$",
        'philly': ""
    }
    def filter_df(df, city):
        df = df[df['parsed_city'].fillna("").str.contains(city)]
        # print(city, df['parsed_city'].value_counts())
        return df




    add_df = pd.concat(
        [
            filter_df(
                pd.read_csv(data_dict['intermediate'][city]['parcel'] + '/addresses.csv',
                            usecols=['parsed_addr_zip', 'parsed_city']),
                city=city_dict[city]

            ) if os.path.exists(data_dict['intermediate'][city]['parcel'] + '/addresses.csv') else
            filter_df(
                pd.read_csv(data_dict['intermediate'][city]['business_location'] + '/business_location.csv',
                            usecols=['primary_cleaned_addr_zip' ,'primary_cleaned_city']).rename(columns={
                'primary_cleaned_addr_zip': "parsed_addr_zip", 'primary_cleaned_city': "parsed_city"
            }).assign(parsed_city = "philadelphia"),
                city=city_dict[city]

            )
            for city in city_dict.keys()

        ]

    )
    bls_df = pd.read_csv(bls_data_dict['appended zipcode'] + "appended_zip.csv")
    bls_df = filter_bls_data(df = bls_df, address_df = add_df)
    bls_df.to_csv(bls_data_dict['appended zipcode'] + "filtered_appended_zip.csv", index = False)