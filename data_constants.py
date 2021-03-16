import os

# path to github repository as path prefix
filePrefix = '/Users/joefish/Documents/GitHub/boring_cities'
dataPrefix = "/Volumes/Seagate Portable Drive/boring_cities"

# address columns
# columns that make a full address
addr_combine_vars = ["parsed_addr_u", "parsed_addr_n1", "parsed_addr_sd", "parsed_addr_sn",
                     'parsed_addr_ss',"parsed_addr_zip", "parsed_city"]

# paths to name parser files 
# non names = list of words that flag a name as not being a person i.e. llc, business, housing authority
# female/male first names are a list of names that appear in the census along w/ their frequency
# last names are equivalent for last names
name_parser_files = {
        'non names':(filePrefix + '/name_parser/non_names.txt'), 
        'female first names':(filePrefix + '/name_parser/dist_female_first.txt'),
        'male first names':(filePrefix + '/name_parser/dist_male_first.txt'),
        'last names':(filePrefix + '/name_parser/dist_all_last.txt'),
    }

classifier_files = {
    'sample images': '/Volumes/Seagate Portable Drive/boring_cities/data/raw/sf/phonebook/sanfranciscosanf1982rlpo_95_206/',
    'phone books': '/Volumes/Seagate Portable Drive/boring_cities/data/raw/sf/phonebook/phonebooks/',
    'training data':'/Volumes/Seagate Portable Drive/boring_cities/data/raw/sf/phonebook/training_data/training_data.csv',
    'random forest': '/Users/joefish/Documents/GitHub/boring_cities/classifier/random_forest.joblib'
}

census_data_dict = {
    'census tract data': dataPrefix  + "/data/census/tract_data/tract_demos_11172020.csv",
    'ca bg shp': dataPrefix + "/data/census/shapefiles/tl_2010_06_bg10/tl_2010_06_bg10.shp",
    'il bg shp':dataPrefix + "/data/census/shapefiles/tl_2010_17_bg10/tl_2010_17_bg10.shp",
    'la bg shp': dataPrefix + "/data/census/shapefiles/tl_2010_22_bg10/tl_2010_22_bg10.shp",
    'mo bg shp': dataPrefix + "/data/census/shapefiles/tl_2010_29_bg10/tl_2010_29_bg10.shp",

    'pa bg shp':dataPrefix + "/data/census/shapefiles/tl_2010_42_bg10/tl_2010_42_bg10.shp"
}

bls_data_dict = {
    'raw zipcode': dataPrefix + "/data/bls/raw_zipcode/county_business_data/",
    'appended zipcode': dataPrefix + "/data/bls/appended_zipcode/"
}

misc_data_dict = {
    'zip count xwalk': dataPrefix + "/data/misc/zipcode_xwalk/ZIP_CBSA_032015.csv"
}

data_stages = [
    'raw',
    'intermediate',
    'final'
]
data_types = [
    'business location',
    'parcel',
    'permit',
    'phonebook'
]
cities = [
    'stl',
    'la',
    'sf',
    'sd',
    'chicago',
    'seattle',
    'abq',
    'philly',
    'ks',
    'tuscon',
    'hartford',
    'baton_rouge',
    'orlando'
]

address_cols = [
    'parcelID',
    'lat',
    'long',
    'address_fa',
    'address_n1',
    'address_n2',
    'address_u',
    'address_sn',
    'address_ss',
    'address_sd',
    'address_zip',
    'address_city'
]

business_cols = [
     "business_id",
     "business_name",
     "dba_name",
     "business_end_date",
     "business_end_year",
     "business_start_date",
     "business_start_year",
     "business_type",
     "cleaned_business_name",
     "cleaned_dba_name",
     "cleaned_mail_address_fa",
     "cleaned_ownership_name",
     "cleaned_primary_address_fa",
     "is_business",
     "lat",
     "long",
     "location_end_date",
     "location_end_year",
     "location_id",
     "location_start_date",
     "location_start_year",
     "locationID",
     "mail_address_city",
     "mail_address_country",
     "mail_address_fa",
     "mail_address_fa1",
     "mail_address_fa2",
     "mail_address_n",
     "mail_address_n2",
     "mail_address_sd",
     "mail_address_sn",
     "mail_address_ss",
     "mail_address_state",
     "mail_address_u",
     "mail_address_zip",
     "mail_cleaned_addr_n1",
     "mail_cleaned_addr_n2",
     "mail_cleaned_addr_name",
     "mail_cleaned_addr_sd",
     "mail_cleaned_addr_sn",
     "mail_cleaned_addr_ss",
     "mail_cleaned_addr_u",
     "mail_cleaned_addr_zip",
     "mail_cleaned_city",
     "mail_cleaned_fullAddress",
     "mail_cleaned_mail_address_n",
     "mail_cleaned_state",
     "naics",
     "naics_descr",
     "ownership_name",
     "ownership_type",
     "parcelID",
     "primary_address_city",
     "primary_address_city_state_zip",
     "primary_address_country",
     "primary_address_fa",
     "primary_address_fa1",
     "primary_address_fa2",
     "primary_address_n1",
     "primary_address_n2",
     "primary_address_sd",
     "primary_address_sn",
     "primary_address_ss",
     "primary_address_state",
     "primary_address_u",
     "primary_address_zip",
     "primary_cleaned_addr_n1",
     "primary_cleaned_addr_n2",
     "primary_cleaned_addr_name",
     "primary_cleaned_addr_sd",
     "primary_cleaned_addr_sn",
     "primary_cleaned_addr_ss",
     "primary_cleaned_addr_u",
     "primary_cleaned_addr_zip",
     "primary_cleaned_city",
     "primary_cleaned_fullAddress",
     "primary_cleaned_state"
]

default_crs = "EPSG:4326"  # WGS84 good for lat long coordinates in North America

# set up directories
def make_data_dict(make_directories=True, use_seagate=False, filePrefix=filePrefix):
    data_dict = {}
    if use_seagate is not False:
        filePrefix = '/Volumes/Seagate Portable Drive/boring_cities'
    for data_stage in data_stages:
        data_dict[data_stage] = {}
        if make_directories is True:
            mkdir = filePrefix + f'/data/{data_stage}'
            if os.path.exists(mkdir) is False:
                os.mkdir(mkdir)
        for city in cities:
            data_dict[data_stage][city] = {}
            mkdir = filePrefix + f'/data/{data_stage}/{city}'
            if os.path.exists(mkdir) is False:
                os.mkdir(mkdir)
            for data_type in data_types:
                directory = filePrefix + f'/data/{data_stage}/{city}/{data_type}/'
                data_dict[data_stage][city][data_type] = directory
                if make_directories is True:
                    if os.path.exists(directory) is False:
                        os.mkdir(directory)
    return data_dict

if __name__ == "__main__":
    data_dict = make_data_dict(use_seagate=True)