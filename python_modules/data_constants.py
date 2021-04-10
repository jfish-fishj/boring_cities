"""
Data constants file:
This file specifies all the paths, column names, column types, and other constants that are used throughout this project
File is split into four groups:
    1. paths and path dicts. These are objects that specify the folder structure needed to run the code and what files
    such as the name parser files are needed beyond the scripts themselves
    2. Misc data constants. These are things that don't quite fit into one category but can be things like what the
    different stages are (raw, int, and final) or what cities I am using for analysis
    3. Data constants. These are objects that specify things about the actual data such as what columns are required,
    what crs should be set, what business types i create, etc.
    4. misc functions. Right now the only function is the make_data_dict which initializes the data dictionary for use
    in other scripts
"""

import os

linux = True

if linux is True:
    filePrefix = "/home/jfish/evictionlab-projects/boring_cities"
    dataPrefix = "/home/jfish/project_data/boring_cities"

else:
    # path to github repository as path prefix
    filePrefix = '/Users/joefish/Documents/GitHub/boring_cities'
    dataPrefix = "/Volumes/Seagate Portable Drive/boring_cities"

# paths to name parser files 
# non names = list of words that flag a name as not being a person i.e. llc, business, housing authority
# female/male first names are a list of names that appear in the census along w/ their frequency
# last names are equivalent for last names
name_parser_files = {
        'non names':(filePrefix + '/name_parser_files/non_names.txt'),
        'female first names':(filePrefix + '/name_parser_files/dist_female_first.txt'),
        'male first names':(filePrefix + '/name_parser_files/dist_male_first.txt'),
        'last names':(filePrefix + '/name_parser_files/dist_all_last.txt'),
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
    'zip county xwalk': dataPrefix + "/data/misc/zipcode_xwalk/ZIP_CBSA_032015.csv",
    "naics": dataPrefix + "/data/misc/naics/2-6 digit_2017_Codes.csv"
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
    "sac",
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

# address columns
# columns that make a full address
addr_combine_vars = ["parsed_addr_u", "parsed_addr_n1", "parsed_addr_sd", "parsed_addr_sn",
                     'parsed_addr_ss',"parsed_addr_zip", "parsed_city"]

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
'business_type_standardized',
'business_type_imputed',
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
'naics_descr_2',
'naics_descr_3',
'naics_descr2_standardized',
'naics_descr3_standardized',

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

# business types are taken from 2 digit naics codes
business_types_granular = ['Agriculture, Forestry, Fishing and Hunting',
                           'Mining, Quarrying, and Oil and Gas Extraction',
                           'Utilities',
                           'Construction',
                           'Wholesale Trade',
                           'Information',
                           'Finance and Insurance',
                           'Real Estate and Rental and Leasing',
                           'Professional, Scientific, and Technical Services',
                           'Management of Companies and Enterprises',
                           'Administrative and Support and Waste Management and Remediation Services',
                           'Educational Services',
                           'Health Care and Social Assistance',
                           'Arts, Entertainment, and Recreation',
                           'Accommodation and Food Services',
                           'Other Services (except Public Administration)',
                           'Public Administration']
business_types_coarse = [
    "retail",
    "food establishment",
    "service",
    "arts",
    "recreation",
    "parking lots",
    "transit",
    "real estate",
    "construction",
    "sole proprietorship",
    "professional",
    "industrial",
    "other",
    "unknown",
    "not a business"
]
business_types_wpc = [
    "work",
    "play",
    "community"
]

naics_two_digit_business_type_coarse_xwalk = {
    'Agriculture, Forestry, Fishing and Hunting': "recreation",
    'Mining, Quarrying, and Oil and Gas Extraction': "industrial",
    'Utilities': "industrial",
    'Construction': "construction",
    'Wholesale Trade': "retail",
    'Retail Trade':"retail",
    'Information': "professional",
    'Finance and Insurance': "professional",
    'Real Estate and Rental and Leasing': "real estate",
    'Professional, Scientific, and Technical Services': "professional",
    'Management of Companies and Enterprises': "professional",
    'Administrative and Support and Waste Management and Remediation Services': "professional",
    'Educational Services': "professional",
    'Health Care and Social Assistance': "professional",
    'Arts, Entertainment, and Recreation': "recreation",
    'Accommodation and Food Services': "food establishment", # this one is weird
    'Other Services (except Public Administration)': "professional",
    'Public Administration': "professional"
}
naics_three_digit_business_type_coarse_xwalk = {
    'Crop Production' : "industrial",
 'Animal Production and Aquaculture': "industrial",
 'Forestry and Logging': "industrial",
 'Fishing, Hunting and Trapping': "industrial",
 'Support Activities for Agriculture and Forestry': "industrial",
 'Oil and Gas Extraction': "industrial",
 'Mining (except Oil and Gas)': "industrial",
 'Support Activities for Mining': "industrial",
 'Utilities': "industrial",
 'Construction of Buildings': "construction",
 'Heavy and Civil Engineering Construction': "construction",
 'Specialty Trade Contractors': "professional",
 'Food Manufacturing': "industrial",
 'Beverage and Tobacco Product Manufacturing': "industrial",
 'Textile Mills': "industrial",
 'Textile Product Mills': "industrial",
 'Apparel Manufacturing': "industrial",
 'Leather and Allied Product Manufacturing': "industrial",
 'Wood Product Manufacturing': "industrial",
 'Paper Manufacturing': "industrial",
 'Printing and Related Support Activities': "industrial",
 'Petroleum and Coal Products Manufacturing': "industrial",
 'Chemical Manufacturing': "industrial",
 'Plastics and Rubber Products Manufacturing': "industrial",
 'Nonmetallic Mineral Product Manufacturing': "industrial",
 'Primary Metal Manufacturing': "industrial",
 'Fabricated Metal Product Manufacturing': "industrial",
 'Machinery Manufacturing': "industrial",
 'Computer and Electronic Product Manufacturing': "industrial",
 'Electrical Equipment, Appliance, and Component Manufacturing': "industrial",
 'Transportation Equipment Manufacturing': "industrial",
 'Furniture and Related Product Manufacturing': "industrial",
 'Miscellaneous Manufacturing': "industrial",
 'Merchant Wholesalers, Durable Goods': "industrial",
 'Merchant Wholesalers, Nondurable Goods': "industrial",
 'Wholesale Electronic Markets and Agents and Brokers': "professional",
 'Motor Vehicle and Parts Dealers': "industrial",
 'Furniture and Home Furnishings Stores': "retail",
 'Electronics and Appliance Stores': "retail",
 'Building Material and Garden Equipment and Supplies Dealers': "retail",
 'Food and Beverage Stores': "food establishment",
 'Health and Personal Care Stores': "retail",
 'Gasoline Stations': "retail",
 'Clothing and Clothing Accessories Stores': "retail",
 'Sporting Goods, Hobby, Musical Instrument, and Book Stores': "retail",
 'General Merchandise Stores': "retail",
 'Miscellaneous Store Retailers': "retail",
 'Nonstore Retailers': "retail",
 'Air Transportation':"transit",
 'Rail Transportation':"transit",
 'Water Transportation':"transit",
 'Truck Transportation':"transit",
 'Transit and Ground Passenger Transportation':"transit",
 'Pipeline Transportation':"transit",
 'Scenic and Sightseeing Transportation':"transit",
 'Support Activities for Transportation':"transit",
 'Postal Service':"transit",
 'Couriers and Messengers':"transit",
 'Warehousing and Storage': "industrial",
 'Publishing Industries (except Internet)': "professional",
 'Motion Picture and Sound Recording Industries': "professional",
 'Broadcasting (except Internet)': "professional",
 'Telecommunications': "professional",
 'Data Processing, Hosting, and Related Services': "professional",
 'Other Information Services': "professional",
 'Monetary Authorities-Central Bank': "professional",
 'Credit Intermediation and Related Activities': "professional",
 'Securities, Commodity Contracts, and Other Financial Investments and Related Activities': "professional",
 'Insurance Carriers and Related Activities': "professional",
 'Funds, Trusts, and Other Financial Vehicles': "professional",
 'Real Estate': "real estate",
 'Rental and Leasing Services': "real estate",
 'Lessors of Nonfinancial Intangible Assets (except Copyrighted Works)': "professional",
 'Professional, Scientific, and Technical Services': "professional",
 'Management of Companies and Enterprises': "professional",
 'Administrative and Support Services': "professional",
 'Waste Management and Remediation Services': "professional",
 'Educational Services': "professional",
 'Ambulatory Health Care Services': "professional",
 'Hospitals':"industrial",
 'Nursing and Residential Care Facilities': "professional",
 'Social Assistance': "professional",
 'Performing Arts, Spectator Sports, and Related Industries':"recreation",
 'Museums, Historical Sites, and Similar Institutions': "recreation",
 'Amusement, Gambling, and Recreation Industries':"recreation",
 'Accommodation':"retail",
 'Food Services and Drinking Places':"food establishment",
 'Repair and Maintenance': "professional",
 'Personal and Laundry Services':"service",
 'Religious, Grantmaking, Civic, Professional, and Similar Organizations':"other",
 'Private Households':"sole proprietorship",
 'Executive, Legislative, and Other General Government Support': "not a business",
 'Justice, Public Order, and Safety Activities': "not a business",
 'Administration of Human Resource Programs': "professional",
 'Administration of Environmental Quality Programs': "professional",
 'Administration of Housing Programs, Urban Planning, and Community Development': "professional",
 'Administration of Economic Programs': "professional",
 'Space Research and Technology': "professional",
 'National Security and International Affairs': "professional"

}

default_crs = "EPSG:4326"  # WGS84 good for lat long coordinates in North America


# set up directories
def make_data_dict(make_directories=True, use_seagate=False, filePrefix=dataPrefix):
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
    data_dict = make_data_dict(use_seagate=False, make_directories=True)