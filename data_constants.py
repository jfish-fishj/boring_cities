import os

# path to github repository as path prefix
filePrefix = '/Users/joefish/Documents/GitHub/boring_cities'

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
    'boston',
    'nyc',
    'houston'
]
# set up directories
def make_data_dict(make_directories=True, use_seagate=False, filePrefix=filePrefix):
    data_dict = {}
    if use_seagate is not False:
        filePrefix = '/Volumes/Seagate Portable Drive/boring_cities'
    for data_stage in data_stages:
        data_dict[data_stage] = {}
        for city in cities:
            data_dict[data_stage][city] = {}
            for data_type in data_types:
                directory = filePrefix + f'/data/{data_stage}/{city}/{data_type}/'
                data_dict[data_stage][city][data_type] = directory
                if make_directories is True:
                    if os.path.exists(directory) is False:
                        os.mkdir(directory)
    return data_dict
