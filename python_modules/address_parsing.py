"""
Descirption: This file contains a set of functions that are designed to use re to parse addresses before sending
them to the usadress file.
Author: Joe Fish
Last Updated: 6/27
"""
import re
import pandas as pd
import numpy as np
from data_constants import filePrefix
from name_parsing import combine_names
from pandas.api.types import is_string_dtype
import time
from helper_functions import write_to_log

# get rid of setting with copy warnings
pd.set_option('mode.chained_assignment', None)


# function for cleaning address columns (remove commas, change unit -> #, get rid of weird characters
def clean_unit_vectorized(column:pd.Series) -> pd.Series:
    """Takes a pandas dataframe and performs any vectorizable string cleaning operations. Does not
    convert floor numbers to numbers like clean_unit
    """
    """Takes a dataframe and a column and cleans the strings including removing periods, apostrophies, weird spaces"""
    if column.isnull().all() == True:
        return column
    if pd.api.types.is_string_dtype(column) == False:
        write_to_log("column is not of type object/string")
        return column

    column = column.str.lower()

    column = column.str.replace(r'\.|!|@|\$|~|\(|\)|\\|\||\*|/|"|`', "", regex=True)

    column = column.str.replace(r"'", "", regex=True)

    column = column.str.strip()

    column = column.str.replace(r'^(.+)([&,]\s?)$', r'\g<1>', regex=True)

    column = column.str.replace(r'\s{2,}', " ", regex=True,flags=re.IGNORECASE)

    column = column.str.replace(r"([0-9]+)(\s-\s|\s-|-\s)([0-9]+)",r"\g<1>-\g<3>", regex=True,flags=re.IGNORECASE)

    column = column.str.replace(
        r'([\s^-])(rm\s|space\s|room\s|units?\b|suite\b|apt\b|un\s|ste\b|number\b|no\b)', r'\g<1>#', regex=True)

    column = column.str.replace(
        r'\s(rm|room|unit|suite|apt|un|ste|number\b|no\b)(\s)([0-9a-zA-Z]+)', r'#\g<3>', regex=True)

    # column = column.str.replace(
    #     r'(rm|space|room|unit|suite|apt|un|ste|no)(\s)([0-9a-zA-Z]+)', r'#\g<3>', regex=True)

    column = column.str.replace(r'##', r'#', regex=True)
    column = column.str.replace(r'#\s', '#', regex=True, flags=re.IGNORECASE)

    column = column.str.replace(
        r'([^#])([0-9]{1,})([abcefgijklmopquvwxyz]{2,})',r'\g<1>\g<2> \g<3>', regex=True,flags=re.IGNORECASE)

    column = column.str.replace(r'([a-z])(#)',r'\g<1> \g<2>', regex=True,flags=re.IGNORECASE)

    column = column.str.replace(r'(no\s?)([0-9-]+)',r'#\g<2>', regex=True,flags=re.IGNORECASE)

    column = column.str.replace(r'([^ ])(,|&)([ ])', r'\g<1>\g<2> \g<3>',regex=True, flags=re.IGNORECASE)

    column = column.str.replace(r'([^ ])(,|&)([^\s])', r'\g<1>\g<2> \g<3>',regex=True, flags=re.IGNORECASE)

    column = column.str.replace(r'([a-z])(,)(a-z])', r'\g<1>\g<2> \g<3>', regex=True, flags=re.IGNORECASE)

    column = column.str.replace(r'([a-z])(&)(a-z])', r'\g<1> \g<2> \g<3>',regex=True, flags=re.IGNORECASE)

    column = column.str.replace(r'(&)(a-z])', r'\g<1> \g<2>',regex=True, flags=re.IGNORECASE)

    column = column.str.replace(r'([a-z])(&)', r'\g<1> \g<2>',regex=True, flags=re.IGNORECASE)

    column = column.str.replace(r'([^#])([a-z]{1,})([0-9]{2,})',r'\g<1>\g<2> \g<3>', regex=True,flags=re.IGNORECASE)

    column = column.str.replace(r'([a-z]{2,})([#])', r'\g<1> \g<2>', regex=True, flags=re.IGNORECASE)

    column = column.str.replace(r'\(.+?\)', "", regex=True, flags=re.IGNORECASE)

    column = column.str.replace(r'##', r"#", regex=True, flags=re.IGNORECASE)
    column = column.str.replace(r'(.+)[\s,\s]{1,3}baltimore,?\s?(md|maryland)?$', r"\g<1>", regex=True,
                                flags=re.IGNORECASE)

    # delete parenthesis
    column = column.str.replace(r'\(.+?\)', r"", regex=True)
    column = column.str.replace(r'\s{2,}', " ", regex=True, flags=re.IGNORECASE)
    column = column.str.replace(r"([0-9]+)(\s-\s|\s-|-\s)([0-9]+)",r"\g<1>-\g<3>", regex=True,flags=re.IGNORECASE)
    column = column.str.replace(
        r'\s(rm|room|space|unit|suite|apt|un|ste|number\b)(\s)([0-9a-zA-Z]+)', r'#\g<3>', regex=True)
    column = column.str.replace(r'##', r"#", regex=True, flags=re.IGNORECASE)

    column = column.str.replace(r'#\s', '#', regex=True, flags=re.IGNORECASE)

    column = column.str.replace(r'(#)([a-z]{1,2})(\s)([0-9]+)',r"\g<1>\g<2>\g<4>", regex=True, flags=re.IGNORECASE)

    column = column.str.replace(r'(\s$|^\s)', '', regex=True,flags=re.IGNORECASE)

    column = column.str.replace(r'(p\s?o)\s?(box)', 'po box', regex=True, flags=re.IGNORECASE)
    column = column.str.replace(r'(p\.?o\.?)\s?(box)', 'po box', regex=True,flags=re.IGNORECASE)
    column = column.replace(r'', np.nan, regex=True )
    column = column.replace(r'nan', np.nan, regex=False)

    column = column.str.replace(r'(.+)[\s,\s]{1,3}baltimore,?\s?(md|maryland)?$', r"\g<1>",regex=True,
                                flags=re.IGNORECASE)
    column = column.str.replace(r'(\s(1st|2nd|3rd)\sfloor)$', r"",regex=True, flags=re.IGNORECASE)

    column = column.str.strip()
    return column


# function for standardizing strings in address columns i.e. street - > st and avenue - > ave
def string_standardize_column_vectorized(column:pd.Series,  log=False) -> pd.Series:
    """Standardizes strings to be in accordance with US Postal Standards."""
    # create column with prefix and old column name
    if column.isna().all() == True:
        if log is not False:
            write_to_log('{} is completely NA... Not attempting to clean'.format(column))
        return column
    if is_string_dtype(column) == False:
        return column
    replacement_dict = {
        'apt': ['apa?rtme?nt', 'apts'],
        'aly': ['allee', 'alle?y'],
        'ave': ['av', 'ave?nu?e?'],
        'blvd': ['boulevard', 'boulv?'],
        'brg': ['br', 'bri?dge?'],
        'canyn': ['canyon', 'cnyn'],
        "condo": ["CONDOMINIUM|CO?NDO?MI?NI?U?MS?|CONDOS|CONDOS|COND"],
        'ctr': ['cent?', 'center', 'ce?ntre?'],
        'cir': ['ci?rcl?e?'],
        'ct': ['co?u?rt'],
        'dr': ['dri?ve?'],
        'e': ['east'],
        'est': ['estate'],
        'expy': ['expr?e?s?s?', 'expressway', 'expw'],
        'ext': ['exte?nsi?o?n'],
        'ft': ['fo?rt'],
        'fwy': ['fre?e?wa?y'],
        'gdn': ['ga?rde?n'],
        'hbr': ['harb', 'ha?rbo?r'],
        'hts': ['ht', 'heights', 'hgts?'],
        'hwy': ['highway', 'highwy', 'hiway', 'hiwy', 'hway', 'hw'],
        'jct': ['ju?ncti?o?n'],
        'ln': ['lane' ],
        'lp': ['lo?o?p'],
        'mt': ['mntain', 'mntn', 'mountain', 'mountin', 'mtin', 'mtn', 'mount'],
        'n': ['north', 'no'],  # no can unfortunately be number or north depending
        'rd': ['road'],
        'pk': ['parks?'],
        'pl': ['place'],
        'plz': ['plaza'],
        'pkwy': ['parkway'],
        'rdge': ['ridge'],
        'riv': ['rive?r'],
        'rte': ['route'],
        'rw': ['row?'],
        'sq': ['squ?a?re?'],
        'ste': ['suite'],
        'st': ['street', 'str', 'saint'],
        's': ['south', 'so'],
        'ter': ['terr', 'terrace', 'ter$', 'te'],
        'trl': ['trail', 'trl'],
        'w': ['west'],
        'way': ['wy'],
        '1': ['one'],
        '2': ['two'],
        '100': ['hundred'],
        '1st': ['first', '01st'],
        '2nd': ['second', '02nd'],
        '3rd': ['third', '03rd'],
        '4th': ['fourth', '04th'],
        '5th': ['fifth', '05th'],
        '6th': ['sixth', '06th'],
        '7th': ['seventh', '07th'],
        '8th': ['eighth', '08th'],
        '9th': ['ninth', '09th'],
        '10th': ['tenth'],
        'AL': ['Alabama'],
        'AK': ['Alaska'],
        'AS': ['American Samoa'],
        'AZ': ['Arizona'],
        'AR': ['Arkansas'],
        'CA': ['California'],
        'CO': ['Colorado'],
        'CT': ['Connecticut'],
        'DE': ['Delaware'],
        'DC': ['District of Columbia'],
        'FL': ['Florida'],
        'GA': ['Georgia'],
        'GU': ['Guam'],
        'HI': ['Hawaii'],
        'ID': ['Idaho'],
        'IL': ['Illinois'],
        'IN': ['Indiana'],
        'IA': ['Iowa'],
        'KS': ['Kansas'],
        'KY': ['Kentucky'],
        'LA': ['Louisiana'],
        'ME': ['Maine'],
        'MD': ['Maryland'],
        'MA': ['Massachusetts'],
        'MI': ['Michigan'],
        'MN': ['Minnesota'],
        'MS': ['Mississippi'],
        'MO': ['Missouri'],
        'MT': ['Montana'],
        'NE': ['Nebraska'],
        'NV': ['Nevada'],
        'NH': ['New Hampshire'],
        'NJ': ['New Jersey'],
        'NM': ['New Mexico'],
        'NY': ['New York'],
        'NC': ['North Carolina'],
        'ND': ['North Dakota'],
        'MP': ['Northern Mariana Islands'],
        'OH': ['Ohio'],
        'OK': ['Oklahoma'],
        'OR': ['Oregon'],
        'PA': ['Pennsylvania'],
        'PR': ['Puerto Rico'],
        'RI': ['Rhode Island'],
        'SC': ['South Carolina'],
        'SD': ['South Dakota'],
        'TN': ['Tennessee'],
        'TX': ['Texas'],
        'UT': ['Utah'],
        'VT': ['Vermont'],
        'VI': ['Virgin Islands'],
        'VA': ['Virginia'],
        'WA': ['Washington'],
        'WV': ['West Virginia'],
        'WI': ['Wisconsin'],
        'WY': ['Wyoming'],
        # boston specific
        'comm': ['co?mm[onwealth]{1,7}'],
        'center': ['centre'],
        'whaler': ['whalers'],
        'lake shore': ['lakehore'],
        # baltimore specific
        'cold spring': ['coldspring']
    }  # update as needed
    # loop through dict and replace values with key
    for item in replacement_dict:
        value_string = ""
        for value in sorted(replacement_dict[item]):
            value_string += value + "|"
        re_string = r'(\b)(%s)(\b)' % value_string[:-1]
        re_replace = r'\g<1>%s\g<3>' % item
        column = column.str.replace(re_string, re_replace, flags=re.IGNORECASE, regex=True)
    # replace double spaces w/ single onesx
    column = column.str.replace(r' {2,}', r' ', flags=re.IGNORECASE, regex=True)
    return column

# wrapper function that calls clean unit vectorized and string standardize
def clean_address_col(address_col):
    address_col = clean_unit_vectorized(address_col)
    address_col = string_standardize_column_vectorized(address_col)
    return address_col


# wrapper function for parsing an address
def clean_parse_address(dataframe, address_col, unit, st_num, st_name, st_sfx, st_d,
                        zipcode, city, country, state, st_num2, prefix1='cleaned_', prefix2='parsed_',
                        legal_description=False, raise_error_on_na=True):
    for cols in [unit, st_num, st_name, st_sfx, st_d, zipcode, city, country, address_col]:
        if cols not in dataframe.columns:
            print('{} not in address df'.format(cols))
            dataframe[cols] = np.nan
    # standardize and clean full address
    dataframe[prefix1 + address_col] = clean_address_col(dataframe[address_col])

    if legal_description is not False:
        dataframe[prefix1 + address_col] = clean_address_col(dataframe[legal_description])
        dataframe = parse_address(dataframe=dataframe, address_col=prefix1 + address_col, unit_col=unit,
                                  st_num_col=st_num,
                                  st_name_col=st_name, st_sfx_col=st_sfx, st_d_col=st_d, zipcode_col=zipcode,
                                  city_col=city,
                                  prefix=prefix2, legal_description_col=prefix1 + legal_description, state_col=state,
                                  st_num2_col=st_num2,
                                  raise_error_on_na=raise_error_on_na
                                  )
    else:
        dataframe = parse_address(dataframe=dataframe, address_col=prefix1 + address_col, unit_col=unit,
                                  st_num_col=st_num,
                                  st_name_col=st_name, st_sfx_col=st_sfx, st_d_col=st_d, zipcode_col=zipcode,
                                  city_col=city,
                                  prefix=prefix2, legal_description_col=legal_description, state_col=state,
                                  st_num2_col=st_num2,
                                  raise_error_on_na=raise_error_on_na
                                  )
    return dataframe



def parse_address(dataframe, address_col, unit_col, st_num_col, st_name_col, st_sfx_col, st_d_col,
                  zipcode_col, city_col, state_col, st_num2_col,
                  prefix='parsed_', legal_description_col=False, raise_error_on_na=True):

    col_list = [address_col, unit_col, st_num_col, st_name_col, st_num2_col,
                st_sfx_col, st_d_col, zipcode_col, city_col, state_col]
    cols_to_check = [address_col, st_name_col]
    if legal_description_col is not False:
        col_list.append(legal_description_col)
        cols_to_check.append(legal_description_col)
    for cols in col_list:
        if cols not in dataframe.columns:
            dataframe[cols] = np.nan
    if all([dataframe[col].isna().all() for col in cols_to_check]):
        if raise_error_on_na == True:
            raise ValueError('All parsable address components are completely NA!')
        else:
            write_to_log('All parsable address components are completely NA! Returning dataframe', warn=True)
            return dataframe
    parsed_unit_col = prefix + 'addr_u'
    parsed_st_num1_col = prefix + 'addr_n1'
    parsed_st_num2_col = prefix + 'addr_n2'
    parsed_st_name_col = prefix + 'addr_sn'
    parsed_ss_col = prefix + 'addr_ss'
    parsed_sd_col = prefix + 'addr_sd'
    parsed_zip_col = prefix + 'addr_zip'
    parsed_address_name = prefix + 'addr_name'
    parsed_city_col = prefix + 'city'
    parsed_state_col = prefix + 'state'
    # replace empty strings w/ np.nan
    for col in col_list:
        if (is_string_dtype(dataframe[col])) and (dataframe[col].isna().all() == False):
            try:
                dataframe[col] = dataframe[col].replace(r'', np.nan, regex=True,)
                dataframe[col] = dataframe[col].replace(r'nan', np.nan, regex=False)
            except AttributeError as error:
                print('{} is numeric, not attempting to remove whitespace'.format(col))
    dataframe_full = dataframe.copy(deep=True)
    merge_cols = [address_col, st_name_col, unit_col, st_num_col, st_sfx_col, st_d_col,
                  zipcode_col, city_col, state_col, st_num2_col]
    initial_shape = dataframe_full.shape[0]
    if legal_description_col is not False:
        dataframe = dataframe[[address_col, unit_col, st_num_col, st_name_col, st_sfx_col,
                               st_d_col, zipcode_col, city_col, legal_description_col, state_col, st_num2_col]]
        dataframe.drop_duplicates(
            subset=merge_cols + [legal_description_col], inplace=True)
    else:
        dataframe = dataframe[
            [address_col, unit_col, st_num_col, st_name_col, st_sfx_col, st_d_col, zipcode_col, city_col, state_col,
             st_num2_col]]
        dataframe.drop_duplicates(
            subset=merge_cols, inplace=True)
    for col in [parsed_zip_col, parsed_sd_col, parsed_unit_col, parsed_address_name, parsed_st_name_col,
                parsed_st_num2_col,
                parsed_st_num1_col, parsed_ss_col]:
        dataframe[col] = np.nan
    final_shape = dataframe_full.shape[0]
    if initial_shape != final_shape:
        write_to_log('initial shape is {} and final shape is {}'.format(initial_shape, final_shape))
        raise ValueError('Initial and final shapes do not agree! Copy was not deep enough')
    og_types = (dataframe_full[[address_col, st_name_col, st_num_col, st_sfx_col, st_d_col]].dtypes.apply(
        lambda x: x.name).to_dict())
    # create new address col
    new_address_col = 'new_' + address_col
    # regex definitions
    st_num = r'([0-9]+-?[a-z]?)'
    st_d = r'([nsewrl]{1,2}\s|rear\s|side\s)'
    st_name = r'(\w{3,}|\w{3,}\s\w{3,}|[a-z\s-]{4,}|[a-z]+|[0-9]+)'
    st_sfx = r'(aly|ave|byu|blf|blvd|bd|br|cswy|ctr|cir|ct|cr|cv|crk|cres|xing|curv|dr|est|expy|ext|frk|ft|fwy|gdn|gtwy|hvn|' \
             r'hwy|hl|jct|ky|lks|ln|lgt|lp|mall|mnr|mdw|msn|mtn|pkwy|pass|path|pl|plz|pt|prt|pw|rst|rdge|rd|rte|rw|shr|' \
             r'sq|st|strm|trak|trl|turnpike|ter|tl|vly|vws|walk|way)'
    zipcode = r'([0-9]{5}|[0-9]{5}-[0-9]{4})'
    unit = r'#\s?([a-z0-9-]+|[0-9-]+-?[a-z]?|[a-z]-?[0-9]+|[up][0-9]?[abcd]|un\s[0-9abcd]|[a-z])'
    unit2 = r'\s([a-z]+-?[0-9]{1,4}|[uptcl][-\s]?[tl]?[0-9]{1,4}[abcd]?|un\s[0-9abcd]{1,4})'  # use this for when you don't need to know that there's a # before
    state = 'AL|AK|AS|AZ|AR|CA|CO|CT|DE|DC|FL|GA|GU|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|MP|OH|OK|OR|PA|PR|RI|SC|SD|TN|TX|UT|VT|VI|VA|WA|WV|WI|WY'
    state_dict = {
        'AL': 'Alabama',
        'AK': 'Alaska',
        'AS': 'American Samoa',
        'AZ': 'Arizona',
        'AR': 'Arkansas',
        'CA': 'California',
        'CO': 'Colorado',
        'CT': 'Connecticut',
        'DE': 'Delaware',
        'DC': 'District of Columbia',
        'FL': 'Florida',
        'GA': 'Georgia',
        'GU': 'Guam',
        'HI': 'Hawaii',
        'ID': 'Idaho',
        'IL': 'Illinois',
        'IN': 'Indiana',
        'IA': 'Iowa',
        'KS': 'Kansas',
        'KY': 'Kentucky',
        'LA': 'Louisiana',
        'ME': 'Maine',
        'MD': 'Maryland',
        'MA': 'Massachusetts',
        'MI': 'Michigan',
        'MN': 'Minnesota',
        'MS': 'Mississippi',
        'MO': 'Missouri',
        'MT': 'Montana',
        'NE': 'Nebraska',
        'NV': 'Nevada',
        'NH': 'New Hampshire',
        'NJ': 'New Jersey',
        'NM': 'New Mexico',
        'NY': 'New York',
        'NC': 'North Carolina',
        'ND': 'North Dakota',
        'MP': 'Northern Mariana Islands',
        'OH': 'Ohio',
        'OK': 'Oklahoma',
        'OR': 'Oregon',
        'PA': 'Pennsylvania',
        'PR': 'Puerto Rico',
        'RI': 'Rhode Island',
        'SC': 'South Carolina',
        'SD': 'South Dakota',
        'TN': 'Tennessee',
        'TX': 'Texas',
        'UT': 'Utah',
        'VT': 'Vermont',
        'VI': 'Virgin Islands',
        'VA': 'Virginia',
        'WA': 'Washington',
        'WV': 'West Virginia',
        'WI': 'Wisconsin',
        'WY': 'Wyoming'}
    # filter rows that have already been parsed
    # create df_list to hold parsed names
    df_list = []
    # fill in where already parsed
    dataframe[parsed_unit_col] = dataframe[unit_col]
    dataframe[parsed_ss_col] = dataframe[st_sfx_col]
    dataframe[parsed_st_name_col] = dataframe[st_name_col]
    dataframe[parsed_sd_col] = dataframe[st_d_col]
    dataframe[parsed_city_col] = dataframe[city_col]
    dataframe[parsed_state_col] = dataframe[state_col]
    dataframe[parsed_st_num2_col] = np.where(
        dataframe[st_num2_col].isna(),
        dataframe[st_num2_col],
        dataframe[st_num2_col].astype(str)
    )
    # do additional parsing from parsed columns
    # (i.e st suffix from st name, st num 1 & 2 from st_num) standardize strings
    if is_string_dtype(dataframe[st_num_col]):
        dataframe['TEMP'] = np.where(
            dataframe[st_num_col].isna(),
            dataframe[st_num_col],
            dataframe[st_num_col].astype(str)
        )
        split1 = dataframe['TEMP'].str.extract(st_num + '\s?[-&]?\s?' + st_num + '?', flags=re.IGNORECASE)
        dataframe[parsed_st_num1_col].fillna(split1[0], inplace=True)
        dataframe[parsed_st_num2_col].fillna(split1[1], inplace=True)
        dataframe.drop(columns=['TEMP'], inplace=True)
        # dataframe[parsed_st_num2_col] = np.nan

    dataframe[parsed_st_num1_col] = np.where(dataframe[st_num_col].notnull(),
                                             dataframe[st_num_col].astype(str),
                                             dataframe[parsed_st_num1_col]
                                             )
    # clean columns
    for col in [parsed_ss_col, parsed_st_name_col, parsed_sd_col, parsed_city_col]:
        try:
            if any(dataframe[col] == 'nan'):
                print('super not glamorous')
                raise ValueError
            # print('cleaning {}'.format(col))
            dataframe[col] = clean_address_col(dataframe[col])
            if any(dataframe[col] == 'nan'):
                print('not glamorous {}'.format(col))
                raise ValueError
        except AttributeError:
            print(f'{col} cant be cleaned')
    # see if you can parse suffix from st_name col (happens if people put full st address in parsed column)
    if is_string_dtype(dataframe[parsed_st_name_col]):
        split2 = dataframe[parsed_st_name_col].str.extract(st_name + '\s' + st_sfx + "$", flags=re.IGNORECASE)
        dataframe[parsed_st_name_col] = dataframe[parsed_st_name_col].fillna(split2[0])
        dataframe[parsed_ss_col] = dataframe[parsed_ss_col].fillna(split2[1])
        dataframe[parsed_st_name_col] = dataframe[parsed_st_name_col].str.replace(st_name + '\s' + st_sfx + "$", r'\g<1>',
                                                                                  flags=re.IGNORECASE, regex=True)
        dataframe[parsed_st_name_col] = dataframe[parsed_st_name_col].str.replace('\s{2,}', ' ', flags=re.IGNORECASE,
                                                                                  regex=True)
        dataframe[parsed_st_name_col] = dataframe[parsed_st_name_col].str.strip()
    # see if you can parse directional from st_suffix column
    if is_string_dtype(dataframe[parsed_st_name_col]):
        split2 = dataframe[parsed_st_name_col].str.extract("(\s|^)([nsew])(\s|$)")
        dataframe[parsed_sd_col] = dataframe[parsed_sd_col].fillna(split2[2])
        dataframe[parsed_st_name_col] = np.where(
            ~(dataframe[parsed_st_name_col].str.contains("^[nsew]$", na=True)),
            dataframe[parsed_st_name_col].str.replace("(\s|^)([nsew])(\s|$)", r"\g<3>", regex=True),
            dataframe[parsed_st_name_col]
        )
        dataframe[parsed_st_name_col] = dataframe[parsed_st_name_col].str.replace('\s{2,}', ' ', flags=re.IGNORECASE, regex=True)
        dataframe[parsed_st_name_col] = dataframe[parsed_st_name_col].str.strip()
    # clean zipcode column
    dataframe[parsed_zip_col] = dataframe[zipcode_col]
    if dataframe[parsed_zip_col].isna().all() == False:
        dataframe[parsed_zip_col] = np.where(dataframe[parsed_zip_col].isna(),
                                             dataframe[parsed_zip_col],
                                             dataframe[parsed_zip_col].astype(str)
                                             )
        dataframe[parsed_zip_col] = dataframe[parsed_zip_col].str.replace(r'(.+)(\.[0-9])', r'\g<1>', regex=True)
        dataframe[parsed_zip_col] = dataframe[parsed_zip_col].str.strip()
        dataframe[parsed_zip_col] = np.where(dataframe[parsed_zip_col].str.contains('^[0-9]{6}$'),
                                             dataframe[parsed_zip_col].str[0:5],
                                             dataframe[parsed_zip_col])
        dataframe[parsed_zip_col] = dataframe[parsed_zip_col].str.strip()
        dataframe[parsed_zip_col] = np.where(dataframe[parsed_zip_col].str.contains('^[0-9]{4}$'),
                                             '0' + dataframe[parsed_zip_col],
                                             dataframe[parsed_zip_col])

        dataframe[parsed_zip_col] = dataframe[parsed_zip_col].str.replace(r'([0-9]{5})-([0-9]+)?', r'\g<1>', regex=True)
        dataframe[parsed_zip_col] = '_' + dataframe[parsed_zip_col]
        dataframe[parsed_zip_col] = dataframe[parsed_zip_col].str.replace(r'^(_)\s?([0-9]{4})$', r'\g<1>0\g<2>', regex=True)

    # extract unit_col from address column
    dataframe[parsed_unit_col] = np.where(
        dataframe[parsed_unit_col] == '',
        np.nan,
        dataframe[parsed_unit_col]
    )
    # dataframe['fillUnit'] = dataframe[address_col].str.extract(unit,flags=re.IGNORECASE).iloc[:,0]
    # try a bunch of different units
    if is_string_dtype(dataframe[address_col]):
        dataframe[parsed_unit_col] = dataframe[parsed_unit_col].fillna(
            dataframe[address_col].str.extract(unit,
                                               flags=re.IGNORECASE).iloc[:, 0])
        dataframe[parsed_unit_col] = dataframe[parsed_unit_col].fillna(
            dataframe[address_col].str.extract(unit2,
                                               flags=re.IGNORECASE).iloc[:, 0])
        dataframe[parsed_unit_col] = dataframe[parsed_unit_col].fillna(
            dataframe[address_col].str.extract(r'\su[-:\s]{1,3}([0-9]+[a-z]+)',
                                               flags=re.IGNORECASE).iloc[:, 0])
        dataframe[parsed_unit_col] = dataframe[parsed_unit_col].fillna(
            dataframe[address_col].str.extract(r'\su[-:\s]{1,3}([0-9]+)',
                                               flags=re.IGNORECASE).iloc[:, 0])
        dataframe[parsed_unit_col] = dataframe[parsed_unit_col].fillna(
            dataframe[address_col].str.extract(r'1-([0-9]+[a-z]+)',
                                               flags=re.IGNORECASE).iloc[:, 0])
        # replace unit_col column w/ empty string
        dataframe[new_address_col] = dataframe[address_col].str.replace(unit, '', regex=True)
        dataframe[new_address_col] = dataframe[new_address_col].str.strip()
        if (dataframe[new_address_col].isna().sum()) > (dataframe[address_col].isna().sum()):
            raise ValueError
        state_names = [keys for keys in state_dict.keys()] + [values for values in state_dict.values()]
        # clean the city column
        if dataframe[parsed_city_col].isna().all() == False:
            # parse state from city col
            dataframe[parsed_state_col] = dataframe[parsed_state_col].fillna(dataframe[parsed_city_col].
                                                                             str.extract(
                '\s({})(\s|$)'.format('|'.join(state_names)),
                flags=re.IGNORECASE).iloc[:, 0])
            # remove state from city
            dataframe[parsed_city_col] = dataframe[parsed_city_col].str.replace(r'(.+)[,\s]({})(\s|$)'.
                                                                                format('|'.join(state_names)), r'\g<1>',
                                                                                flags=re.IGNORECASE, regex=True)
        # parse state from full address
        dataframe[parsed_state_col] = dataframe[parsed_state_col].fillna(
            dataframe[address_col].str.extract('({})'.format('|\b'.join(state_names)),
                                               flags=re.IGNORECASE).iloc[:, 0])
        # standardize parsed state names
        if dataframe[parsed_state_col].isna().all() == False:
            for key in state_dict.keys():
                dataframe[parsed_state_col] = np.where(
                    dataframe[parsed_state_col].isna(),
                    dataframe[parsed_state_col],
                    dataframe[parsed_state_col].str.replace('{}'.format(state_dict[key]), key, regex=True)

                )

        added_cols = [parsed_st_num1_col, parsed_st_name_col, parsed_ss_col]
        dataframe1 = dataframe[~dataframe[added_cols].notna().all(1)]
        dataframe0 = dataframe[dataframe[added_cols].notna().all(1)]
        dataframe0['parsed_from'] = 'already_parsed'
        df_list.append(dataframe0)

        def parse_from_legal_description(dataframe, log=False):
            st_sfx_la = r'(aly|ave|byu|blf|blvd|cl|cswy|ctr|cir|cv|crk|cres|xing|curv|dr|est|expy|ext|frk|ft|fwy|gdn|gtwy|hvn|' \
                        r'hwy|hl|jct|ky|lks|ln|lgt|lp|mall|mnr|mdw|msn|mtn|pkwy|pass|path|plz|pt|prt|rst|rdge|rd|rte|rw|shr|' \
                        r'sq|st|strm|trak|turnpike|vly|vws|walk|way)'
            st_name_la = r'([a-z\s-]+)'
            dataframe_no_ld = dataframe[dataframe[legal_description_col].isna()]
            dataframe_yes_ld = dataframe[~dataframe[legal_description_col].isna()]
            if is_string_dtype(dataframe[legal_description_col]):
                split3 = dataframe_yes_ld[legal_description_col].str.extract(r'(#)\s?([0-9a-z-]{1,6})',
                                                                             flags=re.IGNORECASE)
                dataframe_yes_ld[parsed_unit_col].fillna(split3[1], inplace=True)
                split4 = dataframe_yes_ld[legal_description_col].str.extract(r'\b([0-9a-z-]+)\sof\s([0-9]+)',
                                                                             flags=re.IGNORECASE)
                dataframe_yes_ld[parsed_unit_col].fillna(split4[0], inplace=True)
                dataframe_yes_ld[parsed_st_num1_col].fillna(split4[1])
                dataframe_yes_ld['new' + legal_description_col] = dataframe_yes_ld[legal_description_col].str.replace(
                    'of\s', '', flags=re.IGNORECASE)
                dataframe_yes_ld['new' + legal_description_col] = dataframe_yes_ld[
                    'new' + legal_description_col].str.replace(r'#\s?[0-9a-z]{1,5}', '', flags=re.IGNORECASE, regex=True)
                # parse where string contains num? st suffix
                split1 = dataframe_yes_ld['new' + legal_description_col].str.extract(
                    st_num + r'?-?' + st_num + r'?\b' + r'([nsewrl])?\s?' + st_name_la +
                    r'\s' + st_sfx_la, flags=re.IGNORECASE
                    )
                dataframe_yes_ld[parsed_st_num1_col] = dataframe_yes_ld[parsed_st_num1_col].fillna(split1[0])
                dataframe_yes_ld[parsed_st_num2_col] = dataframe_yes_ld[parsed_st_num2_col].fillna(split1[1])
                dataframe_yes_ld[parsed_sd_col] = dataframe_yes_ld[parsed_sd_col].fillna(split1[2])
                dataframe_yes_ld[parsed_st_name_col] = dataframe_yes_ld[parsed_st_name_col].fillna(split1[3])
                dataframe_yes_ld[parsed_ss_col] = dataframe_yes_ld[parsed_ss_col].fillna(split1[4])
                # parse where string contains #unit? <some word> + condo
                split2 = dataframe_yes_ld['new' + legal_description_col].str.extract(
                    r'(of\s)?#?([0-9]+[a-z]?)?\s?(of\s)?([a-z\s]+)\scondo', flags=re.IGNORECASE)
                dataframe_yes_ld[parsed_unit_col] = dataframe_yes_ld[parsed_unit_col].fillna(split2[1])
                dataframe_yes_ld[parsed_address_name] = dataframe_yes_ld[parsed_address_name].fillna(split2[3])
                dataframe_yes_ld = dataframe_yes_ld.drop(columns=['new' + legal_description_col])
                dataframe_yes_ld['parsed_from'] = 'legal_desc'
            else:
                if log is not False:
                    print('not parsing legal address column')
            dataframe = pd.concat([dataframe_yes_ld, dataframe_no_ld])
            return dataframe
            # drop duplicates based on address & legal address column

        # dataframe2.drop_duplicates(subset=[new_address_col, legal_description_col], inplace=True)
        # extract where format is num st sfx unit, but unit does not need a # signifier
        split0 = dataframe1[new_address_col].str.extract(
            st_num + r'-?\s?' + st_num + r'?\s' + st_d + r'?[\s-]?' + st_name + r'\s' + st_sfx + r'[-\s,]{1,3}' +
            r'#?([0-9-]{1,4}-?[a-z]?|[a-z]-?[0-9]{1,4}|[up][0-9]{1,4}[abcd]|un\s[0-9abcd]{1,4}|[abcd])' + r'([^0-9]+)?',
            flags=re.IGNORECASE)
        dataframe1[parsed_st_num1_col].fillna(split0[0], inplace=True)
        dataframe1[parsed_st_num2_col].fillna(split0[1], inplace=True)
        dataframe1[parsed_sd_col].fillna(split0[2], inplace=True)
        dataframe1[parsed_st_name_col].fillna(split0[3], inplace=True)
        dataframe1[parsed_ss_col].fillna(split0[4], inplace=True)
        dataframe1[parsed_unit_col].fillna(split0[5], inplace=True)

        dataframe2 = dataframe1[~dataframe1[added_cols].notna().all(1)]
        dataframe1 = dataframe1[dataframe1[added_cols].notna().all(1)]

        dataframe1['parsed_from'] = 'num_st_sfx_u'
        df_list.append(dataframe1)

        # extract where format is num num? st sfx zipcode?
        split1 = dataframe2[new_address_col].str.extract(
            st_num + r'-?\s?' + st_num + r'?\s' + st_d + r'?[\s-]?' + st_name +
            r'\s' + st_sfx + r'\s?' + r'#?([0-9-]{1,4}-?[a-z]?|[a-z]-?[0-9]{1,4}|[up][0-9]{1,4}[abcd]|un\s[0-9abcd]{1,4})' + '?'
                                                                                                                             r',?\b' + zipcode + '?',
            flags=re.IGNORECASE)
        dataframe2[parsed_st_num1_col].fillna(split1[0], inplace=True)
        dataframe2[parsed_st_num2_col].fillna(split1[1], inplace=True)
        dataframe2[parsed_sd_col].fillna(split1[2], inplace=True)
        dataframe2[parsed_st_name_col].fillna(split1[3], inplace=True)
        dataframe2[parsed_ss_col].fillna(split1[4], inplace=True)
        dataframe2[parsed_unit_col].fillna(split1[5], inplace=True)
        dataframe2[parsed_zip_col].fillna(split1[6], inplace=True)
        if legal_description_col is not False:
            dataframe2 = parse_from_legal_description(dataframe=dataframe2)

        dataframe3 = dataframe2[~dataframe2[added_cols].notna().all(1)]
        dataframe2 = dataframe2[dataframe2[added_cols].notna().all(1)]
        dataframe2['parsed_from'] = 'num_st_sfx'
        df_list.append(dataframe2)

        # extract where format is num st_d? st
        split2 = dataframe3[new_address_col].str.extract(
            r'^' + st_num + r'-?\s?' + st_num + r'?\s' + st_d + '?' + st_name + '\s?,?\s?#?([0-9a-z\s]{1,4})?$',
            flags=re.IGNORECASE)
        dataframe3[parsed_st_num1_col].fillna(split2[0], inplace=True)
        dataframe3[parsed_st_num2_col].fillna(split2[1], inplace=True)
        dataframe3[parsed_sd_col].fillna(split2[2], inplace=True)
        dataframe3[parsed_st_name_col].fillna(split2[3], inplace=True)
        dataframe3[parsed_unit_col].fillna(split2[4], inplace=True)
        dataframe4 = dataframe3[~dataframe3[[parsed_st_num1_col, parsed_st_name_col]].notna().all(1)]
        dataframe3 = dataframe3[dataframe3[[parsed_st_num1_col, parsed_st_name_col]].notna().all(1)]
        dataframe3['parsed_from'] = 'num_st'
        df_list.append(dataframe3)

        # extract where format is st_st_sfx
        split3 = dataframe4[new_address_col].str.extract(
            '^' + st_d + r'?\s?' + st_name + r'\s' + st_sfx + r'\s?[,-]?\s?$', flags=re.IGNORECASE)
        dataframe4[parsed_sd_col].fillna(split3[0], inplace=True)
        dataframe4[parsed_st_name_col].fillna(split3[1], inplace=True)
        dataframe4[parsed_ss_col].fillna(split3[2], inplace=True)
        dataframe5 = dataframe4[~dataframe4[[parsed_ss_col, parsed_st_name_col]].notna().all(1)]
        dataframe4 = dataframe4[dataframe4[[parsed_ss_col, parsed_st_name_col]].notna().all(1)]
        dataframe4['parsed_from'] = 'st_st_sfx'
        df_list.append(dataframe4)

        # extract where format is st_st_sfx_unit
        split4 = dataframe5[new_address_col].str.extract('^' + st_d + r'?\s?' + st_name +
                                                         r'\s' + st_sfx + r'\s' + unit2 + '.+$', flags=re.IGNORECASE)
        dataframe5[parsed_sd_col].fillna(split4[0], inplace=True)
        dataframe5[parsed_st_name_col].fillna(split4[1], inplace=True)
        dataframe5[parsed_ss_col].fillna(split4[2], inplace=True)
        dataframe5[parsed_unit_col].fillna(split4[3], inplace=True)
        dataframe6 = dataframe5[~dataframe5[[parsed_ss_col, parsed_st_name_col]].notna().all(1)]
        dataframe5 = dataframe5[dataframe5[[parsed_ss_col, parsed_st_name_col]].notna().all(1)]
        dataframe5['parsed_from'] = 'st_st_sfx_unit'
        df_list.append(dataframe5)

        split5 = dataframe6[new_address_col].str.extract('^' + st_d + r'?\s?' + st_name +
                                                         r'\s' + st_sfx + '\s.+$', flags=re.IGNORECASE)
        dataframe6[parsed_sd_col].fillna(split5[0], inplace=True)
        dataframe6[parsed_st_name_col].fillna(split5[1], inplace=True)
        dataframe6[parsed_ss_col].fillna(split5[2], inplace=True)
        dataframe7 = dataframe6[~dataframe6[[parsed_ss_col, parsed_st_name_col]].notna().all(1)]
        dataframe6 = dataframe6[dataframe6[[parsed_ss_col, parsed_st_name_col]].notna().all(1)]
        dataframe6['parsed_from'] = 'st_st_sfx_otherStuff'
        df_list.append(dataframe6)

        # extract where format is (.+)way
        # has to be done like this because broadway doesnt have a st sfx
        split6 = dataframe7[new_address_col].str.extract('^' + st_num + r'?\s?' + '([a-z]+way)' + '\s?' +
                                                         r'#?\s?([0-9-]+-?[a-z]?|[a-z]-?[0-9]+|[up][0-9]?[abcd]|un\s[0-9abcd])' + '?')
        dataframe7[parsed_st_name_col].fillna(split6[1], inplace=True)
        dataframe7[parsed_st_num1_col].fillna(split6[0], inplace=True)
        dataframe7[parsed_unit_col].fillna(split6[2], inplace=True)
        #     dataframe7.drop(columns=['temp_col'], inplace=True)
        dataframe_not_parsed = dataframe7[~dataframe7[[parsed_st_num1_col, parsed_st_name_col]].notna().all(1)]
        dataframe7 = dataframe7[dataframe7[[parsed_st_num1_col, parsed_st_name_col]].notna().all(1)]
        dataframe7['parsed_from'] = 'way'
        dataframe_not_parsed['parsed_from'] = 'not parsed'
        df_list.append(dataframe7)

        dataframe = pd.concat(df_list)
    else:
        dataframe[new_address_col] = dataframe[address_col]
        dataframe_not_parsed = pd.DataFrame(columns=dataframe.columns)
    if is_string_dtype(dataframe[parsed_st_num1_col]):
        dataframe[parsed_st_num1_col] = dataframe[parsed_st_num1_col].str.replace('-', '')
    if is_string_dtype(dataframe[parsed_st_num1_col]):
        dataframe[parsed_st_num1_col] = dataframe[parsed_st_num1_col].str.replace('-', '')
    if is_string_dtype(dataframe[parsed_zip_col]):
        dataframe[parsed_zip_col] = dataframe[parsed_zip_col].str.replace('_$', '', regex=True)
    dataframe[parsed_st_num1_col] = np.where(dataframe[parsed_st_num1_col].isna(),
                                             dataframe[parsed_st_num1_col],
                                             dataframe[parsed_st_num1_col].astype('int32', errors='ignore'))
    for col in [parsed_unit_col, parsed_st_num1_col, parsed_sd_col, parsed_st_name_col, parsed_ss_col,
                parsed_zip_col, parsed_city_col]:
        try:
            dataframe[col] = dataframe[col].str.replace(r'\s{2,}', " ",
                                                        flags=re.IGNORECASE, regex=True)
            dataframe[col] = dataframe[col].str.strip()
            dataframe[col] = dataframe[col].str.replace(r'\.0', " ",
                                                        flags=re.IGNORECASE, regex=True)
            dataframe[col] = np.where(dataframe[col].str.contains('^(\s+)?$', na=True), np.nan, dataframe[col])
            dataframe[col] = np.where(dataframe[col].str.contains('^nan$', na=True), np.nan, dataframe[col])
        except AttributeError as error:
            print('{} is numeric, not attempting to remove whitespace'.format(col))
    # fill unit col w/ n2 col that has letters
    try:
        dataframe['fill_unit'] = np.where((dataframe[parsed_unit_col].isna()) & (
            dataframe[parsed_st_num2_col].str.contains('[a-z]', flags=re.IGNORECASE)),
                                          dataframe[parsed_st_num2_col], np.nan
                                          )
        dataframe[parsed_unit_col].fillna(dataframe['fill_unit'], inplace=True)
        dataframe.drop(columns='fill_unit', inplace=True)
        dataframe[parsed_st_num2_col] = np.where(
            dataframe[parsed_st_num2_col].str.contains('[a-z]', flags=re.IGNORECASE),
            np.nan,
            dataframe[parsed_st_num2_col])
    except AttributeError:
        print('{} is numeric, not attempting to remove fill unit'.format(parsed_st_num2_col))
    # remove n2 that have letters
    # clean unit col
    try:
        dataframe[parsed_unit_col] = dataframe[parsed_unit_col].str.replace(r'-|\.|!|@|\$|~|\(|\)|\\|\||\*|/|"|`|\s|#',
                                                                            "", regex=True)
        dataframe[parsed_unit_col] = dataframe[parsed_unit_col].str.replace('(u)([0-9]+[a-z]?)', r'\g<2>', regex=True)
        dataframe[parsed_unit_col] = dataframe[parsed_unit_col].str.lower()
        dataframe[parsed_unit_col] = dataframe[parsed_unit_col].str.strip()
        # clean unit col
        dataframe[parsed_unit_col] = dataframe[parsed_unit_col].str.replace(r'-|\.|!|@|\$|~|\(|\)|\\|\||\*|/|"|`|\s|#',
                                                                            "", regex=True)
        # parse directional from address and fill na
        split_d = dataframe[new_address_col].str.extract('\s' + '([nsew]{1,2})' + '(\s|,|$)', flags=re.IGNORECASE)
        dataframe[parsed_sd_col].fillna(split_d[0], inplace=True)
        # remove directionals from parsed street name
        dataframe[parsed_st_name_col] = dataframe[parsed_st_name_col].str.replace(r'^([nsewrl])\s(.+)', r'\g<2>', regex=True)
        dataframe[parsed_st_name_col] = dataframe[parsed_st_name_col].str.replace(r'(.+)\s([nsewrl])$', r'\g<1>', regex=True)
    except AttributeError:
        print('{} is not string type, not attempting to remove whitespace'.format(col))
    # standardize street name
    dataframe[parsed_st_name_col] = string_standardize_column_vectorized(dataframe[parsed_st_name_col])
    dataframe[prefix + 'fullAddress'] = combine_names(dataframe[[parsed_unit_col, parsed_st_num1_col,
     parsed_sd_col, parsed_st_name_col, parsed_ss_col,
                             parsed_zip_col, parsed_city_col]],
                  name_cols=[parsed_unit_col, parsed_st_num1_col, parsed_sd_col, parsed_st_name_col, parsed_ss_col,
                             parsed_zip_col, parsed_city_col],  fill='empty'
                  )
    dataframe = pd.concat([dataframe, dataframe_not_parsed])

    dataframe[prefix + 'fullAddress'].fillna(dataframe[address_col], inplace=True)
    if legal_description_col is not False:
        dataframe[prefix + 'fullAddress'].fillna(dataframe[legal_description_col], inplace=True)

    if legal_description_col is not False:
        dataframe_full = pd.merge(dataframe_full, dataframe, how='left',
                                  on=merge_cols + [legal_description_col], indicator=True)
    else:
        dataframe_full = pd.merge(dataframe_full, dataframe, how='left',
                                  on=merge_cols, indicator=True)
    if dataframe_full['_merge'].isin(['left_only']).any():

        raise ValueError('Some addresses didnt get merged right, buddy')
    if dataframe_full.shape[0] != initial_shape:
        raise ValueError('Some addresses got lost, buddy')
    dataframe_full.drop(columns=['_merge'], inplace=True)
    dataframe_full[prefix + 'fullAddress'] = dataframe_full[prefix + 'fullAddress'].str.replace('\.0', '', regex=True)
    return dataframe_full


def make_full_address(df, address_cols=None, addr_col='parsed_fullAddress', fill='empty'):
    if address_cols is None:
        address_cols = ['parsed_addr_u', 'parsed_addr_n1', 'parsed_addr_sd',
                        'parsed_addr_sn', 'parsed_addr_ss', 'parsed_addr_zip', 'parsed_city',
                        'parsed_state'
                        ]
    df[addr_col] = combine_names(dataframe=df, name_cols=address_cols, fill=fill)
