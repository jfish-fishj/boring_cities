import re
import csv
import pandas as pd
import numpy as np
from data_constants import filePrefix, name_parser_files
from helper_functions import write_to_log
import time

# Regex Definitions
ANY_NAME = r'([a-z-]+)'
NOT_MI = r'([a-z-]{2,})'
NOT_SUFFIX = r'((?!jr|sr|i{2,}|iv)[a-z-]+)'
MI = r'([a-z-]{1})'
SUFFIX = r'(jr|sr|i{2,}|iv|ms|mr)'
PROFESSION = r'(rev|jd|phd|md|dds|cpa|rn|lld|esqu?i?r?e?|dr|attorney|pc|dmd|licsw|msw|atty|pa)'
NON_WORDS = r'^(.+)(\st\s?e|\strs?|\sjt|\slt|\sfor life|'\
            r'\set ?als?|j t r s|irrev tr|n?rt|irv|'\
            r'l?i?v?\s?tr|tr\s?o?f?\s?t?h?e?|of\s?.?'\
            r'|tru?s?t?e?e?s?|of\sthe)$'

# bank names
bank_names = [
    'wells\s?fargo',
    'bank\sof\samerica',
    'j\s?p\s?morgan',
    'citigroup',
    'goldman sachs',
    'morgan stanley',
    'us bancorp|us bank',
    'truist financial',
    'pnc financial services',
    'td bank',
    'capital one',
    'bank of ny',
    'tiaa',
    'charles schwab',
    'citizens? bank',
    'hsbc bank',
    'american express',
    'usaa',
    'citizens financial group',
    'narclays',
    'santander bank',
    'deutsche bank',
    'rbc bank',
    'bnp paribas',
    'first national bank',
    'fleet(\snational)?\sbank',
    'lasalle bank',
    'shawmut bank',
    'wachovia bank'
]

# read in csv of name frequencies
with open(file=name_parser_files['female first names'], mode='r') as infile:
    reader = csv.reader(infile, delimiter=',')
    first_name_frequencies_female = {rows[0]: float(rows[1]) for rows in reader}

with open(name_parser_files['male first names'], mode='r') as infile:
    reader = csv.reader(infile, delimiter=',')
    first_name_frequencies_male = {rows[0]: float(rows[1]) for rows in reader}

with open(name_parser_files['last names'], mode='r') as infile:
    reader = csv.reader(infile, delimiter='\t')
    last_name_frequencies = {rows[0]: float(rows[1]) for rows in reader}

first_name_frequencies = {k: (first_name_frequencies_female.get(k, 0) + first_name_frequencies_male.get(k, 0)) / 2
                              for k in set(first_name_frequencies_female) | set(first_name_frequencies_male)}

def clean_name_vectorized(dataframe, column, prefix=False):
    """Takes a dataframe and a column and cleans the strings including removing periods, apostrophies, weird spaces"""
    if dataframe[column].isnull().all()==True:
        if prefix != False:
            new_column = prefix + column
            dataframe[new_column] = dataframe[column]
            return dataframe
        else:
            new_column = column
            return dataframe
    if dataframe[column].dtype != 'object':
        raise ValueError("column is not of type object/string")
    else:

        if prefix != False:
            new_column = prefix + column
        else:
            new_column = 'temp_' + column
        dataframe_dd = dataframe[[column]].drop_duplicates(subset=column)
        dataframe_dd[new_column] = dataframe_dd[column]
        dataframe_dd[new_column] = dataframe_dd[new_column].str.lower()
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'\.|\!|@|#|\$|~|\(|\)|\\|\||\*|/|"|`|\[|\]', "")
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r',{2,}',  ",")
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r"'", "")
        dataframe_dd[new_column] = dataframe_dd[new_column].str.strip()
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'(IRREV(ocable)?)\s?(REAL)?\s?EST(ate)?\s?TR(ust)?$', "", flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'trustees?', "", flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'\str\s', "", flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'([a-z]+\s)(tr,?\s)([a-z]\s+)', r'\g<1>\g<2>', flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'life tenant?', "", flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'(life)?\s?(estate)', "", flags=re.IGNORECASE)

        # replace other deliminators eg + or % w/ &
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'\+|%|;|\{|\}', '&', regex=True)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.strip()
        # remove certain TRAILING characters
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'^(.+)([&,-]\s?)$', r'\g<1>', regex=True)
        # remove certain LEADING characters
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'^([&-,]\s?)(.+)$', r'\g<2>', regex=True)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'^(of\s)(.+)$', r'\g<2>', regex=True)

        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'\s{2,}', " ", regex=True,
                                                          flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r"\s-\s|\s-|-\s", "-", regex=True,
                                                          flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'#\s', '#', regex=True, flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'([0-9])(\s)([sthnd]{2})(\s)',
                                                          r'\g<1>\g<3>', regex=True,
                                                          flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'([0-9]{1,})([abcefgijklmopqrsuvwxyz]{2,})',
                                                          r'\g<1> \g<2>', regex=True,
                                                          flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'([^ ])(,|&)([ ])', r'\g<1>\g<2> \g<3>',
                                                          regex=True, flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'([^ ])(,|&)([^\s])', r'\g<1>\g<2> \g<3>',
                                                                  regex=True, flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'([a-z])(,)(a-z])', r'\g<1>\g<2> \g<3>',
                                                          regex=True, flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'([a-z])(&)(a-z])', r'\g<1> \g<2> \g<3>',
                                                                  regex=True, flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'(&)(a-z])', r'\g<1> \g<2>',
                                                                  regex=True, flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'([a-z])(&)', r'\g<1> \g<2>',
                                                                  regex=True, flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'([^#])([a-z]{1,})([0-9]{2,})',
                                                          r'\g<1>\g<2> \g<3>', regex=True,
                                                          flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'([a-z]{2,})([#])', r'\g<1> \g<2>',
                                                          regex=True, flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'\(.+?\)', "", regex=True,
                                                          flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'##', r"#", regex=True, flags=re.IGNORECASE)
        # print("time elapsed: {:.2f}s".format(time.time() - clean_unit_start_time))
        # delete parenthesis
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'\(.+?\)', r"", regex=True)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'\s{2,}', " ", regex=True,
                                                          flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r" -\s|\s-|-\s", "-", regex=True,
                                                          flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'#\s', '#', regex=True, flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'([0-9])(\s)([sthnd]{2})(\s)',
                                                          r'\g<1>\g<3>', regex=True,
                                                          flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'(\s$|^\s)', '', regex=True, flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.strip()
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'\b([a-z])\s([a-z])\b', r'\g<1>\g<2>')
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'(\s$|^\s)', '', regex=True,
                                                                        flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'\ste$', "", flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'\ste\s', " ", flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'\sts$', "", flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'\sts\s', " ", flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'\str$', "", flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'\str\s', " ", flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'\sfm$', "", flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.replace(r'\setux$', "", flags=re.IGNORECASE)
        dataframe_dd[new_column] = dataframe_dd[new_column].str.strip()

        dataframe = pd.merge(dataframe, dataframe_dd, how='left', on=column)
        if prefix is False:
            dataframe[column] = dataframe[new_column]
            dataframe.drop(columns=new_column, inplace=True)
        return dataframe

def string_standardize_name(dataframe, column, prefix=False):
    """Standardizes strings in dataframe according to a replacement dictionary."""
    # create column with prefix and old column name
    if prefix is not False:
        newCol = prefix+column
    else:
        newCol = 'temp_' + column
    if dataframe[column].isnull().all()==True:
        #write_to_log('{} is completely NA... Not attempting to clean'.format(column))
        return dataframe
    if str(dataframe[column].dtype) != 'object':
        #print('dataframe dtype is %s' % dataframe[column].dtype)
        return dataframe
    dataframe_dd = dataframe[[column]].drop_duplicates(subset=column)
    dataframe_dd[newCol] = dataframe_dd[column]
    replacement_dict = {
        'apt': ['apa?rtme?nt', 'apts'],
        'aly': ['allee', 'alle?y'],
        'ave': ['av', 'ave?nu?e?'],
        'rd':['road'],
        # 'blvd': ['boulevard', 'boulv?'],
        # 'brg': ['br',  'bri?dge?'],
        # 'canyn': ['canyon', 'cnyn'],
        # 'canyn': ['canyon', 'cnyn'],
        # 'ctr': ['cent?', 'center', 'ce?ntre?'],
        #         # 'cir': ['ci?rcl?e?'],
        #         # 'ct': ['co?u?rt'],
        #         # 'dr': ['dri?ve?'],
        #         # 'e': ['east'],
        #         # 'est': ['estate'],
        #         # 'expy': ['expr?e?s?s?', 'expressway', 'expw'],
        #         # 'ext': ['exte?nsi?o?n'],
        #         # 'ft': ['fo?rt'],
        #         # 'fwy': ['fre?e?wa?y'],
        #         # 'gdn': ['ga?rde?n'],
        #         # 'hbr': ['harb', 'ha?rbo?r'],
        #         # 'hts': ['ht'],
        #         # 'hwy': ['highway', 'highwy', 'hiway', 'hiwy', 'hway', 'hw'],
        #         # 'jct': ['ju?ncti?o?n'],
        #         # 'ln': ['lan?e?', ],
        #         # 'lp':   ['lo?o?p'],
        #         # 'mnt': ['mntain', 'mntn', 'mountain', 'mountin', 'mtin', 'mtn'],
        #         # 'n': ['north', 'no'],  # no can unfortunately be number or north depending
        #         # 'rd': ['road'],
        #         # 'pl': ['place'],
        #         # 'plz': ['plaza'],
        #         # 'pkwy': ['parkway'],
        #         # 'riv': ['rive?r'],
        #         # 'rte': ['route'],
        #         # 'sq': ['squ?a?re?'],
        #         # 'ste': ['suite'],
        'st': ['street', 'str', 'saint'],
        # business words
        'limited':['li?m?i?te?d', 'lim[tied]{3,4}'],
        'partnership':['pa?rtne?rshi?p'],
        'company': ['com[pany]{3,4}'],
        "CORPORATION": ["CORP|CP|CORP[ORAITON]{5,10}|CO"],
        'commonwealth':['commwlth', 'comm([onwealth]{1,9})'],
        'llc':['limited liability corp?o?r?a?t?i?o?n?', 'limited corp?o?r?a?t?i?o?n?',
               'limited liability company', 'limited compa?n?y?', r"L L C|L\.L\.C\.|LL", 'llc[a-z]'],
        "LP": ["LPS", r'limited\spa?r?t?n?e?r?s?h?i?p?$', r'limited\spa?rtn?e?r?s?h?i?p?',
               'limited\sp[artnership]{6,10}',r'l\s?p','lllp'],
        'inc': ['inc' 'incorpo?r?a?t?e?d?','in$'],
        'trust':['trus?s?t?', 'trs'],
        'irrevocable':['irr?evo?c?a?b?l?e?', 'irre'],
        '' : ['lt', 'life tenant',r'et\s?als?'],
        'ma': ['mass', 'massa?c?h?u?s?e?t?ts?', 'massa[chuset]{3,10}'],
        'commonwealth of ma': ['comm of ma'],
        'ny': ['new york'],
        '&': ['and'],
        'realty': ['rlty?|RLTY|RE|REL', 'real$','rtly'],
        'realty trust': [r'realt?y?\str?u?s?t?$', "REALT?Y? Tr?"],
                           "HOSPITAL": ["HOSPI?T?A?L?"],
                           "FORECLOSURE": ["FCL"],
                           "CENTER": ["CTR"],
                           "ASSOCIATION": [r"ASSO[ciation]{5,9}|ass?$|asso?c"],
                           "ASSOCIATES": ["ASSO[ciates]{5,7}"],
                           "SQUARE": ["SQ",'sqaure?'],
        'ct corporation system': [r'c\st corporation system'],
                           "GROUP": ["GROUPS"],
                           "APARTMENTS": ["APA?RTM?E?N?T?S?"],
                           "CONDO TRUST": ["CONDO T"],
                           "REVOCABLE": ["REVO?C?A?B?L?E?"],
                           "PARTNERS": ["pa?rtne?rs?|ptnrs"],
                           "PARK": ["PK"],
                           "COOPERATIVE": ["CO-OPERATIVE|CO OPERATIVE|COOP|CO-OP|COOPERA"],
                           "COMPANY": ["CO?MA?NY"],
                           "FAMILY": ["FAM"],
                           "INVESTMENTS": ["INVESMENT|INVESTMENT|INVES"],
                           "TRUSTEES": ["TRSTE?E?S"],
                            "TRUSTEE":['TRU?STEE'],
                           "AUTHORITY": ["AUTH"],
                           "BOSTON": ["BOSTO?N?",'bos$'],
                           "BOSTON HOUSING AUTHORITY": ["BOSTON HOUSING AUTH|BHA"],
                           "HOUSING": ["HOUSNG"],
                           "CONDO": ["CONDOMINIUM|CO?NDO?MI?NI?U?MS?|CONDOS|CONDOS|COND|cond[ominum]{3,8}"],
                           "TRANSPORTATION": ["TRANSP?"],
                           "SOCIETY": ["SOC"],

                            'nominee': ['nom'],
                           "MEDICAL": ["MED"],
                           "NEW ENGLAND": ["NEW ENG"],
                           "SYSTEM": ["SYST"],
                           "SERVICES": ["SERV"],
                           "REDEVELOPMENT": ["REDVLPMNT|REDEVLPMNT|REDEVELPMENT|REDEVELPMNT|REDEVEL|REDEV"],
                           "DEVELOPMENT": [
                               "DEV|DVLPMNT|DEVLPMNT|DEVELO|DEVELOPME|DEVLOP|DEVELOP|DEVELOPMNT|DEVELOPMEN|DEVE"],
                           "NATIONAL": ["NAT|NTL|NATL"],
                           "HOLDINGS": ["HOLDING|HLDNG"],
                           "ARCHDIOCESE": ["ARCH|ARCHDIOCES|DIOCESE"],
                           "MANAGEMENT": [
                               "MNGMT|MGMT|MANAG|MGT|MNGT|MGMNT|MGNT|MANAGE|MNGMNT|MANAGEMNT|MANAGEMEN|MANGEMENT"],
                           "MANAGERS": ["MNGRS|MGRS|MANAGRS|MNGR|MGR|MNGMT|MANAGR|MANAGER"],
                           "MORTGAGE": ["MORTG|MTG|MTGS|MORTGAGES|MORTGAG"],
                           "ORGANIZATION": ["ORGANIZATI"],
                           "ROMAN CATHOLIC": ["ROMAN CATH"],
                           "PROPERTIES": ["PTY|PTYS|PTIES|PROP|PROPERTY|PROPERT|PROPERTI"],
                           "REAL ESTATE": ["REAL EST"],
                           "BROTHERS": ["bros"],
                            'BLDG':     ['building'],
        'living':['liv$']

    }
    # update as needed
    # loop through dict and replace values with key
    for item in replacement_dict:
        value_string = ""
        for value in sorted(replacement_dict[item]):
            value_string += value + "|"
        re_string = r'(^|\s)(%s)(\b)' % value_string[:-1]
        re_replace = r'\g<1>%s\g<3>' % item
        dataframe_dd[newCol] = dataframe_dd[newCol].str.replace(re_string, re_replace, flags=re.IGNORECASE, regex=True)
    # replace double spaces w/ single ones
    dataframe_dd[newCol] = dataframe_dd[newCol].str.replace(r'\s{2,}', r' ', flags=re.IGNORECASE, regex=True)
    dataframe_dd[newCol] = dataframe_dd[newCol].str.strip()
    dataframe_dd[newCol] = dataframe_dd[newCol].str.lower()
    dataframe = pd.merge(dataframe,dataframe_dd, how='left', on=column)
    if prefix is False:
        dataframe[column] = dataframe[newCol]
        dataframe.drop(columns=newCol, inplace=True)
    return dataframe

def clean_name(dataframe, name_column,
               prefix1='cleaned_'):
    # clean name_column
    dataframe = clean_name_vectorized(dataframe, name_column, prefix=prefix1)
    if prefix1 is not False:
        newCol=prefix1+name_column
    else:
        newCol=name_column
    # standardize new column
    dataframe = string_standardize_name(dataframe, newCol, prefix=False)
    return dataframe

def classify_name(dataframe, name_cols,type_col = 'type_name', weight_format=False, probalistic_classification=False):
    # classify new column as business or name
    if type_col in list(dataframe.columns):
        # write_to_log('type column already in dataframe not attempting to classify')
        return dataframe
    else:
        # create temp concatenation column
        begin_time = time.time()
        dataframe_dd = dataframe[name_cols].drop_duplicates(subset=name_cols)
        combine_names(dataframe_dd, name_cols=name_cols, newCol='temp_name')
        # read in lists of words that flag each type of string, e.g. business, professional, name, etc.
        with open(name_parser_files['non names'], 'r') as textfile:
            non_names = textfile.read().replace('\n', r'|(^|\s|-)')
        if weight_format is not False:
            dataframe_dd[type_col] = np.where(
                (dataframe_dd['temp_name'].str.contains(weight_format, regex=True, flags=re.IGNORECASE)) |
                (dataframe_dd['temp_name'].str.contains(non_names, regex=True, flags=re.IGNORECASE)),
                'business',
                'person'
            )
        else:
            dataframe_dd[type_col] = np.where(
                dataframe_dd['temp_name'].str.contains(non_names, regex=True, flags=re.IGNORECASE),
                'business',
                'person')
        if probalistic_classification is not False:
            # read in csv of name frequencies
            with open(file=name_parser_files['female first names'], mode='r') as infile:
                reader = csv.reader(infile, delimiter=',')
                first_name_frequencies_female = {rows[0]: float(rows[1]) for rows in reader}

            with open(name_parser_files['male first names'], mode='r') as infile:
                reader = csv.reader(infile, delimiter=',')
                first_name_frequencies_male = {rows[0]: float(rows[1]) for rows in reader}

            with open(name_parser_files['last names'], mode='r') as infile:
                reader = csv.reader(infile, delimiter='\t')
                last_name_frequencies = {rows[0]: float(rows[1]) for rows in reader}

            first_name_frequencies = {
                k: (first_name_frequencies_female.get(k, 0) + first_name_frequencies_male.get(k, 0)) / 2
                for k in set(first_name_frequencies_female) | set(first_name_frequencies_male)}
            fn_filter_dict = {k:v for (k,v) in first_name_frequencies.items() if v > 0.000}
            ln_filter_dict =  {k:v for (k,v) in last_name_frequencies.items() if v > 0.00}
            # where we already think the name is a person check to make sure it contains one of the names listed in 
            # census names dict
            # if it does keep it as a person else business
            dataframe_dd[type_col] = np.where(
                (dataframe_dd[type_col] == 'person') &
                ((dataframe_dd['temp_name'].str.contains(r'|\b'.join(fn_filter_dict.keys()), regex=True, flags=re.IGNORECASE)) &
                (dataframe_dd['temp_name'].str.contains(r'|\b'.join(ln_filter_dict.keys()), regex=True, flags=re.IGNORECASE)) ),
                'person',
                'business'
            )
        dataframe_dd.drop(columns='temp_name', inplace=True)
        dataframe = pd.merge(dataframe, dataframe_dd, how='left', on=name_cols)
        return dataframe


def combine_names(dataframe, name_cols = ['firstName', 'lastName'], newCol = "combined_fullName", fill = ''):
    if newCol not in dataframe.columns:
        temp_cols = []
        for col in name_cols:
            temp_name = 'temp_' + col
            dataframe[temp_name] = dataframe[col].fillna(fill)
            temp_cols.append(temp_name)
        dataframe[newCol] = dataframe[temp_cols[0]].astype(str)
        for col in temp_cols[1:]:
            dataframe[newCol] = dataframe[newCol].astype(str) + ' ' + dataframe[col].astype(str)
            dataframe.drop(columns={col}, inplace=True)
        dataframe[newCol] = dataframe[newCol].str.replace(' {2,}', ' ')
        dataframe[newCol] = dataframe[newCol].str.strip()
        dataframe[newCol] = dataframe[newCol].replace(r'^\s+$', np.nan)
        dataframe[newCol] = dataframe[newCol].replace(fill, np.nan)
        dataframe.drop(columns=temp_cols[0], inplace=True)
    else:
        dataframe[newCol] = dataframe[newCol].replace(fill, np.nan)
        dataframe[newCol] = dataframe[newCol].replace(r'^\s+$', np.nan)
        temp_cols = []
        for col in name_cols:
            temp_name = 'temp_' + col
            dataframe[temp_name] = dataframe[col].fillna(fill).astype(str)
            temp_cols.append(temp_name)
        dataframe['temp_temp_col'] = dataframe[temp_cols[0]]
        for col in temp_cols[1:]:
            dataframe['temp_temp_col'] = dataframe['temp_temp_col'] + ' ' + dataframe[col]
            dataframe.drop(columns={col}, inplace=True)
        dataframe.drop(columns=temp_cols[0], inplace=True)
        dataframe[newCol].fillna(dataframe['temp_temp_col'], inplace=True)
        dataframe.drop(columns={'temp_temp_col'}, inplace=True)
        dataframe[newCol] = dataframe[newCol].str.replace(' {2,}', ' ')
        dataframe[newCol] = dataframe[newCol].str.strip()
        dataframe[newCol] = dataframe[newCol].replace(r'^\s+$', np.nan)
        dataframe[newCol] = dataframe[newCol].replace(fill, np.nan)


def is_first_name(strng):
    score = 0
    if type(strng) != str:
        return -100000
    else:
        if strng in first_name_frequencies:
            score = score + first_name_frequencies[strng]
        if strng in last_name_frequencies:
            score = score - last_name_frequencies[strng]
        return score

def classify_ambiguous_name(strng1, strng2):
    if type(strng1) is not str or type(strng2) is not str:
        return np.nan
    first_score = is_first_name(strng1)
    second_score = is_first_name(strng2)
    if first_score >0 and second_score >0:
        return 'first_last'
    elif first_score > second_score:
        return 'first_last'
    elif second_score > first_score:
        return 'last_first'
    elif first_score <0 and second_score <0:
        return np.nan

def parse_person(person_df, name_col, first_name_col, middle_name_col,
                 last_name_col, profession_col,suffix_col,alphabetized_col,format_col=None, prefix=False, log=False):
    begin_time = time.time()
    for col in [first_name_col, last_name_col, middle_name_col, suffix_col, profession_col]:
        if col not in list(person_df.columns):
            person_df[col] = np.nan
    if person_df.shape[0] == 0:
        if log is not False:
            write_to_log('person df has no rows. not trying to parse')
        return person_df
    # check for format col and if not in dataframe assume lastName_firstName
    if format_col is None:
        person_df['format_col'] = 'lastName_firstName'
        format_col_new = 'format_col'
    else:
        format_col_new=format_col
        person_df[format_col_new].fillna('lastName_firstName', inplace=True)
    # remove non words (things like life tenant, jt, tr, etc.
    for col in [name_col, first_name_col, middle_name_col, last_name_col]:
        if pd.api.types.is_string_dtype(person_df[col]):
            person_df[col] = person_df[col].str.replace('^(.+)\s(revocable living trust|living trust|revocable liv$|'
                                                        'i?r?revocable ?t?r?u?s?t?$|irrev tr$|irv$|liv tr$|i?r?revocable$)',
                                                                            r'\g<1>', flags=re.IGNORECASE)
            person_df[col] = person_df[col].str.replace('^(.+)\s(tru?s?t?e?e?s?$|jt|lt|t\se$|for life|el ?als?|j t r s|life esta?t?e?|est$)',
                                                        r'\g<1>', flags=re.IGNORECASE)
            person_df[col] = np.where(person_df[col].str.count(' ')> 1,
                                      person_df[col].str.replace('^(.+)\s(he|le)$', r'\g<1>'), person_df[col])
            person_df[col] = person_df[col].str.replace(' {2,}', ' ')
            person_df[col] = person_df[col].str.strip()
            person_df[col] = person_df[col].str.replace('^(.+)\s(revocable living trust|living trust|revocable liv$|'
                                                        'i?r?revocable ?t?r?u?s?t?$|irrev tr$|irv$|liv tr$|i?r?revocable$)',
                                                        r'\g<1>', flags=re.IGNORECASE)
            person_df[col] = person_df[col].str.replace(
                '^(.+)\s(tru?s?t?e?e?s?$|jt|lt|t\se$|for life|el ?als?|j t r s|life esta?t?e?|est$)',
                r'\g<1>', flags=re.IGNORECASE)
            person_df[col] = np.where(person_df[col].str.count(' ') > 1,
                                      person_df[col].str.replace('^(.+)\s(he|le)$', r'\g<1>'), person_df[col])
            person_df[col] = person_df[col].str.replace(' {2,}', ' ')
            person_df[col] = person_df[col].str.strip()
            person_df[col] = person_df[col].str.replace(r'^(est)\s(.+)$',
                                                        r'\g<2>', flags=re.IGNORECASE)
            if log is not False:
                print('{} is string type'.format(col))
    else:
        if log is not False:
            print('{} is not string type'.format(col))
    # drop duplicate full names and merge back on at end
    person_df_full = person_df.drop(columns=[middle_name_col, suffix_col, profession_col]
                                    ).copy(deep=True)
    initial_shape = person_df_full.shape[0]
    person_df.drop_duplicates(subset=[name_col,first_name_col, last_name_col], inplace=True)
    person_df = person_df[[name_col,first_name_col, last_name_col, middle_name_col,
                           suffix_col, profession_col, format_col_new]]
    if prefix is not False:
        colList = [prefix + col for col in [first_name_col, last_name_col, middle_name_col, suffix_col, profession_col] ]
        for col in colList:
            if col not in list(person_df.columns):
                person_df[col] = np.nan
    else:
        prefix=""
    final_shape = person_df_full.shape[0]
    if initial_shape != final_shape:
        raise ValueError('Initial and final shapes do not agree! Copy was not deep enough')
    # first extract suffixes and profession designations and replace w/ ''

    def parse_suffix(strng):
        if type(strng) is not str:
            return np.nan
        else:
            return_string = re.search(r'\b{}\b|,|$'.format(SUFFIX), strng)
            if return_string is not None:
                return return_string.group(1)
            else:
                return np.nan
    def parse_profession(strng):
        if type(strng) is not str:
            return np.nan
        else:
            return_string = re.search(r'\b{}\b'.format(PROFESSION), strng)
            if return_string is not None:
                return return_string.group(1)
            else:
                return np.nan

    def parse_middle_initial(strng):
        if type(strng) is not str:
            return np.nan
        else:
            return_string = re.search(r'\b,?([a-z]{1})\b', strng, flags=re.IGNORECASE)
            if return_string is not None:
                return return_string.group(1)
            else:
                return np.nan

    # create copy of name column to be split and remove profession, and suffix
    new_name_col = name_col + '_split'
    person_df[new_name_col] = person_df[name_col].str.replace(
        r'\b,?{}\b'.format(PROFESSION),
        '', regex=True, flags=re.IGNORECASE)
    person_df[new_name_col] = person_df[new_name_col].str.replace(
        r'\s,?{}\b'.format(SUFFIX),
        '', regex=True, flags=re.IGNORECASE)
    # remove trailing characters following str extract. Happens when you get ,MD
    person_df[new_name_col] = person_df[new_name_col].str.strip()
    person_df[new_name_col] = person_df[new_name_col].str.replace(r'\s{2,}','')
    person_df[new_name_col] = person_df[new_name_col].str.strip()

    # remove certain TRAILING characters
    person_df[new_name_col] = person_df[new_name_col].str.replace(r'^(.+)([&,]\s?)$', r'\g<1>', regex=True)
    person_df[new_name_col] = person_df[new_name_col].str.strip()

    # create df_list to hold parsed names
    df_list = []
    # split where format is ambiguous
    ambig_names = person_df[person_df[format_col_new] == 'unknown']
    person_df = person_df[person_df[format_col_new]!='unknown']
    if ambig_names.shape[0] > 0:
        # parse where format is ambiguous
        start_time = time.time()
        ambig_names[prefix + first_name_col] = ambig_names[first_name_col]
        ambig_names[prefix + middle_name_col] = ambig_names[middle_name_col]
        ambig_names[prefix + last_name_col] = ambig_names[last_name_col]
        # parse suffix/mi/profession from first name column
        # (happens cause people put their initials and professions in the first name column)
        ambig_names[prefix + suffix_col] = ambig_names[prefix + suffix_col].fillna(
            ambig_names[prefix + first_name_col].apply(parse_suffix)
        )
        ambig_names[prefix + profession_col] = ambig_names[prefix + profession_col].fillna(
            ambig_names[prefix + first_name_col].apply(parse_profession)
        )
        ambig_names[prefix + middle_name_col] = ambig_names[prefix + middle_name_col].fillna(
            ambig_names[prefix + first_name_col].apply(parse_middle_initial)
        )
        # clean first name column (again done because ppl like to flex their phds)
        ambig_names[prefix + first_name_col] = ambig_names[prefix + first_name_col].str.replace(
            r'\b,?{}\b'.format(PROFESSION),
            '', regex=True, flags=re.IGNORECASE)
        ambig_names[prefix + first_name_col] = ambig_names[prefix + first_name_col].str.replace(
            r'\s,?{}\b'.format(SUFFIX),
            '', regex=True, flags=re.IGNORECASE)
        ambig_names[prefix + first_name_col] = ambig_names[prefix + first_name_col].str.replace(
            r'\s,?([a-z]{1})\b',
            '', regex=True, flags=re.IGNORECASE)
        # remove trailing characters following str extract. Happens when you get ,MD
        ambig_names[prefix + first_name_col] = ambig_names[prefix + first_name_col].str.strip()
        ambig_names[prefix + first_name_col] = ambig_names[prefix + first_name_col].str.replace(r'\s{2,}', '')
        # remove certain TRAILING characters
        ambig_names[prefix + first_name_col] = ambig_names[prefix + first_name_col].str.replace(r'^(.+)([&,]\s?)$',
                                                                                                r'\g<1>', regex=True)
        # repeat for last name
        # parse suffix/mi/profession from last name column
        # (happens cause people put their initials and professions in the last name column)
        ambig_names[prefix + suffix_col] = ambig_names[prefix + suffix_col].fillna(
            ambig_names[prefix + last_name_col].apply(parse_suffix)
        )
        ambig_names[prefix + profession_col] = ambig_names[prefix + profession_col].fillna(
            ambig_names[prefix + last_name_col].apply(parse_profession)
        )
        ambig_names[prefix + middle_name_col] = ambig_names[prefix + middle_name_col].fillna(
            ambig_names[prefix + last_name_col].apply(parse_middle_initial)
        )
        # clean last name column (again done because ppl like to flex their phds)
        ambig_names[prefix + last_name_col] = ambig_names[prefix + last_name_col].str.replace(
            r'\b,?{}\b'.format(PROFESSION),
            '', regex=True, flags=re.IGNORECASE)
        ambig_names[prefix + last_name_col] = ambig_names[prefix + last_name_col].str.replace(
            r'\s,?{}\b'.format(SUFFIX),
            '', regex=True, flags=re.IGNORECASE)
        ambig_names[prefix + last_name_col] = ambig_names[prefix + last_name_col].str.replace(
            r'\s,?([a-z]{1})\b',
            '', regex=True, flags=re.IGNORECASE)
        # remove trailing characters following str extract. Happens when you get ,MD
        ambig_names[prefix + last_name_col] = ambig_names[prefix + last_name_col].str.strip()
        ambig_names[prefix + last_name_col] = ambig_names[prefix + last_name_col].str.replace(r'\s{2,}', ' ')
        ambig_names[prefix + last_name_col] = ambig_names[prefix + last_name_col].str.strip()
        # remove certain TRAILING characters
        ambig_names[prefix + last_name_col] = ambig_names[prefix + last_name_col].str.replace(r'^(.+)([&,]+\s?)$',
                                                                                              r'\g<1>',
                                                                                              regex=True)
        # rearrange based on first and last name frequency
        ambig_names['first_name_score'] = ambig_names[prefix + first_name_col].apply(is_first_name)
        ambig_names['last_name_score'] = ambig_names[prefix + last_name_col].apply(is_first_name)
        ambig_names['temp_first_names'] = np.where((ambig_names['last_name_score'] > ambig_names['first_name_score']),
                                                   ambig_names[prefix + last_name_col],
                                                   ambig_names[prefix + first_name_col]
                                                   )
        ambig_names['temp_last_names'] = np.where(ambig_names['last_name_score'] <= ambig_names['first_name_score'],
                                                   ambig_names[prefix + last_name_col],
                                                   ambig_names[prefix + first_name_col]
                                                   )
        ambig_names[prefix + first_name_col] = ambig_names['temp_first_names'].fillna(ambig_names[prefix + first_name_col])
        ambig_names[prefix + last_name_col] = ambig_names['temp_last_names'].fillna(ambig_names[prefix + last_name_col])
        ambig_names.drop(columns = ['first_name_score','last_name_score', 'temp_first_names', 'temp_last_names'], inplace=True)
        ambig_names['parsed_from'] = 'unknown format'
        df_list.append(ambig_names)

    # fill in where already parsed
    person_df[prefix + first_name_col] = person_df[first_name_col]
    person_df[prefix + middle_name_col] = person_df[middle_name_col]
    person_df[prefix + last_name_col] = person_df[last_name_col]

    # parse suffix/mi/profession from first name column
    # (happens cause people put their initials and professions in the first name column)
    person_df[prefix + suffix_col] = person_df[prefix + suffix_col].fillna(
        person_df[prefix +first_name_col].apply(parse_suffix)
    )
    person_df[prefix + profession_col] = person_df[prefix + profession_col].fillna(
        person_df[prefix +first_name_col].apply(parse_profession)
    )
    person_df[prefix + middle_name_col] = person_df[prefix + middle_name_col].fillna(
        person_df[prefix +first_name_col].apply(parse_middle_initial)
    )

    # clean first name column (again done because ppl like to flex their phds)
    person_df[prefix + first_name_col] = person_df[prefix +first_name_col].str.replace(
        r'\b,?{}\b'.format(PROFESSION),
        '', regex=True, flags=re.IGNORECASE)
    person_df[prefix +first_name_col] = person_df[prefix +first_name_col].str.replace(
        r'\s,?{}\b'.format(SUFFIX),
        '', regex=True, flags=re.IGNORECASE)
    person_df[prefix +first_name_col] = person_df[prefix +first_name_col].str.replace(
        r'\b,?([a-z]{1})\b',
        '', regex=True, flags=re.IGNORECASE)

    # remove trailing characters following str extract. Happens when you get ,MD
    person_df[prefix +first_name_col] = person_df[prefix +first_name_col].str.strip()
    person_df[prefix +first_name_col] = person_df[prefix +first_name_col].str.replace(r'\s{2,}', '')
    # remove certain TRAILING characters
    person_df[prefix +first_name_col] = person_df[prefix +first_name_col].str.replace(r'^(.+)([&,]\s?)$', r'\g<1>', regex=True)

    # repeat for last name
    # parse suffix/mi/profession from last name column
    # (happens cause people put their initials and professions in the last name column)
    person_df[prefix + suffix_col] = person_df[prefix + suffix_col].fillna(
        person_df[prefix +last_name_col].apply(parse_suffix)
    )
    person_df[prefix + profession_col] = person_df[prefix + profession_col].fillna(
        person_df[prefix +last_name_col].apply(parse_profession)
    )
    person_df[prefix + middle_name_col] = person_df[prefix + middle_name_col].fillna(
        person_df[prefix +last_name_col].apply(parse_middle_initial)
    )

    # clean last name column (again done because ppl like to flex their phds)
    person_df[prefix + last_name_col] = person_df[prefix + last_name_col].str.replace(
        r'\b,?{}\b'.format(PROFESSION),
        '', regex=True, flags=re.IGNORECASE)
    person_df[prefix + last_name_col] = person_df[prefix + last_name_col].str.replace(
        r'\s,?{}\b'.format(SUFFIX),
        '', regex=True, flags=re.IGNORECASE)
    person_df[prefix + last_name_col] = person_df[prefix + last_name_col].str.replace(
        r'\s,?([a-z]{1})\b',
        '', regex=True, flags=re.IGNORECASE)

    # remove trailing characters following str extract. Happens when you get ,MD
    person_df[prefix + last_name_col] = person_df[prefix + last_name_col].str.strip()
    person_df[prefix + last_name_col] = person_df[prefix + last_name_col].str.replace(r'\s{2,}', ' ')
    person_df[prefix + last_name_col] = person_df[prefix + last_name_col].str.strip()
    # remove certain TRAILING characters
    person_df[prefix + last_name_col] = person_df[prefix + last_name_col].str.replace(r'^(.+)([&,]+\s?)$', r'\g<1>',
                                                                                      regex=True)
    person_df[new_name_col] = person_df[new_name_col].str.replace(r'^(.+)([&,]+\s?)$', r'\g<1>',
                                                                                      regex=True)
    # drop rows that have been filled in
    added_cols = [prefix + first_name_col, prefix + last_name_col, prefix + middle_name_col]
    person_df2 = person_df[~person_df[added_cols].notna().any(1)]
    person_df1 = person_df[person_df[added_cols].notna().any(1)]
    person_df1['parsed_from'] = 'already_parsed'
    df_list.append(person_df1)

    # # format = last name,? first name middle name?
    split1 = person_df2[new_name_col].str.extract(
        r'^' + NOT_MI + r'\s?,?\s' + NOT_MI + r'\s?' + ANY_NAME +r'?$'
    )
    # assign names based on format_col
    # this deals w/ cases where counties either put first name last name or last name first name

    # format is first name last name
    person_df2[prefix+first_name_col] = np.where((person_df2[format_col_new]=='firstName_lastName'),
                                               split1[0],
                                               person_df2[first_name_col]
                                               )
    person_df2[prefix+middle_name_col] = np.where( (person_df2[format_col_new]=='firstName_lastName'),
                                               split1[2],
                                               person_df2[middle_name_col]
                                               )
    person_df2[prefix + last_name_col] = np.where( (person_df2[format_col_new]=='firstName_lastName'),
                                                   split1[1],
                                                   person_df2[last_name_col])


    # format is last name first name
    person_df2[prefix + first_name_col] = np.where(
        (person_df2[format_col_new] == 'lastName_firstName'),
        split1[1],
        person_df2[prefix +first_name_col]
        )
    person_df2[prefix + middle_name_col] = np.where(
        (person_df2[format_col_new] == 'lastName_firstName'),
        split1[2],
        person_df2[prefix +middle_name_col]
        )
    person_df2[prefix + last_name_col] = np.where(
        (person_df2[format_col_new] == 'lastName_firstName'),
        split1[0],
        person_df2[prefix +last_name_col]
        )
    # drop rows that have been filled in
    person_df3 = person_df2[~person_df2[added_cols].notna().any(1)]
    person_df2 = person_df2[person_df2[added_cols].notna().any(1)]
    person_df2['parsed_from'] = 'n_n?_n'
    df_list.append(person_df2)

    # format = first name middle INITIAL last name
    split2 = person_df3[new_name_col].str.extract(
        r'^' + ANY_NAME + r'\s' +MI + r'\s' + ANY_NAME + '$'
    )

    person_df3[prefix + middle_name_col].fillna(split2[1], inplace=True)
    person_df3[prefix+first_name_col].fillna(split2[0], inplace=True)
    person_df3[prefix + last_name_col].fillna(split2[2], inplace=True)
    person_df4= person_df3[~person_df3[added_cols].notna().any(1)]
    person_df3 = person_df3[person_df3[added_cols].notna().any(1)]
    person_df3['parsed_from'] = 'fn_mi_ln'
    df_list.append(person_df3)


    ######### parse uncommon formats
    # format is de/la/al/el/von + last name + first name + middle name/initial
    split3 = person_df4[new_name_col].str.extract(
        r'^' + r'((al|del?|el|von|la|le|dos|van|mc|di|san|st|da)\sl?a?\s?[a-z-]+)' + r',?\s' + NOT_MI + r'\s?' + ANY_NAME + '?$'
    )
    person_df4[prefix + last_name_col] = split3[0]
    person_df4[prefix + middle_name_col] = split3[3]
    person_df4[prefix + first_name_col] = split3[2]
    person_df5 = person_df4[~person_df4[added_cols].notna().any(1)]
    person_df4 = person_df4[person_df4[added_cols].notna().any(1)]
    person_df4['parsed_from'] = 'el_ln_fn_mn'
    df_list.append(person_df4)

    # format is ln fn mn (two names) eg artica carlos bonilla mercedes (need to classify names based on format_col)
    # note this will incorrectly classify people w/ two last names eg. artica mercedes carlos bonilla
    split4 = person_df5[new_name_col].str.extract(
        r'^' + NOT_MI + r',?\s' + NOT_MI + r'\s?' + r'([a-z\s]+)' + '$'
    )
    # format is first name last name
    person_df5[prefix + first_name_col] = np.where((person_df5[format_col_new] == 'firstName_lastName'),
                                                   split4[0],
                                                   person_df5[first_name_col]
                                                   )
    person_df5[prefix + middle_name_col] = np.where((person_df5[format_col_new] == 'firstName_lastName'),
                                                    split4[2],
                                                    person_df5[middle_name_col]
                                                    )
    person_df5[prefix + last_name_col] = np.where((person_df5[format_col_new] == 'firstName_lastName'),
                                                  split4[1],
                                                  person_df5[last_name_col])

    # format is last name first name
    person_df5[prefix + first_name_col] = np.where(
        (person_df5[format_col_new] == 'lastName_firstName'),
        split4[1],
        person_df5[prefix + first_name_col]
    )
    person_df5[prefix + middle_name_col] = np.where(
        (person_df5[format_col_new] == 'lastName_firstName'),
        split4[2],
        person_df5[prefix + middle_name_col]
    )
    person_df5[prefix + last_name_col] = np.where(
        (person_df5[format_col_new] == 'lastName_firstName'),
        split4[0],
        person_df5[prefix + last_name_col]
    )

    person_df6 = person_df5[~person_df5[added_cols].notna().any(1)]
    person_df5 = person_df5[person_df5[added_cols].notna().any(1)]
    person_df5['parsed_from'] = 'n_n_nn'
    df_list.append(person_df5)

    # format is mi
    # parse where format is last name middle initial, first name
    split6 = person_df6[new_name_col].str.extract(
        r'^' + NOT_MI + r'\s' + ANY_NAME + r',' + ANY_NAME
    )
    person_df6[prefix + last_name_col] = split6[0]
    person_df6[prefix + middle_name_col] = split6[1]
    person_df6[prefix + first_name_col] = split6[2]
    person_df7 = person_df6[~person_df6[added_cols].notna().any(1)]
    person_df6 = person_df6[person_df6[added_cols].notna().any(1)]
    person_df6['parsed_from'] = 'ln_mi_fn'
    df_list.append(person_df6)

    # parse where format is mi first name last name
    split7 = person_df7[new_name_col].str.extract(
        r'^' + MI + r'\s' + ANY_NAME + r',' + ANY_NAME
    )
    person_df7[prefix + last_name_col] = split7[2]
    person_df7[prefix + middle_name_col] = split7[0]
    person_df7[prefix + first_name_col] = split7[1]
    person_df_not_parsed = person_df7[~person_df7[added_cols].notna().any(1)]
    person_df7 = person_df7[person_df7[added_cols].notna().any(1)]
    person_df7['parsed_from'] = 'mi_fn_ln'
    df_list.append(person_df7)

    person_df_not_parsed['parsed_from'] = 'not parsed'
    df_list.append(person_df_not_parsed)

    # concatenate df_list back into person_df
    person_df = pd.concat(df_list)
    # person_df.drop_duplicates(subset=[prefix+first_name_col, prefix+last_name_col, prefix+middle_name_col, name_col], inplace=True)
    # create the auxillary collumns (alphabetized, best guess, etc).
    combine_names(person_df, [prefix+first_name_col, prefix+last_name_col], newCol=prefix+name_col)

    person_df[prefix + name_col] = person_df[prefix + name_col].replace(r'^\s+$', np.nan,regex=True)
    person_df[prefix + name_col].fillna(person_df[new_name_col], inplace=True)
    person_df[prefix + name_col].fillna(person_df[name_col], inplace=True)
    person_df[prefix + name_col] = person_df[prefix + name_col].str.replace(' {2,}', ' ')
    person_df[prefix + name_col] = person_df[prefix + name_col].str.strip()
    # drop na rows
    # create alphabetized name column
    person_df['splitCol'] = person_df[prefix + name_col].str.split(' ')
    person_df['splitCol'] = person_df['splitCol'].sort_values().apply(lambda x: sorted(x))
    person_df[alphabetized_col] = person_df['splitCol'].str.join(' ')
    person_df[alphabetized_col] = person_df[alphabetized_col].str.replace(' {2,}', ' ')
    person_df[alphabetized_col] = person_df[alphabetized_col].str.strip()
    person_df.drop(columns=['splitCol', new_name_col, format_col_new], inplace=True)
    person_df['business_proper_name_alphabetized'] = person_df[alphabetized_col]
    # create partial name flag

    person_df['partial_name'] = np.where(
        (person_df[prefix + name_col].str.contains(r'\s')),
        'full_name',
        'partial_name'
    )

    person_df_full = person_df_full.merge(person_df, on=[name_col, first_name_col, last_name_col], how='left')
    final_shape = person_df_full.shape[0]
    if initial_shape != final_shape:
        write_to_log('initial shape is {} and final shape is {}'.format(initial_shape, final_shape))

        raise ValueError('Initial and final shapes do not agree!')
    return person_df_full

def parse_name(df, name_col, firstName_col, lastName_col, initial_col,
               suffix_col, profession_col, alphabetized_col, business_main_type_col, business_sub_type_col,numbers_format,
                   business_short_name_col, typeCol, prefix='parsed', id_col=False, split_name=False, format_col=None):
    begin_time = time.time()
    if df.shape[0]==0:
        return df
    df.sort_values(name_col, inplace=True)
    classify_name(df, name_cols=[name_col],type_col=typeCol)
    # fill na full names w/ concatenation of first and last name
    combine_names(df, name_cols=[firstName_col, lastName_col, initial_col], newCol=name_col)
    # split dataframe into two based on typeCol
    business_df = df[df[typeCol]=='business']
    person_df = df[df[typeCol]=='person']

    del df
    # parse person names
    if split_name is not False:
        person_df = parse_and_split(dataframe=person_df, name_col=name_col, firstName_col=firstName_col, lastName_col=lastName_col,
                                    initial_col=initial_col, type_col=typeCol, id_col=id_col, suffix_col=suffix_col)

    business_df = parse_business(df=business_df,
                                 business_name_col=name_col,
                                 business_main_type_col=business_main_type_col,
                                 business_short_name_col=business_short_name_col,
                                 business_sub_type_col=business_sub_type_col,
                                 alphabetized_col=alphabetized_col,
                                 prefix=prefix,
                                 numbers_format=numbers_format
                                 )
    person_df = parse_person(person_df=person_df, name_col=name_col, first_name_col=firstName_col, last_name_col=lastName_col,
                                suffix_col=suffix_col, profession_col=profession_col, alphabetized_col=alphabetized_col,
                                middle_name_col=initial_col, prefix=prefix, format_col=format_col
                           )
    person_df = pd.concat([person_df, business_df])
    person_df[business_main_type_col].fillna('person', inplace=True)
    # flag common names
    person_df['count_names'] = person_df.groupby(prefix + name_col)[business_main_type_col].transform(np.size)
    person_df['common_names'] = np.where(person_df['count_names'] > 10000,
                                  1,
                                  0)
    return person_df

def parse_and_split(dataframe, name_col, firstName_col, lastName_col, initial_col,
                    suffix_col, id_col, type_col, prefix='split_'):
    begin_time = time.time()
    if prefix is not False:
        firstName_col = prefix + firstName_col
        lastName_col = prefix + lastName_col
        initial_col = prefix + initial_col
        suffix_col = prefix +suffix_col
    firstName1 = firstName_col + '1'
    firstName2 = firstName_col + '2'
    firstName3 = firstName_col + '3'
    firstName4 = firstName_col + '4'
    lastName1 = lastName_col + '1'
    lastName2 = lastName_col + '2'
    lastName3 = lastName_col + '3'
    lastName4 = lastName_col + '4'
    middleName1 = initial_col + '1'
    middleName2 = initial_col + '2'
    middleName3 = initial_col + '3'
    middleName4 = initial_col + '4'
    # fullName1 = name_col + '1'
    # fullName2 = name_col + '2'
    # fullName3 = name_col + '3'
    # fullName4 = name_col + '4'
    suffixName1 = suffix_col + '1'
    suffixName2 = suffix_col + '2'
    suffixName3 = suffix_col + '3'
    suffixName4 = suffix_col + '4'

    dataframe.sort_values(name_col, inplace=True)
    classify_name(dataframe, name_cols=[name_col], type_col=type_col)
    dataframe_business = dataframe[dataframe[type_col] == 'business']
    dataframe = dataframe[dataframe[type_col] == 'person']
    for col in [name_col]:
        if pd.api.types.is_string_dtype(dataframe[col]):
            dataframe[col] = dataframe[col].str.replace('^(.+)\s(i?r?revocable .+$|living tru?s?t?|'
                                                        'irrev tr$|irv$|liv tr$|'
                                                        'per rep$|trust)',
                                                                            r'\g<1>', flags=re.IGNORECASE)
            dataframe[col] = dataframe[col].str.replace(' {2,}', ' ')
            dataframe[col] = dataframe[col].str.strip()
            dataframe[col] = dataframe[col].str.replace(
                '^(.+)\s(revocable living tru?s?t?|living tru?s?t?|revocable liv$|'
                'i?r?revocable ?tru?s?t?$|irrev tr$|irv$|liv tr$|i?r?revocable ?t?r?$|'
                'revocable living|per rep$)',
                r'\g<1>', flags=re.IGNORECASE)
            dataframe[col] = dataframe[col].str.replace('^(.+)\s(tru?s?t?e?e?s?$|jt|lt|t\se$|for life|el ?als?|j t r s|life es?t?a?t?e?|est$)',
                                                        r'\g<1>', flags=re.IGNORECASE)
            dataframe[col] = np.where(dataframe[col].str.count(' ')> 1,
                                      dataframe[col].str.replace('\b(he|le)\b', r''), dataframe[col])
            dataframe[col] = dataframe[col].str.replace(' {2,}', ' ')
            dataframe[col] = dataframe[col].str.strip()
            dataframe[col] = dataframe[col].str.replace(r'^(est)\s(.+)$',
                                                        r'\g<2>', flags=re.IGNORECASE)
            # print('{} is string type'.format(col))
    dataframe1 = dataframe[[name_col]]
    dataframe1.drop_duplicates(subset=name_col, inplace=True)
    added_cols = [firstName1, firstName2, firstName3, #firstName4,
                  lastName1, lastName2, lastName3, #lastName4,
                  middleName1, middleName2, middleName3,# middleName4,
                  # fullName1, fullName2, fullName3, #fullName4,
                  suffixName1, suffixName2, suffixName3, #suffixName4
                  ]
    for col in added_cols:
        if col not in dataframe1.columns:
            dataframe1[col] = np.nan
    df_list = []

    # Full Name and Partial Name Cases
    # extract where format is last name first name mi? suffix? & first name middle INITIAL?

    split4 = dataframe1[name_col].str.extract(r'^' + ANY_NAME + r'[\s,]{1,3}' + NOT_MI + r'\s?' + NOT_SUFFIX + '?' + r'\s?' + SUFFIX +'?' + r'[\s,&]{3,5}' +
                                       ANY_NAME + r'\s?' + MI + r'?\s?' + SUFFIX + '?$', expand=True, flags=re.IGNORECASE)

    # assign capture groups back to original dataframe
    dataframe1[lastName1] = split4[0]
    dataframe1[firstName1] = split4[1]
    dataframe1[middleName1] = split4[2]
    dataframe1[suffixName1] = split4[3]
    dataframe1[firstName2] = split4[4]
    dataframe1[middleName2] = split4[5]
    dataframe1[lastName2] = split4[0]
    dataframe1[suffixName2] = split4[6]
    # drop rows that have been filled in
    dataframe2 = dataframe1[~dataframe1[added_cols].notna().any(1)]
    dataframe1 = dataframe1[dataframe1[added_cols].notna().any(1)]
    df_list.append(dataframe1)

    # # extract where format is last name first name mi? suffix? & AMBIGUOUS FIRST/LAST/MIDDLE NAMES
    split5 = dataframe2[name_col].str.extract(
                                r'^' + ANY_NAME + r'[\s,]{1,3}' + NOT_MI + r'\s' + NOT_SUFFIX + r'\s?' + SUFFIX +'?' + r'[\s,&]{3,5}' +
                                ANY_NAME + r'\s?' + NOT_MI + r'\s?' + SUFFIX + '?$', expand=True, flags=re.IGNORECASE)
    dataframe2[lastName1] = split5[0]
    dataframe2[firstName1] = split5[1]
    dataframe2[middleName1] = split5[2]
    dataframe2[suffixName1] = split5[3]
    split5.rename(columns = {4:'first_word', 5:'second_word'}, inplace=True)
    dataframe2['classification'] = np.where((split5['first_word'].isna()==False) & (split5['second_word'].isna()==False),
                                      split5.apply(lambda x: classify_ambiguous_name(x['first_word'], x['second_word']), axis=1),
                                      np.nan)
    # np.where((split5[4].isna()==False) & (split5[5].isna()==False),
    #                                   split5.apply(lambda x: classify_ambiguous_name(x.iloc[:,4], x.iloc[:,5]), axis=1),
    #                                   np.nan
    #                                   )
    dataframe2[firstName2] = np.where(dataframe2['classification']=='first_last',
                                split5['first_word'],
                                split5['second_word']
                                )
    dataframe2[lastName2] = np.where(dataframe2['classification']=='last_first',
                                split5['first_word'],
                                split5['second_word']
                                )
    # dataframe2[lastName2] = split5[0]
    dataframe2[suffixName2] = split5[6]

    # drop rows that have been filled in
    dataframe3 = dataframe2[~dataframe2[added_cols].notna().any(1)]
    dataframe2 = dataframe2[dataframe2[added_cols].notna().any(1)]
    df_list.append(dataframe2)

    # if first and second word are both first names (or unknown) -> assign first word to first name, 2nd to middle
        # if one of first and second words are first name, assign that word to first name, other to middle

    # extract where format is first name middle INITIAL last name & first name middle INITIAL?
    split8 = dataframe3[name_col].str.extract(r'^' + ANY_NAME +r'[\s,]{1,3}' + MI + r'\s' + ANY_NAME + r'\s?' + SUFFIX + '?' + r'[\s,&]{3,5}'+
                                        ANY_NAME + r'\s?' + MI + r'?\s?' + SUFFIX + '?$'
                                       , expand=True, flags=re.IGNORECASE)

    dataframe3[firstName1] = split8[0]
    dataframe3[middleName1] = split8[1]
    dataframe3[lastName1] = split8[2]
    dataframe3[suffixName1] = split8[3]
    dataframe3[firstName2] = split8[4]
    dataframe3[middleName2] = split8[5]
    dataframe3[lastName2] = split8[2]
    dataframe3[suffixName2] = split8[6]

    # drop rows that have been filled in
    dataframe4 = dataframe3[~dataframe3[added_cols].notna().any(1)]
    dataframe3 = dataframe3[dataframe3[added_cols].notna().any(1)]
    df_list.append(dataframe3)
    # extract where format is first name middle INITIAL last name suffix? & AMBIGUOUS FIRST/LAST/MIDDLE NAMES
    split9 = dataframe4[name_col].str.extract(r'^' + ANY_NAME +r'[\s,]{1,3}' + MI + r'\s' + ANY_NAME + r'\s?' + SUFFIX + '?' + r'[\s,&]{3,5}'+
                                        ANY_NAME + r'\s?' + NOT_MI + r'?\s?' + SUFFIX + '?$'
                                       , expand=True, flags=re.IGNORECASE)

    dataframe4[firstName1] = split9[0]
    dataframe4[middleName1] = split9[1]
    dataframe4[lastName1] = split9[2]
    dataframe4[suffixName1] = split9[3]

    split9.rename(columns = {4:'first_word', 5:'second_word'}, inplace=True)
    dataframe4['classification'] = np.where((split9['first_word'].isna()==False) & (split9['second_word'].isna()==False),
                                      split9.apply(lambda x: classify_ambiguous_name(x['first_word'], x['second_word']), axis=1),
                                      np.nan)
    dataframe4[firstName2] = np.where(dataframe4['classification']=='first_middle',
                                split9['first_word'],
                                split9['second_word']
                                )
    dataframe4[middleName2] = np.where(dataframe4['classification']=='middle_first',
                                split9['first_word'],
                                split9['second_word']
                                )
    dataframe4[lastName2] = split9[2]
    dataframe4[suffixName2] = split9[6]
    # drop rows that have been filled in
    dataframe7 = dataframe4[~dataframe4[added_cols].notna().any(1)]
    dataframe4 = dataframe4[dataframe4[added_cols].notna().any(1)]
    df_list.append(dataframe4)

    # Full Name & Full Name cases

    # extract where format is last name,? first name middle name &/, last name first name middle name

    split12 = dataframe7[name_col].str.extract('^'+ANY_NAME+r'[\s,]{1,3}'+NOT_MI+r'\s?'+NOT_SUFFIX + r'?\s?' + SUFFIX + '?'+ r'[\s,&]{3,5}' +
                                        ANY_NAME+r'[\s,]{1,3}'+NOT_MI+r'\s?'+NOT_SUFFIX + r'?\s?' + SUFFIX + '?'+ '$', expand=True, flags=re.IGNORECASE
                                        )

    dataframe7[lastName1] = split12[0]
    dataframe7[firstName1] = split12[1]
    dataframe7[middleName1] = split12[2]
    dataframe7[suffixName1] = split12[3]
    dataframe7[lastName2] = split12[4]
    dataframe7[firstName2] = split12[5]
    dataframe7[middleName2] = split12[6]
    dataframe7[suffixName2] = split12[7]

    # drop rows that have been filled in
    dataframe8= dataframe7[~dataframe7[added_cols].notna().any(1)]
    dataframe7= dataframe7[dataframe7[added_cols].notna().any(1)]
    df_list.append(dataframe7)
    # format is last name,? first name suffix? &/, first name middle INITIAL last name suffix?
    split13 = dataframe8[name_col].str.extract('^'+ANY_NAME+r'[\s,]{1,3}'+ANY_NAME+r'\s?'+NOT_SUFFIX + r'?\s?' + SUFFIX + '?'+ r'[\s,&]{3,5}' +
                                         ANY_NAME +r'[\s,]{1,3}' + MI + r'\s' + ANY_NAME + r'\s?' + SUFFIX + '?'+ '$', expand=True, flags=re.IGNORECASE
                                        )
    dataframe8[lastName1] = split13[0]
    dataframe8[firstName1] = split13[1]
    dataframe8[middleName1] = split13[2]
    dataframe8[suffixName1] = split13[3]
    dataframe8[lastName2] = split13[6]
    dataframe8[firstName2] = split13[4]
    dataframe8[middleName2] = split13[5]
    dataframe8[suffixName2] = split13[7]

    # drop rows that have been filled in
    dataframe9= dataframe8[~dataframe8[added_cols].notna().any(1)]
    dataframe8= dataframe8[dataframe8[added_cols].notna().any(1)]
    df_list.append(dataframe8)

    # More than two names
    # format is last name,? first name middle name suffix & first name middle INITIAL? suffix? & first name middle INITIAL? suffix?
    # ^([a-z-]+)[\s,]{1,3}([a-z-]+)\s?([a-z-]{3,}|[a-z-]{1})?\s?(jr|sr|i{2,}|iv)?[\s,&]{2,5}([a-z-]+)\s([a-z-]{1})?\s?(jr|sr|i{2,}|iv)?[\s,&]{2,5}([a-z-]+)\s?([a-z-]{1})?\s?(jr|sr|i{2,}|iv)?($|[\s,&]{2,5})([a-z-]+)?\s?([a-z-]{1})?$

    split14 =dataframe9[name_col].str.extract('^' + ANY_NAME + r'[\s,]{1,3}' + ANY_NAME + r'\s?' + NOT_SUFFIX + r'?\s?' + SUFFIX + r'?[\s,&]{2,5}' +
                            ANY_NAME+ r'\s' + MI + r'?\s?' + SUFFIX + r'?[\s,&]{2,5}' +
                            ANY_NAME + r'\s?' + MI + r'?\s?' + SUFFIX + r'?($|[\s,&]{2,5})' +
                            ANY_NAME + r'?\s?' + MI + r'?\s?' + SUFFIX + '?$'
                                       , expand=True, flags=re.IGNORECASE)

    dataframe9[lastName1] = split14[0]
    dataframe9[firstName1] = split14[1]
    dataframe9[middleName1] = split14[2]
    dataframe9[suffixName1] = split14[3]
    dataframe9[lastName2] = split14[0]
    dataframe9[firstName2] = split14[4]
    dataframe9[middleName2] = split14[5]
    dataframe9[suffixName2] = split14[6]
    dataframe9[lastName3] = split14[0]
    dataframe9[firstName3] = split14[7]
    dataframe9[middleName3] = split14[8]
    dataframe9[suffixName3] = split14[9]
    # dataframe9[lastName4] = split14[0]
    # dataframe9[firstName4] = split14[11]
    # dataframe9[middleName4] = split14[12]
    # dataframe9[suffixName4] = split14[13]

    # drop rows that have been filled in
    dataframe10= dataframe9[~dataframe9[added_cols].notna().any(1)]
    dataframe9= dataframe9[dataframe9[added_cols].notna().any(1)]
    df_list.append(dataframe9)

    # format is last name, first name, middle name, suffix & FULL NAME & FULL NAME &? FULL NAME? (allow for final name to be cut off)
    split15 = dataframe10[name_col].str.extract(
                                    '^'+ANY_NAME+r'[\s,]{1,3}'+ANY_NAME+r'\s?'+NOT_SUFFIX + r'?\s?' + SUFFIX + '?' + r'[\s,&]{3,5}' +
                                        ANY_NAME+r'[\s,]{1,3}'+NOT_MI+r'\s?'+NOT_SUFFIX + '?' + r'\s?' + SUFFIX  + '?'+ r'[\s,&]{3,5}' +
                                        ANY_NAME +r'([\s,]{1,3})?'+ NOT_MI +r'?\s?'+NOT_SUFFIX + '?' + r'\s?' + SUFFIX + '?' + r'([\s,&]{2,5})?' +
                                        ANY_NAME+ '?'+r'([\s,]{1,3})?'+NOT_MI+ '?'+r'\s?'+NOT_SUFFIX + r'?\s'+ '?' + SUFFIX + '?'+ '$'
                                        , expand=True, flags=re.IGNORECASE
                                        )
    dataframe10[lastName1] = split15[0]
    dataframe10[firstName1] = split15[1]
    dataframe10[middleName1] = split15[2]
    dataframe10[suffixName1] = split15[3]
    dataframe10[lastName2] = split15[4]
    dataframe10[firstName2] = split15[5]
    dataframe10[middleName2] = split15[6]
    dataframe10[suffixName2] = split15[7]
    dataframe10[lastName3] = split15[8]
    dataframe10[firstName3] = split15[10]
    dataframe10[middleName3] = split15[11]
    dataframe10[suffixName3] = split15[12]
    # dataframe10[lastName4] = split15[14]
    # dataframe10[firstName4] = split15[16]
    # dataframe10[middleName4] = split15[17]
    # dataframe10[suffixName4] = split15[18]

    # drop rows that have been filled in
    dataframe11= dataframe10[~dataframe10[added_cols].notna().any(1)]
    dataframe10 = dataframe10[dataframe10[added_cols].notna().any(1)]
    df_list.append(dataframe10)

    # extract where format is last name first name mi? suffix? & middle INITIAL first name

    split6 = dataframe11[name_col].str.extract(r'^' + ANY_NAME + r'[\s,]{1,3}' + NOT_MI + r'\s?' + NOT_SUFFIX + '?' + r'\s?' + SUFFIX +'?' + r'[\s,&]{3,5}' +
                                       MI + r'\s?' + ANY_NAME + r'\s?' + SUFFIX + '?$', expand=True, flags=re.IGNORECASE)
    # assign capture groups back to original dataframe
    dataframe11[lastName1] = split6[0]
    dataframe11[firstName1] = split6[1]
    dataframe11[middleName1] = split6[2]
    dataframe11[suffixName1] = split6[3]
    dataframe11[firstName2] = split6[5]
    dataframe11[middleName2] = split6[4]
    dataframe11[lastName2] = split6[0]
    dataframe11[suffixName2] = split6[6]

    # drop rows that have been filled in
    dataframe12 = dataframe11[~dataframe11[added_cols].notna().any(1)]
    # print('nrows df12 {}'.format(dataframe12.shape[0]))
    dataframe11 = dataframe11[dataframe11[added_cols].notna().any(1)]
    df_list.append(dataframe11)

    # # extract where format is first name & first name last name
    split7 = dataframe12[name_col].str.extract(r'^' + ANY_NAME + r'\s?' + MI + '?' + r'[\s,&]{3,5}' + ANY_NAME + r'\s' +
                                               ANY_NAME + '$', expand=True, flags=re.IGNORECASE)
    # assign capture groups back to original dataframe
    dataframe12[lastName1] = split7[3]
    dataframe12[firstName1] = split7[0]
    dataframe12[middleName1] = split7[1]
    dataframe12[firstName2] = split7[2]
    dataframe12[lastName2] = split7[3]

    # drop rows that have been filled in
    unparsed_names = dataframe12[~dataframe12[added_cols].notna().any(1)]
    dataframe12 = dataframe12[dataframe12[added_cols].notna().any(1)]
    df_list.append(dataframe12)

    parsed_names = pd.concat(df_list)
    parsed_names.dropna(subset=[name_col], inplace=True)

    parsed_names['was_parsed'] = 1
    # concatenate un parsed names
    parsed_names = pd.concat([parsed_names, unparsed_names])
    dropped = dataframe[~dataframe[name_col].isin(parsed_names[name_col])].shape[0]
    # ('num dropped names is {}'.format(dropped))
    # append names back to original dataframe
    parsed_names_merge = pd.merge(dataframe, parsed_names, on=name_col, how='left')
    parsed_names_merge['keep_these'] = np.where(parsed_names_merge['was_parsed']==1,
                                                np.nan,
                                                1)
    # reshape and then parse singular names
    df_reshape = pd.wide_to_long(parsed_names_merge, stubnames=[
        firstName_col,
        lastName_col,
        initial_col,
        suffix_col
    ], i=id_col, j='name_number')
    df_reshape.reset_index(inplace=True)
    df_reshape.sort_values(name_col, inplace=True)
    if df_reshape.shape[0] == 0:
        raise ValueError('There are no rows in the dataframe post reshape! Something has gone terribly awry!')
    df_reshape.dropna(subset=[firstName_col, lastName_col, initial_col,  'keep_these'],how='all', inplace=True)
    df_reshape.drop_duplicates(subset=[firstName_col, lastName_col, initial_col, suffix_col, id_col], inplace=True)
    df_reshape.drop(columns=['was_parsed', 'keep_these'], inplace=True)
    # print(set([x for x in list(df_reshape.columns) if list(df_reshape.columns).count(x) > 1]))
    # print(set([x for x in list(dataframe_business.columns) if list(dataframe_business.columns).count(x) > 1]))
    df_reshape = pd.concat([df_reshape, dataframe_business])
    return df_reshape


def clean_business_name(df, business_name_col, numbers_format=False):
    # df[business_name_col] = df[business_name_col].apply(remove_commas)
    df[business_name_col] = df[business_name_col].str.replace(r',|-|:|;|\?|\.', ' ')
    df[business_name_col] = df[business_name_col].str.replace(r'(\s)(no\s|number\s|unit\s)([0-9]+)', r'g<1>#\g<3>')
    df[business_name_col] = df[business_name_col].str.replace(r'(\b)(the)(\b)', r'\g<1>')
    df[business_name_col] = df[business_name_col].str.replace(r'\s{2,}', ' ')
    df[business_name_col] = df[business_name_col].str.replace(r'\b([a-z])\s([a-z])\b', r' \g<1>\g<2> ')
    df[business_name_col] = df[business_name_col].str.replace(r'\b(tr)\b', r' trust ')
    df[business_name_col] = df[business_name_col].str.replace(r'\s{2,}', ' ')
    df[business_name_col] = df[business_name_col].str.replace(r'\b(mort|mrtg|mrtge|mtge)\b', 'mortgage')
    df[business_name_col] = df[business_name_col].str.replace(r'(.+)\s(#?[0-9]+)$', r'\g<1>')
    df[business_name_col] = df[business_name_col].str.strip()
    # replace empty strings w/ np.nan
    df[business_name_col] = df[business_name_col].replace(r'^(\s+)?$', np.nan)
    df[business_name_col].fillna('', inplace=True)
    if numbers_format is not False:
        # conditionally move numbers to the front i.e blue ave trust 112 -> 112 blue ave trust
        df[business_name_col] = np.where(
            (df[numbers_format] == 'numbers_last') & (df[business_name_col].str.contains(r'\s([0-9]{2,}|[0-9]+\s[0-9]+)$')),
            df[business_name_col].str.replace(r'^(.+)\s([0-9]+[\s-]{1,3}[0-9]+)$', r'\g<2> \g<1>'),
            df[business_name_col]
        )
        df[business_name_col] = np.where(
            (df[numbers_format] == 'numbers_last') & (df[business_name_col].str.contains(r'\s([0-9]{2,}|[0-9]+\s[0-9]+)$')),
            df[business_name_col].str.replace(r'^(.+)\s([0-9]+)$', r'\g<2> \g<1>'),
            df[business_name_col]
    )
    df[business_name_col] = df[business_name_col].replace(r'\s{2,}', ' ')
    df[business_name_col] = df[business_name_col].replace(r'^(\s+)?$', np.nan)
    return df


def make_business_main_type_col(df, business_main_type_col, business_name_col):

    df[business_main_type_col] = np.where(df[business_name_col].str.contains(
        pat=r'\b(corp|inco?r?p?o?r?a?t?e?d?|i?n?corpo?r?a?t?i?o?n?|inc)\b', flags=re.IGNORECASE, na=False),
        'corporation',
        None
    )

    df[business_main_type_col] = np.where(
        (df[business_name_col].str.contains(pat=r'\b(llc)\b', flags=re.IGNORECASE, na=False)) & (
            df[business_main_type_col].isna()),
        'llc',
        df[business_main_type_col]
    )
    df[business_main_type_col] = np.where(
        (df[business_name_col].str.contains(
            pat=r'\b(ll?p|limited\spartnership|association|asso?c?i?a?t?e?s?|limited)\b', flags=re.IGNORECASE,
            na=False)) & (df[business_main_type_col].isna()),
        'partnership/association',
        df[business_main_type_col]
    )
    df[business_main_type_col] = np.where((df[business_name_col].str.contains(
        pat=r'\b(gov|government|commonwealth of ma|us postal)\b', flags=re.IGNORECASE, na=False)) & (
                                              df[business_main_type_col].isna()),
                                          'state or federal government',
                                          df[business_main_type_col]
                                          )
    df[business_main_type_col] = np.where((df[business_name_col].str.contains(
        pat=r'\b(condom?i?n?i?u?m? trust?|master\sdeed|condo)\b', flags=re.IGNORECASE, na=False)) & (
                                              df[business_main_type_col].isna()),
                                          'condo trust',
                                          df[business_main_type_col]
                                          )

    df[business_main_type_col] = np.where((df[business_name_col].str.contains(
        pat=r'\b(trust?|irrevocable|revocable|living|nominee|tr)\b', flags=re.IGNORECASE, na=False)) & (
                                              df[business_main_type_col].isna()),
                                          'trust',
                                          df[business_main_type_col]
                                          )

    df[business_main_type_col] = np.where((df[business_name_col].str.contains(
        pat=r'\b((city|town) of|^[a-z]+\s([a-z]\s)?(city|town)$)\b', flags=re.IGNORECASE, na=False)) & (
                                              df[business_main_type_col].isna()),
                                          'municipality',
                                          df[business_main_type_col]
                                          )
    df[business_main_type_col] = np.where((df[business_name_col].str.contains(
        pat=r'\b(housing\sauthority|r?e?development|chamber\sof\scommerce)\b', flags=re.IGNORECASE, na=False)) & (
                                              df[business_main_type_col].isna()),
                                          'quasi government',
                                          df[business_main_type_col]
                                          )

    df[business_main_type_col] = np.where((df[business_name_col].str.contains(
        pat=r'\b(family)\b', flags=re.IGNORECASE, na=False)) & (
                                              df[business_main_type_col].isna()),
                                          'trust',
                                          df[business_main_type_col]
                                          )
    df[business_main_type_col].fillna('other', inplace=True)
    return df


def make_business_sub_type_col(df, business_sub_type_col, business_name_col):
    df[business_sub_type_col] = np.where((df[business_name_col].str.contains(
        pat=r'\b(church|baptist|methodist|protestant|jewish|episcopal|buddhist)\b', flags=re.IGNORECASE, na=False)),
        'religious',
        np.nan
    )
    df[business_sub_type_col] = np.where(
        (df[business_name_col].str.contains(pat=r'|'.join(bank_names), flags=re.IGNORECASE, na=False)),
        'bank',
        df[business_sub_type_col]
    )
    df[business_sub_type_col] = np.where((df[business_name_col].str.contains(
        pat=r'\b(realty|real estate)\b', flags=re.IGNORECASE, na=False)) & (
                                             df[business_sub_type_col].isna()),
                                         'realty',
                                         df[business_sub_type_col]
                                         )
    return df

def make_business_short_name_col(df, business_short_name_col, business_name_col, business_sub_type_col, bank_names):
    df[business_short_name_col] = np.nan
    df[business_short_name_col].fillna(df[business_name_col].str.extract(
        r'(.+)(\sof(boston|ma|new\sengland|lowell|medford|chelsea))',  # make sure to add more city names
        flags=re.IGNORECASE).iloc[:, 0], inplace=True)
    df[business_short_name_col].fillna(df[business_name_col], inplace=True)
    # consolodate bank names i.e bank of america & bank of america na
    df[business_short_name_col] = np.where(df[business_sub_type_col] == 'bank',
                                           df[business_name_col].str.extract('(' +
                                                                             r'|'.join(bank_names) + ')'
                                                                             ).iloc[:, 0],
                                           df[business_short_name_col])
    df[business_short_name_col] = df[business_short_name_col].str.replace(
        r'^(.+)\s([0-9]{1,4}[a-z]?|ii?i?v?|vi?i?i?|xi?i?|one|two|'
        r'three|four|five|six|seven|eight|nine|ten|et\s?al)$', r'\g<1>')
    # remove leading and trailing business words like llc
    df[business_short_name_col] = df[business_short_name_col].str.replace(
        r'\b(inc|incorporated|(living)?\s?((ir)?rev(ocable)?\s|living\s)?trust|llc|lp|corporate|corp(oration)?|'
        r'estate of power|(life\s?)?estate(\sof)?|co-?op[rative]{5,9}|trust|(city|town)\sof|limited|llp|partnership|company)$',
        "",
        flags=re.IGNORECASE)
    df[business_short_name_col] = df[business_short_name_col].str.replace(
        r'^(inc|incorporated|(living)?\s?((ir)?rev(ocable)?\s|living\s)?trust|llc|lp|corporate|corp(oration)?|'
        r'estate of power|(life\s?)?estate(\sof)?|co-?op[rative]{5,9}|trust|(city|town)\sof|limited|llp|partnership|company)\b',
        "",
        flags=re.IGNORECASE)
    # repeat one more time for stuff like corporation llc
    df[business_short_name_col] = df[business_short_name_col].str.replace(
        r'\b(inc|incorporated|(living)?\s?((ir)?rev(ocable)?\s|living\s)?trust|llc|lp|corporate|corp(oration)?|'
        r'estate of power|(life\s?)?estate(\sof)?|co-?op[rative]{5,9}|trust|(city|town)\sof|limited|llp|partnership|company)$',
        "",
        flags=re.IGNORECASE)
    df[business_short_name_col] = df[business_short_name_col].str.replace(
        r'^(inc|incorporated|(living)?\s?((ir)?rev(ocable)?\s|living\s)?trust|llc|lp|corporate|corp(oration)?|'
        r'estate of power|(life\s?)?estate(\sof)?|co-?op[rative]{5,9}|trust|(city|town)\sof|limited|llp|partnership|company)\b',
        "",
        flags=re.IGNORECASE)
    # remove words that come after store
    df[business_short_name_col] = df[business_short_name_col].str.replace(
        r'^(.+)\s(stores?.+)$',r'\g<1>',
        flags=re.IGNORECASE)
    # remove cities (note this assumes an exhaustive list of cities and needs to always be thoroughly checked on new data
    # but its good for going from starbucks of pasadena -> starbucks
    cities = pd.read_csv(filePrefix + "/name_parser/city_list.csv")
    cities = cities[cities['count'] >= 1000]
    city_list = '|'.join(cities['primary_addr_city'])
    df[business_short_name_col] = df[business_short_name_col].str.replace(
        f'^(.+)\s((of\s)?{city_list})$', r'\g<1>',
        flags=re.IGNORECASE)


    df[business_short_name_col] = df[business_short_name_col].str.replace(r' {2,}', r' ')
    df[business_short_name_col] = df[business_short_name_col].str.strip()
    df[business_short_name_col] = df[business_short_name_col].str.replace(r'^([a-z\s]+)(\s)(city|town)$', r'\g<1>')

    # clean superfluous words from short name
    df[business_short_name_col] = df[business_short_name_col].str.replace(
        r'\b(the)\b', ' ', regex=True, flags=re.IGNORECASE
    )
    df[business_short_name_col] = df[business_short_name_col].str.replace(
        r'\s{2,}', ' ', regex=True, flags=re.IGNORECASE
    )
    # consolidate fannie mae and freddie mac
    df[business_short_name_col] = np.where((df[business_name_col].str.contains('federal home') &
                                            df[business_name_col].str.contains('mort')) |
                                           df[business_name_col].str.contains('freddie mac'),
                                           'federal home loan mortgage',
                                           df[business_short_name_col]
                                           )
    df[business_short_name_col] = np.where((df[business_name_col].str.contains('federal national') &
                                            df[business_name_col].str.contains('mort')) |
                                           df[business_name_col].str.contains('fannie mae'),
                                           'federal national mortgage association',
                                           df[business_short_name_col]
                                           )
    df[business_short_name_col] = df[business_short_name_col].str.strip()
    df[business_short_name_col] = df[business_short_name_col].replace(r'^(\s+)?$', np.nan)
    df[business_short_name_col] = df[business_short_name_col].replace(r'', np.nan)
    df[business_short_name_col].fillna(df[business_name_col].str.extract(r'^((\w+\s){1,2})(.+)$').iloc[:, 0],
                                       inplace=True)
    return df


def make_business_proper_name_col(df, business_proper_name_col, business_name_col,
                                  business_short_name_col, business_main_type_col):
    df[business_proper_name_col] = df[business_short_name_col]
    df[business_proper_name_col] = df[business_proper_name_col].str.replace(
        r'\b(realty|real|leasing|groups?|assoc?i?a?t?i?o?n?s?|assoc?i?a?t?e?s?|condos?|owners?|(asset)?\s?ma?na?ge?me?nt|'
        r'tenant\s?|properties|property|nomi?n?e?e?|partners?|member|i?r?revocable|rev|trust|investors?|investments?|'
        r'developments?|holdings?|apartments?|\&|\.|,|\-|;|:|\?|and|services?|company|residential|office|systems?|private|'
        r'capital|enterprises?|technologies|technology|land|bldg|family|foundation|newtwork|construction|builders|law|legal'
        r'|church|holings?|parent|fund|managers?|master|project|essor|tr|et\sal)\b',
        ''
    )
    df[business_proper_name_col] = df[business_proper_name_col].str.replace('&', '')
    df[business_proper_name_col] = df[business_proper_name_col].str.replace(r' {2,}', r' ')
    df[business_proper_name_col] = df[business_proper_name_col].str.strip()
    # remove letters or numbers at end of string i.e 1st st realty 1 -> 1st st realty
    df[business_proper_name_col] = df[business_proper_name_col].str.replace(
        r'^(.+)\s([0-9]{1,4}[a-z]?|ii?i?v?|vi?i?i?|xi?i?|one|two|'
        r'three|four|five|six|seven|eight|nine|ten|et\s?al)$', r'\g<1>')
    # remove trailing single letters from business proper name
    # remove single initals where
    # df[business_proper_name_col] = df[business_proper_name_col].str.replace(r'^(.+)([a-z])$', r'\g<1>')
    df[business_proper_name_col] = df[business_proper_name_col].str.replace(r' {2,}', r' ')
    df[business_proper_name_col] = df[business_proper_name_col].str.strip()

    # remove certain trailing letters/suffixes
    df[business_proper_name_col] = np.where(df[business_main_type_col].isin(['trust', 'family', 'condo trust', 'other']),
                                          df[business_proper_name_col].str.replace(
                                              r'\b([a-z])\b',
                                              ''
                                          ),
                                          df[business_proper_name_col])
    # filter out municipalities (note this requires an exhaustive list of cities)
    df[business_proper_name_col] = np.where(df[business_main_type_col] == 'municipality',
                                          df[business_proper_name_col].str.replace(
                                              r'^(chelsea|boston|cambridge|arlington|'
                                              r'revere|beverly|danvers|marblehead|salem|'
                                              r'swampscott|lynn|nahant|linnfield|saugus|'
                                              r'wakefield|reading|wilmington|pinehurst|'
                                              r'burlington|woburn|stoneham|melrose|malden|'
                                              r'everett|winthrop|medford|winchester|woburn'
                                              r'|lexington|belmont|waltham|watertown|'
                                              r'somerville|newton|brookline|wellesley|'
                                              r'needham|dedham|dover|milton|quincy|randolph'
                                              r'|braintree|weymoth|holbrook|abington|'
                                              r'brockton)(.+)$', r'\g<1>'),
                                          df[business_proper_name_col])
    df[business_proper_name_col] = np.where(df[business_main_type_col] == 'municipality',
                                          df[business_proper_name_col].str.replace(
                                              r'^(town\sof\s)(chelsea|boston|cambridge|arlington|'
                                              r'revere|beverly|danvers|marblehead|salem|'
                                              r'swampscott|lynn|nahant|linnfield|saugus|'
                                              r'wakefield|reading|wilmington|pinehurst|'
                                              r'burlington|woburn|stoneham|melrose|malden|'
                                              r'everett|winthrop|medford|winchester|woburn'
                                              r'|lexington|belmont|waltham|watertown|'
                                              r'somerville|newton|brookline|wellesley|'
                                              r'needham|dedham|dover|milton|quincy|randolph'
                                              r'|braintree|weymoth|holbrook|abington|'
                                              r'brockton)(.+)$', r'\g<2>'),
                                          df[business_proper_name_col])
    # replace empty strings w/ nas
    df[business_proper_name_col] = df[business_proper_name_col].replace(r'^(\s+)?$', np.nan)
    df[business_proper_name_col] = df[business_proper_name_col].replace(r'', np.nan)
    # fill nas w/ business short name
    df[business_proper_name_col].fillna(df[business_short_name_col].str.extract(r'^((\w+\s){1,2})(.+)$').iloc[:, 0],
                                      inplace=True)
    df[business_proper_name_col].fillna(df[business_short_name_col].str.extract(r'^((\w+))(.+)$').iloc[:, 0],
                                      inplace=True)
    df[business_proper_name_col].fillna(df[business_name_col],
                                      inplace=True)
    return df


def make_alphabetized_name(df, name_column, alphabetized_col):
    df['splitCol'] = df[name_column].str.split(' ')
    df['splitCol'] = df['splitCol'].sort_values().apply(lambda x: sorted(x))
    df[alphabetized_col] = df['splitCol'].str.join(' ')
    df[alphabetized_col] = df[alphabetized_col].str.replace(' {2,}', ' ')
    df[alphabetized_col] = df[alphabetized_col].str.strip()
    df.drop(columns=['splitCol'], inplace=True)
    return df


def parse_business(df, business_name_col, business_main_type_col = 'business_main_type', business_sub_type_col = 'business_sub_type',
                   business_short_name_col ='business_short_name', numbers_format=False,  business_proper_name_col='business_proper_name',
                   alphabetized_col='alphabetizedName', log=False, use_business_name_as_base=False):
    if use_business_name_as_base is True:
        business_main_type_col = business_name_col + "_main_type"
        business_sub_type_col = business_name_col + "_sub_type"
        business_short_name_col = business_name_col + "_short_name"
        business_proper_name_col = business_name_col + "_proper_name"
    for col in [business_short_name_col, business_sub_type_col, business_main_type_col]:
        if col not in list(df.columns):
            # write_to_log('adding {}'.format(col))
            df[col] = np.nan
    if df.shape[0] == 0:
        if log is not False:
            write_to_log('Dataframe has no shape not trying to parse')
        return df
    df = clean_business_name(df=df, business_name_col=business_name_col, numbers_format=numbers_format)
    df_full = df.drop(columns=[business_short_name_col, business_sub_type_col, business_main_type_col]).copy(deep=True)
    df = df[[business_name_col]].drop_duplicates(subset=[business_name_col])
    df = make_business_main_type_col(df=df, business_name_col=business_name_col, business_main_type_col=business_main_type_col)
    # subtype
    df = make_business_sub_type_col(df=df, business_sub_type_col=business_sub_type_col, business_name_col=business_name_col)
    # short name
    df = make_business_short_name_col(df=df, business_short_name_col=business_short_name_col,
                                      business_name_col=business_name_col, bank_names=bank_names,
                                      business_sub_type_col=business_sub_type_col)
    # create proper name col (this col removes words such as realty, condo, holdings, etc from short name col)
    df = make_business_proper_name_col(
        df=df, business_name_col=business_name_col, business_short_name_col=business_short_name_col,
        business_main_type_col=business_main_type_col, business_proper_name_col=business_proper_name_col
    )
    # create alphabetized full name column
    df = make_alphabetized_name(df=df, name_column=business_name_col, alphabetized_col=alphabetized_col)
    # make alphabetized proper name column
    df = make_alphabetized_name(df=df, name_column=business_proper_name_col, alphabetized_col='business_proper_name_alphabetized')
    df_full = df_full.merge(df, on=business_name_col, how='left')

    return df_full


def parse_and_clean_name(dataframe, name_column='fullName', firstName_col='firstNameCol', lastName_col='lastNameCol',
               initial_col='initialCol', suffix_col='suffixCol', profession_col='professionCol', alphabetized_col='alphabetizedName',
                type_col='entityType',  business_main_type_col='businessMainType',
                business_sub_type_col='businessSubType', business_short_name_col='businessShortName',
                     split_name=False, format_col=None,
               prefix1='cleaned_',prefix2='parsed_', id_col='recordID', weight_format=False,numbers_format = 'name_number_format'):
    """Cleans and standardizes name column, then classifies name based on classify name function.
    Names that are deemed businesses are parsed according to the parse_business function.
    Names that are deemed people are:
        split into multiple names based on split name function
        parsed according to the parse_name function.
        filled according to fill_missing names function
        reshaped from wide->long w/ missing names dropped
    """
    # in the case that the name column is completely na we just clean the first and last name columns and move from there
    begin_time = time.time()
    dataframe['cutOffFlag'] = np.where(dataframe[name_column].str.len() > 29,
                                           'possibly cut off',
                                           'probably safe'
                                           )
    # add in additional columns
    for col in [name_column, firstName_col, lastName_col, initial_col, suffix_col, profession_col,
                business_main_type_col, business_short_name_col, business_sub_type_col, business_sub_type_col

                ]:
        if col not in dataframe.columns:
            dataframe[col] = np.nan
    dataframe = clean_name(dataframe, name_column, prefix1)
    dataframe = clean_name(dataframe, firstName_col, prefix1)
    dataframe = clean_name(dataframe, lastName_col, prefix1)
    dataframe = clean_name(dataframe, initial_col, prefix1)
    dataframe=classify_name(dataframe=dataframe, weight_format=weight_format,
                  name_cols=[prefix1+name_column, prefix1+firstName_col,
                             prefix1+lastName_col, prefix1+initial_col],
                  type_col=type_col)
    df = parse_name(
            df=dataframe, name_col=prefix1+name_column, firstName_col=prefix1+firstName_col, lastName_col=prefix1+lastName_col,
            initial_col=prefix1+initial_col, suffix_col=prefix1+suffix_col, profession_col=prefix1+profession_col,
            alphabetized_col=alphabetized_col,
            typeCol=type_col, business_sub_type_col=business_sub_type_col,
            business_short_name_col=business_short_name_col, business_main_type_col=business_main_type_col,
             id_col=id_col, prefix=prefix2, split_name=split_name, format_col=format_col,numbers_format=numbers_format
               )

    return df


def clean_classify_parse_name(dataframe, name_column='fullName', firstName_col='firstNameCol', lastName_col='lastNameCol',
               initial_col='initialCol', suffix_col='suffixCol', profession_col='professionCol', alphabetized_col='alphabetizedName',
                typeCol='entityType',prefix1='cleaned_',prefix2='parsed_', id_col='recordID', weight_format=False):
    dataframe['cutOffFlag'] = np.where(dataframe[name_column].str.len() > 29,
                                       'possibly cut off',
                                       'probably safe'
                                       )
    # add in additional columns
    for col in [name_column, firstName_col, lastName_col, initial_col, suffix_col, profession_col, alphabetized_col]:
        if col not in dataframe.columns:
            dataframe[col] = np.nan
    dataframe=clean_name(dataframe, name_column, prefix1)
    dataframe=clean_name(dataframe, firstName_col, prefix1)
    dataframe=clean_name(dataframe, lastName_col, prefix1)
    dataframe=clean_name(dataframe, initial_col, prefix1)
    dataframe=classify_name(dataframe=dataframe, weight_format=weight_format,
                  name_cols=[prefix1 + name_column, prefix1 + firstName_col,
                             prefix1 + lastName_col, prefix1 + initial_col],
                  type_col=typeCol)
    # split dataframe into two based on typeCol
    business_df = dataframe[dataframe[typeCol] == 'business']
    person_df = dataframe[dataframe[typeCol] == 'person']
    del dataframe
    # parse person names
    person_df = parse_and_split(dataframe=person_df, name_col=prefix1 + name_column, firstName_col=prefix1 +firstName_col,
                                    lastName_col=prefix1 + lastName_col,
                                    initial_col=prefix1 + initial_col, type_col=typeCol, id_col=id_col, suffix_col=prefix1 + suffix_col)

    person_df = pd.concat([person_df, business_df])
    return person_df

