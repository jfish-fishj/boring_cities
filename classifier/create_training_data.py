import pytesseract
import random
import cv2
import pandas as pd
import numpy as np
from data_constants import *
import pdf2image
import re
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
pd.set_option('display.max_rows', 100)

# data constants
st_num = r'([0-9]+-?[a-z]?)'
st_d = r'([nsewrl]{1,2}\s|rear\s|side\s)'
st_name = r'(\w{3,}|\w{3,}\s\w{3,}|[a-z\s-]{4,}|[a-z]+|[0-9]+)'
st_sfx = r'(aly|av|byu|blf|blvd|bd|bldg|rm|br|cswy|ctr|cir|ct|cr|cv|crk|cres|xing|curv|dr|est|expy|ext|frk|ft|fwy|' \
         r'gdn|gtwy|hvn|' \
             r'hwy|hl|jct|ky|lks|ln|lgt|lp|mall|mnr|mdw|msn|mtn|pkwy|pass|path|pl|plz|pt|prt|pw|rst|rdge|rd|rte|rw|shr|' \
             r'sq|st|strm|trak|turnpike|ter|tl|vly|vws|walk|way)'
zipcode = r'([0-9]{5}|[0-9]{5}-[0-9]{4})'
unit = r'#\s?([a-z0-9-]+|[0-9-]+-?[a-z]?|[a-z]-?[0-9]+|[up][0-9]?[abcd]|un\s[0-9abcd]|[a-z])'
unit2 = r'\s([a-z]+-?[0-9]{1,4}|[uptcl][-\s]?[tl]?[0-9]{1,4}[abcd]?|un\s[0-9abcd]{1,4})' # use


def create_samples(phonebook):
    pdf2image.convert_from_path(phonebook, output_folder=classifier_files['sample images'], thread_count=4, fmt='png', dpi=400)

def get_text_from_image(image_path, show_image=True):
    image = cv2.imread(image_path)
    if show_image is True:
        cv2.imshow("image", image)
        cv2.waitKey(0)
        cv2.destroyAllWindows()
    image_text = pytesseract.image_to_string(image)
    return image_text

def create_training_data(image_dir):
    use_this = False
    while use_this is False:
        path = random.choice(os.listdir(image_dir))
        text = get_text_from_image(image_dir + path)
        use_this = input('Use this text?')
        if use_this == 'False':
            use_this = False
            print('skipping this image')
    text_list = text.split('\n')
    types = ['complete', 'partial', 'ignore', 'header']
    df = pd.DataFrame()
    for index, text in enumerate(text_list):
        classification = input(f'{text} is of {types}')
        if classification == 'break':
            df.to_csv(classifier_files['training data'], mode='a+')
            break
        if classification == 'new image':
            df.to_csv(classifier_files['training data'], mode='a+', header=False)
            create_training_data(image_dir=image_dir)
        df = df.append(pd.DataFrame(data={'text':[text], 'classification':[classification]}))


def custom_ord(string):
    if type(string) is str:
        return ord(string)
    else:
        return -100000


class TextClassifier:
    def __init__(self, method,type):
        self.method = method
        self.df = df
        self.type = type

    def create_variables(self, df, text):
        df = df[df[text].notnull()]
        with pd.option_context('mode.chained_assignment', None):
            stripped_text = 'stripped_' + text
            df[stripped_text] = df[text].str.replace('[^\x20-\x7E]', '', flags=re.IGNORECASE).str.replace(r'(\s{2,}|\t)', ' ').str.strip()
            df['all_caps'] = np.where(
                df[stripped_text].str.contains(r'^[A-Z\s&-]+$'), 1, 0
            )
            df['str_len'] = df[stripped_text].str.len()
            df['contains_misc_characters'] = np.where(df[stripped_text].str.contains('[^\w",\*\(\)]'), 1, 0)
            df['ends_with_number'] = np.where(df[stripped_text].str.contains('[0-9]$'),1,0)
            df['starts_with_number'] = np.where(df[stripped_text].str.contains('^[0-9]'),1,0)
            df['all_numbers'] = np.where(df[stripped_text].str.contains('^[0-9-]+$'),1,0)
            df['starts_with_street_suffix'] = np.where(df[stripped_text].str.contains(f'^{st_sfx} ', flags=re.IGNORECASE),1,0)
            df['ends_with_street_suffix'] = np.where(df[stripped_text].str.contains(f' {st_sfx}$', flags=re.IGNORECASE), 1, 0)
            df['contains_PHONE'] = np.where(df[stripped_text].str.contains('PHONE'), 1,0)
            df['first_character'] = df[stripped_text].str[0].apply(custom_ord)

        def create_lag_variable(df, var, shift=1, prefix='lag'):
            with pd.option_context('mode.chained_assignment', None):
                df[f'{prefix}{str(shift)}_{var}'] = df[var].shift(periods = shift)
            return df
        for var in ['str_len', 'contains_misc_characters', 'ends_with_number', 'starts_with_number', 'all_numbers',
                    'starts_with_street_suffix', 'ends_with_street_suffix', 'contains_PHONE', 'first_character']:
            df = create_lag_variable(df=df, var=var, shift=1)
            df = create_lag_variable(df=df, var=var, shift=2)
            df = create_lag_variable(df=df, var=var, shift=3)
            df = create_lag_variable(df=df, var=var, shift=-1, prefix='lead')
            df = create_lag_variable(df=df, var=var, shift=-2, prefix='lead')
            df = create_lag_variable(df=df, var=var, shift=-3, prefix='lead')
        for col in df.columns:
            df[col].fillna(-100000, inplace=True)
        return df



if __name__ == "__main__":
    # create_samples("/Volumes/Seagate Portable Drive/boring_cities/data/raw/sf/phonebook/phonebooks/sanfranciscosanf1982rlpo_150_350.pdf")
    # create_training_data(classifier_files['sample images'])
    df = pd.read_csv('/Volumes/Seagate Portable Drive/boring_cities/data/raw/sf/phonebook/training_data/training_data.csv',encoding = "ISO-8859-1")
    df = df[df['classification'].isin(['complete', 'ignore', 'header', 'partition', 'ignore'])]
    print(df['classification'].value_counts())
    text = TextClassifier( method='', type = '')
    new_df = text.create_variables(df=df, text='text')
    factor = pd.factorize(new_df['classification'])
    definitions = factor[1]
    new_df.classification = factor[0]
    model = RandomForestClassifier()
    vars = [col for col in new_df.columns if col not in ['index', 'text', 'stripped_text', 'classification']]
    X_train, X_test, y_train, y_test = train_test_split(new_df[vars], new_df['classification'], test_size = 0.5, random_state = 21)
    model.fit(X=X_train[vars], y=y_train)
    y_pred = model.predict(X_test)
    reversefactor = dict(zip(range(3), definitions))
    y_test = np.vectorize(reversefactor.get)(y_test)
    y_pred = np.vectorize(reversefactor.get)(y_pred)
    # Making the Confusion Matrix
    print(pd.crosstab(y_test, y_pred, rownames=['Actual Species'], colnames=['Predicted Species']))