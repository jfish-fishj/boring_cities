import pdf2image
import pytesseract
import re
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer,TfidfVectorizer
from sklearn.base import TransformerMixin
from sklearn.pipeline import Pipeline
from create_training_data import create_samples, get_text_from_image, create_training_data, custom_ord, create_variables
import os
from sklearn.externals import joblib
import numpy as np
from data_constants import classifier_files

def make_images_from_pdf(infile, outdir, image_format='png', num_thread=4):
    assert infile.endswith('pdf')
    if os.path.exists(outdir) is False:
        os.mkdir(outdir)
    pdf2image.convert_from_path(infile, output_folder=outdir, thread_count=num_thread, fmt=image_format,
                                dpi=400)

def make_text_from_images(indir, outdir, mode='individual', file_prefix=""):
    file_list = os.listdir(indir)
    if mode is not 'individual':
        df_list = []
    if os.path.exists(outdir) is False:
        os.mkdir(outdir)
    for file_number, file in enumerate(file_list):
        file_path = indir + file
        if bool(re.search('(png|jpeg)$', file_path)):
            text = get_text_from_image(file_path, show_image=False)
            text_list = text.split('\n')
            df = pd.DataFrame(data = {'text':text_list})
            if mode is not 'individual':
                df_list = df_list.append(df)
            else:
                outpath = outdir + file_prefix + str(file_number) + '.csv'
                df.to_csv(outpath, index=False)
    if mode is not 'individual':
        out_df = pd.concat(df_list)
        out_df.to_csv(outdir + file_prefix + 'concatenated.csv', index=False)

def classify_text(df, classifier):
    model = joblib.load(classifier)
    created_vars = create_variables(df=df, text='text', return_vars=True)
    df = created_vars[0]
    predict_vars = created_vars[1]
    with pd.option_context('mode.chained_assignment', None):
        df['classification'] = model.predict(X=df[predict_vars])
        df['classification'] = df['classification'].map({0: 'complete', 1: 'partial', 2: 'ignore', 3: 'header'})
    df = df[['text','stripped_text', 'classification']+[c for c in df if c not in ['text','stripped_text', 'classification']]]
    return df

def process_classified_text(df, classification_col, text_col):

    df = df[df[classification_col] != 'ignore']
    n_text = text_col
    # iteratively process partials
    df['running_partial'] = (df[classification_col].groupby((df[classification_col] != df[classification_col].shift()).cumsum()).cumcount() + 1)

    def iteratively_process_partial(df, value='partial', until=0):
        max_partial = df[df[classification_col] == value]['running_partial'].max()
        while max_partial > until:
            df[n_text] = np.where(
                (df['running_partial'].shift(-1) == max_partial) & (df[classification_col].shift(-1) == value),
                df[n_text] + ' ' + df[n_text].shift(-1),
                df[n_text]
            )
            df = df[~((df['running_partial'] == max_partial) & (df[classification_col] == value))]
            max_partial = max_partial - 1
        return df

    df = iteratively_process_partial(df, value='partial', until=0)
    df = iteratively_process_partial(df,  value='header', until=1)
    # sort into headers
    df['header'] = np.where(df[classification_col] == 'header', df[n_text], np.nan)
    df['header'].fillna(method='ffill', inplace=True)
    # df = df[df[classification_col] != 'header']
    return df


if __name__ == "__main__":
    classifier = classifier_files['random forest']
    # make_images_from_pdf(
    #     infile="/Volumes/Seagate Portable Drive/boring_cities/data/raw/sf/phonebook/phonebooks/sanfranciscosanf1982rlpo_95_206.pdf",
    #     outdir="/Volumes/Seagate Portable Drive/boring_cities/data/raw/sf/phonebook/sanfranciscosanf1982rlpo_95_206/"
    # )
    make_text_from_images(
        indir="/Volumes/Seagate Portable Drive/boring_cities/data/raw/sf/phonebook/sanfranciscosanf1982rlpo_95_206/",
        outdir="/Volumes/Seagate Portable Drive/boring_cities/data/raw/sf/phonebook/sanfranciscosanf1982rlpo_95_206_images"
    )
    for file in os.listdir("/Volumes/Seagate Portable Drive/boring_cities/data/raw/sf/phonebook/sanfranciscosanf1982rlpo_95_206_images/"):
        path = "/Volumes/Seagate Portable Drive/boring_cities/data/raw/sf/phonebook/sanfranciscosanf1982rlpo_95_206_images/" + file
        if file.endswith('csv'):
            dataframe= pd.read_csv(path)
            dataframe = classify_text(dataframe, classifier)
            dataframe = process_classified_text(dataframe,classification_col= 'classification',text_col= 'stripped_text')
            dataframe.to_csv("/Volumes/Seagate Portable Drive/boring_cities/data/raw/sf/phonebook/sanfranciscosanf1982rlpo_95_206_images_classified/" + file, index=False)

