import PyPDF2
import pytesseract
import pdftotext
import re
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer,TfidfVectorizer
from sklearn.base import TransformerMixin
from sklearn.pipeline import Pipeline

sample_text = "\nQUILTING\n\nCrazy Quilt 900 North Point St\n" \
              "Kimball Walter Quilting Shop 226 Monterey Blvd\nMitchell Antommette 955 Natoma St\n" \
              "NERA QUILTING COMPANY 1118 Quesada\nAv (94124), Tel (415) 622-6260\n" \
              "Quilta Limited pier 39 The Embarcadero N Strwy\n4\n\nRADIATOR MFRS—STEAM\n" \
              "AND HOT WATER\n\nDuro Auto & Truck Radiator Works 685 Harmson\nSt\n\n*RADIATOR REPAIRE"

sample_list = sample_text.split('\n')

