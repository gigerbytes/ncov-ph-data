# The purpose of this file is to parse the data from the DOH's daily case summary
# and aggregate it to the mongo database

import os
import requests
import pprint
import datetime
import dateutil.parser
import datefinder
import camelot
import pandas as pd

from dotenv import load_dotenv, find_dotenv
from pymongo import MongoClient, GEO2D

load_dotenv(find_dotenv())
# pp = pprint.PrettyPrinter(indent=4)
#
# # Mongo setup
# client = MongoClient(os.getenv("MONGODB_URI"))
# db = client['ncov']


tables = camelot.read_pdf('./pdfs/Cases as of March 16, 2020.pdf',  pages='all')

df_cases = pd.DataFrame()
print(df_cases)
for t in tables:
    df_cases = df_cases.append(t.df)
    print(t.df)

table2 = camelot.read_pdf('./pdfs/Cases as of March 16, 2020.pdf', pages="8")
print(table2[0].df)
df_cases = df_cases.iloc[2:] # Remove pdf header rows, take only data rows
df_cases.columns = ["case_no", "age", "sex", "nationality", 'travel_hx', "date_symptom", "date_admission","date_confirmation","facility","residence" ]

print(df_cases)
df_cases.to_csv('pdf_cases.csv')
