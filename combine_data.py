# Combine Data
import csv
import re
import os
import pprint
import datetime as dt
import datefinder
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from pymongo import MongoClient


load_dotenv(find_dotenv())
pp = pprint.PrettyPrinter(indent=4)

# Mongo setup
client = MongoClient(os.getenv("MONGODB_URI"))
db = client['ncov']


# Gets a date string, returns either a formatted date, or the string itself
def parse_date(date_str):
    try:
        return list(datefinder.find_dates(date_str))[-1]
    except:
        return date_str

def query_cases_to_csv():
    cases_ph = db['cases_ph']
    cases_ph_cur = cases_ph.find({'dashboard_last_updated':dt.datetime(2020, 3, 18, 12, 0)})

    cases_ph_ar = []
    for case in cases_ph_cur:
        case_obj = {
            'FID' : case['FID'],
            'case_no' : case['PH_masterl'],
            'sex' : case['kasarian'],
            'age': case['edad'],
            'nationality' : case['nationalit'],
            'residence' : case['residence'],
            'travel_hist': case['travel_hx'],
            'symptoms': case['symptoms'],
            'facility': case['facility'],
            'date_lab_confirmed': case['confirmed'],
            'latitude': case['latitude'],
            'longitude': case['longitude'],
            'status': case['status'],
            'dashboard_last_updated': case['dashboard_last_updated'],
            'queried_at': case['inserted_at']
        }
        cases_ph_ar.append(case_obj)
    keys = cases_ph_ar[0].keys()
    with open('./data/cases_ph.csv', 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(cases_ph_ar)

### COMBINE PDF DATA WITH QUERIED DASHBOARD DATA
def combine_cases_ph():
    cases_api = pd.read_csv('./data/cases_ph.csv', index_col=[0])
    cases_pdf = pd.read_csv('./data/cases_pdf_ph.csv', index_col=[0])

    # Drop useless colums from cases_pdf
    cases_pdf = cases_pdf.drop(['age','sex','nationality','facility','residence','date_confirmation'], axis=1)

    df_cases = pd.merge(left=cases_api, right=cases_pdf, left_on='case_no', right_on='case_no', how="left")

    # Transform to date string to date
    df_cases['date_symptoms'] = df_cases['date_symptoms'].apply(parse_date)
    df_cases['date_admission'] = df_cases['date_admission'].apply(parse_date)

    # Rearrange columns
    df_cases = df_cases[['case_no', 'age', 'sex', 'nationality','travel_hist','detailed_history','status','symptoms','date_symptoms','date_admission','date_lab_confirmed','facility','latitude','longitude','residence','dashboard_last_updated','queried_at']]

    # Save
    df_cases.to_csv('./data/cases_ph.csv', index=False)

if __name__ == '__main__':
    query_cases_to_csv()
    combine_cases_ph()
