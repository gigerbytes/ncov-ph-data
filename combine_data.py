# Combine Data
import csv
import re
import os
import pprint
import datetime as dt
import datefinder
import pandas as pd
import numpy as np
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
    cases_ph_cur = cases_ph.find({'dashboard_last_updated':dt.datetime(2020, 3, 27, 16, 0)})

    cases_ph_ar = []
    for case in cases_ph_cur:
        case_obj = {
            'FID' : case['FID'],
            'sequ': case['sequ'],
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
    cases_api = pd.read_csv('./data/cases_ph.csv', index_col=[0]).sort_values(by=['sequ'], ascending=False) # FID is now out of sync with case_no, New field sequ appeared Mar 25 which is numerical form of case_no. sort by desc to order dataset.
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

def combine_facilities():
    fac_puis = db['facilities_puis']
    fac_conf = db['facilities_conf']


    # Dashboard
    dashboard_updates = [
        # dt.datetime(2020, 3, 16, 9, 0),
        # dt.datetime(2020, 3, 17, 9, 0),
        dt.datetime(2020, 3, 27, 16, 0),
    ]

    # for Timeseries set generation
    for update in dashboard_updates:

        fac_puis_df = pd.DataFrame(list(fac_puis.find({'dashboard_last_updated': update})))
        fac_conf_df = pd.DataFrame(list(fac_conf.find({'dashboard_last_updated': update})))

        fac_puis_df = fac_puis_df.drop(['_id', 'location','dashboard_version', 'ObjectId'], axis=1)
        fac_conf_df = fac_conf_df.drop(['_id', 'location','dashboard_version', 'ObjectId','dashboard_last_updated'], axis=1)


        ## Create lower case column for merge because case inconsistent
        ## Merge Datasets
        fac_puis_df['facility_lowercase'] = fac_puis_df['hf'].str.lower()
        fac_conf_df['facility_lowercase'] = fac_conf_df['facility'].str.lower()
        facility_df = pd.merge(left=fac_puis_df, right=fac_conf_df, left_on='facility_lowercase', right_on='facility_lowercase', how="outer")
        # facility_df = facility_df.drop(['facility_lowercase'], axis=1) #drop created lowercase col


        facility_df = facility_df.rename(columns={'hf': 'facility_conf', 'PUIs':'puis', 'count_': 'confirmed_cases', 'latitude_x': 'latitude', 'longitude_x': 'longitude', 'inserted_at_x':'inserted_at'})
        print(facility_df.head())

        # fill NA
        facility_df['puis'] = facility_df['puis'].replace(np.nan, int(0)) # if misjoined, then 0 pui or 0 confirmed cases
        facility_df['confirmed_cases'] = facility_df['confirmed_cases'].replace(np.nan, 0) # if misjoined, then 0 pui or 0 confirmed cases
        facility_df['dashboard_last_updated'] = update # if misjoined, then 0 pui or 0 confirmed cases
        facility_df.loc[facility_df['latitude'].isna(),'latitude'] = facility_df['latitude_y']
        facility_df.loc[facility_df['longitude'].isna(),'longitude'] = facility_df['longitude_y']
        facility_df.loc[facility_df['inserted_at'].isna(),'inserted_at'] = facility_df['inserted_at_y']
        facility_df.loc[facility_df['facility'].isna(),'facility'] = facility_df['facility_conf']

        ## rearrange columns
        facility_df = facility_df[['facility', 'puis', 'confirmed_cases', 'region', 'latitude','longitude','dashboard_last_updated','inserted_at']]

        facility_df.to_csv('./data/facilities.csv', index=False)


if __name__ == '__main__':
    query_cases_to_csv()
    combine_cases_ph()
    combine_facilities()
