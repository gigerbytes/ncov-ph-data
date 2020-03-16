# Combine Data
import csv
import re
import os
import pprint
import datetime as dt
from dotenv import load_dotenv, find_dotenv
from pymongo import MongoClient, GEO2D

load_dotenv(find_dotenv())
pp = pprint.PrettyPrinter(indent=4)

# Mongo setup
client = MongoClient(os.getenv("MONGODB_URI"))
db = client['ncov']


def combine_cases():
    cases_ph = db['cases_ph']
    cases_ph_cur = cases_ph.find({})

    cases_ph_ar = []
    for case in cases_ph_cur:
        case_obj = {
            'FID' : case['FID'],
            'code' : case['PH_masterl'],
            'gender' : case['kasarian'],
            'age': case['edad'],
            'nationalit' : case['nationalit'],
            'residence' : case['residence'],
            'travel_hist': case['travel_hx'],
            'symptoms': case['symptoms'],
            'facility': case['facility'],
            'confirmed': case['confirmed'],
            'latitude': case['latitude'],
            'longitude': case['longitude'],
            'status': case['status'],
            'dashboard_version': case['dashboard_version'],
            'dashboard_last_updated': case['dashboard_last_updated'],
            'queried_at': case['inserted_at']
        }
        cases_ph_ar.append(case_obj)
    keys = cases_ph_ar[0].keys()
    with open('./data/cases_ph.csv', 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(cases_ph_ar)
if __name__ == '__main__':
    combine_cases()
