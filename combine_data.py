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

def combine_facilities():
    facilities_pui_collection = db['facilities_puis']
    facilities_conf_collection = db['facilities_conf']
    fac_pui_cur = facilities_pui_collection.find({})


    combined_fac_data = []
    for fac in fac_pui_cur:
        fac_conf = facilities_conf_collection.find_one({'facility': re.compile(fac['hf'], re.IGNORECASE), 'dashboard_version':fac['dashboard_version']})

        if(fac_conf):
            fac['patients'] = fac_conf['count_']
        else:
            fac['patients'] = 0


        # Clarity
        combined_fac_obj = {
            'fac_id': fac['ObjectId'],
            'facility': fac['hf'],
            'region': fac['region'],
            'latitude': fac['latitude'],
            'longitude': fac['longitude'],
            'cases': fac['patients'],
            'puis': fac['PUIs'],
            'dashboard_version': fac['dashboard_version'],
            'dashboard_last_updated': fac['dashboard_last_updated'],
            'queried_at': fac['inserted_at']
        }
        combined_fac_data.append(combined_fac_obj)

        # pp.pprint(fac)

    ## Check all facilities with confirmed cases and get the not used ones yet
    fac_conf_cur = facilities_conf_collection.find({})
    for fac in fac_conf_cur:
        fac_pui = facilities_pui_collection.find_one({'hf': re.compile(fac['facility'], re.IGNORECASE), 'dashboard_version':fac['dashboard_version']})
        # If there is a case but no PUI - so no record in PUI collection
        if(not fac_pui):
            print(fac)

                # Clarity
            combined_fac_obj = {
                'facility': fac['facility'],
                # 'region': fac['region'],
                'latitude': fac['latitude'],
                'longitude': fac['longitude'],
                'cases': fac['count_'],
                'puis': 0,
                'dashboard_version': fac['dashboard_version'],
                'dashboard_last_updated': fac['dashboard_last_updated'],
                'queried_at': fac['inserted_at']
            }
            print(fac)
        # combined_fac_data.append(combined_fac_obj)



    keys = combined_fac_data[0].keys()
    with open('./facilities.csv', 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(combined_fac_data)

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
    with open('./cases_ph.csv', 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(cases_ph_ar)
if __name__ == '__main__':
    combine_cases()
