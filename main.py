import os
import requests
import pprint
import datetime
import dateutil.parser
from dotenv import load_dotenv, find_dotenv
from pymongo import MongoClient, GEO2D

load_dotenv(find_dotenv())
pp = pprint.PrettyPrinter(indent=4)

# Mongo setup=
client = MongoClient(os.getenv("MONGODB_URI"))
db = client['ncov']

base_url = "https://services5.arcgis.com/mnYJ21GiFTR97WFg/arcgis/rest/services"

def get_confirmed_cases():
    # https://services5.arcgis.com/mnYJ21GiFTR97WFg/arcgis/rest/services/PH_masterlist/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=FID%20desc&resultOffset=0&resultRecordCount=150&cacheHint=true
    master_list_url = "https://services5.arcgis.com/mnYJ21GiFTR97WFg/arcgis/rest/services/PH_masterlist/FeatureServer/0/query"
    master_list_params = {
        'f':'json',
        'where':'1=1',
        'returnGeometry': 'false',
        'spatialRel': 'esriSpatialRelIntersects',
        'outFields':'*',
        'orderByFields':'FID desc',
        'resultOffset':0,
        'resultRecordCount':200,
        'cacheHint':'true'
        }

    response = requests.get(master_list_url, params=master_list_params)
    json_response = response.json()

    ncov_cases = json_response['features']

    # db.cases.create_index([("location",GEO2D)])
    cases = db.cases

    format_str = '%m/%d/%Y' # The format

    for ncov_case in ncov_cases:
        ncov_case = ncov_case['attributes']
        try:
            if(ncov_case['confirmed'] != None):
                parsed_date = dateutil.parser.parse(ncov_case['confirmed'])
                datetimeobject = datetime.strptime(str(parsed_date),'%Y-%m-%d  %H:%M:%S')
        except:
            ncov_case['confirmed'] = None

        try:
            ncov_case['location'] = {"type": "Point", "coordinates": [float(ncov_case['longitude']), float(ncov_case['latitude']) ]}
        except:
            ncov_case['location'] = {"type": "Point", "coordinates": [0.0, 0.0]}

        print(ncov_case)
        ncov_case['inserted_at'] = datetime.datetime.now()
        cases.insert_one(ncov_case)


#######---------------------------- #############

def get_puis():
    pui_url = "https://services5.arcgis.com/mnYJ21GiFTR97WFg/arcgis/rest/services/PUI_fac_tracing/FeatureServer/0/query"
    pui_list_params = {
        'f':'json',
        'where':'1=1',
        'returnGeometry': 'false',
        'spatialRel': 'esriSpatialRelIntersects',
        'outFields':'*',
        'resultOffset':0,
        'resultRecordCount':200,
        'cacheHint':'true'
        }

    response = requests.get(pui_url, params=pui_list_params)
    json_response = response.json()

    pui_cases = json_response['features']

    puis = db.puis

    for pui in pui_cases:
        pui = pui['attributes']
        try:
            pui['location'] = {"type": "Point", "coordinates": [float(pui['longitude']), float(pui['latitude']) ]}
        except:
            pui['location'] = {"type": "Point", "coordinates": [0.0, 0.0]}
        pui['inserted_at'] = datetime.datetime.now()
        puis.insert_one(pui)

    pp.pprint(pui_cases)


if __name__ == '__main__':
    get_confirmed_cases()
    get_puis()
