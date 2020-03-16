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

def parse_facility_data(fac_obj):
    fac_obj = fac_obj['attributes']
    try:
        fac_obj['location'] = {"type": "Point", "coordinates": [float(fac_obj['longitude']), float(fac_obj['latitude']) ]}
    except:
        fac_obj['location'] = {"type": "Point", "coordinates": [0.0, 0.0]}
    fac_obj['inserted_at'] = datetime.datetime.now()

    return fac_obj

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

    facilities = json_response['features']

    db_puis = db.puis

    for facility in facilities:
        facility_pui = parse_facility_data(facility)
        db_puis.insert_one(facility_pui)


#######---------------------------- #############

def get_conf_facility():
    conf_facility_url = "https://services5.arcgis.com/mnYJ21GiFTR97WFg/arcgis/rest/services/conf_fac_tracking/FeatureServer/0/query"
    conf_facility_params = {
        'f':'json',
        'where':'1=1',
        'returnGeometry': 'false',
        'orderByFields':'count_ desc',
        'spatialRel': 'esriSpatialRelIntersects',
        'outFields':'*',
        'resultOffset':0,
        'resultRecordCount':200,
        'cacheHint':'true'
        }

    response = requests.get(conf_facility_url, params=conf_facility_params)

    json_response = response.json()
    facilities = json_response['features']

    fac_confs = db.facilities_conf

    for facility in facilities:
        facility_conf = parse_facility_data(facility)
        fac_confs.insert_one(facility_conf)


def get_commodities():
    commodities_url = "https://services5.arcgis.com/mnYJ21GiFTR97WFg/arcgis/rest/services/commodities/FeatureServer/0/query"
    commodities_params = {
        'f':'json',
        'where':'1=1',
        'returnGeometry': 'false',
        'spatialRel': 'esriSpatialRelIntersects',
        'outFields':'*',
        'resultOffset':0,
        'resultRecordCount':200,
        'cacheHint':'true'
        }
    response = requests.get(commodities_url, params=commodities_params)
    json_response = response.json()
    commodities = json_response['features']

    db_commodities = db.commodities
    for commodity in commodities:
        commodity = commodity['attributes']
        db_commodities.insert_one(commodity)
    pp.pprint(commodities)

if __name__ == '__main__':
    get_confirmed_cases()
    get_puis()
    get_conf_facility()
    get_commodities()
