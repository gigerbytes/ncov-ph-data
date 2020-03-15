import requests
import pprint

pp = pprint.PrettyPrinter(indent=4)

base_url = "https://services5.arcgis.com/mnYJ21GiFTR97WFg/arcgis/rest/services"

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
print(response.url)
json_response = response.json()
pp.pprint(json_response['features'])
