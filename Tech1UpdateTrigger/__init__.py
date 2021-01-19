import logging
import time
#GENERAL
import http.client
import mimetypes
import json
from pandas import json_normalize
import sys, os
import pandas as pd
import datetime
from copy import deepcopy
import numpy as np
import requests
#ARCGIS
from arcgis.features.manage_data import dissolve_boundaries
from arcgis.features.find_locations import find_centroids
from arcgis.geometry import from_geo_coordinate_string
from arcgis.geocoding import geocode
from arcgis.geometry import lengths, areas_and_lengths, project
from arcgis.geometry import Point, Polyline, Polygon, Geometry
from arcgis.gis import GIS
import arcgis
from arcgis import geometry 
from arcgis import features
from arcgis.geoanalytics import manage_data
from arcgis.features.manage_data import overlay_layers
from arcgis.features import GeoAccessor, GeoSeriesAccessor, FeatureLayer
from arcgis.features import FeatureLayerCollection
import azure.functions as func


def main(msg: func.QueueMessage, msg1: func.Out[str]) -> None:
    logging.info('Python queue trigger function processed a queue item: %s',
                 msg.get_body().decode('utf-8'))
   
    #TECH1 Credentials
    
    test1=os.environ["test1"]#Read API Client ID
    test2=os.environ["test2"]#Read API Client Secret
    conn = http.client.HTTPSConnection("API DOMAIN")
    payload = 'client_id='+ test1 +'&client_secret='+test2 +'&grant_type=client_credentials'
    headers = {  'Content-Type': 'application/x-www-form-urlencoded'}
    conn.request("POST", "/APIURLSTRING/oauth2/access_token", payload, headers)
    res = conn.getresponse()#Send Get Request response
    data = res.read()#Read response
    access_token=data.decode("utf-8")
    # Sign into Tech 1, download any opcodes with zeroes and put them into a search list
    
    url = "APIURL/Api/RaaS/v1/LoggingOperation?q=Active%20%3D1%20and%20LATITUDE%20%3D%20%20%220.000000%22%20and%20LONGITUDE%20%3D%20%220.000000%22&pageSize =300"

    payload = {}
    headers = {'Authorization': access_token.split('"')[3]}#Extract access token from response string
    response = requests.request("GET", url, headers=headers, data = payload)#Send Get request to url
    #print(response.text.encode('utf8'))
    j=json.loads(response.text.encode('utf8'))

    #Convert json response to dataframe and drop unwanted columns
    tc = pd.json_normalize(j['DataSet'])
    tc=tc[tc['T_LATITUDE'].astype(float)==0]
    mc=tc.drop(columns=['T_ID',  'T_TITLE', 'T_AFSCERTIFIEDAREA','T_FSCCERTIFIEDAREA', 'T_STARTDATE', 'T_ENDDATE', 'T_LATITUDE', 'T_LONGITUDE', 'T_ACTIVE', 'T_CREATEDDATETIME', 'T_MODIFIEDDATETIME'])
    
    #Operation Codes recovered into a searchlist
    searchlist=str(list(mc.T_OPERATIONNO.values))[1:-1]

    test = os.environ["testers"]#Read keyvault credentials integrated in the function
    gis = GIS("https://xxxx.maps.arcgis.com", "UserName", test)#Log onto the ESRI Portal


     # Read Asset 1 Centroids from ESRI Portal and save in a dataframe named df
    try:
        item = gis.content.search("HealthLyrPolygonToPoint",item_type="Feature Layer Collection")#Search and download Asset 1 Centroids
        df= gis.content.get(item[0].id).layers[0].query(where = f"Ops_Code in ({searchlist})",out_fields = "*", ).sdf#Extract Spatially only with Operations Code on searchlist Enabled DataFrame
        #df=df[df['Ops_Code'].isin(mc.T_OPERATIONNO)]
        df=df.assign(Long=df.SHAPE.astype(str).apply(lambda x: Point(x).coordinates()).str[0],Lat=df.SHAPE.astype(str).apply(lambda x: Point(x).coordinates()).str[1])#Extract Lat/Long into new columns                    drop(columns=['OBJECTID',  'Count_','ORIG_FID','PLANTATION' ,'AnalysisArea','SHAPE'])#Compute Lat Long and drop unwanted columns
    except:
        pass


    # # Read Asset 2 Centroids from ESRI Portal and save in a dataframe named tableNF

    try:
        itemnf = gis.content.search("nfHealthLyrPolygonToPoint",item_type="Feature Layer Collection")#Search and download Asset 2 Centroids
        ntfobject= gis.content.get(itemnf[0].id).layers[0]
        tableNF=gis.content.get(itemnf[0].id).layers[0].query(where = f"LOIS in ({searchlist})",out_fields = "*", ).sdf
        
        
    except:
        pass

    #Project Asset 2 spatial dataframe to geographic coordinate system from 
    try:
        spr=ntfobject.query().spatial_reference['latestWkid']#Acquire the to be spatial reference
        tableNF['SHAPE'] =tableNF.SHAPE.apply(lambda Y: geometry.project(geometries =[Y],in_sr = spr, out_sr = 4326,gis = gis)).str[0]
    except:
        pass
    #Compute Asset 2 Lat Long in new columns in tableNF spatial dataframe  and drop unwanted columns
    try:
        tableNF=tableNF.join(pd.read_json(tableNF['SHAPE'].to_json(orient='records'), orient='records').rename(columns={'x':'Long','y':'Lat'}))
        tableNF=tableNF.drop(columns=['OBJECTID','Count_', 'AnalysisArea', 'ORIG_FID', 'SHAPE']).rename(columns={'LOIS':'Ops_Code'})
    except:
        pass
    #Drop any opcodes that are NaNs or empty if they exist
    try:
        tableNF['Ops_Code']=tableNF['Ops_Code'].replace({'':np.nan})
        tableNF=tableNF[tableNF['Ops_Code'].notna()]
    except:
        pass

    # Create table with known schema, drop all rows 
    data12='[{"Ops_Code":"AZZ8JF1","Long":117.338241695,"Lat":-32.401822711},{"Ops_Code":"AZZ8JF1","Long":115.535504945,"Lat":-30.371467414},{"Ops_Code":"AZZ2KF1","Long":115.870765563,"Lat":-30.87934509}]'
    table=pd.read_json(data12, orient='records')
    table=table.loc[:-1]

    #Append df/tableNF if they contain any data
    try:
        table=table.append([df,tableNF], ignore_index=True)
    except:
        pass

    #Filter API'S  Opcodes with zeroes and drop unwanted columns. 
    tc1=tc.drop(columns=[ 'T_AFSCERTIFIEDAREA','T_TITLE',
        'T_FSCCERTIFIEDAREA', 'T_STARTDATE', 'T_ENDDATE', 'T_LATITUDE',
        'T_LONGITUDE', 'T_ACTIVE', 'T_CREATEDDATETIME', 'T_MODIFIEDDATETIME'])
    #Merge with tc1 with table
    try:
        missing13=pd.merge(tc1,table, how='right', left_on='T_OPERATIONNO', right_on='Ops_Code')
        missing1=missing13[(missing13['T_ID'].notna())]
        missing1['SystemId']= "FPC"
    except:
        pass
    #Create a new table to append values from missing1 above. This was done to rid of missing one spatial frame properties
    try:
        mis=pd.DataFrame({'a':[0],'b':['b'],'c':['c'],'d':[1.0],'e':[2.0],'f':['f']})
        mis=mis.assign(a=missing1.T_ID,b=missing1.T_OPERATIONNO,c=missing1.Ops_Code,d=missing1.Long.astype(float),e=missing1.Lat.astype(float),f=missing1.SystemId)
    except:
        pass
    
    #Rename columns to allign to TECH1
    try:
        mis.drop(columns=['c','b'],inplace=True)
        mis.columns=['EngagementId','DE_LOD_INF_LAND_LONG', 'DE_LOD_INF_LAND_LAT','SystemId']
    except:
        pass
    # Prepare payload

    try:
        result = mis.to_json(orient="records")
        parsed = json.loads(result)
        p=json.dumps(parsed)
    except Exception:
        pass
    try:
        url = "APIURL/SaveBulk"#API URL with savebulk command

        payload = {'Items': json.loads(p)}

        print(payload['Items'][0]['SystemId'])
        headers = {'Authorization': access_token.split('"')[3],'Content-Type': 'application/json'}

        response = requests.request("POST", url, headers=headers, data=json.dumps(payload))

        print(response.text.encode('utf8'))
    except Exception:
        pass
    msg1.set(json.dumps(payload))#write message used to trigger this function in new queue