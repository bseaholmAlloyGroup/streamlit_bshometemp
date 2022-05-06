import pandas as pd
import streamlit as st
import altair as alt
import numpy as np
from azure.cosmosdb.table.tableservice import TableService
from datetime import datetime

import logging
import requests
from requests_ntlm import HttpNtlmAuth
import pandas as pd 

import os
import json

import streamlit.components.v1 as components
from PIL import Image

#Azure Storage Table connection details
CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=bshometemp;AccountKey=5hWbuKuuPfNySSFcgJ9R9nZoPNLwxhu0aV3cva6xu9bgxagvAJPhigcZ2lhSKAOaVlmro9yxIkqFQl2k+Glwbg==;EndpointSuffix=core.windows.net"
SOURCE_TABLE = "bshometemptable"

##############################################################################################################

#Functions to retrieve data from Azure Storage Table
def set_table_service():
    """ Set the Azure Table Storage service """
    return TableService(connection_string=CONNECTION_STRING)

@st.cache
def get_dataframe_from_table_storage_table(table_service, filter_query):
    """ Create a dataframe from table storage data """
    return pd.DataFrame(get_data_from_table_storage_table(table_service,
                                                          filter_query))

def get_data_from_table_storage_table(table_service, filter_query):
    """ Retrieve data from Table Storage """
    for record in table_service.query_entities(
        SOURCE_TABLE, filter=filter_query
    ):
        yield record


def Make_Hist_Request(url, auth, req_method):
    print('Requesting data...')
    while True:
        if req_method == 'GET':
            req = requests.get(url, auth=auth)
        else:
            print('Not a valid request method')
            break
        if req.status_code == 200:
            js = json.loads(req.text)
            if '@odata.nextLink' in js.keys():
                url = js["@odata.nextLink"]
                print ('Getting next page...')
                yield js['value']
            else:
                print("Last page")
                yield js['value']
                break
        else:
            print(req.status_code)
            print(req.text)
            break


def get_results(url, auth, req_method):
    
    #check if server is down
    if Check_Server(url, auth) == 404:
        print(404)
        return 
    
    # Assume more than one page will be returned
    try:
        result = pd.concat((pd.json_normalize(page) for page in Make_Hist_Request(url, auth, req_method)))
        print('more than one page')
    except:
        print(Make_Hist_Request(url, auth, req_method))
        result = pd.json_normalize(Make_Hist_Request(url, auth, req_method))
        print('one page')
        print(result)
    
    return result

def Check_Server(url, auth):
    req = requests.get(url, auth=auth)
    status = req.status_code
    return status


##############################################################################################################



# Use the full page instead of a narrow central column
st.set_page_config(layout="wide")







with st.expander("Home Temperature Expander"):
    
    image = Image.open('bshometemp.jpg')
    st.image(image, caption='IoT Demo Project')

    #Date picker
    start_date = st.date_input(
        "Select start date",
        datetime(datetime.now().year,datetime.now().month,datetime.now().day - 1))
    stop_date = st.date_input(
        "Select stop date",
        datetime.now())


    #Filter and retrieve Azure Storage Table
    #fq = "Timestamp gt datetime'" + start_date.strftime("%Y-%m-%d") + "T00:00:00.0000000Z'" + " and Timestamp lt datetime'" + stop_date.strftime("%Y-%m-%d") + "T00:00:00.0000000Z'"
    fq = "Timestamp gt datetime'" + start_date.strftime("%Y-%m-%d") + "T00:00:00.0000000Z'" + " and Timestamp lt datetime'" + stop_date.strftime("%Y-%m-%dT%H:%M:%S.0000000Z") + "'"
    #fq = ''
    ts = set_table_service()
    df = get_dataframe_from_table_storage_table(table_service=ts,
                                                filter_query=fq)

    #Clean up Pandas dataframe for charting in Altair
    df = df.drop(labels="PartitionKey", axis=1)
    df = df.drop(labels="RowKey", axis=1)
    df = df.drop(labels="Humidity", axis=1)
    df = df.drop(labels="etag", axis=1)
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    df["Temperature"] = pd.to_numeric(df["Temperature"])

    #Chart in Altair and display in Streamlit
    chart = alt.Chart(df).mark_point().encode(
        x='Timestamp:T',
        y='Temperature:Q',
        tooltip=[alt.Tooltip('Timestamp', timeUnit="hoursminutesseconds"), 'Temperature']
    ).properties(
        width=750,
        height=550
    )
    st.altair_chart((chart).interactive(), use_container_width=True)






    
with st.expander("Test Historian Expander"):
    
    image2 = Image.open('wonderware.jpg')
    st.image(image2, caption='Wonderware Demo Project')

    #Input Widgets    
    hist_tag = st.selectbox(
     'Select Historian Tag',
     ('MR-HSBWTP.Loop040_scaled.5M', 'MR-HSBWTP.Loop040_setpoint.5M', 'MR-HSBWTP.Loop001_scaled.5M', 'MR-HSBWTP.Loop012_setpoint.5M', 'MR-HSBWTP.Lime101_feeder_A_speed.5M', 'MR-HSBWTP.Lime102_feeder_A_speed.5M', 'MR-HSBWTP.Lime101_slaker_temperature.5M', 'MR-HSBWTP.Lime102_slaker_temperature.5M', 'MR-HSBWTP.Loop002_PIDout.5M', 'MR-HSBWTP.Loop006_PIDout.5M', 'MR-HSBWTP.Loop040_scaled.5M', 'MR-HSBWTP.Loop002_scaled.5M', 'MR-HSBWTP.Loop006_scaled.5M', 'MR-HSBWTP.Loop016A_scaled.5M', 'MR-HSBWTP.Loop016B_scaled.5M', 'MR-HSBWTP.Loop002_setpoint.5M', 'MR-HSBWTP.Loop006_setpoint.5M', 'MR-HSBWTP.Loop004_scaled.5M', 'MR-HSBWTP.Loop013_scaled.5M', 'MR-HSBWTP.Loop003_scaled.5M', 'MR-HSBWTP.Loop007_scaled.5M', 'MR-HSBWTP.Loop004_on_time.5M', 'MR-HSBWTP.Loop004_off_time.5M', 'MR-HSBWTP.Loop013_on_time.5M', 'MR-HSBWTP.Loop013_off_time.5M', 'MR-HSBWTP.Loop003_setpoint.5M', 'MR-HSBWTP.Loop004_setpoint.5M', 'MR-HSBWTP.Loop007_setpoint.5M', 'MR-HSBWTP.Loop013_setpoint.5M', 'MR-HSBWTP.Poly001_dosage.5M', 'MR-HSBWTP.Poly002_dosage.5M', 'MR-HSBWTP.Poly003_dosage.5M', 'MR-HSBWTP.Loop042_scaled.5M'))

    start_date2 = st.date_input(
        "Select start date2",
        datetime(datetime.now().year,datetime.now().month,datetime.now().day - 1))
    stop_date2 = st.date_input(
        "Select stop date2",
        datetime.now())
    ret_mode = st.selectbox(
     'Select Retrieval Mode',
     ('Average', 'delta', 'full', 'cyclic'))
    resolution = st.selectbox(
     'Select Resolution',
     ('900000', '1800000', '3600000'))
    version = st.selectbox(
     'Select Version',
     ('Latest', 'Original'))
        
    # Authentication
    user = 'DataUser'
    pswd = '0ldW0rk$59711'
    auth = HttpNtlmAuth(user, pswd)
    req_method = 'GET'

    url = "https://data.copperenv.com:32569/Historian/v1/ProcessValues?$filter=(FQN+eq+"
    url +="'" + hist_tag + "'" + ")+and+DateTime+ge+datetimeoffset'"
    url += start_date2.strftime("%Y-%m-%d") + "T00:00:00.000Z'+and+DateTime+le+datetimeoffset'" + stop_date2.strftime("%Y-%m-%d") + "T00:00:00.000Z'"
    url += "&RetrievalMode=" + ret_mode
    url += "&Resolution=" + resolution
    #url += "&Version=" + version

    output = get_results(url, auth, req_method)

    #Clean up Pandas dataframe for charting in Altair
    output = output.drop(labels="OpcQuality", axis=1)
    output = output.drop(labels="Text", axis=1)
    output = output.drop(labels="FQN", axis=1)
    output["DateTime"] = pd.to_datetime(output["DateTime"])
    output["Value"] = pd.to_numeric(output["Value"])
    
    #Chart in Altair and display in Streamlit
    chart2 = alt.Chart(output).mark_point().encode(
        x='DateTime:T',
        y='Value:Q',
        tooltip=[alt.Tooltip('DateTime', timeUnit="hoursminutesseconds"), 'Value']
    ).properties(
        width=750,
        height=550
    )
    st.altair_chart((chart2).interactive(), use_container_width=True)

col1, col2 = st.columns(2)

with st.expander("iFrame from other Websites Expander"):
    with col1:
        # embed USDA SNOTEL chart
        components.iframe("https://www.nrcs.usda.gov/Internet/WCIS/AWS_PLOTS/siteCharts/POR/WTEQ/CO//Lizard%20Head%20Pass.html", width=600, height=500,scrolling=False)
    with col2:
        # embed USDA SNOTEL chart
        components.iframe("https://www.nrcs.usda.gov/Internet/WCIS/AWS_PLOTS/siteCharts/POR/WTEQ/CO//Scotch%20Creek.html", width=600, height=500,scrolling=False)
 