import pandas as pd
import streamlit as st
import altair as alt
import numpy as np
from azure.cosmosdb.table.tableservice import TableService
from datetime import datetime

#Azure Storage Table connection details
CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=bshometemp;AccountKey=5hWbuKuuPfNySSFcgJ9R9nZoPNLwxhu0aV3cva6xu9bgxagvAJPhigcZ2lhSKAOaVlmro9yxIkqFQl2k+Glwbg==;EndpointSuffix=core.windows.net"
SOURCE_TABLE = "bshometemptable"

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

#Date picker
start_date = st.date_input(
     "Select start date",
     datetime(datetime.now().year,datetime.now().month,datetime.now().day - 1))
stop_date = st.date_input(
     "Select stop date",
     datetime.now())


#Filter and retrieve Azure Storage Table
fq = "Timestamp gt datetime'" + start_date.strftime("%Y-%m-%d") + "T00:00:00.0000000Z'" + " and Timestamp lt datetime'" + stop_date.strftime("%Y-%m-%d") + "T00:00:00.0000000Z'"
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

with st.expander("See explanation"):
     st.write("""
         The chart above shows some numbers I picked for you.
         I rolled actual dice for these, so they're *guaranteed* to
         be random.
     """)
     st.altair_chart((chart).interactive(), use_container_width=True)