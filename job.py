import requests
import time
import pandas as pd
import numpy as np
from datetime import datetime
from decouple import config
import snowflake.connector
import logging
today = datetime.now().date()
log_file_path = f"/home/dshett0/python_project/chatmeter_logs/log_{today}.log"
logging.basicConfig(
    filename=log_file_path,  # Specify the log file name
    level=logging.INFO,      # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s [%(levelname)s]: %(message)s',  # Specify the log message format
    datefmt='%Y-%m-%d %H:%M:%S'  # Specify the date and time format
)

table_name_ref = config('TABLE_NAME_GMB_REF')
table_name_fact = config('TABLE_NAME_GMB_FACT')
table_name_stg = config('TABLE_NAME_GMB_BATCH')
USER_MSO = config('USER_MSO')
PASS = config('PASSWORD')
ACCOUNT = config('ACCOUNT')
DB = config('DB_PROD')
SCHEMA_REF = config('SCHEMA_REF')
SCHEMA_HIST = config('SCHEMA_HIST')
SCHEMA_BATCH = config('SCHEMA_BATCH')

def get_access_token():
    token_url = 'https://live.chatmeter.com/v5/login'
    token_payload = {
        "username": "SearsApi",
        "password": "Ch@tm3t3rAPI!"
    }
    token_headers = {
        'Content-Type': 'application/json',
    }

    response = requests.post(token_url, headers=token_headers, json=token_payload)
    print(response)
    if response.status_code == 200:
        return response.json().get('token', '')
    else:
        raise Exception(f"Failed to get access token. Status code: {response.status_code}")

def make_api_request(url, headers, attempt_refresh=True):
    response = requests.get(url, headers=headers)
    
    if response.status_code == 401 and attempt_refresh:
        new_token = get_access_token()
        headers['Authorization'] = new_token
        return make_api_request(url, headers, attempt_refresh=False)
    
    return response

headers = {
	'Content-Type': 'application/json',
	'Authorization': 'Bearer'
}
url = f"https://live.chatmeter.com/v5/locations?limit=2000"
response = make_api_request(url, headers=headers)
data = response.json()
ref_data = []
fact_data = []

def create_table(connection, table_name, columns):
    create_table_query = f'''
        CREATE OR REPLACE TABLE {table_name} (
            {", ".join([f'"{col}" {columns[col]}' for col in columns])}
        )
    '''
    with connection.cursor() as cursor:
        cursor.execute(create_table_query)
    print(f"Table created: {table_name}")

def insert_data(connection, table_name, data_df):
    data_tuples = [tuple(row) for row in data_df.itertuples(index=False)]
    insert_query = f'INSERT INTO {table_name} VALUES ({",".join(["%s"] * len(data_df.columns))})'
    with connection.cursor() as cursor:
        cursor.executemany(insert_query, data_tuples)
    print(f"Values inserted: {table_name}, Total values inserted: {len(data_df)}")
    connection.commit()

def connect_to_snowflake(config):
    return snowflake.connector.connect(**config)

snowflake_query = f'SELECT * FROM {table_name_fact}'

snowflake_config_fact = {
    'user': USER_MSO,
    'password': PASS,
    'account': ACCOUNT,
    'warehouse': '',
    'database': DB,
    'schema': SCHEMA_BATCH,
}

snowflake_config_ref = {
    'user': USER_MSO,
    'password': PASS,
    'account': ACCOUNT,
    'warehouse': '',
    'database': DB,
    'schema': SCHEMA_REF,
}

snowflake_config_hist = {
    'user': USER_MSO,
    'password': PASS,
    'account': ACCOUNT,
    'warehouse': '',
    'database': DB,
    'schema': SCHEMA_HIST,
}

# Connection
conn_fact = connect_to_snowflake(snowflake_config_fact)
conn_ref = connect_to_snowflake(snowflake_config_ref)
conn_hist = connect_to_snowflake(snowflake_config_hist)

def compare_df_with_snowflake(df, snowflake_query):
    cursor = conn_hist.cursor()
    cursor.execute(snowflake_query)
    snowflake_data = cursor.fetchall()
    cursor.close()
    snowflake_df = pd.DataFrame(snowflake_data, columns=[col[0] for col in cursor.description])
    snowflake_df = snowflake_df.sort_values(by=['RATING_LOAD_DATE', 'INSERT_DATE', 'UPDATE_DATE'], ascending=False).drop_duplicates('STOREID')
    merged_df = pd.merge(df, snowflake_df, on='STOREID', suffixes=('_df1', '_df2'))
    unmatched_df_rows = df[~df['STOREID'].isin(merged_df['STOREID'])]
    different_records = merged_df[(merged_df['CURRENT_RATING_df1'] != merged_df['CURRENT_RATING_df2']) |
                                (merged_df['CURRENT_RATING_COUNT_df1'] != merged_df['CURRENT_RATING_COUNT_df2'])]
    different_records_formatted = different_records[['STOREID', 'CURRENT_RATING_df1', 'CURRENT_RATING_COUNT_df1']]
    different_records_formatted.columns = ['STOREID', 'CURRENT_RATING', 'CURRENT_RATING_COUNT']
    final_result = pd.concat([different_records_formatted, unmatched_df_rows])
    return final_result


if "locations" in data:
    locations = data.get("locations", [])
    for location in locations:
        id = location.get("id", "")
        resellerLocationId = location.get("resellerLocationId", "")
        busName = location.get("busName", "")
        website = location.get("website", "")
        
        address = location.get("address", {})
        mainAddress = address.get("street", "").strip()
        if not mainAddress:  # Check if empty after stripping
            mainAddress = "Not available"
        city = address.get("city", "")
        state = address.get("state", "")
        zipc = address.get("postalCode", "")
        
        primaryPhone = location.get("primaryPhone", "")
        if primaryPhone.startswith('1'):
            primaryPhone = primaryPhone[1:]
        
        customListings = location.get("customListings", [])
        listingurl = customListings[0].get("listingURL", "") if customListings else ""
        
        listingManagementSpecs = location.get("listingManagementSpecs", {})
        status = listingManagementSpecs.get("permanentlyClosed", True)
        
        provide = listingManagementSpecs.get("providerTrackingWebsiteUrls", {})
        googlemap = provide.get("GOOGLEMAP", "") if provide else ""
        if busName == "Sears Appliance Repair" or busName == "Sears Home Services":
            ref_dict = {
                "BUSINESS_NAME": busName,
                "STOREID": resellerLocationId,
                "LISTING_URL": listingurl,
                "GOOGLE_MAP_URL": googlemap,
                "ADDRESS": mainAddress,
                "CITY": city,
                "STATE": state,
                "ZIP": zipc,
                "STATUS": status,
                "PHONE_NUMBER": primaryPhone,            
                "WEBSITE": website,
                "RUN_DATE": datetime.now().date()
            }
            ref_data.append(ref_dict)
    columns_ref = {
		"BUSINESS_NAME": "STRING",
		"STOREID": "STRING",
		"LISTING_URL": "STRING",
		"GOOGLE_MAP_URL": "STRING",
		"ADDRESS": "STRING",
		"CITY": "STRING",
		"STATE": "STRING",
		"ZIP": "STRING",
		"STATUS": "BOOLEAN",
		"PHONE_NUMBER": "STRING",
		"WEBSITE": "STRING",
		"RUN_DATE": "DATE",
	}
    ref_df = pd.DataFrame(ref_data)
    
    #Remove duplicate storeid from dataframe
    df_unique = ref_df.drop_duplicates(subset=['STOREID'])
    print("Unique length of stores",len(df_unique))
    
    create_table(conn_ref, table_name_ref, columns_ref)
    
    insert_data(conn_ref, table_name_ref, df_unique)
    conn_ref.close()
    for location in locations:
        id = location.get("id", "")
        resellerLocationId = location.get("resellerLocationId", "")
        busName = location.get("busName", "")
        website = location.get("website", "")
        
        address = location.get("address", {})
        mainAddress = address.get("street", "")
        city = address.get("city", "")
        state = address.get("state", "")
        zipc = address.get("postalCode", "")
        
        primaryPhone = location.get("primaryPhone", "")
        
        customListings = location.get("customListings", [])
        listingurl = customListings[0].get("listingURL", "") if customListings else ""
        
        listingManagementSpecs = location.get("listingManagementSpecs", {})
        status = listingManagementSpecs.get("permanentlyClosed", True)
        
        provide = listingManagementSpecs.get("providerTrackingWebsiteUrls", {})
        googlemap = provide.get("GOOGLEMAP", "") if provide else ""
        if busName == "Sears Appliance Repair" or busName == "Sears Home Services":
            headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer'
            }
            url = f"https://live.chatmeter.com/v5/dashboard/reviewReport?reportRange=AllTime&locationId={id}"
            time.sleep(1)
            response = make_api_request(url, headers=headers)
            mainData = response.json()

            if(mainData["periodSummary"]):				
                count = mainData["periodSummary"].get("count", 0)
                avgRating = mainData["periodSummary"].get("avgRating", 0)
                mainRate = round(avgRating, 1)
            else:
                count = 0
                mainRate = 0
            fact_dict = {
                "STOREID": resellerLocationId,
                "CURRENT_RATING": mainRate,
                "CURRENT_RATING_COUNT": count,
                "RATING_LOAD_DATE": datetime.now().date(),
                "INSERT_DATE": datetime.now().date(),
                "UPDATE_DATE": datetime.now().date()
            }
            
            fact_data.append(fact_dict)
    fact_df = pd.DataFrame(fact_data)
    columns_fact = {
		"STOREID": "STRING",
		"CURRENT_RATING": "FLOAT",
		"CURRENT_RATING_COUNT": "INT",
		"RATING_LOAD_DATE": "DATE",
		"INSERT_DATE": "DATE",
		"UPDATE_DATE": "DATE",
	}
    create_table(conn_fact, table_name_stg, columns_fact)
    insert_data(conn_fact, table_name_stg, fact_df)
    conn_fact.close()
    different_records_df = compare_df_with_snowflake(fact_df, snowflake_query)
    different_records_df["RATING_LOAD_DATE"] = datetime.now().date()
    different_records_df["INSERT_DATE"] = datetime.now().date()
    different_records_df["UPDATE_DATE"] = datetime.now().date()
    insert_data(conn_hist, table_name_fact, different_records_df)
    conn_hist.close()
