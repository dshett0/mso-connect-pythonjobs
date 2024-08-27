#!/usr/bin/env python3
print('Local Falcon project...')

import sys
import snowflake.connector
import requests
import pandas as pd
import numpy as np
import urllib.parse as ul
from datetime import datetime, timedelta
from decouple import config
from math import radians, sin, cos, sqrt, atan2
import logging
today = datetime.now().date()
#log_file_path = f'/home/dshett0/python_project/logs/log_{today}.log'
log_file_path = f'log_{today}.log'

# Configure the logging module
logging.basicConfig(
    filename=log_file_path,  # Specify the log file name
    level=logging.INFO,      # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s [%(levelname)s]: %(message)s',  # Specify the log message format
    datefmt='%Y-%m-%d %H:%M:%S'  # Specify the date and time format
)

# Config file
api_key = config('API_KEY_SFLAKE')
table_name_gmb = config('TABLE_NAME_REPORT')
table_name_comp = config('TABLE_NAME_COMP')
USER_MSO = config('USER_MSO')
PASS = config('PASSWORD')
ACCOUNT = config('ACCOUNT')
DB = config('DB_PROD')
SCHEMA_HIST = config('SCHEMA_HIST')
SCHEMA_REF = config('SCHEMA_REF')

#Functions
def haversine(lat1, lon1, lat2, lon2):
	# Convert latitude and longitude from degrees to radians
	lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
	# Haversine formula
	dlat = lat2 - lat1
	dlon = lon2 - lon1
	a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
	c = 2 * atan2(sqrt(a), sqrt(1 - a))
	# Earth radius in miles
	R = 3958.8
	# Calculate the distance
	distance = R * c	
	return round(float(distance), 2)

def custom_sort(group):
    return group.sort_values(by=["ATRP", "ARP", "SoLV"], ascending=[True, True, False], na_position="last")

def get_google_map_url (business_name, place_id):	
	encoded_name = ul.quote_plus(business_name)
	final_url = f"https://www.google.com/maps/search/?api=1&query={encoded_name}&query_place_id={place_id}"
	return final_url

def store_to_snowflake (df, method, query_table, dt):
	print("Connecting Snowflake...")
	
	#Create table Parent
	gmb_create_table_query = f'''
		CREATE OR REPLACE TABLE {query_table} (
			"REPORT_KEY" STRING,
			"GMB_STOREID" INT,
			"GMB_BUSINESS_NAME" STRING,
			"KEYWORD" STRING,			
			"GMB_PLACEID" STRING,
			"GMB_ADDRESS" STRING,
			"GMB_STATE" STRING,
			"GMB_CITY" STRING,
			"GMB_ZIP" INT,
			"GMB_GOOGLE_URL" STRING,
			"SCAN_DATE" TIMESTAMP,
			"GMB_RATING" FLOAT,
			"GMB_REVIEWS" INT,
			"GMB_ARP" FLOAT,
			"GMB_ATRP" FLOAT,
			"GMB_SOLV" FLOAT,
			"RUN_DATE" DATE			
		)
	'''
	
	#Create table Competitor
	comp_create_table_query = f'''
		CREATE OR REPLACE TABLE {query_table} (			
			"REPORT_KEY" STRING,
			"RANK" INT,
			"BUSINESS_NAME" STRING,
			"ADDRESS" STRING,
			"PLACEID" STRING,
			"RATING" FLOAT,
			"REVIEWS" INT,
			"GOOGLE_URL" STRING,
			"SCAN_DATE" TIMESTAMP,
			"GRID_SIZE" STRING,
			"RADIUS" STRING,
			"FOUND_IN" INT,
			"FOUND_PERCENTAGE" FLOAT,
			"ARP" FLOAT,
			"ATRP" FLOAT,
			"SOLV" FLOAT,
			"DISTANCE_FROM_CENTER" STRING,	
			"RUN_DATE" DATE			
		)
	'''
	
	#Delete statement
	gmb_delete_query = f'''
		DELETE FROM {query_table} WHERE "SCAN_DATE" = '{dt}'
	'''
	comp_delete_query = f'''
		DELETE FROM {query_table} WHERE "SCAN_DATE" = '{dt}'
	'''
	
	#Select statement
	gmb_select_query = f'''
		SELECT * FROM {query_table} WHERE "SCAN_DATE" = '{dt}'
	'''
	comp_select_query = f'''
		SELECT * FROM {query_table} WHERE "SCAN_DATE" = '{dt}'
	'''
	
	if method == 1:
		# Snowflake connection parameters
		snowflake_config = {
			'user': USER_MSO,
			'password': PASS,
			'account': ACCOUNT,
			'warehouse': '',
			'database': DB,
			'schema': SCHEMA_REF,
		}
		conn = snowflake.connector.connect(**snowflake_config)
		cursor = conn.cursor()
		
		cursor.execute(gmb_create_table_query)
		#cursor.execute(gmb_select_query)
		#selected_data = cursor.fetchall()
		#cursor.execute(gmb_delete_query)
		#conn.commit()
	if method == 2:		
		# Snowflake connection parameters
		snowflake_config = {
			'user': USER_MSO,
			'password': PASS,
			'account': ACCOUNT,
			'warehouse': '',
			'database': DB,
			'schema': SCHEMA_HIST,
		}
		conn = snowflake.connector.connect(**snowflake_config)
		cursor = conn.cursor()
		
		cursor.execute(comp_create_table_query)	
		#cursor.execute(comp_delete_query)
		#conn.commit()
	print("Table deleted.")
	
	data_tuples = [tuple(row) for row in df.itertuples(index=False)]	
	# Upload DataFrame to Snowflake table
	insert_query = f'INSERT INTO {query_table} VALUES ({",".join(["%s"] * len(df.columns))})'
	cursor.executemany(insert_query, data_tuples)
	print("Values inserted.")
	#logging.debug("Values inserted.")
	
	# Commit changes
	conn.commit()
	if method == 2:
		# Close the cursor and connection
		cursor.close()
		conn.close()
		print("Connection closed...")
		#logging.debug("Connection closed...")
	return "Success"

#Get reports
yesterday = today - timedelta(days=1)
updated_date = yesterday.strftime('%m/%d/%Y')
url = f"https://api.localfalcon.com/v1/reports/?api_key={api_key}&limit=100"
#&start_date={updated_date}&end_date={updated_date}
response = requests.get(url)
data = response.json()

if "data" in data and "success" in data:
	if "count" in data["data"] and data["data"]["count"] > 0:
		# API request was successful
		reportCount = data["data"].get("count", "")
		reports = data["data"].get("reports", "") 
		print(f"First count: {len(reports)}")
		next_token = data["data"].get("next_token", "")
		while next_token:
			next_url = f"https://api.localfalcon.com/v1/reports/?api_key={api_key}&limit=100&next_token={next_token}"
			response = requests.get(next_url)
			data = response.json()
			if "data" in data and "success" in data: 
				next_reports = data["data"].get("reports", "")
				print(f"Second count: {len(next_reports)}")
				next_token = data["data"].get("next_token", "")
				reports = reports + next_reports
			else:
				error_message = data.get("message", "")
				print(f"API request failed: {error_message}")
				logging.error(f"API request failed: {error_message}")
	else:
		error_message = f"No reports for given date: {yesterday}"
		print(error_message)
		logging.error(error_message)
		sys.exit(1)
else:
    # Handle the case when "data" or "success" is not present in the response
	error_message = data.get("message", "")
	print(f"API request failed: {error_message}")
	logging.error(f"API request failed: {error_message}")
	sys.exit(1)

print(f"Total count... {len(reports)}")
# Create an empty list to store each location's data
comp_data = []
report_data = []

for counter, report in enumerate(reports, start=1):
	report_key = report["report_key"]	
	curl = f"https://api.localfalcon.com/v1/reports/{report_key}/?api_key={api_key}"
	response = requests.get(curl)
	data = response.json()
	
	if "success" in data:
		name = data["data"]["location"].get("name", "")
		address_full = data["data"]["location"].get("address", "")
		store_code = data["data"]["location"].get("store_code", "")
		store_code = 0 if store_code == '' or str(store_code).lower() == 'false' else int(store_code)
		store_place_id = data["data"]["location"].get("place_id", "")
		keyword = data["data"].get("keyword", "")
		gmb_rating = round(float(data["data"]["location"].get("rating", "")), 1)
		gmb_reviews = int(data["data"]["location"].get("reviews", ""))
		gmb_arp = round(float(data["data"].get("arp", "").replace("20+", "20")), 2)
		gmb_atrp = round(float(data["data"].get("atrp", "").replace("20+", "20")), 2)
		gmb_solv = round(float(data["data"].get("solv", "").replace("20+", "20")), 2)
		scan_date = data["data"].get("date", "")
		# Convert the date string to a datetime object
		date_object = datetime.strptime(scan_date, '%m/%d/%Y %I:%M %p')
		# Format the datetime object as a string in the desired format
		formatted_date = date_object.strftime('%Y-%m-%d %H:%M:%S')
		
		lat1 = float(data["data"]["location"].get("lat", ""))
		lng1 = float(data["data"]["location"].get("lng", ""))
		
		# Split the full address into components
		address_parts = [part.strip() for part in address_full.split(',')]		
		if len(address_parts) == 4 and address_parts[3] not in ["USA", "United States"]:
			address = address_parts[0] + ', ' + address_parts[1]
			city = address_parts[2] if len(address_parts) > 2 else ""
			state_zip = address_parts[3] if len(address_parts) > 3 else ""		
			state_zip_parts = [part.strip() for part in state_zip.rsplit(' ', 1)] if ' ' in state_zip else ['', '']
			state, zip_code = state_zip_parts if state_zip_parts else ('', '')
			zip_code = 0 if zip_code == '' else int(zip_code)
			print(f"New: {address}, {city}, {state}, {zip_code}")
		else:
			address = address_parts[0] if len(address_parts) > 0 else ""
			city = address_parts[1] if len(address_parts) > 1 else ""
			state_zip = address_parts[2] if len(address_parts) > 2 else ""		
			state_zip_parts = [part.strip() for part in state_zip.rsplit(' ', 1)] if ' ' in state_zip else ['', '']
			state, zip_code = state_zip_parts if state_zip_parts else ('', '')
			zip_code = 0 if zip_code == '' else int(zip_code)
			print(f"Old: {address}, {city}, {state}, {zip_code}")
		
		gmb_data_dict = {
			"Report_key": report_key,
			"GMB_StoreID": store_code,
			"GMB_Business_Name": name,
			"Keyword": keyword,			
			"GMB_PlaceId": store_place_id,
			"GMB_Address": address,
			"GMB_State": state,
			"GMB_City": city,
			"GMB_Zip": zip_code,
			"GMB_Google_URL": get_google_map_url(name,store_place_id),
			"Scan_Date": formatted_date,
			"GMB_Rating": gmb_rating,
			"GMB_Reviews": gmb_reviews,
			"GMB_ARP": gmb_arp,
			"GMB_ATRP": gmb_atrp,
			"GMB_SOLV": gmb_solv,
			"Run_Date": today			
		}
		report_data.append(gmb_data_dict)								
		
		grid = data["data"].get("grid_size", "")
		radius = data["data"].get("radius", "")
		places_data = data["data"]["places"]
		rankings_by_arp = data["data"]["rankings"]["by_arp"]
		rankings_by_atrp = data["data"]["rankings"]["by_atrp"]
		rankings_by_solv = data["data"]["rankings"]["by_solv"]				
		
		for rank, (place_id, solv_value) in enumerate(rankings_by_solv.items(), start=1):
			place_data = places_data.get(place_id, {})
			lat2 = place_data.get("lat", "")
			lng2 = place_data.get("lng", "")
			if lat2 and lng2:
				lat2 = float(lat2)
				lng2 = float(lng2)
				distance_in_miles = haversine(lat1, lng1, lat2, lng2)
				distance_in_miles = f"{distance_in_miles} mi"
			else:
				distance_in_miles = "NA"
			data_dict = {				
				"Report_key": report_key,
				"Rank": rank,
				"Business_Name": place_data.get("name", ""),
				"Address": place_data.get("address", ""),
				"PlaceId": place_data.get("place_id", ""),
				"Rating": round(float(place_data.get("rating", "")),1),
				"Reviews": int(place_data.get("reviews", "")),
				"Google_URL": get_google_map_url(place_data.get("name"),place_data.get("place_id")),
				"Scan_Date": formatted_date,
				"Grid_Size": f"{grid}x{grid}",
				"Radius": f"{radius}mi",
				"Found_In": place_data.get("found_in", ""),
				"Found_Percentage": round(float(place_data.get("found_in_pct", "")), 2),
				"ARP": round(float(place_data.get("arp", "").replace("20+", "20")), 2),
				"ATRP": round(float(place_data.get("atrp", "").replace("20+", "20")), 2),
				"SoLV": round(float(place_data.get("solv", "").replace("20+", "20")), 2),
				"Distance_from_Center": distance_in_miles,
				"Run_Date": today				
			}
			comp_data.append(data_dict)
		#break		
	print(f"Processing location {counter}/{len(reports)}")

# Create a DataFrame from the list of dictionaries
gmb_df = pd.DataFrame(report_data)
df = pd.DataFrame(comp_data)

sorted_df = df.groupby("Report_key", group_keys=False).apply(custom_sort)
sorted_df.reset_index(drop=True, inplace=True)
sorted_df['Rank'] = sorted_df.groupby('Report_key').cumcount() + 1

# Remove duplicate entries
#df_reviews = df_reviews.drop_duplicates(subset=["URL", "Author Name"])

# Sort by Solv
#df = df.sort_values(by="SoLV", ascending=False)

#print(df)

# Export the DataFrame to a CSV file
#sorted_df.to_excel(f"LocalFalcon_Comp_{today}.xlsx", index=False)
#print("Comp Excel generated...")
#gmb_df.to_excel(f"LocalFalcon_Report_{today}.xlsx", index=False)
#print("Report Excel generated...")
#print("Skip Excel export")

#Store comp data
method = 1
store_Data = store_to_snowflake(gmb_df, method, table_name_gmb, yesterday)
print(f"Storage status reports: {store_Data}")
logging.info(f"Storage status reports: {store_Data}")

method = 2
store_Data = store_to_snowflake(sorted_df, method, table_name_comp, yesterday)
print(f"Storage status comp: {store_Data}")
logging.info(f"Storage status comp: {store_Data}")