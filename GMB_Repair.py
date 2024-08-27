print('Start GMB Places API...')

import requests
import pandas as pd
import numpy as np
from datetime import datetime
from decouple import config
import snowflake.connector

# Config file
api_key = config('API_KEY_GLE')
table_name_ref = config('TABLE_NAME_GMB_REF')
table_name_fact = config('TABLE_NAME_GMB_FACT')
table_name_stg = config('TABLE_NAME_GMB_BATCH')
USER_MSO = config('USER_MSO')
PASS = config('PASSWORD')
ACCOUNT = config('ACCOUNT')
DB = config('DB')
SCHEMA_REF = config('SCHEMA_REF')
SCHEMA_HIST = config('SCHEMA_HIST')
SCHEMA_BATCH = config('SCHEMA_BATCH')

df = pd.read_excel(r"C:\Users\dshett0\Desktop\tester.xlsx", engine='openpyxl')
urls = df.get("Links")
store = df.get("StoreCode")
temp_urls = list(urls)
temp_store = list(store)

# Create an empty list to store each location's data
ref_data = []
fact_data = []

for counter, i in enumerate(temp_urls, start=1):
	place_id = (i.split('=')[1])
	store_code = temp_store[counter - 1]
	url = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&fields=name,business_status,place_id,rating,user_ratings_total,formatted_address,url,international_phone_number,website&key={api_key}"
	response = requests.get(url)
	data = response.json()	
	
	if "result" in data:
		url = data["result"].get("url", "")
		name = data["result"].get("name", "")
		address_full = data["result"].get("formatted_address", "")
		rating = round(float(data["result"].get("rating", 0)), 1)
		rating_count = int(data["result"].get("user_ratings_total", 0))
		status = data["result"].get("business_status", "")
		#phone = data["result"].get("formatted_phone_number", "")
		int_phone = data["result"].get("international_phone_number", "")
		updt_int_phone = int_phone.replace(" ", "")
		website = data["result"].get("website", "")
		#reviews = data["result"].get("reviews", [])
		
		# Split the full address into components
		address_parts = [part.strip() for part in address_full.split(',')]	
		print(address_parts)
		if len(address_parts) == 4 and address_parts[3] not in ["USA", "United States", "Puerto Rico"]:
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
			if(address_parts[3] == 'Puerto Rico'):
				state_zip_parts = ['PR',state_zip]
			else:
				state_zip_parts = [part.strip() for part in state_zip.rsplit(' ', 1)] if ' ' in state_zip else ['', '']			
			state, zip_code = state_zip_parts if state_zip_parts else ('', '')
			zip_code = 'NA' if zip_code == '' else zip_code
			print(f"Old: {address}, {city}, {state}, {zip_code}")
		
		data_dict = {					
			"Business_Name": name,
			"Store_Code": store_code,
			"PlaceId": place_id,
			"Location_URL": url,
			"Address": address,
			"City": city,
			"State": state,
			"Zip": zip_code,
			"Status": status,
			"Phone_Number": updt_int_phone.replace("-", ""),			
			"Website": website,
			"Run_Date": datetime.now().date()
		}
		ref_data.append(data_dict)
		
		fact_dict = {
			"PlaceId": place_id,
			"Current_Rating": rating,
			"Current_Rating_Count": rating_count,
			"Date": datetime.now().date(),
			"Insert_Date": datetime.now().date(),
			"Update_Date": datetime.now().date()
		}
		fact_data.append(fact_dict)
		
		if counter == 10:
			break
	# Print the counter value to track progress
	print(f"Processing location {counter}/{len(temp_urls)}")

# Create a DataFrame from the list of dictionaries
df = pd.DataFrame(ref_data)
df_fact = pd.DataFrame(fact_data)

# Remove duplicate entries based on all columns
#df_reviews = df_reviews.drop_duplicates(subset=["URL", "Author Name"])

# Display the DataFrame
#print(df)

# Export the DataFrame to a CSV file
#df.to_excel("Repair_Extract_Ref.xlsx", index=False)
#df_fact.to_excel("Repair_Extract_Fact.xlsx", index=False)

#Create table
create_table_query_ref = f'''
    CREATE OR REPLACE TABLE {table_name_ref} (        
        "BUSINESS_NAME" STRING,
		"STOREID" INT,
        "PLACEID" STRING,
        "LOCATION_URL" STRING,
        "ADDRESS" STRING,
        "CITY" STRING,
        "STATE" STRING,
        "ZIP" STRING,
		"STATUS" STRING,
		"PHONE_NUMBER" STRING,
		"WEBSITE" STRING,
		"RUN_DATE" DATE
    )
'''
#print("Connecting to Snowflake...")
# Snowflake connection parameters
#snowflake_config = {
#	'user': USER_MSO,
#	'password': PASS,
#	'account': ACCOUNT,
#	'warehouse': '',
#	'database': DB,
#	'schema': SCHEMA_REF,
#}
#conn = snowflake.connector.connect(**snowflake_config)
#cursor = conn.cursor()
#cursor.execute(create_table_query_ref)
#print(f"Table created: {table_name_ref}")
#data_tuples = [tuple(row) for row in df.itertuples(index=False)]
#insert_query = f'INSERT INTO {table_name_ref} VALUES ({",".join(["%s"] * len(df.columns))})'
#cursor.executemany(insert_query, data_tuples)
#print(f"Values inserted: {table_name_ref}")

create_table_query_fact = f'''
    CREATE OR REPLACE TABLE {table_name_stg} (          
        "PLACEID" STRING,
		"CURRENT_RATING" FLOAT,
		"CURRENT_RATING_COUNT" INT,
		"RATING_LOAD_DATE" DATE,
		"INSERT_DATE" DATE,
		"UPDATE_DATE" DATE
    )
'''

print("Connecting to Snowflake...")
# Snowflake connection parameters
snowflake_config = {
	'user': USER_MSO,
	'password': PASS,
	'account': ACCOUNT,
	'warehouse': '',
	'database': DB,
	'schema': SCHEMA_BATCH,
}
conn = snowflake.connector.connect(**snowflake_config)
cursor = conn.cursor()
cursor.execute(create_table_query_fact)
print(f"Table created: {table_name_stg}")
data_tuples = [tuple(row) for row in df_fact.itertuples(index=False)]
insert_query = f'INSERT INTO {table_name_stg} VALUES ({",".join(["%s"] * len(df_fact.columns))})'
cursor.executemany(insert_query, data_tuples)
print(f"Values inserted: {table_name_stg}")
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()
print("Connection closed...")