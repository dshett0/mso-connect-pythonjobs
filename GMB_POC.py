print('Start GMB Places API...')

import requests
import pandas as pd
import numpy as np
from datetime import datetime
from decouple import config
import snowflake.connector

# Config file
api_key = config('API_KEY_GLE')
table_name = config('TABLE_NAME_GMB')
USER_MSO = config('USER_MSO')
PASS = config('PASSWORD')
ACCOUNT = config('ACCOUNT')
DB = config('DB')
SCHEMA_BATCH = config('SCHEMA_BATCH')

# Create an empty list to store each location's data
ref_data = []
fact_data = []

def get_all_reviews(place_id):
	all_reviews = []
	
	def fetch_reviews(page_token=None):
		url = f'https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&fields=reviews&key={api_key}'
		if page_token:
			url += f'&page_token={page_token}'
		
		response = requests.get(url)
		result = response.json()
		
		if 'error_message' in result:
			print(f"Error: {result['error_message']}")
			return None
		
		reviews = result.get('result', {}).get('reviews', [])
		all_reviews.extend(reviews)
		
		return result.get('next_page_token', None)	

	next_page_token = fetch_reviews()
	
	while next_page_token:		
		next_page_token = fetch_reviews(next_page_token)
	
	return all_reviews

temp = "ChIJQcWmD-Yx3YARD-HvYQdlqdI"
all_reviews = get_all_reviews(temp)
#print(all_reviews)
for review in all_reviews:
	author = review.get("author_name", "")
	author_rating = round(float(review.get("rating", 0)), 1)
	author_review = review.get("text", "")
	author_time = review.get("time", "")
	
	fact_dict = {
		"PlaceId": temp,
		"Author": author,
		"Rating": author_rating,
		"Review": author_review,
		"Review_time": datetime.utcfromtimestamp(author_time).strftime('%Y-%m-%d %H:%M:%S'),
		"Insert_Date": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	}
	fact_data.append(fact_dict)		

# Print the counter value to track progress
#print(f"Processing location {counter}/{len(temp)}")

# Create a DataFrame from the list of dictionaries
df_fact = pd.DataFrame(fact_data)

# Remove duplicate entries based on all columns
#df_reviews = df_reviews.drop_duplicates(subset=["URL", "Author Name"])

# Display the DataFrame
print(df_fact)

# Export the DataFrame to a CSV file
#df.to_excel("Repair_Extract_Ref.xlsx", index=False)
#df_fact.to_excel("Repair_Extract_Fact.xlsx", index=False)

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

#Create table
create_table_query_ref = f'''
    CREATE OR REPLACE TABLE {table_name} (
        "Run_Date" DATE,
        "Business_Name" STRING,
        "PlaceId" STRING,
        "Location_URL" STRING,
        "Address" STRING,
        "State" STRING,
        "City" STRING,
        "Zip" INT,
		"Overall_Rating" FLOAT,
		"Rating_Count" INT,
		"Status" STRING
    )
'''

create_table_query_fact = f'''
    CREATE OR REPLACE TABLE {table_name} (          
        "PLACEID" STRING,
		"AUTHOR" STRING,
		"RATING" INT,
		"REVIEWS" STRING,
		"REVIEW_TS" TIMESTAMP,
		"INSERT_TS" TIMESTAMP
    )
'''

#cursor.execute(create_table_query_fact)
#print("Table created.")

# Convert DataFrame to a list of tuples
data_tuples = [tuple(row) for row in df_fact.itertuples(index=False)]

# Upload DataFrame to Snowflake table
insert_query = f'INSERT INTO {table_name} VALUES ({",".join(["%s"] * len(df_fact.columns))})'
cursor.executemany(insert_query, data_tuples)
print("Values inserted.")

# Commit changes
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()
print("Connection closed...")