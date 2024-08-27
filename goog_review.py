import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
s_counter = 0


def hit_url(row):
	global s_counter
	url = row['url']
	city = row['city']
	username = "er.dheerajshetty"
	password = "OverHaul!ga3837"
	
	s_counter += 1
	print(f"Counter: {s_counter}")	
	
	try:				
		#Hit bs4
		#response = requests.get(url)
		#response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx status codes)				
		#html_content = response.text		
		#soup = BeautifulSoup(html_content, 'html.parser')
		
		#Hit selenium
		#driver_path = r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"
		chrome_options = webdriver.ChromeOptions()
		#chrome_options.binary_location = driver_path
		chrome_options.add_argument("--user-data-dir=C:/User/dshett0/Desktop/chrome_temp_profile")
		chrome_options.add_argument("--profile-directory=Default")
		#remote_debugging_port = 9225
		#chrome_options = webdriver.ChromeOptions()
		#chrome_options.add_experimental_option("debuggerAddress", f"localhost:{remote_debugging_port}")
		
		driver = webdriver.Chrome(options=chrome_options)
		print(url)
		driver.get(url)
		driver.find_element(By.ID, 'identifierId').send_keys(username)
		time.sleep(5)
		driver.find_element(By.ID, 'identifierNext').click()
		#time.sleep(15)
		
		#review_form_div = driver.find_element_by_class_name('goog-reviews-write')
		review_form_div = driver.find_element(By.CLASS_NAME, 'goog-reviews-write')
		
		#script_tags = soup.find_all('iframe')		
		#review_widget_elements = soup.find('div', class_=lambda value: value and 'goog-reviews-write-widget' in value)
				
		print(review_form_div)
		if review_form_div:
			print("The review widget element is present.")
		else:
			print("The review widget element is not present.")
		driver.quit()
	except requests.exceptions.RequestException as e:
		print(f"Error fetching URL: {e}")
		return False	
		
def main():
	#df = pd.read_excel(r"C:\Users\dshett0\Downloads\Pages_to_be_scrapped.xlsx", engine='openpyxl')	
	df = pd.read_excel(r"C:\Users\dshett0\Desktop\tester2.xlsx", engine='openpyxl')	
	all_data = []
	
	tempurls = df["G Review Link"].tolist()	
	tempcity = df["City"].tolist()
	tempStoreId = df["Store code"].tolist()
	
	df_temp = pd.DataFrame({'storeId': tempStoreId,'url': tempurls, 'city': tempcity})
	df_temp['valid_url'] = df_temp.apply(hit_url, axis=1)
	all_data.append(df_temp)
	df_temp = pd.concat(all_data, axis=1)
	print(df_temp)
	#df_temp.to_excel("goog_review_vld.xlsx", index=False, engine='openpyxl')
	#print("Excel generated")

if __name__ == "__main__":
	main()