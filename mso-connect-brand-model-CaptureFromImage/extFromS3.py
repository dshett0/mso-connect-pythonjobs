import json
import boto3
import base64
import requests
import urllib.parse
import os
from datetime import datetime

# Initialize AWS clients
s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb')

# Environment variables
table_name = os.environ.get('DYNAMODBTABLENAME')
api_key = os.environ.get('OPENAIKEY')

# Encode image in base64
def encode_image(image_content):
    return base64.b64encode(image_content).decode('utf-8')

# OpenAI API call    
def openAI(image_base64):
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": "gpt-4o",
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": """
                        Please carefully examine the provided image and extract the relevant product details. Structure your response in the following JSON format:
                        {
                            "modelNumber": "value",
                            "typeOfTheProduct": "value",
                            "Brand": "value",
                            "modelYear": "value"
                        }
                        For each field:
                        - If the detail is explicitly shown in the image, extract it accurately.
                        - If the detail is not visible, infer it from the image context or search for it online using relevant clues.
                        - If the detail cannot be determined, clearly state "Not specified in the image".
                        Ensure the information is as complete and precise as possible.
                        """
                    },
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/jpeg;base64,{image_base64}"
                        }
                    }
                ]
            }
        ],
        "max_tokens": 300
    }

    response = requests.post("https://api.openai.com/v1/chat/completions", json=payload, headers=headers)
    response.raise_for_status()
    return response.json()

# Store image (JSONdata) in DynamoDB
def store_datain_db(data):
    return dynamodb.put_item(
        TableName=table_name,
        Item=data
    )

def inputFromS3(event, context):
    try:
        record = event['Records'][0]
        bucket_name = record['s3']['bucket']['name']
        object_key = urllib.parse.unquote(record['s3']['object']['key'])
        file_name = object_key.split('/')[-1]
        file_id = file_name.split('_')[0]

        # Fetch the image object from S3
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        image_content = response['Body'].read()

        # Convert image data to base64
        image_base64 = encode_image(image_content)

        # OpenAI call
        response = openAI(image_base64)
        message_content = response['choices'][0]['message']['content']
        cleaned_message_content = message_content.replace('```json', '').replace('```', '').strip()
        brand_model = json.loads(cleaned_message_content)

        # Cost calculation
        prompt_tokens = response['usage']['prompt_tokens']
        completion_tokens = response['usage']['completion_tokens']
        cost_prompt = (prompt_tokens / 1_000_000) * 5.00
        cost_completion = (completion_tokens / 1_000_000) * 15.00
        total_cost = cost_prompt + cost_completion

        # Store data in DynamoDB
        current_timestamp = datetime.now().isoformat()
        data = {
            "contactid": {"S": file_id},
            "modelNumber": {"S": brand_model.get("modelNumber", "Not specified")},
            "typeOfTheProduct": {"S": brand_model.get("typeOfTheProduct", "Not specified")},
            "Brand": {"S": brand_model.get("Brand", "Not specified")},
            "modelYear": {"S": brand_model.get("modelYear", "Not specified")},
            "total_cost": {"S": str(total_cost)},
            "timestamp": {"S": current_timestamp}
        }

        store_datain_db(data)
        return {
            'statusCode': 200,
            'body': json.dumps('Image fetching complete and stored in DynamoDB')
        }

    except Exception as e:
        print("Error:", e)
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error processing the image: {str(e)}")
        }
