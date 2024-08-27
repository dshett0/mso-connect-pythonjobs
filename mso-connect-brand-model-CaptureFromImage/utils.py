import boto3
import os

from botocore.exceptions import ClientError

# Initialize the DynamoDB resource
dynamodb = boto3.resource('dynamodb')
table_name = os.environ.get('DYNAMODBTABLENAME')

# Function to retrive data when we pass contactId!!
def retriveData(id):
    table = dynamodb.Table(table_name)

    try:
        # Retrieve item from DynamoDB based on partition key
        response = table.get_item(
            Key={
                'contactid': id
            }
        )
        
        item = response.get('Item')
        
        if not item:
            raise Exception(f"Item with contactId '{contact_id}' not found")
        print("item",item)
        return item
    except Exception as e:
        print(f"Error retrieving item from DynamoDB: {e}")
        raise e  