import json
from utils import retriveData
from extFromS3 import inputFromS3

# Main
def handler(event, context):
    print("event",event)
    if 'Records' in event:
        event_type = event['Records'][0]['eventSource']
    elif 'type' in event:
        event_type = event.get('type')

    try:
        if not event_type:
            raise ValueError("Event 'type' is required")

        if event_type == "aws:s3":
            print("extractImage")
            return inputFromS3(event,context)
        elif event_type == "fetchFromDynamoDB":
            contact_id = event.get('contactId')
            if not contact_id:
                raise ValueError("Event 'contactId' is required for retriveData type")
            return retriveData(contact_id)
        elif event_type == "extractAndPass":
            print("extractAndPass")
        else:
            raise ValueError(f"Unsupported event type: {event_type}")
    except Exception as e:
        print(f"Error handling event: {e}")
        return {"statusCode": 400, "body": str(e)}
    