import pprint

import boto3

client = boto3.client('dynamodb', endpoint_url='http://localhost:8000')
DEFAULT_STORE_NUMBER="47370-257954"

def get_store_location(store_number):
    print("Attempting to retrieve store number {}...\n".format(store_number))
    try:
        resp = client.get_item(
            TableName="StarbucksLocations",
            Key={
                "StoreNumber": {"S": store_number}
            }
        )
        print("Store number found! Here's your store:\n")
        pprint.pprint(resp.get('Item'))
    except Exception as e:
        print("Error getting item:")
        print(e)


if __name__=="__main__":
    get_store_location(DEFAULT_STORE_NUMBER)