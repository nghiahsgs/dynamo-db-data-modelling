import pprint
from typing import Any
import boto3

client = boto3.client('dynamodb', endpoint_url='http://localhost:8000')
DEFAULT_COUNTRY="US"
DEFAULT_STATE="NE"
DEFAULT_CITY="OMAHA"
DEFAULT_POSTCODE="68144"

def query_store_locations(country=DEFAULT_COUNTRY, state=DEFAULT_STATE, city=DEFAULT_CITY, postcode=DEFAULT_POSTCODE):
    statecitypostcode = ''
    
    if state:
        statecitypostcode += state.upper()
    if city and state:
        statecitypostcode += "#" + city.upper()
    if postcode and city and state:
        statecitypostcode += "#" + postcode
    
    
    key_condition_expression = "Country = :country AND begins_with(StateCityPostcode, :statecitypostcode)"
    expression_values:dict[str,Any] = {
        ":country": {"S": country},
        ':statecitypostcode':{"S": statecitypostcode}
    }
    
    try:
        resp = client.query(
            TableName="StarbucksLocations",
            IndexName='StoreLocationIndex',
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeValues=expression_values
        )
        print("Retrieved {} locations.".format(resp['Count']))
        
        pprint.pprint(resp)
    except Exception as e:
        print("Error running query:")
        print(e)


if __name__ == "__main__":
    query_store_locations()