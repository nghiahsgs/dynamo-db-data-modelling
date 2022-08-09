import pprint
from typing import Any
import boto3

client = boto3.client('dynamodb', endpoint_url='http://localhost:8000')
DEFAULT_COUNTRY="US"
DEFAULT_STATE="NE"
DEFAULT_CITY="OMAHA"
DEFAULT_POSTCODE="68144"

def query_store_locations(country, state, city, postcode):
    if postcode and city and state:
        statecitypostcode = f'{state.upper()}#{city.upper()}#{postcode}'
    elif city and state:
        statecitypostcode = f'{state.upper()}#{city.upper()}'
    elif state:
        statecitypostcode = f'{state.upper()}'
    

    key_condition_expression = "Country = :country AND begins_with(StateCityPostcode, :statecitypostcode)"
    expression_values:dict[str,Any] = {
        ":country": {"S": country},
        ':statecitypostcode':{"S": statecitypostcode}
    }
    
    
    resp = client.query(
        TableName="StarbucksLocations",
        IndexName='StoreLocationIndex',
        KeyConditionExpression=key_condition_expression,
        ExpressionAttributeValues=expression_values
    )
    return resp.get('Items')    
    

if __name__ == "__main__":
    country=DEFAULT_COUNTRY
    state=DEFAULT_STATE
    city=DEFAULT_CITY
    postcode=DEFAULT_POSTCODE

    Items = query_store_locations(
        country,
        state,
        city,
        postcode
    )
    print('Items',Items)