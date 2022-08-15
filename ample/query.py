import pprint
from typing import Any
import boto3

client = boto3.client('dynamodb', endpoint_url='http://localhost:8000')

'''
PK = 'ServiceType'    
SK = 'service_type_idservice_type_id'

key_condition_expression = "PK = :PK and begins_with(SK,:SK)"
expression_values = {
    ":PK": {"S": PK},
    ":SK": {"S": SK},
    
}

resp = client.query(
    TableName="ample001",
    # IndexName='StoreLocationIndex',
    KeyConditionExpression=key_condition_expression,
    ExpressionAttributeValues=expression_values
)
Items = resp.get('Items')
# print(Items)    

pprint.pprint(Items)
'''
    

# ToGetAllMode
'''
GSI_ToGetAllMode_PK = 'METADATA'
key_condition_expression = "GSI_ToGetAllMode_PK = :GSI_ToGetAllMode_PK"
expression_values = {
    ":GSI_ToGetAllMode_PK": {"S": GSI_ToGetAllMode_PK},
}

resp = client.query(
    TableName="ample001",
    IndexName='GSI_ToGetAllMode',
    KeyConditionExpression=key_condition_expression,
    ExpressionAttributeValues=expression_values
)
Items = resp.get('Items')
pprint.pprint(Items)
'''


def get_all_mode():
    #ToGetServiceType
    key_condition_expression = "GSI_ToGetServiceType_PK = :GSI_ToGetServiceType_PK"
    expression_values = {
        ":GSI_ToGetServiceType_PK": {"S": 'METADATA'},
    }

    resp = client.query(
        TableName="ample001",
        IndexName='GSI_ToGetServiceType',
        KeyConditionExpression=key_condition_expression,
        ExpressionAttributeValues=expression_values
    )
    Items = resp.get('Items')
    list_modes = []
    for item in Items: 
        mode = item['GSI_ToGetServiceType_SK']['S'].split('#')[0]
        mode = mode[len('mode'):]
        list_modes.append(mode)
    list_modes = list(dict.fromkeys(list_modes))
    return list_modes
# list_modes = get_all_mode()
# print(list_modes)


def get_service_type(mode,country,currencies):
    #ToGetServiceType
    key_condition_expression = "GSI_ToGetServiceType_PK = :GSI_ToGetServiceType_PK and begins_with(GSI_ToGetServiceType_SK,:GSI_ToGetServiceType_SK)"
    expression_values = {
        ":GSI_ToGetServiceType_PK": {"S": 'METADATA'},
        ":GSI_ToGetServiceType_SK": {"S": f'mode{mode}#country{country}#currencies{currencies}'},
    }

    resp = client.query(
        TableName="ample001",
        IndexName='GSI_ToGetServiceType',
        KeyConditionExpression=key_condition_expression,
        ExpressionAttributeValues=expression_values
    )
    Items = resp.get('Items')
    if len(Items) == 0:
        return
    item =   Items[0]
    return {
        "service_type_id":item['SK']['S'][len('service_type_id'):],
        "mode":item['GSI_ToGetServiceType_SK']['S'].split('#')[0][len('mode'):],
        "country":item['GSI_ToGetServiceType_SK']['S'].split('#')[1][len('country'):],
        "currencies":item['GSI_ToGetServiceType_SK']['S'].split('#')[2][len('currencies'):],
        "name":item['GSI_ToGetServiceType_SK']['S'].split('#')[3][len('name'):],
        
    }

service_type = get_service_type(mode='mode1',country='country1',currencies='currencies1')
pprint.pprint(service_type)