from pprint import pprint
from typing import Any
import boto3
import insert

client = boto3.client('dynamodb', endpoint_url='http://localhost:8000')
table_name = "ample001"




# def get_all_mode():
#     key_condition_expression = "GSI1_PK = :GSI1_PK"
#     expression_values = {
#         ":GSI1_PK": {"S": 'METADATA_SERVICETYPE'},
#     }

#     resp = client.query(
#         TableName=table_name,
#         IndexName='GSI1',
#         KeyConditionExpression=key_condition_expression,
#         ExpressionAttributeValues=expression_values
#     )
#     Items = resp.get('Items')
#     list_modes = []
#     for item in Items:
#         mode = item['GSI1_SK']['S'].split('#')[0]
#         mode = mode[len('mode'):]
#         list_modes.append(mode)
#     list_modes = list(dict.fromkeys(list_modes))

#     return list_modes


# instance = insert.ServiceType()
# list_modes = instance.get_all_mode()
# print(list_modes)


list_modes = insert.ServiceType.get_all_mode()
print(list_modes)