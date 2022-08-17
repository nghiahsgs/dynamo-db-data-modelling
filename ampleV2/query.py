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




# list_modes = insert.ServiceType.get_all_mode()
# print(list_modes)



# mode = 'mode1'
# country = 'VN'
# currency = 'currency1'


# key_condition_expression = "GSI1_PK = :GSI1_PK and begins_with(GSI1_SK,:GSI1_SK)"
# expression_values = {
#     ":GSI1_PK": {"S": 'METADATA_SERVICETYPE'},
#     ":GSI1_SK": {"S": f'mode{mode}#country{country}#currency{currency}'},
# }

# resp = client.query(
#     TableName=table_name,
#     IndexName='GSI1',
#     KeyConditionExpression=key_condition_expression,
#     ExpressionAttributeValues=expression_values
# )
# Items = resp.get('Items')
# list_service_types = []
# for item in Items:
#     name = item['GSI1_SK']['S'].split('#')[-1]
#     name = name[len('name'):]
#     service_type_id = item['SK']['S']
#     service_type_id = service_type_id[len('service_type_id'):]
#     list_service_types.append({
#         "service_type_id":service_type_id,
#         "mode":mode,
#         "country":country,
#         "currency":currency,
#         "name":name
#     })
# pprint(list_service_types)

if __name__=="__main__":
    # list_modes = insert.ServiceType.get_all_mode()
    # print(list_modes)

        
    mode = 'mode1'
    country = 'VN'
    currency = 'currency1'
    list_service_types = insert.ServiceType.search_service_type(mode,country,currency)
    print(list_service_types)