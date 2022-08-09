import pprint

import boto3
from typing import Any

client = boto3.client('dynamodb', endpoint_url='http://localhost:8000')

# resp = client.get_item(
#     TableName="Images",
#     Key={
#         "imagePath": {"S": '/root/1734.jpg'}
#     }
# )
# print(resp)

List_gsiHash = ['gsiHash','gsiHash2','gsiHash3']
nb_top = 50
List_resp = []

for gsiHash in List_gsiHash: 
    key_condition_expression = "gsiHash = :gsiHash"
    expression_values = {
        ":gsiHash": {"S": gsiHash}
    }
    resp = client.query(
        ScanIndexForward = False,
        Limit=nb_top,
        TableName="Images",
        IndexName='GSIViewCount',
        KeyConditionExpression=key_condition_expression,
        ExpressionAttributeValues=expression_values
    )
    # print('resp',resp)
    # print([int(e['viewCount']['N']) for e in resp['Items']])
    # print([e['imagePath']['S'] for e in resp['Items']])
    # print([e['author']['S'] for e in resp['Items']])
    List_resp +=resp['Items']

List_resp = [
    {
        'viewCount':int(e['viewCount']['N']),
        'imagePath':(e['imagePath']['S']),
        'author':(e['author']['S']),
    }
     for e in List_resp]
List_resp = sorted(
    List_resp,key=lambda x:-x['viewCount']
)
List_resp = List_resp[:nb_top]
print(List_resp)

print([e['viewCount'] for e in List_resp])
print([e['imagePath'] for e in List_resp])
print([e['author'] for e in List_resp])
