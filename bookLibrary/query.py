import boto3
import random
import uuid
from typing import Any
client = boto3.client('dynamodb', 
    endpoint_url='http://localhost:8000',
    # region_name='ap-southeast-1'
)

#@NOTE 1 select all author
# author_name = "EMLAMWAP"
# key_condition_expression = "PK = :PK AND SK = :SK"
# expression_values = {
#     ":PK": {"S": "AUTHOR#%s"%author_name},
#     ":SK": {"S": "METADATA"},
# }

#@NOTE 2 select all book

#@NOTE 3 select all author of a specific author
# author_name = "EMLAMWAP"
# key_condition_expression = "PK = :PK AND begins_with(SK, :SK)"
# expression_values = {
#     ":PK": {"S": "AUTHOR#%s"%author_name},
#     ":SK": {"S": "METADATA"},
# }

#@NOTE 4 select all book of a specific author
# author_name = "EMLAMWAP"
# key_condition_expression = "PK = :PK AND begins_with(SK, :SK)"
# expression_values = {
#     ":PK": {"S": "AUTHOR#%s"%author_name},
#     ":SK": {"S": "ISBN"},
# }

# resp = client.query(
#     TableName="LibraryV2",
#     # IndexName='Library',
#     KeyConditionExpression=key_condition_expression,
#    ExpressionAttributeValues=expression_values
# )
# Items = resp.get('Items')
# print(Items)



#@NOTE 5 select a book with specific ISBN
ISBN = "8021"
key_condition_expression = "PK = :PK"
expression_values = {
    ":PK": {"S": "ISBN#%s"%ISBN}
}
resp = client.query(
    TableName="LibraryV2",
    IndexName='GSI1',
    KeyConditionExpression=key_condition_expression,
   ExpressionAttributeValues=expression_values
)
Items = resp.get('Items')
print(Items)


