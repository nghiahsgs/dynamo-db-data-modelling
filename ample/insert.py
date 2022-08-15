import boto3
client = boto3.client('dynamodb', 
    endpoint_url='http://localhost:8000'
)



service_types = [
    {
        "service_type_id":"service_type_id1",
        "name":"name1",
        "country":"country1",
        "currencies":"currencies1",
        "mode":"mode1",
    },
    {
        "service_type_id":"service_type_id2",
        "name":"name2",
        "country":"country2",
        "currencies":"currencies2",
        "mode":"mode2",
    },
    {
        "service_type_id":"service_type_id3",
        "name":"name3",
        "country":"country3",
        "currencies":"currencies3",
        "mode":"mode3",
    }
]

'''
for service_type in service_types:
    GSI_ToGetAllMode_PK = "METADATA"
    client.put_item(
        TableName="ample001",
        Item={
            "PK": {"S": 'ServiceType' },
            "SK": {"S": f'service_type_id{service_type["service_type_id"]}' },

            "GSI_ToGetAllMode_PK": {"S": "METADATA" },
            "GSI_ToGetAllMode_SK": {"S": service_type['mode'] },

            "GSI1_ToGetServiceType_PK": {"S": service_type['mode'] },
            "GSI1_ToGetServiceType_SK": {"S": service_type['mode'] },

            "GSI2_ToGetServiceType_PK": {"S": service_type['country'] },
            "GSI2_ToGetServiceType_SK": {"S": service_type['country'] },

            "GSI3_ToGetServiceType_PK": {"S": service_type['currencies'] },
            "GSI3_ToGetServiceType_SK": {"S": service_type['currencies'] },

            
            "service_type_id":{"S":service_type['service_type_id']},
            "name":{"S":service_type['name']},
            "country":{"S":service_type['country']},
            "currencies":{"S":service_type['currencies']},
            "mode":{"S":service_type['mode']}
        },
    )
'''

for service_type in service_types:
    GSI_ToGetAllMode_PK = "METADATA"
    client.put_item(
        TableName="ample001",
        Item={
            "PK": {"S": 'ServiceType' },
            "SK": {"S": f'service_type_id{service_type["service_type_id"]}' },

           
            "GSI_ToGetServiceType_PK": {"S": 'METADATA' },
            "GSI_ToGetServiceType_SK": {"S": f'mode{service_type["mode"]}#country{service_type["country"]}#currencies{service_type["currencies"]}#name{service_type["name"]}'},


            
            "service_type_id":{"S":service_type['service_type_id']},
            "name":{"S":service_type['name']},
            "country":{"S":service_type['country']},
            "currencies":{"S":service_type['currencies']},
            "mode":{"S":service_type['mode']}
        },
    )