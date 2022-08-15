import boto3

client = boto3.client('dynamodb',
    endpoint_url='http://localhost:8000',
)


resp = client.create_table(
    AttributeDefinitions=[
        {
            "AttributeName": "PK",
            "AttributeType": "S"
        },
        {
            "AttributeName": "SK",
            "AttributeType": "S"
        },

        #ToGetServiceType
        {
            "AttributeName": "GSI_ToGetServiceType_PK",
            "AttributeType": "S"
        },
        {
            "AttributeName": "GSI_ToGetServiceType_SK",
            "AttributeType": "S"
        }
    ],
    TableName="ample001",
    KeySchema=[
        {
            "AttributeName": "PK",
            "KeyType": "HASH"
        },
        {
            "AttributeName": "SK",
            "KeyType": "RANGE"
        }
    ],
    GlobalSecondaryIndexes=[
        # {
        #     "IndexName": "GSI_ToGetAllMode",
        #     "KeySchema": [
        #         {
        #             "AttributeName": "GSI_ToGetAllMode_PK",
        #             "KeyType": "HASH"
        #         },
        #         {
        #             "AttributeName": "GSI_ToGetAllMode_SK",
        #             "KeyType": "RANGE"
        #         }
        #     ],
        #     "Projection": {
        #         "ProjectionType": "KEYS_ONLY" # ALL
        #     },
        #     "ProvisionedThroughput": {
        #         "ReadCapacityUnits": 1,
        #         "WriteCapacityUnits": 1
        #     }
        # },
        # {
        #     "IndexName": "GSI1_ToGetServiceType",
        #     "KeySchema": [
        #         {
        #             "AttributeName": "GSI1_ToGetServiceType1_PK",
        #             "KeyType": "HASH"
        #         },
        #         {
        #             "AttributeName": "GSI1_ToGetServiceType_SK",
        #             "KeyType": "RANGE"
        #         }
        #     ],
        #     "Projection": {
        #         "ProjectionType": "KEYS_ONLY"
        #     },
        #     "ProvisionedThroughput": {
        #         "ReadCapacityUnits": 1,
        #         "WriteCapacityUnits": 1
        #     }
        # },
        # {
        #     "IndexName": "GSI2_ToGetServiceType",
        #     "KeySchema": [
        #         {
        #             "AttributeName": "GSI2_ToGetServiceType1_PK",
        #             "KeyType": "HASH"
        #         },
        #         {
        #             "AttributeName": "GSI2_ToGetServiceType_SK",
        #             "KeyType": "RANGE"
        #         }
        #     ],
        #     "Projection": {
        #         "ProjectionType": "KEYS_ONLY"
        #     },
        #     "ProvisionedThroughput": {
        #         "ReadCapacityUnits": 1,
        #         "WriteCapacityUnits": 1
        #     }
        # },
        # {
        #     "IndexName": "GSI3_ToGetServiceType",
        #     "KeySchema": [
        #         {
        #             "AttributeName": "GSI3_ToGetServiceType1_PK",
        #             "KeyType": "HASH"
        #         },
        #         {
        #             "AttributeName": "GSI3_ToGetServiceType_SK",
        #             "KeyType": "RANGE"
        #         }
        #     ],
        #     "Projection": {
        #         "ProjectionType": "KEYS_ONLY"
        #     },
        #     "ProvisionedThroughput": {
        #         "ReadCapacityUnits": 1,
        #         "WriteCapacityUnits": 1
        #     }
        # },

        {
            "IndexName": "GSI_ToGetServiceType",
            "KeySchema": [
                {
                    "AttributeName": "GSI_ToGetServiceType_PK",
                    "KeyType": "HASH"
                },
                {
                    "AttributeName": "GSI_ToGetServiceType_SK",
                    "KeyType": "RANGE"
                }
            ],
            "Projection": {
                "ProjectionType": "KEYS_ONLY"
            },
            "ProvisionedThroughput": {
                "ReadCapacityUnits": 1,
                "WriteCapacityUnits": 1
            }
        },
    ],
    ProvisionedThroughput={
        "ReadCapacityUnits": 1,
        "WriteCapacityUnits": 1
    }
)
print("Table created successfully!")