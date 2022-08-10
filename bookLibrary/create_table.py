import boto3

client = boto3.client('dynamodb',
    endpoint_url='http://localhost:8000',
    # region_name='ap-southeast-1',
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
        {
            "AttributeName": "GSI1PK",
            "AttributeType": "S"
        },
        {
            "AttributeName": "GSI1SK",
            "AttributeType": "S"
        }
    ],
    TableName="LibraryV2",
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
        {
            "IndexName": "GSI1",
            "KeySchema": [
                {
                    "AttributeName": "GSI1PK",
                    "KeyType": "HASH"
                },
                {
                    "AttributeName": "GSI1SK",
                    "KeyType": "RANGE"
                }
            ],
            "Projection": {
                "ProjectionType": "ALL"
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