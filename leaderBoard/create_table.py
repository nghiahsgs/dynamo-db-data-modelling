import boto3

client = boto3.client('dynamodb',
    endpoint_url='http://localhost:8000'
)


resp = client.create_table(
    AttributeDefinitions=[
        {
            "AttributeName": "imagePath",
            "AttributeType": "S"
        },
        # {
        #     "AttributeName": "author",
        #     "AttributeType": "S"
        # },
        {
            "AttributeName": "gsiHash",
            "AttributeType": "S"
        },
        {
            "AttributeName": "viewCount",
            "AttributeType": "N"
        }
    ],
    TableName="Images",
    KeySchema=[
        {
            "AttributeName": "imagePath",
            "KeyType": "HASH"
        }
    ],
    GlobalSecondaryIndexes=[
        {
            "IndexName": "GSIViewCount",
            "KeySchema": [
                {
                    "AttributeName": "gsiHash",
                    "KeyType": "HASH"
                },
                {
                    "AttributeName": "viewCount",
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