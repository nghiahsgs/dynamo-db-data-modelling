import boto3

client = boto3.client('dynamodb',
    endpoint_url='http://localhost:8000',
)


def create_new_table():
    table_name = "ample001"
    read_capacity_units = 1
    write_capacity_units = 1
    list_GSI = [
        {
            "name":"GSI_created_at",
            "pk":"GSI_created_at_PK",
            "sk":"GSI_created_at_SK",
        },
        *[{
            "name":f"GSI{i}",
            "pk":f"GSI{i}_PK",
            "sk":f"GSI{i}_SK",
        } for i in range(1,7)]
    ]


    resp = client.create_table(
        AttributeDefinitions=[
            # PK & SK
            {
                "AttributeName": "PK",
                "AttributeType": "S"
            },
            {
                "AttributeName": "SK",
                "AttributeType": "S"
            },
            # GSI
            *[
                {
                    "AttributeName": gsi['pk'],
                    "AttributeType": "S"
                } for gsi in list_GSI
            ],
            *[
                {
                    "AttributeName": gsi['sk'],
                    "AttributeType": "S"
                } for gsi in list_GSI
            ]
        ],
        
        TableName=table_name,
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
                "IndexName": gsi['name'],
                "KeySchema": [
                    {
                        "AttributeName": gsi['pk'],
                        "KeyType": "HASH"
                    },
                    {
                        "AttributeName": gsi['sk'],
                        "KeyType": "RANGE"
                    }
                ],
                "Projection": {
                    "ProjectionType": "ALL"
                },
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": read_capacity_units,
                    "WriteCapacityUnits": write_capacity_units
                }
            } for gsi in list_GSI
        ],
        ProvisionedThroughput={
            "ReadCapacityUnits": read_capacity_units,
            "WriteCapacityUnits": write_capacity_units
        }
    )
    print("Table created successfully!")

if __name__ =="__main__":
    create_new_table()