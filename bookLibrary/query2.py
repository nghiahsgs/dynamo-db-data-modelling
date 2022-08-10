import boto3


table = boto3.resource("dynamodb",endpoint_url='http://localhost:8000').Table("LibraryV2")

author_name = "EMLAMWAP"
response = table.get_item(
    Key={
        "PK": f"AUTHOR#{author_name}",
        "SK": "METADATA"
    }
)

print(response["Item"])

