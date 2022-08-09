import codecs
import csv
import boto3
import random

client = boto3.client('dynamodb', 
    endpoint_url='http://localhost:8000'
)

def write_item(imagePath,author,gsiHash,viewCount):
    client.put_item(
        TableName="Images",
        Item={
            "imagePath": {"S": imagePath },
            "author": {"S": author },
            "gsiHash": {"S": gsiHash },
            "viewCount": {"N": viewCount }
        },
    )


if __name__ == "__main__":
    dem = 70000
    for i_author in range(100):
        for i in range(10000):
            print(dem)
            dem+=1
            imagePath = f'/root/{dem}.jpg'
            author = f'goku_{i_author}'
            gsiHash = 'gsiHash3'
            viewCount = '%s'%random.randint(0,1001)
            write_item(imagePath,author,gsiHash,viewCount)