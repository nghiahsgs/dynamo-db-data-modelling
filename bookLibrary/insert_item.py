import boto3
import random
import uuid
client = boto3.client('dynamodb', 
    endpoint_url='http://localhost:8000',
    # region_name='ap-southeast-1'
)

list_author = [
    {   
        "Name":"nghiahsgs",
        "Birthday":"1997-01-03",
    },
    {   
        "Name":"emlamwap",
        "Birthday":"1998-21-09",
    }
]

list_book = [
    *[
        {   
            "Author":"nghiahsgs",
            "Title":"chua te nhung chiec vo %s"%i,
            "ISBN":'%s'%random.randint(1111,9999)
        } for i in range(8)
    ],
    *[
        {   
            "Author":"emlamwap",
            "Title":"thanh nien %s"%i,
            "ISBN":'%s'%random.randint(1111,9999)
        } for i in range(8)
    ],
]



# process data
list_author = [
    {
        **e,
        "PK":f"AUTHOR#{e.get('Name').upper()}",
        "SK":"METADATA",
        "GSI1PK":"METADATA",
        "GSI1SK":"METADATA",
        "type":"AUTHOR",
    } 
    for e in list_author
]
list_book = [
    {
        **e,
        "PK":f"AUTHOR#{e.get('Author').upper()}",
        "SK":f"ISBN#{e.get('ISBN')}",
        "GSI1PK":f"ISBN#{e.get('ISBN')}",
        "GSI1SK":"METADATA",
        "type":"BOOK"
    } 
    for e in list_book
]

list_items = list_author + list_book
for item in list_items:
    client.put_item(
        TableName="LibraryV2",
            Item={
                key:{"S":value} for key,value in item.items()
            }
    )