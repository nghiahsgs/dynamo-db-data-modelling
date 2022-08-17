### list all table
```
aws dynamodb list-tables $LOCAL_DB
```

### desc a table
```
aws dynamodb describe-table \
  --table-name StarbucksLocations \
  $LOCAL_DB
```

### count number row of table
```
aws dynamodb scan --table-name StarbucksLocations --select COUNT $LOCAL_DB
```



### create new table
```
aws dynamodb create-table \
  --table-name UsersTable \
  --attribute-definitions '[
    {
        "AttributeName": "Username",
        "AttributeType": "S"
    }
  ]' \
  --key-schema '[
    {
        "AttributeName": "Username",
        "KeyType": "HASH"
    }
  ]' \
  --provisioned-throughput '{
    "ReadCapacityUnits": 1,
    "WriteCapacityUnits": 1
  }' \
  $LOCAL_DB
```

###  put item to table
```
 aws dynamodb put-item \
    --table-name UsersTable \
    --item '{
      "Username": {"S": "daffyduck"},
      "Name": {"S": "Daffy Duck"},
      "Age": {"N": "81"}
    }' \
    $LOCAL
```

## get item
```
aws dynamodb get-item \
    --table-name StarbucksLocations \
    --key '{
      "StoreNumber": {"S": "47609-253286"}
    }' \
    $LOCAL_DB
```

## scan table
```
aws dynamodb scan --table-name ample001 $LOCAL_DB
```

## deleta table
```
aws dynamodb delete-table --table-name ample001 $LOCAL_DB
```