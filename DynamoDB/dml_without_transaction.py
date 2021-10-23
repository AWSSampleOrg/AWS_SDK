# -*- encoding: utf-8 -*-
import boto3
from boto3.dynamodb.conditions import Key,Attr

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("table")

"""
Example DynamoDB Table Scheme
| id  |
| "1" |
"""
#get a record
response = table.get_item(
    Key = {
        "id" : "1"
    }
)
#write a record
response = table.put_item(
    Item = {
        "id" : "1",
        "key" : "value"
    }
)
#update a record
response = table.update_item(
    Key = {
        "id" : "1"
    },
    UpdateExpression = "set #age = :val",
    ExpressionAttributeNames = {
        "#age" : "age"
    },
    ExpressionAttributeValues = {
        ":val" : 95
    },
    ReturnValues = "UPDATED_NEW"
)
#delete a record
response = table.delete_item(
    Key = {
        "id" : "1"
    }
)

"""
Encryption and Decryption with Customer managed CMK
"""
response = kms.encrypt(
    KeyId = "Enter a key ID of the CMK that was used to encrypt the ciphertext.",
    Plaintext = "Encryption Test".encode("UTF-8")
)
response = table.put_item(
    Item = {
        "id" : "1",
        "name" : response["CiphertextBlob"]
    }
)


response = table.get_item(
    Key = {
        "id" : "1"
    }
)
response = kms.decrypt(
    KeyId = "Enter a key ID of the CMK that was used to encrypt the ciphertext.",
    CiphertextBlob = response["Item"]["name"].value
)
print(response["Plaintext"].decode("UTF-8"))

"""
Example DynamoDB table Scheme
id   = Primary partition key
name = Primary sort key
| id  | name | age  |     job      |
| "1" |  "X" |  18  |  "engineer"  |
| "2" |  "Y" |  35  |  "sales"     |
"""
response  = table.query(
    KeyConditionExpression = "#partition = :t1 and #sort = :t2",
    FilterExpression = "#job = :t3",
    ExpressionAttributeNames = {
        "#partition" : "id",
        "#sort" : "name",
        "#job" : "job"
    },
    ExpressionAttributeValues = {
        ":t1" : "1",
        ":t2" : "X",
        ":t3" : "engineer"
    },
    ProjectionExpression = "#partition,#sort,#job"
)

response  = table.scan(
    # KeyConditionExpression  Not Allowed
    FilterExpression = "#partition = :t1 or #sort > :t2",
    ExpressionAttributeNames = {
        "#partition" : "job",
        "#sort" : "age"
    },
    ExpressionAttributeValues = {
        ":t1" : "engineer",
        ":t2" : 23
    },
    ProjectionExpression = "id,#partition,#sort"
)

#write records as a batch process
with table.batch_writer() as batch:
    for i in range(10 ** 6):
        batch.put_item(
            Item = {
                "id" : str(i + 1),
                "key" : f"key{i + 1}"
            }
        )


# truncate  or delete from table
delete_items = []
parameters   = {}
while True:
    response = table.scan(**parameters)
    delete_items.extend(response["Items"])
    if "LastEvaluatedKey" in response:
        parameters["ExclusiveStartKey"] = response["LastEvaluatedKey"]
    else:
        break
# get the hash key and range key
key_names = [ x["AttributeName"] for x in table.key_schema ]
delete_keys = [ { k:v for k,v in x.items() if k in key_names } for x in delete_items ]
#delete datas as batch process
with table.batch_writer() as batch:
    for key in delete_keys:
        batch.delete_item(Key = key)
