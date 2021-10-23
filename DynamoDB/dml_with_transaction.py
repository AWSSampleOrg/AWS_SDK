#-*- encoding:utf-8 -*-
import boto3

# DynamoDB client
dynamodb_client = boto3.client('dynamodb')
table_name = "table"

# DynamoDB resource
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(table_name)


def init():
    table.put_item(
        Item = {
            'id': "1",
            'balance': 100
        }
    )

    table.put_item(
        Item = {
            'id': "2",
            'balance': 50
        }
    )

    table.update_item(
        Key = {
            "id" : "1"
        },
        UpdateExpression = "set #balance = :val",
        ExpressionAttributeNames = {
            "#balance" : "balance"
        },
        ExpressionAttributeValues = {
            ":val" : 80
        },
        ReturnValues = "UPDATED_NEW"
    )

def transact_write_items():
    # update both items together, and they would be failed
    try:
        dynamodb_client.transact_write_items(
            TransactItems=[
                {
                    'Update': {
                        'TableName': table_name,
                        'Key': {
                            'id': {'S': '1'}
                        },
                        'ConditionExpression': 'balance = :bc',
                        'UpdateExpression': 'SET balance = :bu',
                        'ExpressionAttributeValues': {
                            ':bc': {'N': '100'},  # expected to be 100
                            ':bu': {'N': '90'},  # 100 - 10 = 90
                        }
                    }
                },
                {
                    'Update': {
                        'TableName': table_name,
                        'Key': {
                            'id': {'S': '2'}
                        },
                        'ConditionExpression': 'balance = :bc',
                        'UpdateExpression': 'SET balance = :bu',
                        'ExpressionAttributeValues': {
                            ':bc': {'N': '50'},  # expected to be 50
                            ':bu': {'N': '60'},  # 50 + 10 = 60
                        }
                    }
                }
            ]
        )
    except Exception as e:
        print(f"type = {type(e)} , message = {e}")


    # repay
    response = dynamodb_client.transact_write_items(
        TransactItems=[
            {
                'Update': {
                    'TableName': table_name,
                    'Key': {
                        'id': {'S': '1'}
                    },
                    'ConditionExpression': 'balance = :bc',
                    'UpdateExpression': 'SET balance = :bu',
                    'ExpressionAttributeValues': {
                        ':bc': {'N': '80'},  # fix 80
                        ':bu': {'N': '70'},  # 80 - 10 = 70
                    }
                }
            },
            {
                'Update': {
                    'TableName': table_name,
                    'Key': {
                        'id': {'S': '2'}
                    },
                    'ConditionExpression': 'balance = :bc',
                    'UpdateExpression': 'SET balance = :bu',
                    'ExpressionAttributeValues': {
                        ':bc': {'N': '50'},  # not changed
                        ':bu': {'N': '60'},  # 50 + 10 = 60
                    }
                }
            }
        ]
    )
    print(response)


def transact_get_items():
    # check it out both items are updated
    response = dynamodb_client.transact_get_items(
        TransactItems=[
            {
                'Get': {
                    'TableName': table_name,
                    'Key': {
                        'id': {'S': '1'}
                    }
                }
            },
            {
                'Get': {
                    'TableName': table_name,
                    'Key': {
                        'id': {'S': '2'}
                    }
                }
            }
        ]
    )
    print(response)

if __name__ == "__main__":
    init()
    transact_write_items()
    transact_get_items()
