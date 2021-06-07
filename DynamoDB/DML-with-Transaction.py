#-*- encoding:utf-8 -*-
import boto3
import sys

# DynamoDB client
dynamodb_client = boto3.client('dynamodb')
table_name = "table"

# DynamoDB resource
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(table_name)


def init():
    # 次にアイテムを2件登録する
    # Dynamoさんの口座の残高を 100 に、DBさんの口座の残高を 50 に設定するイメージ
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

    # Dynamoさんの口座から 20 引き出されたイメージ
    response = table.update_item(
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

if 1 < len(sys.argv):
    init()
    exit(0)

# 2件まとめて更新して両方共エラーになることを確認する
# Dynamoさんの口座からDBさんの口座に 10 振り込む処理の裏側で同時に先程のDynamoさんの口座の引き落としが発生していたイメージ
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
                        ':bc': {'N': '100'},  # 元々は 100 の想定
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
                        ':bc': {'N': '50'},  # 元々は 50 の想定
                        ':bu': {'N': '60'},  # 50 + 10 = 60
                    }
                }
            }
        ]
    )
except Exception as e:
    print(f"type = {type(e)} , message = {e}")



# 再度振込処理を実行するイメージ
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
                    ':bc': {'N': '80'},  # 80 に修正
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
                    ':bc': {'N': '50'},  # 変更なし
                    ':bu': {'N': '60'},  # 50 + 10 = 60
                }
            }
        }
    ]
)
print(response)


# 両方のアイテムが想定通り更新されていることを確認する
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
