# -*- encoding: utf-8 -*-
import boto3
# from boto3.dynamodb.conditions import Key,Attr

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("table")
dynamodb_client = boto3.client('dynamodb')
autoscaling_client = boto3.client('application-autoscaling')

#Delete a table
def delete_table():
    response = dynamodb_client.list_tables()
    if 'TableNames' in response:
        for table_name in response['TableNames']:
            if table_name == "DynamoDB Table name to be deleted":
                dynamodb_client.delete_table(TableName = table_name)
                waiter = dynamodb_client.get_waiter("table_not_exists")
                waiter.wait(TableName = table_name)
                # When you config the table with ScalingPolicy, delete ScalingPolicy to delete CloudWatchAlarm
                autoscaling_client.delete_scaling_policy(
                    PolicyName = f'{table_name}ReadCapacity',
                    ServiceNamespace = "dynamodb",
                    ResourceId = f"table/{table_name}",
                    ScalableDimension = "dynamodb:table:ReadCapacityUnits"
                )
                autoscaling_client.delete_scaling_policy(
                    PolicyName = f'{table_name}WriteCapacity',
                    ServiceNamespace = "dynamodb",
                    ResourceId = f"table/{table_name}",
                    ScalableDimension = "dynamodb:table:WriteCapacityUnits"
                )



# Create a table
def create_table():
    table_name = "table"
    dynamodb.create_table(
        TableName = table_name,
        KeySchema = [{
            "AttributeName" : "id",
            "KeyType" : "HASH"
        }],
        AttributeDefinitions = [{
            "AttributeName" : "id",
            "AttributeType" : "S"
        }],
        ProvisionedThroughput = {
            "ReadCapacityUnits" : 1,
            "WriteCapacityUnits" : 1
        }
    )
    waiter = dynamodb_client.get_waiter("table_exists")
    waiter.wait(TableName = table_name)
    # AutoScaling setting of Target Tracking
    autoscaling_client.register_scalable_target(
        ServiceNamespace = "dynamodb",
        ResourceId = f"table/{table_name}",
        ScalableDimension = "dynamodb:table:ReadCapacityUnits",
        MinCapacity = 1,
        MaxCapacity = 10,
        RoleARN = "IAM Role ARN to auto scale"
    )
    autoscaling_client.register_scalable_target(
        ServiceNamespace = "dynamodb",
        ResourceId = f"table/{table_name}",
        ScalableDimension = "dynamodb:table:WriteCapacityUnits",
        MinCapacity = 1,
        MaxCapacity = 10,
        RoleARN = "IAM Role ARN to auto scale"
    )
    # set up the autoscaling policy
    autoscaling_client.put_scaling_policy(
        ServiceNamespace='dynamodb',
        ResourceId = f"table/{table_name}",
        PolicyType = "TargetTrackingScaling",
        PolicyName = f"{table_name}ReadCapacity",
        ScalableDimension = "dynamodb:table:ReadCapacityUnits",
        TargetTrackingScalingPolicyConfiguration={
            "TargetValue" : 70,
            "PredefinedMetricSpecification": {
                "PredefinedMetricType": "DynamoDBReadCapacityUtilization"
            },
        "ScaleOutCooldown" : 70,
        "ScaleInCooldown" : 70
    }
    )
    autoscaling_client.put_scaling_policy(
        ServiceNamespace='dynamodb',
        ResourceId = f"table/{table_name}",
        PolicyType = "TargetTrackingScaling",
        PolicyName = f"{table_name}WriteCapacity",
        ScalableDimension='dynamodb:table:WriteCapacityUnits',
        TargetTrackingScalingPolicyConfiguration={
            "TargetValue" : 70,
            "PredefinedMetricSpecification": {
                "PredefinedMetricType": "DynamoDBWriteCapacityUtilization"
            },
        "ScaleOutCooldown" : 70,
        "ScaleInCooldown" : 70
    }
    )



#update the table scheme
def update_table():
    dynamodb_client.update_table(
        AttributeDefinitions = [
            {
                'AttributeName': 'string',
                'AttributeType': 'S'|'N'|'B'
            },
        ],
        TableName = 'string',
        BillingMode = 'PROVISIONED'|'PAY_PER_REQUEST',
        ProvisionedThroughput = {
            'ReadCapacityUnits': 123,
            'WriteCapacityUnits': 123
        }
    )
