# -*- encoding: utf-8 -*-
import boto3
lambda_client = boto3.client('lambda')

###################################
#   Invoke Lambda
###################################
response = lambda_client.invoke(
    FunctionName = 'Lambda Function Name',
    InvocationType = 'Event'|'RequestResponse'|'DryRun',
    LogType = 'None'|'Tail',
    Payload = b'bytes'|file
)
