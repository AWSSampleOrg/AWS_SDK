# -*- encoding: utf-8 -*-
import boto3
import time

lambda_client = boto3.client('lambda')
expected_status = "Enabled" #when disable, Disabled
progressing_status = "Enabling" #when disable, Disabling
Enabled = True #when disable, False


def lambda_handler(event,context):
    response = lambda_client.list_event_source_mappings(
        EventSourceArn = "Kinesis or DynamoDB ARN",
        FunctionName = "Lambda Function name"
    )
    if "EventSourceMappings" in response:
        for e in response["EventSourceMappings"]:
            if e["State"] != expected_status or e["State"] != progressing_status:
                response = lambda_client.update_event_source_mapping(
                    UUID = e["UUID"],
                    FunctionName = e["FunctionArn"],
                    Enabled = Enabled,
                    BatchSize = 100
                )
            if response["State"] != expected_status:
                while True:
                    response = lambda_client.get_event_source_mapping(
                        UUID = e["UUID"]
                    )
                    if response["State"] == expected_status:
                        break
                    time.sleep(10)
