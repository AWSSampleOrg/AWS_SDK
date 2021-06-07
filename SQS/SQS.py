# -*- encoding: utf-8 -*-
import boto3

sqs = boto3.client('sqs')
QUEUE_URL= "SQS Queue URL"

#Get all messages from Queue of SQS
while True:
    sqs_message = sqs.receive_message(
        QueueUrl = QUEUE_URL,
        MaxNumberOfMessages = 10
    )
    if "Messages" in sqs_message:
        for message in sqs_message["Messages"]:
            try:
                print(message)
                #When you get a message, delete it. Otherwise, You get a duplicate message
                sqs.delete_message(
                    QueueUrl = QUEUE_URL,
                    ReceiptHandle = message["ReceiptHandle"]
                )
            except Exception as e:
                print(f"type = {type(e)} , message = {e}")
    else:
        break