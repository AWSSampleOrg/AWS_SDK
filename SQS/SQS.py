# -*- encoding: utf-8 -*-
from logging import getLogger, StreamHandler, DEBUG
import boto3

sqs = boto3.client('sqs')
QUEUE_URL= "SQS Queue URL"

# logger setting
logger = getLogger(__name__)
handler = StreamHandler()
handler.setLevel(DEBUG)
logger.setLevel(os.getenv("LOG_LEVEL", DEBUG))
logger.addHandler(handler)
logger.propagate = False

#Get all messages from Queue of SQS
while True:
    sqs_message = sqs.receive_message(
        QueueUrl = QUEUE_URL,
        MaxNumberOfMessages = 10
    )
    if "Messages" not in sqs_message:
        break
    for message in sqs_message["Messages"]:
        try:
            # When you got a message, delete it, or you would got a duplicate message
            sqs.delete_message(
                QueueUrl = QUEUE_URL,
                ReceiptHandle = message["ReceiptHandle"]
            )
        except Exception as e:
            logger.warn(e)
