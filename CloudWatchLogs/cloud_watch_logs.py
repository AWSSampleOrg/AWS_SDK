# -*- encoding: utf-8 -*-
from datetime import datetime,timezone,timedelta
import os
import boto3
import time
from logging import getLogger, StreamHandler, DEBUG

#logger setting
logger = getLogger(__name__)
handler = StreamHandler()
handler.setLevel(DEBUG)
logger.setLevel(os.getenv("LOG_LEVEL", DEBUG))
logger.addHandler(handler)
logger.propagate = False

logs = boto3.client("logs")
BUCKET_NAME = os.environ["BUCKET_NAME"]
WAITING_TIME = int(os.environ["WAITING_TIME"])


UTC = timezone(timedelta(hours=0))

#DateFormat when you log out into S3
DATE_FORMAT = "%Y-%m-%d"

def lambda_handler(event, context):
    """
    Export CloudWatchLogs to S3 just one day that is defined
    AM 00:00:00.000000 ~ PM 23:59:59.999999
    """
    # yesterday PM23:59:59.999999
    today = datetime.now(UTC).replace(hour=0,minute=0,second=0,microsecond=0) - timedelta(microseconds=1)
    # yesterday AM00:00:00.000000
    yesterday = (today - timedelta(days=1)) + timedelta(microseconds=1)
    # S3 prefix
    target_date = yesterday.strftime(DATE_FORMAT)
    # convert datetime pbject to UNIX time stamp with microseconds
    unix_today = int(today.timestamp() * 1000)
    unix_yesterday = int(yesterday.timestamp() * 1000)


    #Get the CloudWatchLogGroups from your environment variable
    logGroups = os.environ["LOG_GROUPS"].split(",")
    for logGroupName in logGroups:
        taskId = logs.create_export_task(
            logGroupName = logGroupName,
            fromTime = unix_yesterday,
            to = unix_today,
            destination = BUCKET_NAME,
            destinationPrefix = f"Logs{logGroupName}/{target_date}"
        )["taskId"]

        while True:
            response = logs.describe_export_tasks(
                taskId = taskId
            )
            status = response["exportTasks"][0]["status"]["code"]
            logger.debug({"taskId": taskId, "status": status})
            if status != "PENDING" and status != "PENDING_CANCEL" and status != "RUNNING":
                break
            else:
                time.sleep(WAITING_TIME)
                continue
