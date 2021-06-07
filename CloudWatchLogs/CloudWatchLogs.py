# -*- encoding: utf-8 -*-
from datetime import datetime,timezone,timedelta
import os
import boto3
import time
from logging import getLogger, StreamHandler, DEBUG, INFO, WARNING, ERROR, CRITICAL
import traceback

#logger setting
logger = getLogger(__name__)
handler = StreamHandler()
handler.setLevel(DEBUG)
logger.setLevel(os.getenv("LogLevel", DEBUG))
logger.addHandler(handler)
logger.propagate = False

logs = boto3.client("logs")
BUCKET_NAME = os.environ["BUCKET_NAME"]
WAITING_TIME = int(os.environ["WAITING_TIME"])

#define timezone as JST
JST = timezone(timedelta(hours=9),"JST")

#DateFormat when you log out into S3
DATE_FORMAT = "%Y-%m-%d"

def lambda_handler(event, context):
    """
    Export CloudWatchLogs to S3 just one day that is defined
    AM 00:00:00.000000 ~ PM 23:59:59.999999
    """
    try:
        # yesterday PM23:59:59.999999
        tmp_today = datetime.now(JST).replace(hour=0,minute=0,second=0,microsecond=0) - timedelta(microseconds=1)
        # yesterday AM00:00:00.000000
        tmp_yesterday = (tmp_today - timedelta(days=1)) + timedelta(microseconds=1)
        # S3 prefix
        target_date = tmp_yesterday.strftime(DATE_FORMAT)
        # convert datetime pbject to UNIX time stamp with microseconds
        today = int(tmp_today.timestamp() * 1000)
        yesterday = int(tmp_yesterday.timestamp() * 1000)


        #Get the CloudWatchLogGroups from your environment variable
        logGroups = os.environ["LOG_GROUPS"].split(",")
        for logGroupName in logGroups:
            try:
                keys = ["logGroupName","yesterday","today","target_date"]
                values = [logGroupName,yesterday,today,target_date]
                payload = dict(zip(keys,values))

                #Output CloudWatchLogs into S3 (Async Process)
                response = logs.create_export_task(
                    logGroupName = payload["logGroupName"],
                    fromTime = payload["yesterday"],
                    to = payload["today"],
                    destination = BUCKET_NAME,
                    destinationPrefix = "Logs" + payload["logGroupName"] + "/" + payload["target_date"]
                )

                taskId = response["taskId"]
                while True:
                    response = logs.describe_export_tasks(
                        taskId = taskId
                    )
                    status = response["exportTasks"][0]["status"]["code"]
                    if status != "PENDING" and status != "PENDING_CANCEL" and status != "RUNNING":
                        logger.info(f"taskId {taskId} has finished exporting")
                        break
                    else:
                        logger.info(f"taskId {taskId} is now exporting")
                        time.sleep(WAITING_TIME)
                        continue

            except Exception as e:
                traceback.print_exc()
                logger.warning(f"type = {type(e)} , message = {e}",exc_info=True)

    except Exception as e:
        traceback.print_exc()
        logger.error(f"type = {type(e)} , message = {e}",exc_info=True)
        raise
