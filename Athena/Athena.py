# -*- encoding: utf-8 -*-
import boto3
import time
athena = boto3.client("athena")

class QueryFailed(Exception):
    """
    when The Query fails to finish , This Exception class called
    """
    pass

#do query as a async process
start_query_response = athena.start_query_execution(
    QueryString = 'Query',
    QueryExecutionContext={
        "Database": "Your GlueDB name"
    },
    ResultConfiguration={
        "OutputLocation" : "s3://xxx/yyy"
    }
)
query_execution_id = start_query_response["QueryExecutionId"]
#Check the query status
while True:
    query_status = athena.get_query_execution(
        QueryExecutionId = query_execution_id
    )
    query_execution_status = query_status['QueryExecution']['Status']['State']
    if query_execution_status == 'SUCCEEDED':
        break
    elif query_execution_status == "FAILED" or query_execution_status == "CANCELLED":
        raise QueryFailed(f"query_execution_status = {query_execution_status}")
    else:
        time.sleep(10)
#Get query result. Just only for your query successed
query_results = athena.get_query_results(QueryExecutionId = query_execution_id)