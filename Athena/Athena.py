# -*- encoding:utf-8 -*-
import os
import time
from logging import getLogger, StreamHandler, DEBUG
import boto3

# logger setting
logger = getLogger(__name__)
handler = StreamHandler()
handler.setLevel(DEBUG)
logger.setLevel(os.getenv("LOG_LEVEL", DEBUG))
logger.addHandler(handler)
logger.propagate = False

athena = boto3.client("athena")

GLUE_DATABASE = os.environ["GLUE_DATABASE"]
GLUE_TABLE = os.environ["GLUE_TABLE"]
OUTPUT_LOCATION = os.environ["OUTPUT_LOCATION"]

class QueryFailedException(Exception):
    """
    when The Query fails to finish , This Exception class called
    """
    pass

def get_query_execution_state(query_execution_id):
    return athena.get_query_execution(
        QueryExecutionId = query_execution_id
    )['QueryExecution']['Status']['State']

def wait_for_query(query_execution_id):
    while True:
        query_execution_state = get_query_execution_state(query_execution_id)
        logger.debug({ "query_execution_state":  query_execution_state })
        if query_execution_state == "SUCCEEDED":
            return query_execution_id
        elif query_execution_state == "FAILED" or query_execution_state == "CANCELLED":
            raise QueryFailedException(f"query_execution_state = {query_execution_state}")
        else:
            time.sleep(10)

def query(sql):
    logger.debug({"sql": sql})
    query_execution_id = athena.start_query_execution(
        QueryString = sql,
        ResultConfiguration={
            "OutputLocation" : OUTPUT_LOCATION
        }
    )["QueryExecutionId"]
    logger.debug({"query_execution_id": query_execution_id})
    return query_execution_id

def get_query_results(query_execution_id, max_results = 1000):
    params = {
        "MaxResults": max_results
    }
    while True:
        result = athena.get_query_results(
            QueryExecutionId = query_execution_id,
            **params
        )
        columns = [ c["Name"] for c in result["ResultSet"]["ResultSetMetadata"]["ColumnInfo"] ]
        yield [
            dict(zip(
                columns,
                [ c["VarCharValue"] if "VarCharValue" in c else None for c in row["Data"] ]
            )) for row in result["ResultSet"]["Rows"]
        ]
        if "NextToken" not in result:
            break
        params["NextToken"] = result["NextToken"]

def drop_partitions_sql(partitions):
    return f"ALTER TABLE {GLUE_DATABASE}.{GLUE_TABLE} DROP IF EXISTS {', '.join(partitions) };"

def add_partitions_sql(partitions, locations):
    if len(partitions) != len(locations):
        raise Exception("ALTER TABLE ADD STATEMENTS requires partitions and locations the same list length.")
    return f"""ALTER TABLE {GLUE_DATABASE}.{GLUE_TABLE} ADD IF NOT EXISTS {' '.join(
        [ f"{partitions[i]} LOCATION '{locations[i]}'" for i in range(len(partitions))]
	)};"""

def show_partitions_sql():
    return f"SHOW PARTITIONS {GLUE_DATABASE}.{GLUE_TABLE};"

def sync_execute_query(sql):
    return wait_for_query(query(sql))

def sync_query(sql):
    query_execution_id = sync_execute_query(sql)
    for records in get_query_results(query_execution_id):
        yield records
