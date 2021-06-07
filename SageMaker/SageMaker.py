# -*- encoding: utf-8 -*-
import boto3

sagemaker = boto3.client("sagemaker-runtime")

#Invoke Endpoint and get a predicted result
response = sagemaker.invoke_endpoint(
    EndpointName = "SageMaker Endpoint Name",
    Body=b'bytes'|file,
    ContentType = 'text/csv', #The MIME type of the input data in the request body.
    Accept = 'application/json' #The desired MIME type of the inference in the response.
)