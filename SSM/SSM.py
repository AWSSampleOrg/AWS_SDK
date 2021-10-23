# -*- encoding: utf-8 -*-
import boto3

ssm = boto3.client('ssm')
parameters = "your SSM parameter"
response = ssm.get_parameters(
    Names=[
        parameters,
    ],
    #Return decrypted values for secure string parameters. This flag is ignored for String and StringList parameter types.
    WithDecryption = True
)
print(response['Parameters'][0]['Value'])
