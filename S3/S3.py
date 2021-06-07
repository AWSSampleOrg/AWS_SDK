# -*- encoding: utf-8 -*-
import boto3

#kms
kms = boto3.client("kms")

#s3
s3 = boto3.client('s3')

BUCKET_NAME = "Bucket_Name"

"""
One object Process
"""
#Write object
s3.put_object(
    Bucket = BUCKET_NAME,
    Body = "test body".encode("UTF-8"),
    Key = "path/to/file"
)

#Read object
s3.get_object(
    Bucket = BUCKET_NAME,
    Key = "path/to/file"
)["Body"].read().decode("UTF-8")

#Delete object
s3.delete_object(
    Bucket = BUCKET_NAME,
    Key = "path/to/file"
)

#copy object
s3.copy_object(
    Bucket = BUCKET_NAME,
    Key = "ENd S3 Key ex) s3:://Bucket_Name/path/to/file",
    CopySource = {
        "Bucket" : "Source Bucket_Name",
        "Key" : "Source S3 Key"
    }
)

"""
Multiple Objects Process
"""
#Get all objects under the specified s3 prefix
contents = []
kwargs = {
    "Bucket" : BUCKET_NAME,
    "Prefix" : "The Prefix to be Searched"
}
while True:
    response = s3.list_objects_v2(**kwargs)
    if "Contents" in response:
        contents.extend(response["Contents"])
        if 'NextContinuationToken' in response:
            kwargs["ContinuationToken"] = response['NextContinuationToken']
            continue
    break


"""
Server Side Encryption
1. Default Encryption
  1-1. SSE with AES-256
  1-2. SSE with KMS AWS Managed Keys
  1-3. SSE with KMS CMK(Customer Managed Keys)
2. Non Default Encryption
  2-1. SSE with AES-256
  2-2. SSE with KMS AWS Managed Keys
  2-3. SSE with KMS CMK(Customer Managed Keys)
  2-4. SSE with Client operations key. This is not the key which S3 or KMS operates
"""
#1-1. SSE with AES-256
#1-2. SSE with KMS AWS Managed Keys
#1-3. SSE with KMS CMK(Customer Managed Keys)
response = s3.put_object(
    Bucket = BUCKET_NAME,
    Key = "test",
    Body = "Encrypted".encode("UTF-8")
)
print(f'ServerSideEncryption'.ljust(20) + f' = {response["ServerSideEncryption"]}')
#just only for KMS. check the KeyManager
if response["ServerSideEncryption"] == "aws:kms":
    KeyManager = kms.describe_key(
        KeyId = response["SSEKMSKeyId"]
    )["KeyMetadata"]["KeyManager"]
    print(f"KeyManager".ljust(20) + f" = {KeyManager}")


#2-1. SSE with AES-256
response = s3.put_object(
    Bucket = BUCKET_NAME,
    Key = "test",
    Body = "Encrypted".encode("UTF-8"),
    ServerSideEncryption = "AES256"
)
#2-2. SSE with KMS AWS Managed Keys
response = s3.put_object(
    Bucket = BUCKET_NAME,
    Key = "test",
    Body = "Encrypted".encode("UTF-8"),
    ServerSideEncryption = "aws:kms"
)
#2-3. SSE with KMS CMK(Customer Managed Keys)
response = s3.put_object(
    Bucket = BUCKET_NAME,
    Key = "test",
    Body = "Encrypted".encode("UTF-8"),
    ServerSideEncryption = "aws:kms",
    SSEKMSKeyId = "Your Customer Manged Key ID"
)
#2-4. SSE with Client operations key. This is not the key which S3 or KMS operates
response = s3.put_object(
    Bucket = BUCKET_NAME,
    Key = "test",
    Body = "Encrypted".encode("UTF-8"),
    SSECustomerAlgorithm = "AES256",
    SSECustomerKey = "The Key You generated. ex) SSE_CUSTOMER_KEY=$(cat /dev/urandom | base64 -i | fold -w 32 | head -n 1)"
)
