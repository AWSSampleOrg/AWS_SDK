#-*- encoding:utf-8 -*-
import base64

#KMS
kms = boto3.client("kms")

#Decrypt a Lambda Enviroment variable encrypted by Customer managed CMK (Customer Master Key)
DECRYPTED = kms.decrypt(
    #You can decrypt the value without KeyId, also can do with it.
    # KeyId = "Enter a key ID of the CMK that was used to encrypt the ciphertext.",
    CiphertextBlob = base64.b64decode(os.environ['name']),
    EncryptionContext={'LambdaFunctionName': os.environ['AWS_LAMBDA_FUNCTION_NAME']}
)['Plaintext'].decode("UTF-8")
