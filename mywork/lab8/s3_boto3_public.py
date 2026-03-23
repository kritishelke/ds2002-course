import boto3
from botocore.exceptions import ClientError

s3 = boto3.client('s3', region_name='us-east-1')

bucket = 'ds2002-bae7kx'
local_file = 'penguin.jpeg'
key_name = 'penguin.jpeg'

try:
    with open(local_file, 'rb') as f:
        response = s3.put_object(
            Body=f,
            Bucket=bucket,
            Key=key_name,
            ACL='public-read'
        )
    print("Uploaded file publicly:", key_name)
    print("Public URL:")
    print("https://s3.amazonaws.com/" + bucket + "/" + key_name)
except FileNotFoundError:
    print("Local file not found:", local_file)
except ClientError as e:
    print("Upload failed:", e)
