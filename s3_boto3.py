import boto3
from botocore.exceptions import ClientError

# create client
s3 = boto3.client('s3', region_name='us-east-1')

bucket = 'ds2002-bae7kx'
local_file = 'cat.jpg'
key_name = 'cat.jpg'

# list all buckets
print("All buckets:")
response = s3.list_buckets()
for r in response['Buckets']:
    print(r['Name'])

print()

# upload file privately using put_object
try:
    with open(local_file, 'rb') as f:
        response = s3.put_object(
            Body=f,
            Bucket=bucket,
            Key=key_name
        )
    print("Uploaded file privately:", key_name)
except FileNotFoundError:
    print("Local file not found:", local_file)
    raise SystemExit
except ClientError as e:
    print("Upload failed:", e)
    raise SystemExit

print()

# list objects in the bucket
print("Bucket contents after upload:")
try:
    response = s3.list_objects_v2(Bucket=bucket)
    if 'Contents' in response:
        for obj in response['Contents']:
            print(obj['Key'])
    else:
        print("Bucket is empty.")
except ClientError as e:
    print("Could not list bucket contents:", e)
    raise SystemExit

print()

# delete the uploaded file
try:
    s3.delete_object(Bucket=bucket, Key=key_name)
    print("Deleted file:", key_name)
except ClientError as e:
    print("Delete failed:", e)
    raise SystemExit

print()

# confirm deletion
print("Bucket contents after deletion:")
try:
    response = s3.list_objects_v2(Bucket=bucket)
    if 'Contents' in response:
        for obj in response['Contents']:
            print(obj['Key'])
    else:
        print("Bucket is empty.")
except ClientError as e:
    print("Could not list bucket contents:", e)

 s3 = boto3.client('s3', region_name='us-east-1')


