#!/bin/bash

# Usage:
# ./presigned-upload.sh <local_file> <bucket_name> <expiration_seconds>

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <local_file> <bucket_name> <expiration_seconds>"
    exit 1
fi

LOCAL_FILE="$1"
BUCKET_NAME="$2"
EXPIRATION="$3"

if [ ! -f "$LOCAL_FILE" ]; then
    echo "Error: File '$LOCAL_FILE' does not exist."
    exit 1
fi

FILE_NAME=$(basename "$LOCAL_FILE")
S3_PATH="s3://$BUCKET_NAME/$FILE_NAME"

# Upload file to private bucket
aws s3 cp "$LOCAL_FILE" "$S3_PATH"

if [ "$?" -ne 0 ]; then
    echo "Error: Upload failed."
    exit 1
fi

# Generate presigned URL
PRESIGNED_URL=$(aws s3 presign "$S3_PATH" --expires-in "$EXPIRATION")

if [ "$?" -ne 0 ]; then
    echo "Error: Failed to generate presigned URL."
    exit 1
fi

echo "Upload successful."
echo "Presigned URL:"
echo "$PRESIGNED_URL"
