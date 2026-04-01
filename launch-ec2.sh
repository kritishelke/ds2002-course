#!/bin/bash

AMI="ami-0ec10929233384c7f"
INSTANCE_TYPE="t2.nano"
INSTANCE_NAME="ds2002-bae7kx"
KEY_NAME="key-ec2"
SECURITY_GROUP_ID="sg-0be6679b34372f15a"
SUBNET_ID="subnet-085a38474f2e3b60c"

aws ec2 run-instances \
  --image-id "$AMI" \
  --instance-type "$INSTANCE_TYPE" \
  --key-name "$KEY_NAME" \
  --security-group-ids "$SECURITY_GROUP_ID" \
  --subnet-id "$SUBNET_ID" \
  --count 1 \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$INSTANCE_NAME}]"
