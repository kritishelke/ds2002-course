#!/bin/bash
aws ec2 describe-instances \
  --query 'Reservations[].Instances[].{Name:Tags[?Key==`Name`]|[0].Value,ID:InstanceId,State:State.Name,Key:KeyName,PublicIP:PublicIpAddress}' \
  --output table
