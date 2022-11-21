# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Purpose

AWS Lambda function that handles calls from an Amazon API Gateway REST API.
"""

import json
import boto3
import uuid
import requests


def lambda_handler(event, context):

    client = boto3.client('kinesis')
    body = json.loads(event['body'])

    url = "https://nearblocks.io/api/blocks?limit=1&offset=0"
    headers = {"accept": "application/json"}
    near_text = requests.post(url, headers=headers).text
    block = json.loads(near_text)['blocks'][0]['block_height']

    body['block'] = block

    response = client.put_record(
        StreamName='canaal-input',
        Data=json.dumps(body),
        PartitionKey=str(uuid.uuid4()))

    return {
        'statusCode': 200,
    }
