import httplib2
import datetime
import datetime
import ndjson
import pandas as pd
import sys
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import bigquery
import os
import boto3
from boto3.dynamodb.conditions import Key

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/apple/Downloads/PemKeys/charged-mission-258720-e5c930f3e11b.json"
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('daniyal-test')
time =  "2017-01-30 00:00:00+00"
os.environ["BUCKET_NAME"] = "daniyal-ec2-bucket"
bucket_name =os.environ["BUCKET_NAME"]


response = table.query(
        ProjectionExpression="#ts",
        ExpressionAttributeNames={"#ts": "timestamp"},
        KeyConditionExpression=
            Key('timestamp').eq(time),
        ScanIndexForward= False,
            Limit= 1    
    )
print(len(response['Items']))

if len(response['Items']) ==0 :
    query = 'SELECT * FROM `charged-mission-258720.test.auditlogs` limit 10'
else:
    query = 'SELECT * FROM `charged-mission-258720.test.auditlogs` WHERE receiveTimestamp > TIMESTAMP("2017-01-30 00:00:00+00") limit 2'

client = bigquery.Client()
QUERY = ( query)
query_job = client.query(QUERY).to_dataframe()  # API request
rows = query_job.to_json(orient='records')  # Waits for query to finish
print(rows)

dt = str(datetime.datetime.now())

file_name = "data-"+dt+"-.json"

with open(file_name, 'w') as f:
    writer = ndjson.writer(f, ensure_ascii=False)
    writer.writerow(rows)


# # WRITING DATA TO A FILE


s3_client = boto3.client('s3')
response = s3_client.upload_file(file_name, bucket_name, file_name)


