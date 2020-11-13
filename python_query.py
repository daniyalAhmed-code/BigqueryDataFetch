import httplib2
import datetime
import ndjson
import json
import pytz
import pandas as pd
import sys
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import bigquery
import os
import boto3
from boto3.dynamodb.conditions import Key,Attr

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/apple/Downloads/PemKeys/charged-mission-258720-e5c930f3e11b.json"
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('daniyal-test')
time =  "2020-05-27T23:26:39.230Z"
bucket_name ="daniyal-ec2-bucket"
query = """
    SELECT * FROM `charged-mission-258720.test.auditlogs` _where_  _limit_  """


response = table.scan(FilterExpression=Attr("timestamp").gt(time))




if len(response['Items']) ==0 :
    query = query.replace("_where_", "")
    query = query.replace("_limit_", "")
else:
   query = query.replace("_where_", " WHERE timestamp > @time ")
   query = query.replace("_limit_", "limit 2") 

client = bigquery.Client()

QUERY = query
job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("time", "STRING", time),
    ]
)

query_job = client.query(QUERY, job_config=job_config).to_dataframe()  # API request

rows = query_job.to_json(orient='records',date_format='iso') # Waits for query to finish

dt = str(datetime.datetime.now())

file_name = "data-"+dt+".ndjson"

data = json.loads(rows)

with open(file_name, 'w') as f:
    writer = ndjson.writer(f, ensure_ascii=False)
    writer.writerow(data )


# # WRITING DATA TO A FILE


s3_client = boto3.client('s3')
response = s3_client.upload_file(file_name, bucket_name, file_name)

