
from google.cloud import bigquery
from pprint import pprint
import os
import datetime
import json
import ndjson
import boto3
from boto3.dynamodb.conditions import Key,Attr
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['table_name'])
time =  os.environ['time']

bucket_name =os.environ["bucket-name"]
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/path/to/pem/keys.json"
query = """
    SELECT * FROM `charged-mission-258720.test.auditlogs`  _where_
"""

response = table.scan(FilterExpression=Attr("timestamp").gt(time))

if len(response['Items']) ==0 :
    query = query.replace("_where_", "")
else:
   query = query.replace("_where_", " WHERE timestamp > PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ',  @time)")


# Construct a BigQuery client object.
client = bigquery.Client()

query = query
job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("time", "STRING", time),
    ]
)

query_job = client.query(query,job_config=job_config)  # Make an API request.

query_job.result()  # Wait for the query to complete.

# Get the destination table for the query results.
#
# All queries write to a destination table. If a destination table is not
# specified, the BigQuery populates it with a reference to a temporary
# anonymous table after the query completes.
destination = query_job.destination

# Get the schema (and other properties) for the destination table.
#
# A schema is useful for converting from BigQuery types to Python types.
destination = client.get_table(destination)

# Download rows.
#
# The client library automatically handles pagination.
print("The query data:")


total_rows = []
next_token = "NOT_NULL"
count = 1

def upload_to_s3(file_name,bucket_name,object_name=None):
    if object_name == None:
        object_name = file_name
    s3_client = boto3.client('s3')
    response = s3_client.upload_file(file_name, bucket_name, object_name)

while not next_token is None:
    params = {
        "table": destination,
        "max_results": 1000
    }
    if next_token != "NOT_NULL":
        params['page_token'] = next_token

    rows = client.list_rows(**params)
    total_rows.extend(list(rows))
    next_token = rows.next_page_token
    count = count + 1
    dt = str(datetime.datetime.now())

    file_name = "data-"+dt+".ndjson"
    results = query_job.result()
    data_input=query_job.to_dataframe().to_json(orient='records',date_format='iso')
    data = json.loads(data_input)
    with open(file_name, 'w') as f:
        writer = ndjson.writer(f, ensure_ascii=False)
        writer.writerow(data)
        upload_to_s3(file_name,bucket_name,None)
    

def upload_to_s3(file_name,bucket_name,object_name=None):
    if object_name == None:
        object_name = file_name
    s3_client = boto3.client('s3')
    response = s3_client.upload_file(file_name, bucket_name, object_name)
