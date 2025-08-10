import azure.functions as func
import logging
from azure.storage.blob import BlobServiceClient, BlobClient
import requests
import os
import json

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="createevent")
def createevent(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Retrieve Okta Token from Azure Blob Storage
    connect_str = os.getenv('CSOD_STORAGE_CONNECTION_STRING')
    container_name = 'oktacredentials'
    blob_name = 'okta_token.json'
    okta_token = get_blob_content(connect_str, container_name, blob_name)
    access_token = okta_token.get('access_token')

    # Prepare the Snowflake SQL API request
    snowflake_url = "https://edwsce.west-us-2.azure.snowflakecomputing.com/api/v2/statements?X-Snowflake-Authorization-Token-Type=OAUTH"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }
    data = {
            "statement": "select * from PROD_CS_SS.CS_CSOD_BIC_SS.BILL_LINE_REV",
            "database": "PROD_CS_SS",
            "schema": "CS_CSOD_BIC_SS",
            "warehouse": "PROD_SYSC_CSLEGACY_CNSMPADHOCWH2",
            "role": "SF_CSOD_BIC_PROD_PROD_ROLE"
            }
    
    # Execute the SQL query using the Snowflake SQL API
    response = requests.post(snowflake_url, headers=headers, json=data)
    if response.status_code == 200:
        return func.HttpResponse(response.text, status_code=200,headers={"Content-Type": "application/json"})
    else:
        return func.HttpResponse(f"Error executing query: {response.text}", status_code=response.status_code)

'''
    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello there, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             f"This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
'''
def get_blob_content(connect_str, container_name, blob_name):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_content = blob_client.download_blob().readall()
        token_info = json.loads(blob_content)
        return token_info
    
    except Exception as e:
        logging.error(f"Error retrieving blob content: {str(e)}")
        return None


@app.timer_trigger(schedule="* * * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def timer_trigger(myTimer: func.TimerRequest) -> None:
    
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')