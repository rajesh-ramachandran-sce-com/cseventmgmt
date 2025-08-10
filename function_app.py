import azure.functions as func
import logging
from azure.storage.blob import BlobServiceClient, BlobClient
import requests
import os
import json
from datetime import datetime

# Get URLs 
snowflake_url = os.getenv('SNOWFLAKE_SQL_API_URL')
okta_endpoint = os.getenv('OKTA_ENDPOINT_URL')

# Get OKTA CLIENT Credentials
client_id = os.getenv('OKTA_CLIENT_ID')
client_secret = os.getenv('OKTA_CLIENT_SECRET')

# Get Azure Blob Storage and Container and set blob client
connect_str = os.getenv('CSOD_STORAGE_CONNECTION_STRING')
container_name = 'oktacredentials'
blob_name = 'okta_token.json'
try:
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
except Exception as e:
    logging.error(f"Error retrieving blob content: {str(e)}")



app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)
@app.route(route="addevent")

# Http Trigger Function for the API 
def addevent(req: func.HttpRequest) -> func.HttpResponse:
    
    if req.method == 'POST':
        body = req.get_body()
        try:
            # Attempt to parse the JSON body
            body = req.get_json()

            # Now, validate the format of the JSON body
            # expected format {"KEY": "string", "VALUE": string}
            if 'eventData' in body:

                okta_token = get_blob_content(blob_client)
                access_token = okta_token.get('access_token')

                # Prepare the Snowflake SQL API request
                params = {'X-Snowflake-Authorization-Token-Type':'OAUTH'}
                headers = {
                            'Authorization': f'Bearer {access_token}',
                            'Content-Type': 'application/json',     
                            'Accept': 'application/json',
                          }
                # Build SQL Query from body JSON
                sql_base = "insert into PROD_CS_SS.CS_CSOD_BIC_SS.API_TARGET values"
                values = ", ".join([f"('{item['KEY']}', '{item['VALUE']}', CURRENT_TIMESTAMP)" for item in body['eventData'] if 'KEY' in item and 'VALUE' in item ])
                sql_query = f"{sql_base} {values}"

                data = {
                        "statement": f"{sql_query}",
                        "database": "PROD_CS_SS",
                        "schema": "CS_CSOD_BIC_SS",
                        "warehouse": "PROD_SYSC_CSLEGACY_CNSMPADHOCWH2",
                        "role": "SF_CSOD_BIC_PROD_PROD_ROLE"
                        }
            
                # Execute the SQL query using the Snowflake SQL API
                response = requests.post(snowflake_url, headers=headers, params=params, json=data)

                if response.status_code == 200:
                    logging.info("Http Trigger function executed successfully")
                    return func.HttpResponse(response.text, status_code=200,headers={"Content-Type": "application/json"})
                else:
                    return func.HttpResponse(f"Error executing query: {response.text}", status_code=response.status_code)
            else:
                 # Incorrect JSON format found in Body
                return func.HttpResponse("Invalid body specified, expected in JSON format {'KEY' : KeyString, 'VALUE' : ValueString}.", status_code=400)
        except ValueError:
            # If error occurs in JSON parsing
            return func.HttpResponse("Invalid body specifed, expected in JSON format {'KEY' : KeyString, 'VALUE' : ValueString}.", status_code=400)
        except Exception as e:
            # Handle other unforeseen errors
            return func.HttpResponse(f"Error: {str(e)}", status_code=500)
    else:
        return func.HttpResponse(f"{req.method} method is not defined for this API",status_code=200)

# read Blob data for Okta Token
def get_blob_content(blob_client):
    try:
        blob_content = blob_client.download_blob().readall()
        token_info = json.loads(blob_content)
        return token_info
    
    except Exception as e:
        logging.error(f"Error retrieving blob content: {str(e)}")
        return None
    
# write Blob data with new Okta Token    
def put_blob_content(blob_client, content):
    try:
        blob_client.upload_blob(content, overwrite=True)
        return None
    
    except Exception as e:
        logging.error(f"Error retrieving blob content: {str(e)}")
        return None


@app.timer_trigger(schedule="0 0 10 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=True) 

# Http Time Trigger Function to refresh the Okta token everyday at 10.00 AM UTC
def timer_trigger(myTimer: func.TimerRequest) -> None:
    
    if myTimer.past_due:
        logging.info('The timer is past due!')

    okta_token = get_blob_content(blob_client)
    refresh_token = okta_token.get('refresh_token')

    # Prepare data for the token refresh request
    data = {
            'grant_type': 'refresh_token',
            'scope': 'session:role:SF_CSOD_BIC_PROD_PROD_ROLE offline_access',
            'refresh_token': refresh_token
           }
    
    auth = requests.auth.HTTPBasicAuth(client_id, client_secret)

    # Make the request to refresh the token
    response = requests.post(url=okta_endpoint, auth=auth, data=data)
    last_modified = datetime.now().isoformat()

    if response.status_code == 200:
        new_token_data = response.json()
        new_token_data.update({'last_modified':last_modified})

        # Update the blob with the new token data
        put_blob_content(blob_client,content=json.dumps(new_token_data))
        logging.info(f"Okta token refreshed and updated in blob storage at {last_modified}")
    else:
        logging.error(f"Failed to refresh Okta token: {response.text}")




