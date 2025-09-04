import requests
import json
import os
from dotenv import load_dotenv
import time 

load_dotenv()

tenant_id = os.environ.get('TENANT_ID')
client_id = os.environ.get('CLIENT_ID')
client_secret = os.environ.get('CLIENT_SECRET')
subscription_id = os.environ.get('SUBSCRIPTION_ID')
resource_group = os.environ.get('RESOURCE_GROUP')

# factory_name = 'mf-fund-pipeline-factory' 
# pipeline_name = 'TEST_PIPELINE'

def hit_data_factory_api(factory_name, pipeline_name):

    resource = 'https://management.core.windows.net/'

    token_url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/token'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
        }
    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'resource': resource
        }
    response = requests.post(token_url, headers=headers, data=data)
    access_token = response.json()['access_token']

    api_url = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.DataFactory/factories/{factory_name}/pipelines/{pipeline_name}/createRun?api-version=2018-06-01"
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + access_token
    }
    payload = {
        'parameters': {
            'servernamefrompipeline': 'masteradbdemo',
            'dbnamefrompipeline': 'masteradbdb',
            'inputtablernamefrompipeline': 'inputtable',
            'outputtablernamefrompipeline': 'outputtable'
        }
    }
    
    response = requests.post(api_url, headers=headers, data=json.dumps(payload))
    
    if response.status_code == 200:
        run_id = response.json().get('runId')
        print(f'Pipeline run triggered successfully. Run ID: {run_id}')
    else:
        print(f'Pipeline run failed with status code {response.status_code}.')

    run_status_url = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.DataFactory/factories/{factory_name}/pipelineruns/{run_id}?api-version=2018-06-01"

    while True:
        status_response = requests.get(run_status_url, headers=headers)
        if status_response.status_code == 200:
            status = status_response.json().get('status')
            print(f'Current status: {status}')
            
            if status in ['Succeeded', 'Failed', 'Cancelled']:
                print(f'Pipeline run completed with status: {status}')
                break
        else:
            print(f'Failed to get run status. HTTP {status_response.status_code}')
            break

        time.sleep(20)