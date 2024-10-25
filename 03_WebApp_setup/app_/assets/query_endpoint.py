import requests
import json
import pandas as pd
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
Endpoint_URL_ = 'https://e2-demo-field-eng.cloud.databricks.com/serving-endpoints/airbnb_ny_pred/invocations'
scope_name_ = 'Demo_Airbnb_Space'
secret_name_ = 'pat_cc'

def create_tf_serving_json(data):
  return {'inputs': {name: data[name].tolist() for name in data.keys()} if isinstance(data, dict) else data.tolist()}

def score_model(dataset):
    url = Endpoint_URL_
    PAT_ = w.dbutils.secrets.get(scope=scope_name_, key=secret_name_)
    headers = {
        'Authorization': f'Bearer {PAT_}',
        'Content-Type': 'application/json'}
    data_dict = {'dataframe_split': dataset.to_dict(orient='split')} if isinstance(dataset, pd.DataFrame) else create_tf_serving_json(dataset)
    data_json = json.dumps(data_dict, allow_nan=True)
    response = requests.request(method='POST', headers=headers, url=url, data=data_json)
    return response