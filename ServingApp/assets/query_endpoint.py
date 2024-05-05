import requests
import json
import pandas as pd

def create_tf_serving_json(data):
  return {'inputs': {name: data[name].tolist() for name in data.keys()} if isinstance(data, dict) else data.tolist()}

def score_model(dataset):
    url = 'https://e2-demo-field-eng.cloud.databricks.com/serving-endpoints/g_testab_endpoint/invocations'
    TOKEN = ""
    headers = {
        'Authorization': f'Bearer {TOKEN}',
        'Content-Type': 'application/json'}
    data_dict = {'dataframe_split': dataset.to_dict(orient='split')} if isinstance(dataset, pd.DataFrame) else create_tf_serving_json(dataset)
    data_json = json.dumps(data_dict, allow_nan=True)
    print(data_json)
    response = requests.request(method='POST', headers=headers, url=url, data=data_json)
    print(response)
    return response