import dash
from dash import html, dcc, callback, Input, Output, ctx
import dash_bootstrap_components as dbc
import pandas as pd
from dash.exceptions import PreventUpdate

dash.register_page(__name__, path='/', name='Home', title='Model Serving')

############################################################################################
# Function to generate feature layout
_fields = ["Latitude", "Longitude", "Is Entire Apt", "Is Private Room", "Is Shared Room", "Min nights",
           "Nr of Reviews", "Reviews per month", "Host listing count", "Availability 365"]

def generate_feature_layout(_fields):
    _div = []
    for i, _f in enumerate(_fields):
        new_row = dbc.Row([
            dbc.Col([str(_f)+":"], width = 7),
            dbc.Col([dcc.Input(id = "feature"+str(i), type = "number", placeholder = "insert value", className = "my-input")], width = 5)
        ], className = 'IO-inner-row')
        _div.append(new_row)
    button_row = dbc.Row([
        dbc.Col([html.Button('Submit', id='submit-button', n_clicks=0, className='my-button'),], width = 12)
    ], className = 'IO-inner-row-wButton')
    _div.append(button_row)
    return html.Div(_div, className = "IO-outer-col")

############################################################################################
# Function to perform inference
from assets.query_endpoint import score_model

############################################################################################
# Page layout
layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H2(['Model Serving Inference'])
        ], width=12)
    ], style = {"margin":"0px 0px 15px 0px"}),
    dbc.Row([
        dbc.Col([], width = 1),
        dbc.Col([
            html.H4(['Input Features'])
        ], width=5),
        dbc.Col([
            html.H4(['Generated Inference'])
        ], width=5),
        dbc.Col([], width = 1)
    ]),

    #### Generate input objects for each field & host inference
    dbc.Row([
        dbc.Col([], width = 1),
        dbc.Col([
            generate_feature_layout(_fields)
        ], width = 5),
        dbc.Col([
            html.Div([
                dbc.Row([
                    dbc.Col([], width = 1),
                    dbc.Col([], id="inference-col", width = 10),
                    dbc.Col([], width = 1)
                ], className = 'IO-inner-row')
            ], className = "IO-outer-col")
        ], width = 5),
        dbc.Col([], width = 1)
    ]),
    
])

############################################################################################
# Inference content
@callback(
    Output(component_id='inference-col', component_property='children'),
    Input(component_id='feature0', component_property='value'),
    Input(component_id='feature1', component_property='value'),
    Input(component_id='feature2', component_property='value'),
    Input(component_id='feature3', component_property='value'),
    Input(component_id='feature4', component_property='value'),
    Input(component_id='feature5', component_property='value'),
    Input(component_id='feature6', component_property='value'),
    Input(component_id='feature7', component_property='value'),
    Input(component_id='feature8', component_property='value'),
    Input(component_id='feature9', component_property='value'),
    Input(component_id='submit-button', component_property='n_clicks'))
def data_transform(feature0, feature1, feature2, feature3, feature4, feature5, feature6, feature7, feature8, feature9, button_clicks):
    _output = []
    if ctx.triggered_id == 'submit-button':
        # Verify input values' format
        try:
            feature0clean = float(feature0)
            feature1clean = float(feature1)
            feature2clean = float(feature2)
            feature3clean = float(feature3)
            feature4clean = float(feature4)
            feature5clean = float(feature5)
            feature6clean = float(feature6)
            feature7clean = float(feature7)
            feature8clean = float(feature8)
            feature9clean = float(feature9)
            feature_df = pd.DataFrame({
                'latitude': [feature0clean],
                'longitude': [feature1clean],
                'is_entire_apt': [feature2clean],
                'is_private_room': [feature3clean],
                'is_shared_room': [feature4clean],
                'minimum_nights': [feature5clean],
                'number_of_reviews': [feature6clean],
                'reviews_per_month': [feature7clean],
                'calculated_host_listings_count': [feature8clean],
                'availability_365': [feature9clean]})
        except:
            _output = dbc.Alert(children=['Insert valid features'], color='warning', class_name='alert-style')
            return _output
        # Query endpoint API
        _response = score_model(feature_df)
        if _response.status_code != 200: # Failure
            _output = dbc.Alert(children=['Request failed with status: '+str(_response.status_code)+', '+str(_response.text)],
                                color='danger', class_name='alert-style-danger')
        else: # Success
            try: # Try extracting prediction
                _pred = _response.json()['predictions']
                _pred_str = []
                for p in _pred:
                    _pred_str.append(str(round(p, 3)))
                _output = ["Predicted listing prices: ", dbc.Alert(children=_pred_str,color='success', class_name='alert-style')]
            except:
                _output = dbc.Alert(children=['Request succeeded but prediction was not available'], color='danger', class_name='alert-style-danger')
    else:
        raise PreventUpdate
    return _output
