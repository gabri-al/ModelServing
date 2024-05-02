import dash
from dash import html, dcc, callback, Input, Output, State
import dash_bootstrap_components as dbc

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
        ], width = 5),
        dbc.Col([], width = 1)
    ]),


    
])

############################################################################################

