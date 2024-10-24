import dash
from dash import html, dcc, callback, Input, Output, ctx
import dash_bootstrap_components as dbc
import pandas as pd
from dash.exceptions import PreventUpdate

dash.register_page(__name__, path='/', name='Home', title='Model Serving')

############################################################################################
# Import dependencies
from assets.data_prep import encoding_mapping, prepare_input
from assets.query_endpoint import score_model

############################################################################################
# Function to generate feature inputs
_fields = {
    "host_identity_verified": "Host Identity",
    "neighbourhood_group": "Neighbourhood L1",
    "neighbourhood": "Neighbourhood L2",
    "lat": "Latitude",
    "long": "Longitude",
    "instant_bookable": "Instant Booking",
    "cancellation_policy": "Cancellation Policy",
    "room_type": "Accomodation or Room Type",
    "construction_year": "Construction Year",
    "minimum_nights": "Min Nights",
    "number_of_reviews": "Nr of Reviews",
    "review_rate_number": "Review Rate",
    "calculated_host_listings_count": "Nr of Host Listings",
    "availability_365": "Yearly Availability"
}

def generate_feature_layout(_fields):
    _div = []; i = 0
    for k_, v_ in _fields.items():
        display_name = v_
        if k_ in list(encoding_mapping.keys()):
            dropdown_values = list(encoding_mapping[k_].keys())
            input__ = dcc.Dropdown(
                id = "feature"+str(i),
                options = dropdown_values,
                placeholder = 'choose value',
                searchable = False,
                clearable = False,
                multi = False,
                className = "my-dropdown",
                persistence=True, 
                persistence_type='session'
            )
        else:
            input__ = dcc.Input(
                id = "feature"+str(i),
                type = "number",
                placeholder = "insert value",
                className = "my-input",
                persistence=True, 
                persistence_type='session'                
            )
        new_row = dbc.Row([
            dbc.Col([str(display_name)+":"], width = 7),
            dbc.Col([input__], width = 5)
        ], className = 'IO-inner-row')
        _div.append(new_row)
        i += 1

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
    Input(component_id='feature10', component_property='value'),
    Input(component_id='feature11', component_property='value'),
    Input(component_id='feature12', component_property='value'),
    Input(component_id='feature13', component_property='value'),
    Input(component_id='submit-button', component_property='n_clicks'))
def data_transform(feature0, feature1, feature2, feature3, feature4, feature5, feature6, feature7, feature8, feature9, feature10, feature11, feature12, feature13, button_clicks):
    _output = []
    if ctx.triggered_id == 'submit-button':
        try: # Verify input values' format
            tmp_df = pd.DataFrame()
            field_names = list(_fields.keys())
            feature_values_ = [feature0, feature1, feature2, feature3, feature4, feature5, feature6, feature7, feature8, feature9, feature10, feature11, feature12, feature13]
            for i in range(len(field_names)):
                tmp_df[ field_names[i] ] = [feature_values_[i]]
            feature_df = prepare_input(tmp_df)
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
                for p in _pred: # Only 1 at the time is allowed
                    _output = ["Predicted listing price: ", dbc.Alert(children=str(round(p, 3)),color='success', class_name='alert-style')]
            except:
                _output = dbc.Alert(children=['Request succeeded but prediction was not available'], color='danger', class_name='alert-style-danger')
    else:
        raise PreventUpdate
    return _output
