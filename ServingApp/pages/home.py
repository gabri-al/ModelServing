import dash
from dash import html, callback, Input, Output, State
import dash_bootstrap_components as dbc

dash.register_page(__name__, path='/', name='Home', title='Model Serving')

############################################################################################
# Page layout
layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H2(['Model Serving Inference'])
        ], width=12)
    ])
])

############################################################################################

