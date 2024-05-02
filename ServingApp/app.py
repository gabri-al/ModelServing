from dash import Dash, dcc, callback, Input, Output, State
import dash_bootstrap_components as dbc
import dash
from dash import html

_font = "https://fonts.googleapis.com/css2?family=Lato&display=swap"
app = Dash(__name__, use_pages=True, external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME, _font],
		   suppress_callback_exceptions=True, prevent_initial_callbacks=True)
server = app.server

############################################################################################
# Import assets
from assets.footer import _footer

############################################################################################
# App Layout
app.layout = dbc.Container([
	dbc.Row([
        dbc.Col([], width = 2),
        dbc.Col([
            dbc.Row([dash.page_container])
	    ], className = 'page-content', width = 8),
		dbc.Col([], width = 2)
    ]),
    ## Footer
    dbc.Row([
        dbc.Col([], width = 3),
        dbc.Col([html.Hr([], className = 'hr-footer')], width = 6),
        dbc.Col([], width = 3)
    ]),
    dbc.Row([
        dbc.Col([], width = 3),
        dbc.Col([
            dbc.Row([_footer])
	    ], width = 6),
		dbc.Col([], width = 3)
    ])
], fluid=True)

############################################################################################
# Run App
if __name__ == '__main__':
	app.run_server(debug=True)