from dash import html
import dash_bootstrap_components as dbc

_footer = html.Div([
    dbc.Container([
        dbc.Row([
            dbc.Col([], width = 1),
            dbc.Col(['Showcasing Databricks Model Serving via Plotly Dash'], width = 6),
            dbc.Col([], width = 1),
	        dbc.Col([
                html.Ul([
                    html.Li([
                        html.A([ html.I(className="fa-brands fa-github me-2 fa-1x")], href='https://github.com/gabri-al'),
                        html.A([ html.I(className="fa-brands fa-linkedin me-2 fa-1x")], href='https://www.linkedin.com/in/gabriele-albini-85100549/'),
                        html.A([ html.I(className="fa-brands fa-medium me-2 fa-1x")], href='https://medium.com/@gabri-albini')
                    ])
                ], className='list-unstyled d-flex justify-content-center justify-content-md-start')
            ], width = 3),
            dbc.Col([], width = 1)
        ])
    ], fluid=True)
], className = 'footer')