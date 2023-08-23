import dash
import db_dtypes
from dash.dependencies import Input, Output
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from dash import dcc, html

app = dash.Dash(__name__)
server = app.server

# Initialize the BigQuery Client
credentials = service_account.Credentials.from_service_account_file('creds/service_account.json')
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# Get top 20 complaints for the dropdown list
sql = '''
SELECT complaint_description, COUNT(complaint_description) AS count
FROM `bigquery-public-data.austin_311.311_service_requests`
GROUP BY complaint_description
ORDER BY count DESC
LIMIT 20;
'''
job_config = bigquery.QueryJobConfig()
# Set any required configurations for job_config, if needed

query_job = client.query(sql, job_config=job_config)

try:
    results = query_job.result()
except TypeError:
    results = query_job.result(timeout=None)
top_complaints = results.to_dataframe()


app.layout = html.Div(style={
    'textAlign': 'center',
    'padding': '50px 0'
}, children=[
    html.H1('Austin 311 Complaints Dashboard', style={'color': '#007BFF'}),

    html.Div(style={
        'display': 'inline-block',
        'width': '80%',
        'textAlign': 'left'  # This ensures the components are aligned to the left inside the div.
    }, children=[
        html.Div(children=[
            dcc.Dropdown(
                id='complaint-dropdown',
                options=[{'label': complaint, 'value': complaint} for complaint in
                         top_complaints['complaint_description']],
                value=top_complaints['complaint_description'].iloc[0],
                style={
                    'backgroundColor': '#f9f9f9',
                    'border': '1px solid #d6d6d6',
                    'padding': '10px',
                    'borderRadius': '5px',
                    'width': '100%',
                    'margin': '20px 0'
                }
            )
        ], style={'width': '30%', 'margin': '0 auto'}),  # This will center the dropdown.

        html.Div(children=[
            dcc.RadioItems(
                id='time-scale',
                options=[
                    {'label': 'Daily', 'value': 'daily'},
                    {'label': 'Weekly', 'value': 'weekly'},
                    {'label': 'Monthly', 'value': 'monthly'}
                ],
                value='daily',
                style={
                    'backgroundColor': '#f9f9f9',
                    'border': '1px solid #d6d6d6',
                    'padding': '10px',
                    'borderRadius': '5px',
                    'width': '100%',
                    'margin': '20px 0'
                }
            )
        ], style={'width': '30%', 'margin': '0 auto'})  # This will center the radio items.
    ]),

    dcc.Graph(id='graph-output')
])


@app.callback(
    Output('graph-output', 'figure'),
    [Input('complaint-dropdown', 'value'),
     Input('time-scale', 'value')]
)
def update_graph(selected_complaint, time_scale):
    if time_scale == 'daily':
        sql = '''
            SELECT complaint_description, DATE(created_date) AS date, COUNT(complaint_description) AS count
            FROM `bigquery-public-data.austin_311.311_service_requests`
            WHERE complaint_description = "{}"
            GROUP BY complaint_description, date
            ORDER BY date;
        '''.format(selected_complaint)
        date_column_based_on_time_scale = 'date'

    elif time_scale == 'weekly':

        sql = '''
            SELECT complaint_description, CONCAT(CAST(EXTRACT(YEAR FROM created_date) AS STRING), '-W', CAST(EXTRACT(WEEK FROM created_date) AS STRING)) AS year_week, COUNT(complaint_description) AS count
            FROM `bigquery-public-data.austin_311.311_service_requests`
            WHERE complaint_description = "{}"
            GROUP BY complaint_description, year_week
            ORDER BY year_week;
        '''.format(selected_complaint)

        date_column_based_on_time_scale = 'year_week'

    else:
        sql = '''
            SELECT complaint_description, CONCAT(CAST(EXTRACT(YEAR FROM created_date) AS STRING), '-', LPAD(CAST(EXTRACT(MONTH FROM created_date) AS STRING), 2, '0')) AS year_month, COUNT(complaint_description) AS count
            FROM `bigquery-public-data.austin_311.311_service_requests`
            WHERE complaint_description = "{}"
            GROUP BY complaint_description, year_month
            ORDER BY year_month;
        '''.format(selected_complaint)
        date_column_based_on_time_scale = 'year_month'

    df = client.query(sql).to_dataframe()

    return {
        'data': [{
            'x': df[date_column_based_on_time_scale],
            'y': df['count'],
            'type': 'bar'
        }],
        'layout': {
            'title': f'Number of {selected_complaint} complaints over time'
        }
    }


if __name__ == '__main__':
    import os
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("DEBUG", False)
    server.run(debug=debug, host='0.0.0.0', port=port)
