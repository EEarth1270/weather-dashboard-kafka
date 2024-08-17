import os
import dash
from datetime import datetime
from dash import dcc, html
from dash.dependencies import Input, Output, State
import plotly.express as px
import plotly.graph_objects as go
import sqlalchemy
import pandas as pd
import pytz
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
interval_ = int(os.getenv('UPDATE_INTERVAL', 500000))
interval_ = 1800000 # override to 30 min interval since data change every 0.5-1hr 
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Bangkok Temperature Dashboard"),
    dcc.Graph(id='map-plot', style={'height': '600px'}),
    dcc.Graph(id='line-plot'),
    html.Button("Clear Selection", id='clear-button', n_clicks=0),
    dcc.Interval(
        id='interval-component',
        interval=interval_,  # update every 1 min 
        n_intervals=0
    ),
    dcc.Store(id='selected-districts', data=[])
])

# Configuration
database_url = os.getenv('DATABASE_URL')
# Create SQLAlchemy engine
engine = sqlalchemy.create_engine(database_url)

@app.callback(
    [Output('line-plot', 'figure'),
     Output('map-plot', 'figure'),
     Output('selected-districts', 'data')],
    [Input('interval-component', 'n_intervals'),
     Input('map-plot', 'clickData'),
     Input('clear-button', 'n_clicks')],
    [State('selected-districts', 'data')]
)
def update_graphs(n, clickData, clear_clicks, selected_districts):
    try:
        # Query for line plot data
        query_line = "SELECT * FROM weather_data_real ORDER BY timestamp DESC LIMIT 43200"
        df_line = pd.read_sql_query(query_line, engine)

        # Query for map plot data
        query_map = """SELECT district , 
            max(city) as city,
            max(cross_check_name) as cross_check_name,
            max(temperature) as temperature,
            max(real_feel) as real_feel,
            max(lat) as lat,
            max(long) as long,
            max(timestamp) as timestamp 
            FROM weather_data_real 
            GROUP BY district 
            ORDER BY timestamp DESC;"""
        df_map = pd.read_sql_query(query_map, engine)

        print(f'current time {datetime.now().strftime("%H:%M:%S")} Size of DF map : {len(df_map)}, line : {len(df_line)}')
        if df_line.empty or df_map.empty:
            print("DataFrames are empty")
            return {}, {}, selected_districts

        # Convert Unix timestamp to datetime
        bangkok_tz = pytz.timezone('Asia/Bangkok')
        df_line['timestamp'] = pd.to_datetime(df_line['timestamp'], unit='s').dt.tz_localize('UTC').dt.tz_convert(bangkok_tz)
        df_map['timestamp'] = pd.to_datetime(df_map['timestamp'], unit='s').dt.tz_localize('UTC').dt.tz_convert(bangkok_tz)

        # Clear selection if clear button is clicked
        ctx = dash.callback_context
        # print(f'**Before Outside Clear IF {selected_districts},{clickData}')
        if ctx.triggered and ctx.triggered[0]['prop_id'] == 'clear-button.n_clicks':
            selected_districts = []
            clicked_district =''
            clickData = []
            print("Clear Selected")
        elif clickData:
            clicked_district = clickData['points'][0]['hovertext']
            if clicked_district not in selected_districts:
                selected_districts.append(clicked_district)
            print(f"Selected districts: {selected_districts}")
        # print(f'**After Outside Clear IF {selected_districts},{clickData}')
        # Update line plot based on the selected districts
        if selected_districts:
            df_filtered = df_line[df_line['district'].isin(selected_districts)]
            line_fig = px.line(df_filtered, x='timestamp', y='temperature',hover_data='real_feel', markers=True,color='district',symbol='district', text='temperature')
            line_fig.update_traces(textposition="bottom right")
            line_fig.update_layout(yaxis_title='Temperature (°C)', xaxis_title='Time')
        else:
            line_fig = px.box(df_line, x='district', y='temperature')
            line_fig.update_layout(yaxis_title='Temperature (°C)', xaxis_title='District')

        # Map plot
        map_fig = px.scatter_mapbox(df_map, lat='lat', lon='long', color='temperature', size='real_feel',
                                    hover_name='district', hover_data=['temperature', 'real_feel', 'timestamp'],
                                    color_continuous_scale='Viridis', zoom=9.5)
        map_fig.update_layout(mapbox_style='open-street-map')

        return line_fig, map_fig, selected_districts

    except Exception as e:
        print(f"Error occurred: {e}")
        return {}, {}, selected_districts

if __name__ == '__main__':
    app.run_server(debug=True)
