import os
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import sqlalchemy
import pandas as pd
import pytz
from dotenv import load_dotenv

# load env variables
load_dotenv()


app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Weather Data Dashboard"),
    html.H2("last 12 hours (generated)"),
    dcc.Dropdown(
        id='city-dropdown',
        options=[{'label': city, 'value': city} for city in [
            'Bangkok', 'Samut Prakan', 'Nonthaburi', 'Pathum Thani', 'Phra Nakhon Si Ayutthaya', 'Ang Thong', 'Loburi',
            'Sing Buri', 'Chai Nat', 'Saraburi', 'Nakhon Nayok', 'Nakhon Sawan', 'Uthai Thani', 'Kamphaeng Phet', 'Sukhothai',
            'Phitsanulok', 'Phichit', 'Phetchabun', 'Suphan Buri', 'Nakhon Pathom', 'Samut Sakhon', 'Samut Songkhram',
            'Chon Buri', 'Rayong', 'Chanthaburi', 'Trat', 'Chachoengsao', 'Prachin Buri', 'Sa Kaeo', 'Nakhon Ratchasima',
            'Buri Ram', 'Surin', 'Si Sa Ket', 'Ubon Ratchathani', 'Yasothon', 'Chaiyaphum', 'Amnat Charoen', 'buogkan',
            'Nong Bua Lam Phu', 'Khon Kaen', 'Udon Thani', 'Loei', 'Nong Khai', 'Maha Sarakham', 'Roi Et', 'Kalasin',
            'Sakon Nakhon', 'Nakhon Phanom', 'Mukdahan', 'Chiang Mai', 'Lamphun', 'Lampang', 'Uttaradit', 'Phrae', 'Nan',
            'Phayao', 'Chiang Rai', 'Mae Hong Son', 'Tak', 'Ratchaburi', 'Kanchanaburi', 'Phetchaburi', 'Prachuap Khiri Khan',
            'Nakhon Si Thammarat', 'Krabi', 'Phangnga', 'Phuket', 'Surat Thani', 'Ranong', 'Chumphon', 'Songkhla', 'Satun',
            'Trang', 'Phatthalung', 'Pattani', 'Yala', 'Narathiwat'
        ]],
        multi=False,
        placeholder="Select a city"
    ),
    dcc.Graph(id='live-update-graph'),
    dcc.Interval(
        id='interval-component',
        interval=int(os.getenv('UPDATE_INTERVAL', 5000)),  # in milliseconds
        n_intervals=0
    )
])

# Configuration
weather_db = os.getenv('WEATHER_DB', 'postgres')
username = os.getenv('DB_USERNAME', 'postgres')
password = os.getenv('DB_PASSWORD', 'admin')
host = os.getenv('DB_HOST', 'localhost')
database_url = f'postgresql+psycopg2://{username}:{password}@{host}/{weather_db}'
# Create SQLAlchemy engine
engine = sqlalchemy.create_engine(database_url)

@app.callback(Output('live-update-graph', 'figure'),
              [Input('interval-component', 'n_intervals'),
               Input('city-dropdown', 'value')])
def update_graph_live(n, selected_city):
    try:
        # Use SQLAlchemy engine to read data into pandas DataFrame
        query = "SELECT * FROM weather_data ORDER BY timestamp DESC LIMIT 43200"
        df = pd.read_sql_query(query, engine)

        if df.empty:
            return {}
        # Convert Unix timestamp to datetime
        bangkok_tz = pytz.timezone('Asia/Bangkok')
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s' ).dt.tz_localize('UTC').dt.tz_convert(bangkok_tz) 

        # Filter by selected city
        if selected_city:
            df = df[df['city'] == selected_city]
            
        fig = px.scatter(df, x='timestamp', y='temperature', color='city')
        fig.update_layout(
            yaxis_title='Temperature (°C)'
            ,xaxis_title='Time (seconds)'
        )
        return fig
    except Exception as e:
        return {}

if __name__ == '__main__':
    app.run_server(debug=True)
