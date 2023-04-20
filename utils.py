from os import getenv

import pandas as pd
import sqlalchemy as db
from dotenv import load_dotenv

load_dotenv()

# Connect to database
cnx = db.create_engine(f"postgresql://{getenv('USER')}:{getenv('PASS')}@{getenv('HOST')}:{getenv('PORT')}/{getenv('DB')}")

# Db functions
def df_from_table(table_name: str):
    return pd.read_sql(f'select * from {table_name}', cnx)

# Plotting functions

image_path = 'C:/Users/tabm9/OneDrive - Caissa Analytica/Documents/DS_Portfolio/Projects/Images/RLCS/'

line_color = '#f1f5f9'
font_color = '#f1f5f9'
background_color = '#032137'

layout_dict = dict(
    font_color=font_color,
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    xaxis=dict(gridcolor=line_color, linecolor=line_color,
               zerolinecolor=line_color),
    yaxis=dict(gridcolor=line_color, linecolor=line_color,
               zerolinecolor=line_color),
    legend=dict(x=0, y=1, bgcolor=background_color),
    margin=dict(r=0, l=0, b=0)
)

tablet_args = dict(default_width='700px', default_height='300px')
mobile_args = dict(default_width='400px', default_height='175px')