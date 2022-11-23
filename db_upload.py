from copy import deepcopy
from os import getenv
import requests as req
import json

import pandas as pd
import sqlalchemy as db
from dotenv import load_dotenv

# import octane_api as oapi

load_dotenv()

# DB connection
cnx = db.create_engine(f"postgresql://{getenv('USER')}:{getenv('PASS')}@{getenv('HOST')}:{getenv('PORT')}/{getenv('DB')}")

# Load colnames dict
with open('api_columns.json') as f:
    api_columns = json.load(f)

# General format function
def format_df(df: pd.DataFrame, df_name: str):
    _df = df.copy()
    col_dict = api_columns[df_name]
    for col in df.columns:
        if col not in set(col_dict.keys()):
            _df.drop(columns=[col], inplace=True)
        else:
            _df.rename(columns={col: col_dict[col].lower()}, inplace=True)
    return _df

# General page loop
def load_pages(url, key_name, format_function, per_page=500, params={}):
    print(f'Loading data into db for {url}...')
    _params = deepcopy(params)
    _params['perPage'] = per_page
    page = 1

    while True:
        _params['page'] = page
        response = req.get(url, params=_params).json()
        format_function(response[key_name])
        if response['pageSize'] < per_page: break
        page += 1


# Data-loading functions
def load_events(_dict):
    events = pd.json_normalize(_dict, sep='_')
    stages = pd.json_normalize(_dict, sep='_', record_path='stages', meta='_id', meta_prefix='event')

    events = format_df(events, 'events')
    stages = format_df(stages, 'stages')

    # Upload to db
    events.to_sql(name='events', con=cnx, schema='rocket_league', if_exists='append', index=False, method='multi')
    stages.to_sql(name='events_stages', con=cnx, schema='rocket_league', if_exists='append', index=False, method='multi')
    print(f'\tLoaded {events.__len__()} rows to events and {stages.__len__()} rows to stages.')

    return events, stages


def load_matches(_dict):
    matches = pd.json_normalize(_dict, sep='_')
    
    matches = format_df(matches, 'matches')

    # Upload to db
    matches.to_sql(name='matches', con=cnx, schema='rocket_league', if_exists='append', index=False, method='multi')
    print(f'\tLoaded {matches.__len__()} rows to matches.')

    return matches


if __name__ == '__main__':
    # # Load events
    # load_pages('https://zsr.octane.gg/events', 'events', load_events)

    # Load matches
    load_pages('https://zsr.octane.gg/matches', 'matches', load_matches)