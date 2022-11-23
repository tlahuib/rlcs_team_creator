from copy import deepcopy
from os import getenv
import requests as req

import pandas as pd
import sqlalchemy as db
from dotenv import load_dotenv

# import octane_api as oapi

load_dotenv()

# DB connection
cnx = db.create_engine(f"postgresql://{getenv('USER')}:{getenv('PASS')}@{getenv('HOST')}:{getenv('PORT')}/{getenv('DB')}")

# General page loop
def load_pages(url, key_name, format_function, per_page=100, params={}):
    print(f'Loading data into db for {url}...')
    _params = deepcopy(params)
    _params['perPage'] = per_page
    page = 1

    while True:
        _params['page'] = page
        response = req.get(url, params=_params).json()
        format_function(pd.DataFrame(response[key_name]))
        if response['pageSize'] < per_page: break
        page += 1


# Data-loading functions
def load_events(df):
    # Event data
    events = df.copy()
    events['startDate'] = pd.to_datetime(events['startDate'])
    events['endDate'] = pd.to_datetime(events['endDate'])
    events = pd.concat([events, events['prize'].apply(pd.Series)], axis=1)
    events.rename(
        columns={
            '_id': 'id',
            'startDate': 'start_date',
            'endDate': 'end_date',
            'amount': 'prize_amount',
            'currency': 'prize_currency'
            },
        inplace=True)

    # Parse stages data
    stages = events[['id', 'stages']].rename(columns={'id': 'event_id'})
    stages = stages.explode('stages')
    stages = pd.concat([stages['event_id'], stages['stages'].apply(pd.Series)], axis=1)
    stages['startDate'] = pd.to_datetime(stages['startDate'])
    stages['endDate'] = pd.to_datetime(stages['endDate'])
    stages = pd.concat([stages, stages['prize'].apply(pd.Series)], axis=1)
    if 'location' in set(stages.columns):
        stages = pd.concat([stages, stages['location'].apply(pd.Series)], axis=1)
    stages.rename(
        columns={
            '_id': 'id',
            'startDate': 'start_date',
            'endDate': 'end_date',
            'amount': 'prize_amount',
            'currency': 'prize_currency'
        },
        inplace=True)
    

    # Clean columns
    events_drop_cols = ['stages', 'prize', 0]
    for col in events_drop_cols:
        if col in set(events.columns):
            events.drop(columns=[col], inplace=True)

    stages_drop_cols = ['substages', 'location', 'prize', 0]
    for col in stages_drop_cols:
        if col in set(stages.columns):
            stages.drop(columns=[col], inplace=True)

    # Upload to db
    events.to_sql(name='events', con=cnx, schema='rocket_league', if_exists='append', index=False, method='multi')
    stages.to_sql(name='events_stages', con=cnx, schema='rocket_league', if_exists='append', index=False, method='multi')
    print(f'\tLoaded {events.__len__()} rows to events and {stages.__len__()} rows to stages.')

    return events, stages


def load_matches():
    matches = pd.read_json('test/outputs/api_matches.json')

    return matches


if __name__ == '__main__':
    # Load events
    load_pages('https://zsr.octane.gg/events', 'events', load_events)

    # matches = load_matches()
    # print(matches)
    # print(matches.info())