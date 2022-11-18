from os import getenv

import pandas as pd
import sqlalchemy as db
from dotenv import load_dotenv

import octane_api as oapi

load_dotenv()

# DB connection
cnx = db.create_engine(f"postgresql://{getenv('USER')}:{getenv('PASS')}@{getenv('HOST')}:{getenv('PORT')}/{getenv('DB')}")

# Data-loading functions
def load_events():
    # Event data
    events = pd.DataFrame(oapi.get_events())
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
    events.drop(columns=['stages', 'prize', 0], inplace=True)
    stages.drop(columns=['substages', 'location', 'prize', 0], inplace=True)

    # Upload to db
    events.to_sql(name='events', con=cnx, schema='rocket_league', if_exists='append', index=False, method='multi')
    stages.to_sql(name='events_stages', con=cnx, schema='rocket_league', if_exists='append', index=False, method='multi')

    return events, stages


if __name__ == '__main__':
    events, stages = load_events()
    print(events.info())
    print(stages.info())