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
    col_set = set(col_dict.keys())
    
    drop_cols = set(_df.columns) - col_set
    rename_cols = {col: col_dict[col] for col in _df.columns if col in col_set}

    _df.drop(columns=drop_cols, inplace=True)
    _df.rename(columns=rename_cols, inplace=True)
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
    # Format match data
    matches = pd.json_normalize(_dict, sep='_')
    
    matches = format_df(matches, 'matches')

    # Format players data
    blue_players = [item for item in _dict if 'blue' in set(item.keys())]
    blue_players = [item for item in blue_players if 'players' in set(item['blue'].keys())]
    blue_players = pd.json_normalize(blue_players, sep='_', record_path=[['blue', 'players']], meta=['_id', ['event', '_id'], ['stage', '_id'], ['blue', 'team', 'team', '_id']], meta_prefix='match', errors='ignore')
    blue_players['color'] = 'blue'
    
    orange_players = [item for item in _dict if 'orange' in set(item.keys())]
    orange_players = [item for item in orange_players if 'players' in set(item['orange'].keys())]
    orange_players = pd.json_normalize(orange_players, sep='_', record_path=[['orange', 'players']], meta=['_id', ['event', '_id'], ['stage', '_id'], ['orange', 'team', 'team', '_id']], meta_prefix='match', errors='ignore')
    orange_players['color'] = 'orange'

    players = pd.concat([blue_players, orange_players])
    del blue_players, orange_players
    players = format_df(players, 'matches_players')
    players.drop_duplicates(subset=['match_id', 'id'], inplace=True)

    # Upload to db
    matches.to_sql(name='matches', con=cnx, schema='rocket_league', if_exists='append', index=False, method='multi')
    players.to_sql(name='matches_players', con=cnx, schema='rocket_league', if_exists='append', index=False, method='multi')
    print(f'\tLoaded {matches.__len__()} rows to matches and {players.__len__()} rows to matches_players.')

    return matches, players


def load_games(_dict):
    # Format match data
    games = pd.json_normalize(_dict, sep='_')
    
    games = format_df(games, 'games')

    # Format players data
    blue_players = [item for item in _dict if 'blue' in set(item.keys())]
    blue_players = [item for item in blue_players if 'players' in set(item['blue'].keys())]
    blue_players = pd.json_normalize(blue_players, sep='_', record_path=[['blue', 'players']], meta=['_id', ['event', '_id'], ['stage', '_id'], ['match', '_id'], ['blue', 'team', 'team', '_id']], meta_prefix='game', errors='ignore')
    blue_players['color'] = 'blue'
    
    orange_players = [item for item in _dict if 'orange' in set(item.keys())]
    orange_players = [item for item in orange_players if 'players' in set(item['orange'].keys())]
    orange_players = pd.json_normalize(orange_players, sep='_', record_path=[['orange', 'players']], meta=['_id', ['event', '_id'], ['stage', '_id'], ['match', '_id'], ['orange', 'team', 'team', '_id']], meta_prefix='game', errors='ignore')
    orange_players['color'] = 'orange'

    players = pd.concat([blue_players, orange_players])
    del blue_players, orange_players
    players = format_df(players, 'games_players')
    players.drop_duplicates(subset=['game_id', 'id'], inplace=True)

    # Upload to db
    games.to_sql(name='games', con=cnx, schema='rocket_league', if_exists='append', index=False, method='multi')
    players.to_sql(name='games_players', con=cnx, schema='rocket_league', if_exists='append', index=False, method='multi')
    print(f'\tLoaded {games.__len__()} rows to games and {players.__len__()} rows to games_players.')

    return games, players


if __name__ == '__main__':
    # # Load events
    # load_pages('https://zsr.octane.gg/events', 'events', load_events)

    # # Load matches
    # load_pages('https://zsr.octane.gg/matches', 'matches', load_matches)

    # Load games
    load_pages('https://zsr.octane.gg/games', 'games', load_games)

    # with open('test/outputs/api_games.json') as f:
    #     _dict = json.load(f)

    # print(players)
    # print(players.info(verbose=1))
    
    # games.columns.to_frame().to_csv('game_cols.csv')