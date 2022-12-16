from copy import deepcopy
from os import getenv
import requests as req
import json

import pandas as pd
import sqlalchemy as db
from dotenv import load_dotenv

load_dotenv()

# DB connection
cnx = db.create_engine(
    f"postgresql://{getenv('USER')}:{getenv('PASS')}@{getenv('HOST')}:{getenv('PORT')}/{getenv('DB')}")

# Load colnames dict
with open('db_setup/api_columns.json') as f:
    api_columns = json.load(f)

table_dates = {'events': 'start_date', 'matches': 'date', 'games': 'date'}


# General Functions --------------------------------------------------------------------------
# General page loop
def load_pages(url:str , key_name: str, format_function, params={'perPage': 500}):
    print(f'Loading data into db for {url}...')
    _params = deepcopy(params)
    if key_name in {'events', 'matches', 'games'}:
        last_date = cnx.execute(f'select max(updated_at) from rocket_league.{key_name}').fetchone()[0]
        _params['after'] = str(last_date.date())
    page = 1

    while True:
        _params['page'] = page
        response = req.get(url, params=_params).json()
        n_added = format_function(response[key_name])
        print(page, _params['perPage'], response['pageSize'], n_added)
        if response['pageSize'] < _params['perPage']:
            break
        page += 1


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


# Separate new records from existing ones
def get_new_records(api_df: pd.DataFrame, table: str, col: str = 'id'):
    db_df = pd.read_sql(f'select {col} from rocket_league.{table}', cnx)
    loaded_records = set(db_df[col])
    api_records = set(api_df[col])
    new_records = api_records - loaded_records
    return api_df[api_df[col].apply(lambda x: x in new_records)]



# Data-loading functions--------------------------------------------------------------------------
def load_teams(_dict: dict):
    # Format teams data
    teams = pd.json_normalize(_dict, sep='_')

    teams = format_df(teams, 'teams')

    # Check for new teams
    teams = get_new_records(teams, 'teams')


    # Upload to db
    if not teams.empty:
        teams.to_sql(name='teams', con=cnx, schema='rocket_league',
                    if_exists='append', index=False, method='multi')
        print(f'\tLoaded {teams.__len__()} rows to teams.')

    return teams.__len__()


def load_players(_dict):
    # Format players data
    players = pd.json_normalize(_dict, sep='_')

    players = format_df(players, 'players')

    # Format accounts data
    accounts = [item for item in _dict if 'accounts' in set(item.keys())]
    accounts = pd.json_normalize(accounts, sep='_', record_path='accounts', meta='_id', meta_prefix='player')

    accounts = format_df(accounts,'players_accounts')
    accounts.dropna(inplace=True)

    # Check for new players
    players = get_new_records(players, 'players')
    accounts = get_new_records(accounts, 'players_accounts', 'platform_id')

    # Upload to db
    if not players.empty:
        players.to_sql(name='players', con=cnx, schema='rocket_league',
                    if_exists='append', index=False, method='multi')

    if not accounts.empty:
        accounts.to_sql(name='players_accounts', con=cnx, schema='rocket_league',
                    if_exists='append', index=False, method='multi')

    print(f'\tLoaded {players.__len__()} rows to players and {accounts.__len__()} to players_accounts.')

    return players.__len__(), accounts.__len__()


def load_events(_dict):
    events = pd.json_normalize(_dict, sep='_')
    stages = pd.json_normalize(_dict, sep='_', record_path='stages', meta='_id', meta_prefix='event')

    events = format_df(events, 'events')
    stages = format_df(stages, 'stages')

    # Check for new events
    events = get_new_records(events, 'events')
    stages = get_new_records(stages, 'events_stages', 'event_id')

    # Upload to db
    if not events.empty:
        events.to_sql(name='events', con=cnx, schema='rocket_league',
                    if_exists='append', index=False, method='multi')

    if not stages.empty:
        stages.to_sql(name='events_stages', con=cnx, schema='rocket_league',
                    if_exists='append', index=False, method='multi')
    
    print(f'\tLoaded {events.__len__()} rows to events and {stages.__len__()} rows to stages.')

    return events.__len__(), stages.__len__()


def load_matches(_dict):
    # Format match data
    matches = pd.json_normalize(_dict, sep='_')

    matches = format_df(matches, 'matches')

    # Format players data
    blue_players = [item for item in _dict if 'blue' in set(item.keys())]
    blue_players = [
        item for item in blue_players if 'players' in set(item['blue'].keys())]
    blue_players = pd.json_normalize(
        blue_players, 
        sep='_', 
        record_path=[['blue', 'players']], 
        meta=[
            '_id', 
            ['event', '_id'], 
            ['stage', '_id'], 
            ['blue', 'team', 'team', '_id']
        ], 
        meta_prefix='match', 
        errors='ignore'
    )
    blue_players['color'] = 'blue'
    blue_players.rename(columns={'matchblue_team_team__id': 'team_id'}, inplace=True)

    orange_players = [item for item in _dict if 'orange' in set(item.keys())]
    orange_players = [
        item for item in orange_players if 'players' in set(item['orange'].keys())]
    orange_players = pd.json_normalize(
        orange_players, 
        sep='_', 
        record_path=[['orange', 'players']],
        meta=[
            '_id', 
            ['event', '_id'], 
            ['stage', '_id'], 
            ['orange', 'team', 'team', '_id']
        ], 
        meta_prefix='match', 
        errors='ignore'
    )
    orange_players['color'] = 'orange'
    orange_players.rename(columns={'matchorange_team_team__id': 'team_id'}, inplace=True)

    players = pd.concat([blue_players, orange_players])
    del blue_players, orange_players
    players = format_df(players, 'matches_players')
    players.drop_duplicates(subset=['match_id', 'id'], inplace=True)

    # Check for new matches
    matches = get_new_records(matches, 'matches')
    players = get_new_records(players, 'matches_players', 'match_id')

    # Upload to db
    if not matches.empty:
        matches.to_sql(name='matches', con=cnx, schema='rocket_league',
                    if_exists='append', index=False, method='multi')

    if not players.empty:
        players.to_sql(name='matches_players', con=cnx, schema='rocket_league',
                    if_exists='append', index=False, method='multi')

    print(f'\tLoaded {matches.__len__()} rows to matches and {players.__len__()} rows to matches_players.')

    return matches.__len__(), players.__len__()


def load_games(_dict):
    # Format match data
    games = pd.json_normalize(_dict, sep='_')

    games = format_df(games, 'games')

    # Format players data
    blue_players = [item for item in _dict if 'blue' in set(item.keys())]
    blue_players = [item for item in blue_players if 'players' in set(item['blue'].keys())]
    blue_players = pd.json_normalize(
        blue_players, 
        sep='_', 
        record_path=[['blue', 'players']], 
        meta=[
            '_id', 
            ['match', 'event', '_id'], 
            ['match', 'stage', '_id'], 
            ['match', '_id'], 
            ['blue', 'team', 'team', '_id']
        ], 
        meta_prefix='game', 
        errors='ignore'
    )
    blue_players['color'] = 'blue'
    blue_players.rename(columns={'gameblue_team_team__id': 'team_id'}, inplace=True)

    orange_players = [item for item in _dict if 'orange' in set(item.keys())]
    orange_players = [item for item in orange_players if 'players' in set(item['orange'].keys())]
    orange_players = pd.json_normalize(
        orange_players, 
        sep='_', 
        record_path=[['orange', 'players']], 
        meta=[
            '_id', 
            ['match', 'event', '_id'], 
            ['match', 'stage', '_id'], 
            ['match', '_id'], 
            ['orange', 'team', 'team', '_id']
        ], 
        meta_prefix='game', 
        errors='ignore'
    )
    orange_players['color'] = 'orange'
    orange_players.rename(columns={'gameorange_team_team__id': 'team_id'}, inplace=True)

    players = pd.concat([blue_players, orange_players])
    del blue_players, orange_players
    players = format_df(players, 'games_players')
    players.drop_duplicates(subset=['game_id', 'id'], inplace=True)

    #  Check for new games
    games = get_new_records(games, 'games')
    players = get_new_records(players, 'games_players', 'game_id')

    # Upload to db
    if not games.empty:
        games.to_sql(name='games', con=cnx, schema='rocket_league',
                    if_exists='append', index=False, method='multi')

    if not players.empty:
        players.to_sql(name='games_players', con=cnx, schema='rocket_league',
                    if_exists='append', index=False, method='multi')

    print(f'\tLoaded {games.__len__()} rows to games and {players.__len__()} rows to games_players.')

    return games.__len__(), players.__len__()


if __name__ == '__main__':
    pass
    # # print(json.dumps(api_columns, indent=4))

    # # Load teams
    # load_pages('https://zsr.octane.gg/teams', 'teams', load_teams)

    # # Load players
    # load_pages('https://zsr.octane.gg/players', 'players', load_players)

    # # Load events
    # load_pages('https://zsr.octane.gg/events', 'events', load_events)

    # # Load matches
    # load_pages('https://zsr.octane.gg/matches', 'matches', load_matches)

    # # Load games
    # load_pages('https://zsr.octane.gg/games', 'games', load_games)

