from copy import deepcopy

import requests as req


def unpaginate_api_calls(url, params, key_name, per_page=100):
    _params = deepcopy(params)
    _params['perPage'] = per_page
    results = []
    page = 1

    while True:
        print(f"\t{page}")
        _params['page'] = page
        response = req.get(url, params=_params).json()
        results += response[key_name]
        if response['pageSize'] < per_page: break
        page += 1

    return results


def get_events(**kwargs):
    print('Starting events dowload...')
    return unpaginate_api_calls('https://zsr.octane.gg/events', kwargs, 'events')

def get_matches(**kwargs):
    print('Starting matches dowload...')
    return unpaginate_api_calls('https://zsr.octane.gg/matches', kwargs, 'matches')

def get_games(**kwargs):
    print('Starting games dowload...')
    return unpaginate_api_calls('https://zsr.octane.gg/games', kwargs, 'games')

def get_players(**kwargs):
    print('Starting players dowload...')
    return unpaginate_api_calls('https://zsr.octane.gg/players', kwargs, 'players')

def get_teams(**kwargs):
    print('Starting teams dowload...')
    return unpaginate_api_calls('https://zsr.octane.gg/teams', kwargs, 'teams')

def get_active_teams(**kwargs):
    print('Starting active teams dowload...')
    return unpaginate_api_calls('https://zsr.octane.gg/teams/active', kwargs, 'teams')
