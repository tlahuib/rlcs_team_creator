import json
from copy import deepcopy

import requests as req


def unpaginate_api_calls(url, params, key_name, per_page=100):
    _params = deepcopy(params)
    _params['perPage'] = per_page
    results = []
    page = 1

    while True:
        _params['page'] = page
        response = req.get(url, params=_params).json()
        results += response[key_name]
        if response['pageSize'] < per_page:
            print(page)
            break
        page += 1

    return results


def get_events(**kwargs):
    return unpaginate_api_calls('https://zsr.octane.gg/events', kwargs, 'events')


if __name__ == '__main__':
    events = get_events(name='RLCS')
    with open('test_outputs/api_response.json', 'w') as f:
        json.dump(events, f, indent=4)
