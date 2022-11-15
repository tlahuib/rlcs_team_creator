import requests as req
import json 

response = req.get('https://zsr.octane.gg/events', params={'name': 'RLCS'})

with open('test_outputs/api_response.json', 'w') as f:
    json.dump(response.json(), f, indent=4)