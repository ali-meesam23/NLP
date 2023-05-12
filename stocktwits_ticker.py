
import json
import requests
from datetime import datetime
from tqdm import tqdm
import pandas as pd
import sys
import time


global keys
def stocks_stream(symbols=['SPY'],since=None,_max=None):
    #AUTH
    with open('keys.json','r') as f:
        data = f.read()
        f.close()
    keys = json.loads(data)
    #URL
    url = "https://api.stocktwits.com/api/2/streams/symbols.json"
    #HEADERS
    headers= {
        'Authorization': f'OAuth {keys["access_token"]}'
    }
    #PARAMS
    params = {
        'symbols':symbols
    }
    if since:
        params['since'] = since
    elif _max:
        params['max'] = _max
    
    resp = requests.get(url,headers=headers,params=params)
        
    if resp.status_code==200:
        data = resp.json()
        return data, resp.status_code
    else:
        return 'Error', resp.status_code
symbols = ['AMC']
max = None
df = pd.DataFrame()

for i in tqdm(range(100)):
    result = stocks_stream(symbols,_max=_max)
    if result[1]!=200:
        print(result)
    elif result[1]==200:
        data = result[0]
    elif result[1] in [400,401,429]:
        sys.exit()
    else:
        print("Error while requesting data...")
        sys.exit()

    if result[1]==200:
        data = result[0]
        _max = result[0]['cursor']['max']

    df = df.append(data['messages'])

print(df)