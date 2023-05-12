############## IMPORTS ##############
import time
import pandas as pd
import os
import requests
import json
from datetime import datetime, timedelta
from tqdm import tqdm
import sys
import ast
import pandas_datareader.data as web
import matplotlib.pyplot as plt

plt.style.use('dark_background')
# %matplotlib inline

############## HELPER FUNCTIONS ##############
def stream_suggested(since=None,_max=None):
    #AUTH
    with open('keys.json','r') as f:
        data = f.read()
        f.close()
    keys = json.loads(data)
    #URL
    url = "https://api.stocktwits.com/api/2/streams/suggested.json"
    #HEADERS
    headers= {
        'Authorization': f'OAuth {keys["access_token"]}'
    }

    #PARAMS
    params = {}
    if since:
        params['since'] = since
    elif _max:
        params['max'] = _max
    if len(params.keys())>0:
        resp = requests.get(url,headers=headers,params=params)
    else:
        resp = requests.get(url,headers=headers)
        
    if resp.status_code==200:
        data = resp.json()
        return data, resp.status_code
    else:
        return 'Error', resp.status_code


def twit_usr_sentiment(twit):
    _sentiment = twit['sentiment']
    if _sentiment:
        _sentiment = _sentiment['basic']
    else:
        _sentiment = str(_sentiment)
    return _sentiment


def tickers_list(items):
    tickers = ''
    if type(items)==list:
        for x in items:
            tickers+=x['symbol']+","
        tickers = tickers.strip(",")
    else:
        tickers = ''
    return tickers


# TODAY'S DATE FOR ANALYSIS
_date = datetime.now().date().strftime("%Y-%m-%d")

# CHECK CSV FILE AND GET LAST ENTRY ID ELSE CREATE NEW DF
CSV_FILE = f'Suggested_Stream-{_date}.csv'
if os.path.exists(CSV_FILE):
    df = pd.read_csv(CSV_FILE,index_col=0)
    df.symbols = df.symbols.apply(lambda x: ast.literal_eval(x) if str(x)!="nan" else "")
    _max = int(df.id.min())
    # GET THE EARLIEST CREATED AT TIME
    current_date = datetime.strptime(df.created_at.min(),'%Y-%m-%d %H:%M:%S')
else:
    df = pd.DataFrame()
    _max = None
    current_date = datetime.now()

# PATH TO DOWNLOAD OHLC TICKERS
TICKER_PATH = os.path.join("OHLC",_date)
if not os.path.exists(TICKER_PATH):
    os.mkdir(TICKER_PATH)


# INITIALIZE
req_counter = 0
time_elapsed = 0 #seconds
empty_query = 0
start = datetime.now()

# WHILE TWITS ARE FROM THE SAME DAY ->>LOOP
while datetime.strptime(_date,'%Y-%m-%d') < current_date:
    # COUNTER
    if req_counter>=300:
        print(f"{req_counter} requests in the last {time_elapsed}")
        print()
        break
        
    _result = stream_suggested(_max=_max)
    # COUNTER
    req_counter+=1
    
    # TIME PASSED & COUNTER ADJUSTMENT
    time_elapsed = (datetime.now()-start).total_seconds()
    if time_elapsed%10==0:
        req_counter-=1
    
    if _result[1]==200:
        result = _result[0]

        # GOING BACK IN TIME
        _max = result['cursor']['max']

        new_df = pd.DataFrame()
        for msg in result['messages']:
            new_df = new_df.append(msg,ignore_index=True)

        # DF CLEANUP:
        # 1. CREATED AT DATE
        new_df.created_at = new_df.created_at.apply(lambda x: datetime.strptime(x,"%Y-%m-%dT%H:%M:%SZ"))
        # 2. SENTIMENT
        new_df['sentiment'] = new_df.entities.apply(twit_usr_sentiment)
        # 3. SYMBOL STRING
        new_df['tickers'] = new_df.symbols.apply(tickers_list)
        # 4. EARLIEST DATE
        current_date = new_df.created_at.min()
        if len(new_df)>0:
            df = df.append(new_df,ignore_index=True)
            df = df[~df.duplicated(subset=['id'],keep='last')]
            df.to_csv(f"Suggested_Stream-{_date}.csv")
            print(f"Twits_Count:{len(df)}\tCounter:{req_counter}",end="\r")
            empty_query = 0
        else:
            print(f"Empty Query Returned: {empty_query}",_result)
            empty_query+=1
            if empty_query>3:
                break
    else:
        print(f"ERROR: {result}",end="\r")
        break
print()
# GET TICKERS LOCALLY - DATA_DICT
DOWNLOAD = input("Enter DataDict Style (INTERNET, LOCAL): ") or 'INTERNET'

if DOWNLOAD == "LOCAL":
    data_dict = {}
    for i in tqdm(os.listdir('OHLC')):
        ticker = i.split("-")[1].split(".")[0]
        data_dict[ticker] = pd.read_csv(os.path.join(TICKER_PATH,i))
    all_tickers = list(data_dict.keys())

else:
    # GET TICKERS FROM DF
    get_tickers = ''
    for txt in df.tickers:
        if type(txt)==str:
            if txt!="":
                get_tickers+=txt+","
    get_tickers = get_tickers.rstrip(",")
    all_tickers = get_tickers.split(",")

    # DOWNLOAD TICKERS OHLC DATA
    DAYS_BACK = 30
    _start_date = (datetime.today().date())-timedelta(days=DAYS_BACK)

    unique_tickers = list(set(all_tickers))
    BAD_TICKERS = []
    data_dict = {}
    for i,ticker in enumerate(unique_tickers):
        try:
            print_statement = f"Downloading:{ticker}   COUNT:{i}   BAD_TICKS:{len(BAD_TICKERS)}"
            max_len = 60
            if len(print_statement)< max_len:
                print_statement+=" "*(max_len-len(print_statement))
            print(print_statement,end='\r')
            data_dict[ticker] = web.DataReader(ticker,'yahoo',start=_start_date)
            _tick_path = os.path.join(TICKER_PATH,f"{ticker}.csv")
            data_dict[ticker].to_csv(_tick_path)
        except:
            BAD_TICKERS.append(ticker)
print()
print("Total Tickers:", len(data_dict.keys()))

dict_len = [len(data_dict[t]) for t in data_dict]
dict_len = list(set(dict_len))

close_df = pd.DataFrame()
pct_df = pd.DataFrame()
for ticker in data_dict:
    tick_df = data_dict[ticker]
    account_days=20
    if len(tick_df)>=account_days:
        close_df[ticker] = tick_df['Adj Close'].iloc[-account_days:]
        pct_df[ticker] =(tick_df['Adj Close'].pct_change()).iloc[-account_days:]

(close_df.iloc[-1]/close_df.iloc[0]-1).plot.bar(figsize=(15,5))
plt.draw()

twty_performance = close_df.iloc[-1]/close_df.iloc[0]-1

plt.figure(figsize=(15,5))

plt.hist(twty_performance[(twty_performance<1.0)],
         bins=15,
         alpha=0.5,
         edgecolor='orange', 
         linewidth=5)
plt.draw()

# MENTIONS
ticker_count = {}
for ticker in close_df:
    count = 0
    for t_str in df.tickers.to_list():
        if type(t_str)==str:
            if ticker in t_str:
                count+=1
    ticker_count[ticker] = count

# MENTIONS
ticker_count = {}
for ticker in close_df:
    count = 0
    for t_str in df.tickers.to_list():
        if type(t_str)==str:
            if ticker in t_str:
                count+=1
    ticker_count[ticker] = count

tick_count_arr = pd.Series(ticker_count,index=ticker_count.keys())

tick_count_df = pd.DataFrame(tick_count_arr,columns=['Count'])

tick_count_df['20D_pct'] = round((close_df.iloc[-1]/close_df.iloc[0]-1)*100,2)

### CONDITIONS:
# * 20D pct∆ > µ & **<** µ+σ
# * tickcount > µ **&** < µ+σ

selected_tickers = tick_count_df[
    (tick_count_df['20D_pct']>tick_count_df['20D_pct'].mean())&
    (tick_count_df['20D_pct']<tick_count_df['20D_pct'].mean()+tick_count_df['20D_pct'].std())&
    (tick_count_df.Count>tick_count_df.Count.mean())&
    (tick_count_df.Count<(tick_count_df.Count.mean()+tick_count_df.Count.std()))
]

selected_tickers = selected_tickers.sort_values(by='20D_pct',ascending=False)

def ticker_target(df,P_TARGET=0.9):
    df['Daily_Gain'] = df.Close/df.Open-1
    df['daily_perc'] = df['Adj Close'].pct_change()
    # 80% of the HIGHLOW Perc Range
    df['HighLowRange'] = (df.High/df.Low-1)*0.8

    df.dropna(inplace=True)

    i = 0.001
    x_perc = 0
    p=1

    while p>P_TARGET:
        x_perc+=i
        p = len(df[df.HighLowRange>x_perc])/len(df)

    #print(f"{len(df[df.HighLowRange>x_perc])} out of {len(df)}")

    #print(f"% Target: {round(x_perc*100,2)}%")
    return round(x_perc*100,2)

ticker_target_rate = {}
for ticker in selected_tickers.index.to_list():
    #print("_"*30)
    #print("TICKER:",ticker)
    ticker_target_rate[ticker] = [ticker_target(data_dict[ticker])]
    #print("*"*30)
print("DONE....")

selected_tickers['%_Daily_Target'] = pd.DataFrame(ticker_target_rate).T

selected_tickers['%_Daily_Target'].plot.bar(figsize=(15,5),title='Daily % Target')
plt.draw()

prime_tickers = selected_tickers[selected_tickers['%_Daily_Target']>2]

potential_tickers = selected_tickers[selected_tickers['%_Daily_Target']<=2]

print("*"*50)
print("PRIME TICKERS")
print(prime_tickers)
print("*"*50)
print("POTENTIAL TICKERS")
print(potential_tickers)

plt.show()