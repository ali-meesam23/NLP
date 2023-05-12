import json
import requests
from datetime import datetime
from tqdm import tqdm
import pandas as pd
import flair
import sys
import time

global keys
global sentiment_model

sentiment_model = flair.models.TextClassifier.load('en-sentiment')

with open('keys.json','r') as f:
    data = f.read()
    f.close()

keys = json.loads(data)

summary_stamp = {}
trend_df_main = pd.DataFrame()
def sentiment(tweet):
    sentence = flair.data.Sentence(tweet)
    sentiment_model.predict(sentence)
    return sentence.labels[0].value, sentence.labels[0].score

def trending_stream(since=None,_max=None):
    
    
    
    
    url = "https://api.stocktwits.com/api/2/streams/trending.json"
    headers= {
        'Authorization': f'OAuth {keys["access_token"]}'
    }
    if since:
        params = {
            'since':since
        }
    elif _max:
        params = {
            'max':_max
        }
    
    if since or _max:
        resp = requests.get(url,headers=headers,params=params)
    else:
        resp = requests.get(url,headers=headers)
        
    if resp.status_code==200:
        data = resp.json()
        return data, resp.status_code
    else:
        return 'Error', resp.status_code


def get_sentiment_tag_stats(tick_df):
    bull_count = 0
    bear_count = 0
    neutral_count = 0

    for i in range(len(tick_df)):
        usr_sentiment = tick_df['usr_sentiment'].iloc[i]
        if str(usr_sentiment)=='nan':
            neutral_count+=1
            #flair_sentiment = ast.literal_eval(tick_df.flair_sentiment.iloc[i])
            #if flair_sentiment[0]=='NEGATIVE' and flair_sentiment[1]>0.8:
                #bear_count+=1
            #elif flair_sentiment[0]=='POSITIVE' and flair_sentiment[1]>0.8:
                #bull_count+=1
            #else:
                #neutral_count+=1
        elif str(usr_sentiment)=='Bullish':
            bull_count+=1
        elif str(usr_sentiment)=='Bearish':
            bear_count+=1

    sentiment_tag = ''
    if bull_count>bear_count:
        if bull_count> neutral_count:
            sentiment_tag = 'BULL'
        else:
            sentiment_tag = 'NEUTRAL'
    elif bear_count>bull_count:
        if bear_count>neutral_count:
            sentiment_tag = "BEAR"
        else:
            sentiment_tag = 'NEUTRAL'
    return bull_count, bear_count, neutral_count, sentiment_tag


minutes_pause = 1
since = None
scrape_start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# for i in range(60):
result = trending_stream(since)
if result[1]!=200:
    print(result)
elif result[1]==200:
    data = result[0]
elif result[1] in [400,401,429]:
    sys.exit()
else:
    print("Error while requesting data...")
    sys.exit()

# GET QUERIES

query_result = []
for item in tqdm(data['messages']):
    _id = item['id']
    # CREATION
    created_at = datetime.strptime(item['created_at'],"%Y-%m-%dT%H:%M:%SZ")

    # SYMBOLS
    post_symbols = [t['symbol'] for t in item['symbols']]

    # BODY
    body = item['body']

    # FLAIR SENTIMENT
    flair_sentiment = sentiment(body)

    # USER SENTIMENT
    twt_usr_sentiment = "" if not item['entities']['sentiment'] else item['entities']['sentiment']['basic']

    query_result.append([_id, created_at, post_symbols, body, twt_usr_sentiment, flair_sentiment])

# DATAFRAME
df = pd.DataFrame(query_result,columns=['id','created_at','symbols','text','usr_sentiment','flair_sentiment'])
df.set_index('id',inplace=True)


# SINCE
since = df.sort_values(['created_at'],ascending=False).iloc[0].name

# APPENDING TO MAIN DF
trend_df_main = trend_df_main.append(df)
# SAVE LOCALLY
# trend_df_main.to_csv(f'Trend_Twits-{scrape_start_time}.csv')

# GET TICKERS
tickers = []
for tick_list in trend_df_main.symbols.to_list():
    tickers+=tick_list

tickers = list(set(tickers))

summary_df = pd.DataFrame(columns=['symbol','rank','sentiment','bull_count','bear_count','neutral_count'])
for tick in tickers:
    tick_df = trend_df_main[trend_df_main.symbols.apply(lambda x: (tick in x))]
    bull_count,bear_count,neutral_count,sentiment_tag = get_sentiment_tag_stats(tick_df)
    temp_df = pd.DataFrame([tick, len(tick_df),sentiment_tag,bull_count,bear_count,neutral_count]).T
    temp_df.columns = ['symbol','rank','sentiment','bull_count','bear_count','neutral_count']
    summary_df = summary_df.append(temp_df,ignore_index=True)

top_rank_tickers = summary_df.sort_values('rank',ascending=False).iloc[:10]
top_rank_tickers.set_index('symbol',inplace=True)
top_bull_tickers = summary_df[summary_df['sentiment']=='BULL'].sort_values('bull_count',ascending=False).iloc[:10]
top_bull_tickers.set_index('symbol',inplace=True)
top_bear_tickers = summary_df[summary_df['sentiment']=='BEAR'].sort_values('bear_count',ascending=False).iloc[:10]
top_bear_tickers.set_index('symbol',inplace=True)


print("TOP RANKINGS:",end=" ")
print(*top_rank_tickers.index.to_list())
print('\n')
print("BULLS: ", end=" ")
print(*top_bull_tickers.index.to_list())
print('\n')
print('BEARS: ',end=" ")
print(*top_bear_tickers.index.to_list())

print("**************************TOP **************************")
print(top_rank_tickers.iloc[:3])
print()
print("**************************BULL**************************")
print(top_bull_tickers.iloc[:3])
print()
print("**************************BEAR**************************")
print(top_bear_tickers.iloc[:3])


# print(f"Total Trending Tickers: {len(tickers)}")
# print("*"*50)

# trend_summary = {}
# for tick in tickers:
#     tick_df = df[df.symbols.apply(lambda x: (tick in x))]
#     print(f"{tick}: \t{len(tick_df)} : {set(tick_df.usr_sentiment.to_list())}")
#     trend_summary[tick] = (len(tick_df),set(tick_df.usr_sentiment.to_list()))
# time_stamping = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# summary_stamp[time_stamping] = len(tickers),trend_summary

# time.sleep(10*minutes_pause)


    