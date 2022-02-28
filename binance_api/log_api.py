import requests
import json
import time
import sys
import pandas as pd
import calendar
from datetime import datetime, timedelta

def get_unix_ms_from_date(date):
    return int(calendar.timegm(date.timetuple()) * 1000 + date.microsecond/1000)

'''
Fetching trades
With Binance’s API and using the aggTrades endpoint, we can get at most 1,000 trades in one request, 
and if we use start and end parameters, they can be at most one hour apart. After some failures, 
by fetching using time intervals (at some point or another, the liquidity would go crazy and I 
would lose some precious trades), I decided to try the from_id strategy.
The aggTrades endpoint is chosen because it returns the compressed trades. In that way, we won’t lose any precious information.
Get compressed, aggregate trades. Trades that fill at the same time, from the same order, with the same price
 will have the quantity aggregated.
The from_id strategy goes like this: We are going to get the first trade of the starting_date by sending date
 intervals to the endpoint. After that, we will fetch 1,000 trades, starting with the first fetched trade ID. 
 Then, we will check if the last trade happened after our ending_date. If so, we have gone through all the time periods 
 and we can save the results to file. Otherwise, we will update our from_id variable to get the last trade ID and start the loop all over again.
'''


'''
call the api to get a trade id
gap size is the second that we want to go back from now
'''
def get_first_trade_id_from_date_time(symbol, gap_size):
    end_date = datetime.now()
    start_date = end_date - timedelta(seconds=gap_size)
    r = requests.get('https://api.binance.com/api/v3/aggTrades', 
        params = {
            "symbol" : symbol,
            "startTime": get_unix_ms_from_date(start_date),
            "endTime": get_unix_ms_from_date(end_date)
        })
        
    if r.status_code != 200:
        print('somethings wrong!', r.status_code)
        print('sleeping for 10s... will retry')
        time.sleep(10)
        get_first_trade_id_from_date_time(symbol, gap_size)
    
    response = r.json()
    if len(response) > 0:
        return response[0]['a']
    else:
        raise Exception('no trades found')


'''
call the trade api by id 
'''
def get_trades(symbol, from_id):
    r = requests.get("https://api.binance.com/api/v3/aggTrades",
        params = {
            "symbol": symbol,
            "limit": 1000,
            "fromId": from_id
            })

    if r.status_code != 200:
        print('somethings wrong!', r.status_code)
        print('sleeping for 10s... will retry')
        time.sleep(10)

    return r.json()


def trim(df, to_date):
    return df[df['T'] <= get_unix_ms_from_date(to_date)]        
