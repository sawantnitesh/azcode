import logging
import azure.functions as func
import time
import pandas as pd
import numpy as np
import pytz
from datetime import datetime
import os
import json
from urllib import request

from .util import UTIL
from .azureutil import AZUREUTIL
from .trade import TRADE

def main(mytimer: func.TimerRequest) -> None:

    if mytimer.past_due:
        logging.info('The timer is past due!')
    
    UTIL.reset_log_lines()
    UTIL.reset_prices()
    UTIL.append_log_line("_________________________________________________________________", True)
    UTIL.append_log_line(datetime.now(pytz.timezone("Asia/Calcutta")).strftime('%Y-%m-%d %H:%M:%S') + " AlgoTrade : Start __--..>>~~^^*****>>>>>>^^^^^^^^^^", True)
    UTIL.append_log_line("_________________________________________________________________", True)

    smartAPI = UTIL.getSmartAPI()

    todays_date = datetime.now()
    current_hour = todays_date.astimezone(pytz.timezone("Asia/Calcutta")).hour
    current_minute = todays_date.astimezone(pytz.timezone("Asia/Calcutta")).minute
    
    if (current_hour == 9 and current_minute < 7) :
        
        UTIL.save_meta(smartAPI)
        
        data = request.urlopen('https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json').read()
        allscrips = json.loads(data)
        df = pd.DataFrame(allscrips, index=None)
        df = df.loc[(df['symbol'].str.contains('NIFTY')) & (df['name'] == 'NIFTY') & (df['instrumenttype'] == 'OPTIDX')]
        df['strike'] = df['strike'].astype(float).div(100).astype(int)
        df = df[df['strike'] % 100 == 0]
        df['expiry'] = pd.to_datetime(df['expiry'], format='%d%b%Y')
        df = df[df['expiry'] >= datetime.today()]
        df = df.sort_values(by=['strike','symbol'], ascending=True)
        expiry_dates = df['expiry'].unique()
        upcoming_expiry_date = min(expiry_dates)
        upcoming_expiry_date_str = datetime.strftime(upcoming_expiry_date, '%d%b%Y').upper()
        df = df.loc[(df['expiry'] == upcoming_expiry_date_str)]
        df = df[["token", "symbol", "strike"]]
        df.to_csv('/tmp/nifty_strikes.csv', index=False)
        AZUREUTIL.save_file('nifty_strikes.csv', "meta", True)
    
    trade_time_flag = (current_hour == 9 and current_minute > 29) or (current_hour >= 10 and current_hour <= 14) or (current_hour == 15 and current_minute < 15)
    
    if trade_time_flag :
        TRADE.instant_capture(smartAPI)

    UTIL.save_logs(smartAPI)
    
    logging.info('Fetch : Analyze : Trade : Complete !!!!!!!!!!!!!!!!!!')
