import logging
import azure.functions as func
import time
import pandas as pd
import pytz
from datetime import datetime
import os

from .util import UTIL
from .azureutil import AZUREUTIL
from .analyzer import ANALYZER


def main(mytimer: func.TimerRequest) -> None:
    
    if mytimer.past_due:
        logging.info('The timer is past due!')
    
    UTIL.reset_log_lines()
    UTIL.append_log_line("_________________________________________________________________", True)
    UTIL.append_log_line(datetime.now(pytz.timezone("Asia/Calcutta")).strftime('%Y-%m-%d %H:%M:%S') + " AlgoTrade : Start __--..>>~~^^*****>>>>>>^^^^^^^^^^", True)
    UTIL.append_log_line("_________________________________________________________________", True)

    stocks_input = "stocks_NIFTY_500.csv"
    
    AZUREUTIL.get_file(stocks_input, "meta")
    df = pd.read_csv("/tmp/" + stocks_input, header=None)
    all_stocks = df.values.tolist()
    UTIL.append_log_line("stocks_NIFTY_500 loaded from azure container:meta..")

    smartAPI = UTIL.getSmartAPI()

    todays_date = datetime.now()
    current_hour = todays_date.astimezone(pytz.timezone("Asia/Calcutta")).hour
    current_minute = todays_date.astimezone(pytz.timezone("Asia/Calcutta")).minute
    
    all_stocks_historical_data = {}
    token_symbol_map = {}

    start = datetime.now()
    for i, stock in enumerate(all_stocks):
        try :
            #logging.info("Trade : token >> " + str(i) + " >> " + str(stock[0]) + "_" + str(stock[1]))
            if (i + 1) % 3 == 0:
                time.sleep(0.8)

            stock_data = UTIL.fetch_historical_data(smartAPI, stock[0], stock[1], 184, "ONE_DAY")

            all_stocks_historical_data[stock[0]] = stock_data
            token_symbol_map[stock[0]] = stock[1]

        except Exception as se:
            UTIL.append_log_line("Error : Fetch_____" + str(se) + " __ " + str(stock[0]) + "_" + str(stock[1]))
    
    time_taken = datetime.now() - start
    UTIL.append_log_line("Historical Data Loaded for " + str(len(all_stocks_historical_data)) + " stocks.....................time=" + str(time_taken), True)

    flat_stocks_data = ANALYZER.find_flat_stocks_on_daily_candles(all_stocks_historical_data)

    UTIL.append_log_line("_________________________________________________________________")
    UTIL.append_log_line(datetime.now(pytz.timezone("Asia/Calcutta")).strftime('%Y-%m-%d %H:%M:%S') + " AlgoTrade : Finish __--..>>~~^^*****>>>>>>^^^^^^^^^^", True)
    UTIL.append_log_line("_________________________________________________________________")
    
    if len(flat_stocks_data) > 0:
        UTIL.save_flat_stocks_data(flat_stocks_data)
    
    logging.info('Fetch : Analyze : Trade : Complete !!!!!!!!!!!!!!!!!!')

