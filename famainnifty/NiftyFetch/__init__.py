import azure.functions as func
import logging
import azure.functions as func
import time
import pandas as pd
import numpy as np
import pytz
from datetime import datetime

from .util import UTIL
from .azureutil import AZUREUTIL

def main(mytimer: func.TimerRequest) -> None:
    
    if mytimer.past_due:
        logging.info('The timer is past due!')
    
    UTIL.reset_log_lines()
    UTIL.append_log_line("_________________________________________________________________")
    UTIL.append_log_line(datetime.now(pytz.timezone("Asia/Calcutta")).strftime('%Y-%m-%d %H:%M:%S') + " Nifty data fetch : Start __--..>>~~^^*****>>>>>>^^^^^^^^^^")
    UTIL.append_log_line("_________________________________________________________________")

    stocks_input = "stocks_NIFTY_500.csv"
    
    AZUREUTIL.get_file(stocks_input, "meta")
    df = pd.read_csv("/tmp/" + stocks_input, header=None)
    all_stocks = df.values.tolist()
    UTIL.append_log_line(stocks_input + " loaded from azure container:meta..")
    
    smartAPI = UTIL.getSmartAPI()
    
    all_stocks_historical_data = {}
    token_symbol_map = {}
    
    start = datetime.now()
    for i, stock in enumerate(all_stocks):
        try :
            #logging.info("Trade : token >> " + str(i) + " >> " + str(stock[0]) + "_" + str(stock[1]))
            if (i + 1) % 3 == 0:
                time.sleep(0.8)

            stock_data = UTIL.fetch_historical_data(smartAPI, stock[0], stock[1], 30, "FIFTEEN_MINUTE")

            all_stocks_historical_data[stock[0]] = stock_data
            token_symbol_map[stock[0]] = stock[1]

        except Exception as se:
            UTIL.append_log_line("Error : Fetch_____" + str(se) + " __ " + str(stock[0]) + "_" + str(stock[1]))
    
    #Fetch Nifty Historical Data
    time.sleep(1)
    nifty_data = UTIL.fetch_historical_data(smartAPI, "99926000", "NIFTY", 7, "FIFTEEN_MINUTE")
    all_stocks_historical_data["99926000"] = nifty_data
    token_symbol_map["99926000"] = "NIFTY"
    #Fetch BankNifty Historical Data
    banknifty_data = UTIL.fetch_historical_data(smartAPI, "99926009", "BANKNIFTY", 7, "FIFTEEN_MINUTE")
    all_stocks_historical_data["99926009"] = banknifty_data
    token_symbol_map["99926009"] = "BANKNIFTY"
    
    time_taken = datetime.now() - start
    UTIL.append_log_line("Historical Data Loaded for " + str(len(all_stocks_historical_data)) + " stocks.....................time=" + str(time_taken))

    UTIL.save_historical_data(smartAPI, all_stocks_historical_data, token_symbol_map)
    
    logging.info('Nifty fetch data Complete !!!!!!!!!!!!!!!!!!')
    