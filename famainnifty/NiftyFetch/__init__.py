import azure.functions as func
import logging
import azure.functions as func
import time
import pandas as pd
import numpy as np
import pytz
from datetime import datetime, timedelta
import os

from .util import UTIL
from .azureutil import AZUREUTIL

def main(mytimer: func.TimerRequest) -> None:
    
    if mytimer.past_due:
        logging.info('The timer is past due!')
    
    UTIL.reset_log_lines()
    UTIL.append_log_line("_________________________________________________________________")
    UTIL.append_log_line(datetime.now(pytz.timezone("Asia/Calcutta")).strftime('%Y-%m-%d %H:%M:%S') + " Nifty data fetch : Start __--..>>~~^^*****>>>>>>^^^^^^^^^^")
    UTIL.append_log_line("_________________________________________________________________")

    smartAPI = UTIL.getSmartAPI()
    
    save_nifty_data(smartAPI)
    
    save_oi_data(smartAPI)
    
def save_nifty_data(smartAPI) :
    stocks_input = "stocks_NIFTY_500.csv"
    
    AZUREUTIL.get_file(stocks_input, "meta")
    df = pd.read_csv("/tmp/" + stocks_input, header=None)
    all_stocks = df.values.tolist()
    UTIL.append_log_line(stocks_input + " loaded from azure container:meta..")
    
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

def save_oi_data(smartAPI):
    
    nifty_strikes_csv_name = "nifty_strikes.csv"
    AZUREUTIL.get_file(nifty_strikes_csv_name, "meta")
    df = pd.read_csv("/tmp/" + nifty_strikes_csv_name)

    os.remove(os.path.join('', '/tmp/' + nifty_strikes_csv_name))

    df['strike'] = df['strike'].astype(int)
    df = df.sort_values(by=['strike','symbol'], ascending=True)

    main_df = pd.DataFrame()

    for index, row in df.iterrows():
        symbol = row['symbol']
        token = str(row['token'])

        todays_date = datetime.now()
        todays_date = todays_date.astimezone(pytz.timezone("Asia/Calcutta"))
        from_date_str = todays_date.strftime("%Y-%m-%d 09:00")
        to_date_str = todays_date.strftime("%Y-%m-%d 16:00")

        historicOIParam={
            "exchange": "NFO",
            "symboltoken": token,
            "interval": "ONE_MINUTE",
            "fromdate": from_date_str,
            "todate": to_date_str
        }

        logging.info('index----------------' + str(index) + '------------------------------------' + symbol)
        if (index + 1) % 3 == 0:
            time.sleep(1)
        
        oi_history_data = smartAPI.getOIData(historicOIParam)
        df_oi = pd.DataFrame(oi_history_data['data'], index=None)
        df_oi['symbol'] = symbol

        main_df = main_df._append(df_oi,ignore_index=True)

    oi_file_name = datetime.now().strftime("%Y%m%d") + "_oihistory.csv"

    main_df.to_csv('/tmp/' + oi_file_name , index=False)
    AZUREUTIL.save_file(oi_file_name, "trades", True)
