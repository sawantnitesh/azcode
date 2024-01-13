import datetime
import logging
import azure.functions as func
import time
import pandas as pd
import pytz
from datetime import datetime
import os

from .util import UTIL
from .azureutil import AZUREUTIL
from .trade import TRADE

def main(mytimer: func.TimerRequest) -> None:

    if mytimer.past_due:
        logging.info('The timer is past due!')
    
    UTIL.append_log_line("_________________________________________________________________")
    UTIL.append_log_line(str(datetime.now()) + "AlgoTrade : Start __--..>>~~^^*****>>>>>>^^^^^^^^^^")
    UTIL.append_log_line("_________________________________________________________________")

    stocks_input = "stocks.csv"
    
    AZUREUTIL.get_file(stocks_input, "meta")
    df = pd.read_csv("/tmp/" + stocks_input, header=None)
    all_stocks = df.values.tolist()
    UTIL.append_log_line("Save Model : stocks.csv loaded from azure container:meta..")

    smartAPI = UTIL.getSmartAPI()

    todays_date = datetime.now()
    current_hour = todays_date.astimezone(pytz.timezone("Asia/Calcutta")).hour
    current_minute = todays_date.astimezone(pytz.timezone("Asia/Calcutta")).minute
    fundBalance = None
    fund_balance_file_name = "fund_balance.txt"
    if (current_hour == 9 and current_minute < 30) :
        fundBalance = smartAPI.rmsLimit()['data']['net']
        with open('/tmp/' + fund_balance_file_name, 'w') as f:
            f.write(fundBalance)
        AZUREUTIL.save_file(fund_balance_file_name, "meta", True)
        UTIL.append_log_line("Fund Balance loaded from Angel. Saved to Azure Blob. Fund Balance=" + fundBalance)
    else : 
        AZUREUTIL.get_file(fund_balance_file_name, "meta")
        fundBalance = open("/tmp/" + fund_balance_file_name, "r").read().strip()
        UTIL.append_log_line("Fund Balance loaded from Azure Blob. Fund Balance=" + fundBalance)
        os.remove(os.path.join('', '/tmp/' + fund_balance_file_name))
    
    trade_time_flag = (current_hour == 9 and current_minute > 29) or (current_hour >= 10 and current_hour <= 13) or (current_hour == 14 and current_minute < 35)

    if True or trade_time_flag :
        all_stocks_historical_data = {}
        for i, stock in enumerate(all_stocks):
            try :
                #logging.info("Trade : token >> " + str(i) + " >> " + str(stock[0]) + "_" + str(stock[1]))
                if (i + 1) % 3 == 0:
                    time.sleep(1.1)

                stock_data = UTIL.fetch_historical_data(smartAPI, stock[0], stock[1], 7, "FIFTEEN_MINUTE")

                all_stocks_historical_data[stock[0]] = stock_data

            except Exception as se:
                UTIL.append_log_line("Error : Fetch_____" + str(se) + " __ " + str(stock[0]) + "_" + str(stock[1]))
        
        UTIL.append_log_line("Historical Data Loaded...............................")

        trades = TRADE.find_trades(all_stocks_historical_data)
        UTIL.execute_trades(smartAPI, trades, fundBalance)
        
        TRADE.trail_stop_loss(smartAPI)
    
    squareoff_time_flag = (current_hour == 14 and current_minute > 44) or (current_hour == 15)
    if squareoff_time_flag :
        TRADE.square_off_all(smartAPI)
    
    UTIL.append_log_line(str(datetime.now()) + "AlgoTrade : Finish __--..>>~~^^*****>>>>>>^^^^^^^^^^")
    UTIL.append_log_line('Fetch : Analyze : Trade : Complete........................!!')

    #Write Log to Azure
    log_file_name = datetime.now().strftime('%Y%m%d') + '.log'
    log_file_text = None
    if AZUREUTIL.is_blob_exists(log_file_name, "trades") :
        AZUREUTIL.get_file(log_file_name, "trades")
        log_file_text = open("/tmp/" + log_file_name, "r").read().strip()
        log_file_text = log_file_text + '\n\n_________________________________________________________________\n\n'
        log_file_text = log_file_text +  '\n'.join(UTIL.LOG_LINES)
        os.remove(os.path.join('', '/tmp/' + log_file_name))
    else :
        log_file_text = '\n'.join(UTIL.LOG_LINES)
    
    with open('/tmp/' + log_file_name, 'w') as f:
        f.write(log_file_text)
    
    AZUREUTIL.save_file(log_file_name, "trades")

    logging.info('Fetch : Analyze : Trade : Complete !!!!!!!!!!!!!!!!!!')
