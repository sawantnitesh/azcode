import datetime
import logging
import azure.functions as func
import time
import pandas as pd
import pytz

from .util import UTIL
from .azureutil import AZUREUTIL
from .trade import TRADE

def main(mytimer: func.TimerRequest) -> None:

    if mytimer.past_due:
        logging.info('The timer is past due!')
    
    stocks_input = "stocks.csv"
    
    AZUREUTIL.get_file(stocks_input, "meta")
    df = pd.read_csv("/tmp/" + stocks_input, header=None)
    all_stocks = df.values.tolist()
    logging.info("Save Model : stocks_all.csv loaded from azure container:meta..")

    smartAPI = UTIL.getSmartAPI()

    todays_date = datetime.datetime.now()
    current_hour = todays_date.astimezone(pytz.timezone("Asia/Calcutta")).hour
    current_minute = todays_date.astimezone(pytz.timezone("Asia/Calcutta")).minute
    fundBalance = None
    fund_balance_file_name = "fund_balance.txt"
    if (current_hour == 9 and current_minute < 30) :
        fundBalance = smartAPI.rmsLimit()['data']['net']
        with open('/tmp/' + fund_balance_file_name, 'w') as f:
            f.write(fundBalance)
        AZUREUTIL.save_file(fund_balance_file_name, "meta", True)
        logging.info("Fund Balance loaded from Angel. Saved to Azure Blob. " + fundBalance)
    else : 
        AZUREUTIL.get_file(fund_balance_file_name, "meta")
        fundBalance = open("/tmp/" + fund_balance_file_name, "r").read().strip()
        logging.info("Fund Balance loaded from Azure Blob. " + fundBalance)
    
    all_stocks_historical_data = {}
    for i, stock in enumerate(all_stocks):
        try :
            #logging.info("Trade : token >> " + str(i) + " >> " + str(stock[0]) + "_" + str(stock[1]))
            if (i + 1) % 3 == 0:
                time.sleep(1.1)

            stock_data = UTIL.fetch_historical_data(smartAPI, stock[0], stock[1], 7, "FIFTEEN_MINUTE")

            all_stocks_historical_data[stock[0]] = stock_data

        except Exception as se:
            logging.exception("Error : Fetch_____" + str(se) + " __ " + str(stock[0]) + "_" + str(stock[1]))
    
    trades = TRADE.find_trades(smartAPI, all_stocks_historical_data)
    UTIL.execute_trades(smartAPI, trades, fundBalance)

    if len(trades) > 0 :
        trades_csv_text = "\n".join(trades)
        file_name = datetime.datetime.now().strftime('%y%m%d%H%M') + '.csv'
        with open('/tmp/' + file_name, 'w') as f:
            f.write(trades_csv_text)
    
        AZUREUTIL.save_file(file_name, "trades")
    
    logging.info('Fetch : Analyze : Trade : Complete !!')
