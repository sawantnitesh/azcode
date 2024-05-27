import logging
from .smartConnect import SmartConnect
import pyotp
from datetime import datetime
from datetime import timedelta
import pytz
import math
import time
import pysftp
import os
import json
import pandas as pd

from .azureutil import AZUREUTIL

class UTIL(object):

    LOG_LINES = []
    TRADE_BOM_LOG_LINES = []
    TRADE_SETUP_COUNT = 2

    @staticmethod
    def reset_log_lines():
        UTIL.LOG_LINES = []
        UTIL.TRADE_BOM_LOG_LINES = []
    
    @staticmethod
    def append_log_line(log_object, to_append_in_trade_bom_log=False):
        UTIL.LOG_LINES.append(str(log_object))
        if to_append_in_trade_bom_log:
            UTIL.TRADE_BOM_LOG_LINES.append(str(log_object))
    
    @staticmethod
    def getSmartAPI():
        api_key = 'ifWvWAZ3'
        clientId = 'N213065'
        pwd = '4444'
        smartApi = SmartConnect(api_key)
        token = "2EUX45DGRXRAZ7VNQ2FGJTZZQM"
        totp = pyotp.TOTP(token).now()
        smartApi.generateSession(clientId, pwd, totp)
        return smartApi
    
    @staticmethod
    def upload_to_sftp(file_path, remote_path):
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None   
        with pysftp.Connection('31.170.161.108', port=65002, username='u571677883', password='aqpt$dfaadf#&FmEE_x8aaaa1111', cnopts=cnopts) as sftp:
            with sftp.cd('/home/u571677883/domains/tradebom.com/public_html/data'):
                logging.info('uploading to sftp' + file_path)
                sftp.put(file_path, remote_path)
    
    @staticmethod
    def fetch_historical_data(smartAPI, token, symbol, timedelta_days, interval):

        todays_date = datetime.now()
        previous_date = todays_date - timedelta(timedelta_days)
        todays_date = todays_date.astimezone(pytz.timezone("Asia/Calcutta")).strftime("%Y-%m-%d %H:%M")
        previous_date = previous_date.astimezone(pytz.timezone("Asia/Calcutta")).strftime("%Y-%m-%d %H:%M")

        historicParam = {
            "exchange": "NSE",
            "symboltoken": token,
            "interval": interval,
            "fromdate": previous_date,
            "todate": todays_date
        }

        data = smartAPI.getCandleData(historicParam)

        return [[token,symbol,row[0],row[1],row[2],row[3],row[4]] for row in data['data']]  # date, open, high, low, close
    
    @staticmethod
    def save_flat_stocks_data(flat_stocks_data):

        flat_stocks_rows = []
        
        for token, stock_data in flat_stocks_data.items():
            flat_stocks_rows.extend(stock_data)

        df_all = pd.DataFrame(flat_stocks_rows, index=None, columns=['token','symbol', 'date', 'open', 'high', 'low', 'close'])
        df_all['Sr'] = range(1, len(df_all)+1)

        df_all.to_csv('/tmp/flat_stocks_data.csv', index=False)
        UTIL.upload_to_sftp('/tmp/flat_stocks_data.csv', 'flat_stocks_data.csv')
        AZUREUTIL.save_file('flat_stocks_data.csv', "analysis", True)
        
        logging.info("Flat Stocks Data Saved____________________:ExpiryDate=")
