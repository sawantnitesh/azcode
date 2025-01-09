import logging
from .smartConnect import SmartConnect
import pyotp
from datetime import datetime
from datetime import timedelta
import pytz
import math
import time
import pysftp
import pandas as pd

from .azureutil import AZUREUTIL

class UTIL(object):

    LOG_LINES = []
    
    @staticmethod
    def reset_log_lines():
        UTIL.LOG_LINES = []
    
    @staticmethod
    def append_log_line(log_object):
        UTIL.LOG_LINES.append(str(log_object))
    
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
    def save_historical_data(smartAPI, all_stocks_historical_data, token_symbol_map):

        trimmed_historical_data = []
        for token, stock_data in all_stocks_historical_data.items():

            symbol = token_symbol_map[token]
            latest_day_data = []
            latest_day = None
            previous_day_close = -1
            todays_date = datetime.today().strftime('%Y-%m-%d') #2021-11-16

            for stock in reversed(stock_data):

                print(stock[2])
                
                if latest_day is None:
                    latest_day = stock[2][0:10] #2021-11-16T09:15:00+05:30 >> 2021-11-16

                if todays_date != latest_day : 
                    continue

                if stock[2].startswith(latest_day):
                    latest_day_data.append([symbol, stock[2], stock[6], 0])
                    previous_day_close = stock[6]
                else:
                    i = 0
                    while i < 4:
                        i += 1
                        try:
                            previous_day_close = smartAPI.ltpData("NSE", symbol, token)['data']['close']
                            break
                        except Exception as se:
                            UTIL.append_log_line("Error : error getLtpData_____attempt number=" + str(i) + "________" + str(se) + " __ " + str(token))
                            time.sleep(1)
                            continue
                    
                    time.sleep(0.2)
                    break
            
            for price_data in latest_day_data:
                price_data[3] = round(((price_data[2] - previous_day_close)/previous_day_close)*100,2)

            latest_day_data.reverse()
            trimmed_historical_data.extend(latest_day_data)
        
        df = pd.DataFrame(trimmed_historical_data, columns=['Name', 'Date', 'Price', 'Percentchange'])
        df.to_csv('/tmp/nifty_prices.csv', index=False)
        
        UTIL.upload_to_sftp('/tmp/nifty_prices.csv', 'nifty_prices.csv')
        AZUREUTIL.save_file('nifty_prices.csv', "trades", True)
    