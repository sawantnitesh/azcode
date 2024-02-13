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
    FUND_BALANCE = None

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
    def get_summary(smartAPI, overall_gain):
        text = datetime.now(pytz.timezone("Asia/Calcutta")).strftime('%d-%b-%Y %H:%M')
        
        positions = smartAPI.position()
        gain = 0
        if positions['data'] :
            for position in positions['data']:
                if position['realised'] :
                    gain = gain + float(position['realised'])
                if position['unrealised'] :
                    gain = gain + float(position['unrealised'])
        
        text = text + "\n" + str(round(gain,2))
        text = text + "\n" + str(round(gain + overall_gain,2))
        return text
    
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
    def execute_trades(smartAPI, trades, fund_balance):

        trade_intraday_cap = (float(fund_balance) / UTIL.TRADE_SETUP_COUNT) * 4
        
        for trade in trades:
            #EmaCrossover,20374,COALINDIA-EQ,SELL,501

            try :
                trade_info = trade.split(',')

                ltpData = smartAPI.ltpData("NSE", trade_info[2], trade_info[1])
                ltp = ltpData['data']['ltp']
                
                ema1_price = float(trade_info[4])
                order_price = math.ceil(ltp + ema1_price)/2

                quantity = math.floor(trade_intraday_cap / ltp)

                if trade_info[1] == "BUY" :
                    target = math.ceil(order_price + order_price*0.02)
                    stoploss_price = math.floor(order_price - order_price*0.01)
                else :
                    target = math.ceil(order_price - order_price*0.02)
                    stoploss_price = math.floor(order_price + order_price*0.01)
                
                squareoff = abs(math.ceil(order_price) - target)
                stoploss = abs(math.floor(order_price) - stoploss_price)
                
                orderParam = {
                    "exchange": "NSE",
                    "symboltoken": trade_info[1],
                    "tradingsymbol": trade_info[2],
                    "variety": "ROBO",
                    "transactiontype": trade_info[3],
                    "ordertype": "LIMIT",
                    "producttype": "BO",
                    "duration": "DAY",
                    "quantity": quantity,
                    "producttype": "INTRADAY",
                    "price":order_price,
                    "squareoff":squareoff,
                    "stoploss":stoploss,
                    "trailingStopLoss":1
                }

                UTIL.append_log_line("orderParam:" + str(orderParam))

                time.sleep(0.5)

                order_output = smartAPI.placeOrder(orderParam)

                UTIL.append_log_line("order_output:" + str(order_output))
            
            except Exception as se:
                UTIL.append_log_line("Error : TradeExecution_____" + str(se) + "_____" + trade)
    
    @staticmethod
    def set_overall_gain():
        overall_gain = 0
        if AZUREUTIL.is_blob_exists('gain.txt', "trades") :
            AZUREUTIL.get_file('gain.txt', 'trades')
            gain_text = open('/tmp/gain.txt', "r").read().strip()
            overall_gain = float(gain_text.split('\n')[2].strip())
            os.remove(os.path.join('', '/tmp/gain.txt'))
        
        with open('/tmp/overall_gain.txt', 'w') as f:
            f.write(str(round(overall_gain,2)))
        AZUREUTIL.save_file('overall_gain.txt', "trades", True)
    

    @staticmethod
    def save_meta(smartAPI):
        nifty_previous_day_close = 21500
        i = 0
        while i < 4:
            i += 1
            try:
                nifty_previous_day_close = smartAPI.ltpData("NSE", "NIFTY", "99926000")['data']['close']
                break
            except Exception as se:
                UTIL.append_log_line("Error : error getLtpData_____attempt number=" + str(i) + "________" + str(se) + " __ NIFTY")
                time.sleep(1)
                continue
        time.sleep(1)
        offset = (nifty_previous_day_close * 0.0295)
        nifty_oi_lower_bound = int((nifty_previous_day_close - offset)/100) * 100
        nifty_oi_upper_bound = int((nifty_previous_day_close + offset)/100) * 100 + 100

        meta_text = "NIFTY_OI_LOWER_BOUND=" + str(nifty_oi_lower_bound) + "\n"
        meta_text = meta_text + "NIFTY_OI_UPPER_BOUND=" + str(nifty_oi_upper_bound) + "\n"
        with open('/tmp/meta.txt', 'w') as f:
            f.write(meta_text)
        AZUREUTIL.save_file("meta.txt", "meta", True)
    
    
    @staticmethod
    def save_logs(smartAPI):
        #Write Log to Azure
        log_file_name = datetime.now().strftime('%Y%m%d') + '.log'
        log_file_tradebom_name = datetime.now().strftime('%Y%m%d') + '_tradebom.log'
        log_file_text = None
        log_file_tradebom_text = None
        
        if AZUREUTIL.is_blob_exists(log_file_name, "trades") :
            AZUREUTIL.get_file(log_file_name, "trades")
            log_file_text = open("/tmp/" + log_file_name, "r").read().strip()
            log_file_text = log_file_text + '\n\n_________________________________________________________________\n\n'
            log_file_text = log_file_text +  '\n'.join(UTIL.LOG_LINES)
        else :
            log_file_text = '\n'.join(UTIL.LOG_LINES)

        if AZUREUTIL.is_blob_exists(log_file_tradebom_name, "trades") :
            AZUREUTIL.get_file(log_file_tradebom_name, "trades")
            log_file_tradebom_text = open("/tmp/" + log_file_tradebom_name, "r").read().strip()
            log_file_tradebom_text = log_file_tradebom_text + '\n\n_________________________________________________________________\n\n'
            log_file_tradebom_text = log_file_tradebom_text +  '\n'.join(UTIL.TRADE_BOM_LOG_LINES)
        else :
            log_file_tradebom_text = '\n'.join(UTIL.TRADE_BOM_LOG_LINES)
        
        with open('/tmp/' + log_file_name, 'w') as f:
            f.write(log_file_text)

        with open('/tmp/' + log_file_tradebom_name, 'w') as f:
            f.write(log_file_tradebom_text)
        
        UTIL.upload_to_sftp('/tmp/' + log_file_tradebom_name, 'log.txt')
        
        overall_gain = 0
        if AZUREUTIL.is_blob_exists('overall_gain.txt', "trades") :
            AZUREUTIL.get_file('overall_gain.txt', 'trades')
            overall_gain_text = open('/tmp/overall_gain.txt', "r").read().strip()
            overall_gain = float(overall_gain_text.strip())
        
        gain_summary = UTIL.get_summary(smartAPI, overall_gain)
        with open('/tmp/gain.txt', 'w') as f:
            f.write(gain_summary)
        UTIL.upload_to_sftp('/tmp/gain.txt', 'gain.txt')
        
        AZUREUTIL.save_file(log_file_name, "trades", True)
        AZUREUTIL.save_file(log_file_tradebom_name, "trades", True)
        AZUREUTIL.save_file('gain.txt', "trades", True)

    @staticmethod
    def save_historical_data(smartAPI, all_stocks_historical_data, token_symbol_map):

        trimmed_historical_data = []
        for token, stock_data in all_stocks_historical_data.items():

            symbol = token_symbol_map[token]
            latest_day_data = []
            latest_day = None
            previous_day_close = -1

            for stock in reversed(stock_data):

                print(stock[2])
                
                if latest_day is None:
                    latest_day = stock[2][0:10] #2021-11-16T09:15:00+05:30 >> 2021-11-16

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
    