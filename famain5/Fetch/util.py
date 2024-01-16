import logging
import json
from urllib import request
import pandas as pd
#from SmartApi import SmartConnect
from .smartConnect import SmartConnect
import pyotp
from datetime import datetime
from datetime import timedelta
import pytz
import math
import time

class UTIL(object):

    LOG_LINES = []
    TRADE_SETUP_COUNT = 5

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
                
                ema20_price = float(trade_info[4])
                order_price = math.ceil(ltp + ema20_price)/2

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
