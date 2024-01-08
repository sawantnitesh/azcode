import logging
import json
from urllib import request
import pandas as pd
import pandas_ta as ta
from SmartApi import SmartConnect
import pyotp
from datetime import datetime
from datetime import timedelta
import pytz
import time

class TRADE(object):

    @staticmethod
    def find_trades(smartAPI, all_stocks_historical_data):
        try:

            trades = []
            
            ema_crossover_trades = TRADE.trade_ema_crossover_9_14(all_stocks_historical_data)
            if ema_crossover_trades :
                trades.extend(ema_crossover_trades)
            
            TRADE.square_off_all_14_30(smartAPI)

            logging.info("trades found____________________" + str(trades))

            return trades

        except Exception as e:
            logging.exception("Error : FETCH.fetch_historical_data_____" + str(e))
            raise Exception("Error : FETCH.fetch_historical_data_____") from e


    @staticmethod
    def trade_ema_crossover_9_14(all_stocks_historical_data):

        trades = []

        todays_date = datetime.now()
        current_hour = todays_date.astimezone(pytz.timezone("Asia/Calcutta")).hour
        current_minute = todays_date.astimezone(pytz.timezone("Asia/Calcutta")).minute

        if (current_hour == 9 and current_minute > 30) or (current_hour >= 10 and current_hour <= 13) or (current_hour == 14 and current_minute < 35) :

            for token, stock_data in all_stocks_historical_data.items():
                try:
                    df = pd.DataFrame(stock_data)
                    ema20 = ta.ema(df[6], length=20)
                    ema50 = ta.ema(df[6], length=50)

                    buyORSell = None

                    if len(ema20) > 1 and len(ema50) > 1 :
                        if ema20.iloc[-2] < ema50.iloc[-2] and ema20.iloc[-1] > ema50.iloc[-1] :
                            #up crossover
                            #buyORSell = "BUY"
                            buyORSell = None #Disable BUY trades on emacrossover
                        elif ema20.iloc[-2] > ema50.iloc[-2] and ema20.iloc[-1] < ema50.iloc[-1] :
                            #down crossover
                            buyORSell = "SELL"
                    
                    if buyORSell:
                        trades.append("EmaCrossover," + str(token) + "," + stock_data[-1][1] + "," + buyORSell)
                
                except Exception as se:
                    logging.exception("Error : emacrossover_____" + str(se) + " __ " + str(token))
        
        return trades
    
    @staticmethod
    def square_off_all_14_30(smartAPI):

        todays_date = datetime.now()
        current_hour = todays_date.astimezone(pytz.timezone("Asia/Calcutta")).hour
        current_minute = todays_date.astimezone(pytz.timezone("Asia/Calcutta")).minute

        if (current_hour == 14 and current_minute > 29) or (current_hour == 15) :

            #Cancel all open orders including ROBO orders
            order_book = smartAPI.orderBook()
            if order_book['data'] :
                for order in order_book['data']:
                    if order['status'] == 'open' or order['status'] == 'trigger pending':
                        cancelOrderOutput = smartAPI.cancelOrder(order['orderid'],'NORMAL')
                        logging.info("Cancelling Open Order............ " + str(cancelOrderOutput))
                        time.sleep(0.5)
            
            #Place fresh squareoff MARKET orders
            positions = smartAPI.position()
            if positions['data'] :
                for position in positions['data']:
                    if position['producttype'] == 'INTRADAY' and int(position['netqty']) != 0:

                        buyORSell = 'BUY'
                        quantity = position['netqty']
                        if int(quantity) > 0 :
                            buyORSell = 'SELL'
                        
                        orderParam = {
                            "variety": "NORMAL",
                            "tradingsymbol": position['tradingsymbol'],
                            "symboltoken": position['symboltoken'],
                            "transactiontype": buyORSell,
                            "exchange": "NSE",
                            "ordertype": "MARKET",
                            "producttype": "INTRADAY",
                            "duration": "DAY",
                            "quantity": abs(int(quantity)),
                            "price": "0",
                            "squareoff": "0",
                            "stoploss": "0"
                        }

                        smartAPI.placeOrder(orderParam)

                        time.sleep(0.5)
