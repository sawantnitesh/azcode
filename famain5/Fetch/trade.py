import logging
import pandas as pd
import pandas_ta as ta
from datetime import datetime
import math
import time
import pytz
import os

from .util import UTIL
from .azureutil import AZUREUTIL

class TRADE(object):

    @staticmethod
    def instant_capture(smartAPI):
        try:

            nifty_strikes_csv_name = "nifty_strikes.csv"
            AZUREUTIL.get_file(nifty_strikes_csv_name, "meta")
            df = pd.read_csv("/tmp/" + nifty_strikes_csv_name)

            os.remove(os.path.join('', '/tmp/' + nifty_strikes_csv_name))

            df['strike'] = df['strike'].astype(int)
            df = df.sort_values(by=['strike','symbol'], ascending=True)

            nifty_price = smartAPI.ltpData("NSE", "NIFTY", "99926000")['data']['ltp']

            pe_row = df[df['strike'] < nifty_price].iloc[-1]
            ce_row = df[df['strike'] > nifty_price].iloc[0]

            pe_symbol = pe_row['symbol']
            pe_token = pe_row['token']
            ce_symbol = ce_row['symbol']
            ce_token = ce_row['token']
            
            p_ce=[]
            p_pe=[]

            t=0

            logging.info("444444444," + str(pe_symbol) + "," + str(pe_token) + "," + str(ce_symbol) + "," + str(ce_token))

            while True:
                logging.info("t counter=" + str(t))
                time.sleep(1)
                
                pe_price = -1
                ce_price = -1
                try:
                    pe_price = smartAPI.ltpData("NFO", pe_symbol, str(pe_token))['data']['ltp']
                    ce_price = smartAPI.ltpData("NFO", ce_symbol, str(ce_token))['data']['ltp']
                except Exception as se:
                    logging.info("Error : error ltpData in while loop. " + str(se))
                    time.sleep(1)
                    continue

                time_now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                
                p_pe.append(pe_symbol + "," + time_now_str + "," + str(pe_price))
                p_ce.append(ce_symbol + "," + time_now_str + "," + str(ce_price))
                
                t=t+1
                if t > 250:
                    break
            
            
            UTIL.set_prices(p_pe, p_ce)
        
        except Exception as e:
            UTIL.append_log_line("Error : TRADE.instant_capture_____" + str(e))
            raise Exception("Error : TRADE.instant_capture_____" + str(e)) from e
    
"""
    @staticmethod
    def find_trades(all_stocks_historical_data):
        try:

            trades = []
            
            ema_crossover_trades = TRADE.trade_ema_crossover(all_stocks_historical_data)
            if ema_crossover_trades :
                trades.extend(ema_crossover_trades)

            UTIL.append_log_line("trades found............" + str(len(trades)), True)
            for trade in trades:
                UTIL.append_log_line(trade, True)
            
            return trades

        except Exception as e:
            UTIL.append_log_line("Error : FETCH.fetch_historical_data_____" + str(e))
            raise Exception("Error : FETCH.fetch_historical_data_____") from e
    
    @staticmethod
    def trade_ema_crossover(all_stocks_historical_data):

        trades = []

        for token, stock_data in all_stocks_historical_data.items():
            try:
                df = pd.DataFrame(stock_data)
                ema1 = ta.ema(df[6], length=50)
                ema2 = ta.ema(df[6], length=200)

                buyORSell = None

                if len(ema1) > 10 and len(ema2) > 10 :
                    i = -10
                    sell_signal = True
                    while (i <= -3) :
                        if ema1.iloc[i] <= ema2.iloc[i] :
                            sell_signal = False
                            break
                        i = i+1
                    
                    if sell_signal and ema1.iloc[-1] <= ema2.iloc[-1] and ema1.iloc[-2] < ema2.iloc[-2] :
                        #down crossover
                        buyORSell = "SELL"
                
                if buyORSell:
                    trades.append("EmaCrossover," + str(token) + "," + stock_data[-1][1] + "," + buyORSell + "," + str(ema1.iloc[-1]))
            
            except Exception as se:
                UTIL.append_log_line("Error : emacrossover_____" + str(se) + " __ " + str(token))
    
        return trades
    
    @staticmethod
    def revisit_orderbook(smartAPI):

        ltp_map = {}

        order_book = smartAPI.orderBook()
        if order_book['data'] :
            for order in order_book['data']:
                
                if order['status'] == 'open' and order['ordertype'] == 'LIMIT' and order['disclosedquantity'] == '0' :
                    order_last_update_time = datetime.strptime(order['updatetime'], "%d-%b-%Y %H:%M:%S")
                    current_time = datetime.strptime(datetime.now(pytz.timezone("Asia/Calcutta")).strftime('%d-%b-%Y %H:%M:%S'), "%d-%b-%Y %H:%M:%S")
                    time_diff = (current_time - order_last_update_time).seconds
                    if time_diff >= 3600: #Cancel open order more than 1 hr old. Price is not reaching at desired level.
                        UTIL.append_log_line('Cancelling Order 1 hr old order : ....... ' + str(order))
                        smartAPI.cancelOrder(order['orderid'],'ROBO')
                
                if (order['status'] == 'open' or order['status'] == 'trigger pending') and order['ordertype'] == 'STOPLOSS_LIMIT' :
                    
                    order_last_update_time = datetime.strptime(order['updatetime'], "%d-%b-%Y %H:%M:%S")
                    time_diff = (datetime.now() - order_last_update_time).seconds

                    if time_diff > 300 : #5 minute gap mandatory

                        if order['transactiontype'] == 'BUY' :

                            stoploss_price = float(order['triggerprice'])
                            ltp = None

                            if order['tradingsymbol'] in ltp_map :
                                ltp = ltp_map[order['tradingsymbol']]
                            else :
                                ltpData = smartAPI.ltpData("NSE", order['tradingsymbol'], order['symboltoken'])
                                ltp = float(ltpData['data']['ltp'])
                                ltp_map[order['tradingsymbol']] = ltp
                            
                            if (stoploss_price-ltp)/ltp >= 0.015 :
                                new_trigger_price = math.ceil(ltp + ltp*0.01)
                                order['triggerprice'] = new_trigger_price
                                orderOutput = smartAPI.modifyOrder(order)
                                UTIL.append_log_line("Trailing Stop Loss Order placed.............. ltp=" + str(ltp) + ".....STOPLOSS_new_trigger_price=" + str(new_trigger_price) + "...." + str(orderOutput))
                            
                            time.sleep(0.5)
    
    @staticmethod
    def square_off_all(smartAPI):

        #Cancel all open orders including ROBO orders
        order_book = smartAPI.orderBook()
        if order_book['data'] :
            for order in order_book['data']:
                if order['status'] == 'open' or order['status'] == 'trigger pending':
                    cancelOrderOutput = smartAPI.cancelOrder(order['orderid'],'NORMAL')
                    UTIL.append_log_line("Cancelling Open Order............ " + str(cancelOrderOutput))
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

                    order_output = smartAPI.placeOrder(orderParam)
                    UTIL.append_log_line("Position squareoff Open Order............ " + str(order_output))

                    time.sleep(0.5)
"""