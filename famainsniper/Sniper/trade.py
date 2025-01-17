import logging
import pandas as pd
from datetime import datetime
import time
import os

from .util import UTIL
from .azureutil import AZUREUTIL

class TRADE(object):

    @staticmethod
    def instant_sniper(smartAPI):
        try:

            fund_balance = smartAPI.rmsLimit()

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

                p_pe.append([time_now_str, pe_price])
                p_ce.append([time_now_str, ce_price])
                
                if len(p_ce) > 3 :
                    ##### CE
                    date_format = "%Y-%m-%d %H:%M:%S"
                    time1 = datetime.strptime(p_ce[-2][0], date_format)
                    time2 = datetime.strptime(p_ce[-1][0], date_format)
                    time_difference = time2 - time1
                    if time_difference.total_seconds() > 1 : #skip rows having diff more than 1 seconds
                        continue

                    p3 = p_ce[-4][1]
                    p2 = p_ce[-3][1]
                    p1 = p_ce[-2][1]
                    p0 = p_ce[-1][1]
                    
                    if 2 > p0 - p1 > 1 and 2 > p1 - p2 > 1 and 2 > p2 - p3 > 1 :  
                        logging.info('______________sniper trade__________________')
                        QUANTITY = float(fund_balance) // 75

                        place_order_time = datetime.now()

                        orderparams = {
                            "variety":"NORMAL",
                            "tradingsymbol":ce_symbol,
                            "symboltoken":ce_token,
                            "transactiontype":"BUY",
                            "exchange":"NFO",
                            "ordertype":"MARKET",
                            "producttype":"INTRADAY",
                            "duration":"DAY",
                            "quantity":QUANTITY
                            }
                        
                        logging.info(orderparams)
                        
                        smartAPI.placeOrder(orderparams)

                        #logic to exit after 12 seconds
                        while True:
                            time.sleep(1)
                            current_time = datetime.now()
                            time_difference = current_time - place_order_time
                            if time_difference.total_seconds() > 8 :
                                orderparams = {
                                    "variety":"NORMAL",
                                    "tradingsymbol":ce_symbol,
                                    "symboltoken":ce_token,
                                    "transactiontype":"SELL",
                                    "exchange":"NFO",
                                    "ordertype":"MARKET",
                                    "producttype":"INTRADAY",
                                    "duration":"DAY",
                                    "quantity":QUANTITY
                                    }
                                
                                logging.info(orderparams)
                                
                                smartAPI.placeOrder(orderparams)
                                t=t+8
                                break

                t=t+1
                if t > 550:
                    break
            
        
        except Exception as e:
            UTIL.append_log_line("Error : TRADE.instant_sniper_____" + str(e))
            raise Exception("Error : TRADE.instant_sniper_____" + str(e)) from e
    