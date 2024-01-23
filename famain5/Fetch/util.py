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

from .azureutil import AZUREUTIL

class UTIL(object):

    LOG_LINES = []
    TRADE_BOM_LOG_LINES = []
    TRADE_SETUP_COUNT = 5
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
    def get_summary(smartAPI):
        text = datetime.now(pytz.timezone("Asia/Calcutta")).strftime('%d-%b-%Y %H:%M')
        text = text + "\n" + "Nitesh S | Proprietary Algo Trading Strategies"
        text = text + "\n" + "Funds | " + str(UTIL.FUND_BALANCE)
        
        positions = smartAPI.position()
        gain = 0
        if positions['data'] :
            for position in positions['data']:
                if position['realised'] :
                    gain = gain + float(position['realised'])
                if position['unrealised'] :
                    gain = gain + float(position['unrealised'])
        if gain > 0:
            text = text + "\n" + "Todays Profit | +" + str(gain)
        else :
            text = text + "\n" + "Todays Profit | " + str(gain)
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
    def save_logs():
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
            log_file_tradebom_text = log_file_text + '\n\n_________________________________________________________________\n\n'
            log_file_tradebom_text = log_file_tradebom_text +  '\n'.join(UTIL.TRADE_BOM_LOG_LINES)
        else :
            log_file_tradebom_text = '\n'.join(UTIL.TRADE_BOM_LOG_LINES)
        
        with open('/tmp/' + log_file_name, 'w') as f:
            f.write(log_file_text)

        with open('/tmp/' + log_file_tradebom_name, 'w') as f:
            f.write(log_file_tradebom_text)
        
        UTIL.upload_to_sftp('/tmp/' + log_file_tradebom_name, 'log.txt')
        
        AZUREUTIL.save_file(log_file_name, "trades", True)
        AZUREUTIL.save_file(log_file_tradebom_name, "trades", True)
        
        gain = UTIL.get_summary(smartAPI)
        with open('/tmp/gain.txt', 'w') as f:
            f.write(gain)
        UTIL.upload_to_sftp('/tmp/gain.txt', 'gain.txt')
        os.remove(os.path.join('', '/tmp/gain.txt'))
        