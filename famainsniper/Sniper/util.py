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
    TRADE_SETUP_COUNT = 10
    
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
            log_file_tradebom_text = log_file_tradebom_text + '\n\n_________________________________________________________________\n\n'
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
