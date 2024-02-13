import logging
from datetime import datetime
import requests
import json
import pytz
import pandas as pd
import os
from datetime import datetime

from .azureutil import AZUREUTIL

class UTIL(object):

    @staticmethod
    def load_oi():
        
        UTIL.clean_up()

        headers = {
            'User-Agent': 'My User Agent 1.0',
            'From': 'Test12121212@gmail.com'
        }

        data  = requests.get('https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY', headers=headers).text

        oi_json = json.loads(data)

        expiry_date = oi_json['records']['expiryDates'][0]
        oi_data = oi_json['records']['data']

        oi_rows = []
        current_time = datetime.now(pytz.timezone("Asia/Calcutta")).strftime('%H:%M')
        
        nifty_lower_bound, nifty_upper_bound = UTIL.find_lower_upper_bound()

        for d in oi_data:
            if d['expiryDate'] == expiry_date:
                if 'CE' in d:
                    oi_d = d['CE']
                    if oi_d['strikePrice'] >= nifty_lower_bound and oi_d['strikePrice'] <= nifty_upper_bound :
                        oi_rows.append([-1,current_time, oi_d['strikePrice'], 'CE', oi_d['openInterest'], oi_d['lastPrice']])
                if 'PE' in d:
                    oi_d = d['PE']
                    if oi_d['strikePrice'] >= nifty_lower_bound and oi_d['strikePrice'] <= nifty_upper_bound :
                        oi_rows.append([-1,current_time, oi_d['strikePrice'], 'PE', oi_d['openInterest'], oi_d['lastPrice']])
        
        df = pd.DataFrame(oi_rows, index=None, columns=['Sr','time','strike', 'CEPE', 'oi', 'price'])
        
        df_all = None
        if AZUREUTIL.is_blob_exists("oi_data.csv", "oidata"):
            AZUREUTIL.get_file("oi_data.csv", "oidata")
            df_all = pd.read_csv("/tmp/oi_data.csv")
            df_all = pd.concat([df_all, df])
            os.remove(os.path.join('', '/tmp/oi_data.csv'))
        else :
            df_all = df
        
        df_all['Sr'] = range(1, len(df_all)+1)

        df_all.to_csv('/tmp/oi_data.csv', index=False)
        AZUREUTIL.save_file('oi_data.csv', "oidata", True)
        UTIL.upload_to_sftp('/tmp/oi_data.csv', 'oi_data.csv')

        logging.info("OI Data Saved____________________:ExpiryDate=" + expiry_date)
    

    @staticmethod
    def clean_up():
        todays_date = datetime.now()
        current_hour = todays_date.astimezone(pytz.timezone("Asia/Calcutta")).hour
        current_minute = todays_date.astimezone(pytz.timezone("Asia/Calcutta")).minute
        
        if (current_hour == 9 and current_minute < 30) :
            if AZUREUTIL.is_blob_exists("oi_data.csv", "oidata"):
                archive_name = "oi_data_" + datetime.now(pytz.timezone("Asia/Calcutta")).strftime('%Y%m%d%H%M')+ ".csv"
                AZUREUTIL.rename_file("oi_data.csv", archive_name, "oidata")
                logging.info("Renamed oi_data.csv to " + archive_name)


    @staticmethod
    def find_lower_upper_bound():
        nifty_lower_bound = 0
        nifty_upper_bound = 99999999999
        if AZUREUTIL.is_blob_exists('meta.txt', "meta") :
            AZUREUTIL.get_file('meta.txt', 'meta')
            meta_text = open('/tmp/meta.txt', "r").read().strip()
            for keyvalue in meta_text.split('\n'):
                kv = keyvalue.strip().split('=')
                if kv[0].strip() == "NIFTY_OI_LOWER_BOUND":
                    nifty_lower_bound = int(kv[1])
                if kv[0].strip() == "NIFTY_OI_UPPER_BOUND":
                    nifty_upper_bound = int(kv[1])
        
        return nifty_lower_bound, nifty_upper_bound
    

    @staticmethod
    def upload_to_sftp(file_path, remote_path):
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None   
        with pysftp.Connection('31.170.161.108', port=65002, username='u571677883', password='aqpt$dfaadf#&FmEE_x8aaaa1111', cnopts=cnopts) as sftp:
            with sftp.cd('/home/u571677883/domains/tradebom.com/public_html/data'):
                logging.info('uploading to sftp' + file_path)
                sftp.put(file_path, remote_path)
