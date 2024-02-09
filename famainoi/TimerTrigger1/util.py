import logging
from datetime import datetime
import requests
import json
import pytz
import pandas as pd
import os

from .azureutil import AZUREUTIL

class UTIL(object):

    @staticmethod
    def load_oi():
        
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
        
        for d in oi_data:
            if d['expiryDate'] == expiry_date:
                if 'CE' in d:
                    oi_d = d['CE']
                    oi_rows.append([current_time, oi_d['strikePrice'], 'CE', oi_d['openInterest'], oi_d['lastPrice']])
                if 'PE' in d:
                    oi_d = d['PE']
                    oi_rows.append([current_time, oi_d['strikePrice'], 'PE', oi_d['openInterest'], oi_d['lastPrice']])

        df = pd.DataFrame(oi_rows, index=None, columns=['time','strike', 'CEPE', 'oi', 'price'])
        
        df_all = None
        if AZUREUTIL.is_blob_exists("oi_data.csv", "oidata"):
            AZUREUTIL.get_file("oi_data.csv", "oidata")
            df_all = pd.read_csv("/tmp/oi_data.csv")
            df_all = pd.concat([df_all, df])
            os.remove(os.path.join('', '/tmp/oi_data.csv'))
        else :
            df_all = df
        
        df_all.to_csv('/tmp/oi_data.csv', index=False)
        AZUREUTIL.save_file('oi_data.csv', "oidata", True)

        logging.info("OI Data Saved____________________" + expiry_date)
