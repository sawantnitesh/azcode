import logging
from datetime import datetime
import requests
import json
import pytz
import pandas as pd
import os
from datetime import datetime
import pysftp

from .azureutil import AZUREUTIL

class UTIL(object):

    @staticmethod
    def load_oi():
        
        UTIL.back_up()

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
                if int(d['strikePrice'])%100 == 0:
                    if 'CE' in d:
                        oi_d = d['CE']
                        oi_rows.append([-1,current_time, oi_d['strikePrice'], 'CE', oi_d['openInterest'], oi_d['lastPrice']])
                    if 'PE' in d:
                        oi_d = d['PE']
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
        UTIL.upload_to_sftp('/tmp/oi_data.csv', 'oi_data.csv')
        AZUREUTIL.save_file('oi_data.csv', "oidata", True)

        UTIL.write_oi_csv_meta_data_to_sftp()
        
        logging.info("OI Data Saved____________________:ExpiryDate=" + expiry_date)
    

    @staticmethod
    def analyze_oi():

        AZUREUTIL.get_file("meta.csv", "meta")
        df_meta = pd.read_csv("/tmp/meta.csv")
        os.remove(os.path.join('', '/tmp/meta.csv'))
        nifty_price = df_meta[df_meta['key'] == 'nifty_price']['value'][0]

        df_all = pd.read_csv("/tmp/oi_data.csv")
        os.remove(os.path.join('', '/tmp/oi_data.csv'))
        
        all_strikes = df_all['strike'].unique()
        result = filter(lambda x: abs(x - nifty_price) <= 100, all_strikes)
        strikes = list(result)

        for strike in strikes :
            df_ce = df_all[(df_all['strike'] == strike) & (df_all['CEPE'] == 'CE')][-2:]    
            df_pe = df_all[(df_all['strike'] == strike) & (df_all['CEPE'] == 'PE')][-2:]
            
            ce_oi1 = df_ce[-2:]['oi'].iloc[0]
            ce_oi2 = df_ce[-1:]['oi'].iloc[0]
            
            pe_oi1 = df_pe[-2:]['oi'].iloc[0]
            pe_oi2 = df_pe[-1:]['oi'].iloc[0]
            
            oi_trades = "Direction,Strike,ce1,ce2,pe1,pe2\n"
            if (ce_oi2/ce_oi1 >= 1.15 and pe_oi1/pe_oi2 >= 1.15) :
                oi_trades = "DOWN", strike, ce_oi1, ce_oi2, pe_oi1, pe_oi2 + "\n"
            if (ce_oi1/ce_oi2 >= 1.15 and pe_oi2/pe_oi1 >= 1.15) :
                oi_trades = oi_trades + "UP", strike, ce_oi1, ce_oi2, pe_oi1, pe_oi2 + "\n"
            
            with open('/tmp/oi_trades.csv', 'w') as f:
                f.write(oi_trades)
            
            UTIL.upload_to_sftp('/tmp/oi_trades.csv', 'oi_trades.csv')
            AZUREUTIL.save_file('oi_trades.csv', "trades", True)


    @staticmethod
    def back_up():
        todays_date = datetime.now()
        current_hour = todays_date.astimezone(pytz.timezone("Asia/Calcutta")).hour
        current_minute = todays_date.astimezone(pytz.timezone("Asia/Calcutta")).minute
        
        if (current_hour == 9 and current_minute < 10) : #only backup at 9:00
            if AZUREUTIL.is_blob_exists("oi_data.csv", "oidata"):
                archive_name = "oi_data_" + datetime.now(pytz.timezone("Asia/Calcutta")).strftime('%Y%m%d%H%M')+ ".csv"
                AZUREUTIL.rename_file("oi_data.csv", archive_name, "oidata")
                logging.info("Renamed oi_data.csv to " + archive_name)
                
                AZUREUTIL.get_file(archive_name, "oidata")
                UTIL.upload_to_sftp('/tmp/' + archive_name, archive_name)
                os.remove(os.path.join('', '/tmp/' + archive_name))
    

    @staticmethod
    def upload_to_sftp(file_path, remote_path):
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None   
        with pysftp.Connection('31.170.161.108', port=65002, username='u571677883', password='aqpt$dfaadf#&FmEE_x8aaaa1111', cnopts=cnopts) as sftp:
            with sftp.cd('/home/u571677883/domains/tradebom.com/public_html/data/oi'):
                logging.info('uploading to sftp' + file_path)
                sftp.put(file_path, remote_path)
    

    @staticmethod
    def write_oi_csv_meta_data_to_sftp():
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None   
        oi_csvs = []
        with pysftp.Connection('31.170.161.108', port=65002, username='u571677883', password='aqpt$dfaadf#&FmEE_x8aaaa1111', cnopts=cnopts) as sftp:
            with sftp.cd('/home/u571677883/domains/tradebom.com/public_html/data/oi'):
                dirlist = sftp.listdir()
                for dir in dirlist:
                    if "oi_data_" in dir:
                        oi_csvs.append(dir)

        df = pd.DataFrame(oi_csvs, columns=["value"])
        df.to_csv('/tmp/meta_oi.csv', index=False)
        UTIL.upload_to_sftp('/tmp/meta_oi.csv', 'meta_oi.csv')
