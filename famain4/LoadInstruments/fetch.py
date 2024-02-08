import logging
import json
from urllib import request
import pandas as pd

from .util import UTIL

class FETCH(object):

    @staticmethod
    def get_allscrips():
        url = 'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'
        data = request.urlopen(url).read()
        allscrips = json.loads(data)
        return allscrips

    @staticmethod
    def fetch_fno_stocks(allscrips):
        try:

            df = pd.DataFrame(allscrips, index=None)
            df = df.loc[df['symbol'].str.contains('-EQ') & ~(df['symbol'].str.contains('TEST'))]
            df = df.drop(['expiry', 'strike', 'lotsize', 'instrumenttype', 'exch_seg', 'tick_size'], axis=1)

            df = df[["token", "symbol", "name"]]

            df1 = pd.DataFrame(allscrips, index=None)

            nfo_distinct_instrument_names = df1.loc[df1['exch_seg'].str.contains('NFO')]['name'].unique()

            df2 = df1.loc[df1['symbol'].str.contains('-EQ') & ~(df['symbol'].str.contains('TEST')) & df1['name'].isin(
                nfo_distinct_instrument_names)]

            df2 = df2[['token', 'symbol', 'name']]
            return df2

        except Exception as e:
            logging.exception("Error : FETCH.fetch_____" + str(e))
    
    @staticmethod
    def fetch_nifty_stocks(allscrips, category):
        try:

            df = pd.DataFrame(allscrips, index=None)
            df = df.loc[df['symbol'].str.contains('-EQ') & ~(df['symbol'].str.contains('TEST'))]
            df = df.drop(['expiry', 'strike', 'lotsize', 'instrumenttype', 'exch_seg', 'tick_size'], axis=1)

            df = df[["token", "symbol", "name"]]

            if category == 'NIFTY_50':
                df = df.loc[df['name'].isin(UTIL.NIFTY_50)]
            elif category == 'NIFTY_500':
                df = df.loc[df['name'].isin(UTIL.NIFTY_500)]
            elif category == 'BANK_NIFTY':
                df = df.loc[df['name'].isin(UTIL.BANK_NIFTY)]
            
            return df
        
        except Exception as e:
            logging.exception("Error : FETCH.fetch_____" + str(e))
