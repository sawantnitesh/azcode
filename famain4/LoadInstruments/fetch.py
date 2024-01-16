import logging
import json
from urllib import request
import pandas as pd

from .util import UTIL

class FETCH(object):

    @staticmethod
    def fetch_nifty500_stocks():
        try:

            url = 'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'
            data = request.urlopen(url).read()
            allscrips = json.loads(data)

            df = pd.DataFrame(allscrips, index=None)
            df = df.loc[df['symbol'].str.contains('-EQ') & ~(df['symbol'].str.contains('TEST'))]
            df = df.drop(['expiry', 'strike', 'lotsize', 'instrumenttype', 'exch_seg', 'tick_size'], axis=1)

            df = df[["token", "symbol", "name"]]

            df = df.loc[df['name'].isin(UTIL.NIFTY_500)]

            return df
        
        except Exception as e:
            logging.exception("Error : FETCH.fetch_____" + str(e))
