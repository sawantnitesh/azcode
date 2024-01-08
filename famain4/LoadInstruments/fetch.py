import logging
import json
from urllib import request
import pandas as pd

class FETCH(object):

    @staticmethod
    def fetch_fno_stocks():
        try:

            url = 'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'
            data = request.urlopen(url).read()
            allscrips = json.loads(data)

            df = pd.DataFrame(allscrips, index=None)
            df = df.loc[df['symbol'].str.contains('-EQ') & ~(df['symbol'].str.contains('TEST'))]
            df = df.drop(['expiry', 'strike', 'lotsize', 'instrumenttype', 'exch_seg', 'tick_size'], axis=1)

            df = df[["token", "symbol", "name"]]

            df1 = pd.DataFrame(allscrips, index=None)

            nfo_distinct_instrument_names = df1.loc[df1['exch_seg'].str.contains('NFO')]['name'].unique()

            df2 = df1.loc[df1['symbol'].str.contains('-EQ') & ~(df['symbol'].str.contains('TEST')) & df1['name'].isin(
                nfo_distinct_instrument_names)]

            df2 = df2[['token', 'symbol']]
            return df2

        except Exception as e:
            logging.exception("Error : FETCH.fetch_____" + str(e))
