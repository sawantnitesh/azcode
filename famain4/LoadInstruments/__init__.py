import datetime
import logging

import azure.functions as func

from .azureutil import AZUREUTIL
from .fetch import FETCH

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')
    
    allscrips = FETCH.get_allscrips()

    all_stocks_df = FETCH.fetch_nifty_stocks(allscrips, 'NIFTY_50')
    all_stocks_df.to_csv("/tmp/stocks.csv", index=False, header=False)#stocks_NIFTY50.csv
    AZUREUTIL.save_file("stocks.csv", "meta")#stocks_NIFTY50.csv

    all_stocks_df = FETCH.fetch_nifty_stocks(allscrips, 'NIFTY_500')
    all_stocks_df.to_csv("/tmp/stocks_NIFTY_500.csv", index=False, header=False)
    AZUREUTIL.save_file("stocks_NIFTY_500.csv", "meta")

    all_stocks_df = FETCH.fetch_nifty_stocks(allscrips, 'BANK_NIFTY')
    all_stocks_df.to_csv("/tmp/stocks_BANK_NIFTY.csv", index=False, header=False)
    AZUREUTIL.save_file("stocks_BANK_NIFTY.csv", "meta")
    
    logging.info("Stocks Saved _ _ _ _ _ _ _ !!")

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
