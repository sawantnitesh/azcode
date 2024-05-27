import logging
import pandas as pd

from .util import UTIL

class ANALYZER(object):

    @staticmethod
    def find_flat_stocks_on_daily_candles(all_stocks_historical_data):
        
        flat_stocks = {}
        stocks_std_deviation = {}

        for token, stock_data in all_stocks_historical_data.items():
            try:
                df = pd.DataFrame(stock_data)
                stocks_std_deviation[token] = df[6].std() #4:high, 5:low
            
            except Exception as se:
                UTIL.append_log_line("Error : emacrossover_____" + str(se) + " __ " + str(token))

        logging.info("stocks_std_deviation=" + str(stocks_std_deviation))
        sorted_std_deviation_stocks = sorted(stocks_std_deviation.items(), key=lambda x:x[1])

        counter = 0
        for token, stock_data in sorted_std_deviation_stocks:
            flat_stocks[token] = all_stocks_historical_data[token]
            counter = counter + 1
            if counter > 20:
                break
        
        return flat_stocks
