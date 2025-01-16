import logging
import azure.functions as func
import time
import pandas as pd
import numpy as np
import pytz
from datetime import datetime
import os
import json
from urllib import request

from .util import UTIL
from .azureutil import AZUREUTIL
from .trade import TRADE

def main(mytimer: func.TimerRequest) -> None:

    if mytimer.past_due:
        logging.info('The timer is past due!')
    
    UTIL.reset_log_lines()
    UTIL.append_log_line("_________________________________________________________________", True)
    UTIL.append_log_line(datetime.now(pytz.timezone("Asia/Calcutta")).strftime('%Y-%m-%d %H:%M:%S') + " AlgoTrade : Start __--..>>~~^^*****>>>>>>^^^^^^^^^^", True)
    UTIL.append_log_line("_________________________________________________________________", True)

    smartAPI = UTIL.getSmartAPI()

    todays_date = datetime.now()
    current_hour = todays_date.astimezone(pytz.timezone("Asia/Calcutta")).hour
    current_minute = todays_date.astimezone(pytz.timezone("Asia/Calcutta")).minute
        
    trade_time_flag = (current_hour == 9 and current_minute > 29) or (current_hour >= 10 and current_hour <= 14) 
    
    if trade_time_flag :
        TRADE.instant_sniper(smartAPI)

    UTIL.save_logs(smartAPI)
    
    logging.info('Fetch : Analyze : Trade : Complete !!!!!!!!!!!!!!!!!!')
