# -*- coding: utf-8 -*-
"""
Created on Fri Aug 27 21:24:36 2021

@author: Partha
"""

from smartapi import SmartConnect
from datetime import datetime,timedelta
import json
import pandas as pd
import requests
import numpy as np

#This changes according to the user
apikey='VXdBs3Rp'
username='P319380'
pwd='partha2001'
obj=SmartConnect(api_key=apikey)
data = obj.generateSession(username,pwd)
refreshToken= data['data']['refreshToken']
feedToken=obj.getfeedToken()
userProfile= obj.getProfile(refreshToken)
#

def SMA(token,number):
    to_date=datetime.now()
    from_date=to_date-timedelta(days=number*2)
    from_date_format=from_date.strftime("%Y-%m-%d %H:%M")
    to_date_format=to_date.strftime("%Y-%m-%d %H:%M")
    try:
        historicParam={"exchange":"NSE",
                       "symboltoken":token,
                       "interval":"ONE_DAY",
                       "fromdate":from_date_format,
                       "todate":to_date_format
                      }
        res_json=obj.getCandleData(historicParam)
        columns=['timestamp','O','H','L','C','V']
        df=pd.DataFrame(res_json['data'],columns=columns)
        df['timestamp']=pd.to_datetime(df['timestamp'],format='%Y-%m-%dT%H:%M:%S')
        return df['C'].rolling(window=number).mean().iloc[-1]
    except Exception as e:
        return False