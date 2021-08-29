'''
Prototype For Swing Algo Trading
'''

from smartapi import SmartConnect
from datetime import datetime,timedelta
import pandas as pd
import time

apikey='VXdBs3Rp'
username='P319380'
pwd='partha2001'
obj=SmartConnect(api_key=apikey)
data = obj.generateSession(username,pwd)
refreshToken= data['data']['refreshToken']
feedToken=obj.getfeedToken()
userProfile= obj.getProfile(refreshToken)

def orderPlacement(tradingsymbol, symboltoken, buy_sell, quantity):
    try:
        orderparams = {
            "variety": "NORMAL",
            "tradingsymbol": tradingsymbol,
            "symboltoken": symboltoken,
            "transactiontype": buy_sell,
            "exchange": "NSE",
            "ordertype": "MARKET",
            "producttype": "DELIVERY",
            "duration": "DAY",
            "price": "0",
            "squareoff": "0",
            "stoploss": "0",
            "quantity": quantity
            }
        orderId=obj.placeOrder(orderparams)
        print("The order id is: {}".format(orderId))
    except Exception as e:
        print("Order placement failed: {}".format(e.message))     

def historicalData(token,period):
    to_date=datetime.now()
    from_date=to_date-timedelta(days=period*1.5)
    try:
        historicParam={"exchange":"NSE",
                       "symboltoken":token,
                       "interval":"ONE_DAY",
                       "fromdate":from_date.strftime("%Y-%m-%d %H:%M"),
                       "todate":to_date.strftime("%Y-%m-%d %H:%M")
                      }
        return(obj.getCandleData(historicParam))
    except Exception as e:
         print("Error: {}".format(e.message))    
        
def SMA(token,period):
    try:
        data=historicalData(token,period)
        df=pd.DataFrame(data['data'],columns=['T','O','H','L','C','V'])
        return round(df['C'].rolling(window=period).mean().iloc[-1],2)
    except Exception as e:
         print("Error: {}".format(e.message))
                
def updateData():
    df=pd.read_csv("stock_data.csv")
    rv=pd.read_csv("stock_data.csv").set_index('token')
    for i, row in df.iterrows():
        rv.loc[row['token'],'dma50']=SMA(row['token'], 50)
        time.sleep(0.5)
        rv.loc[row['token'],'dma200']=SMA(row['token'], 200)
        time.sleep(0.5)
    rv.to_csv("stock_data.csv")
