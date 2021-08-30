'''--------------------------------------------------------------------------------------------------------

Prototype For Swing Algo Trading.

--------------------------------------------------------------------------------------------------------'''

from smartapi import SmartConnect
from datetime import datetime,timedelta
import pandas as pd
import time

'''--------------------------------------------------------------------------------------------------------

Defining the Dictionaries to work upon. Need to be deleted at the end of the day.

--------------------------------------------------------------------------------------------------------'''

sl_1day={}
sl_15minute={}
sl_5minute={}
sl_1minute={}
sl_ltp={}

'''--------------------------------------------------------------------------------------------------------

Storing the credentials for the api to work succesfully, along with generating the session.

--------------------------------------------------------------------------------------------------------'''

apikey='VXdBs3Rp'
username='P319380'
pwd='partha2001'
obj=SmartConnect(api_key=apikey)
data = obj.generateSession(username,pwd)
refreshToken= data['data']['refreshToken']
feedToken=obj.getfeedToken()
userProfile= obj.getProfile(refreshToken)

'''--------------------------------------------------------------------------------------------------------

This function will be called whenever a Buy / Sell order needs to be placed.

--------------------------------------------------------------------------------------------------------'''

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

'''--------------------------------------------------------------------------------------------------------

This function returns the Open, High, Low, Close and Volume data in the form of a Data Frame

--------------------------------------------------------------------------------------------------------'''

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
        time.sleep(0.35)
        return(pd.DataFrame(obj.getCandleData(historicParam)['data'],columns=['T','O','H','L','C','V']))
    except:
        return None  

'''--------------------------------------------------------------------------------------------------------

This function takes input as the Historical Data, the period i.e 10 to 210, and the last day for which the 
SMA is needed and finally returns the corresponding Daily Moving Average value for the stock.

--------------------------------------------------------------------------------------------------------'''
        
def SMA(data,period,last_n_day):
    try:
        return round(data['C'].rolling(window=period).mean().iloc[-1*last_n_day],2)
    except:
         return(-1)

'''--------------------------------------------------------------------------------------------------------

This function takes input as the token of the stock, and then returns the LTP value of the stock. This 
function can only be called after the sl_1day  dictionary has been created, since it takes input from 
this dictionary.

--------------------------------------------------------------------------------------------------------'''     
 
def getLtpData(token):
    time.sleep(0.1)          
    return(obj.ltpData('NSE', sl_1day[token][0], token)['data']['ltp'])

'''--------------------------------------------------------------------------------------------------------

This function updates the csv database of 500 stocks based on the latest values  of 50 and 200 moving 
average calculated for the individual stocks on a daily basis.

--------------------------------------------------------------------------------------------------------'''    
        
def updateData():
    df=pd.read_csv("stock_data.csv")
    rv=pd.read_csv("stock_data.csv").set_index('token')        
    for i, row in df.iterrows():
        try:            
            data=historicalData(row['token'],200)
            rv.loc[row['token'],'dma50']=SMA(data, 50, 1) 
            rv.loc[row['token'],'dma200']=SMA(data, 200, 1)
        except:
            pass
    rv.to_csv("stock_data.csv")

'''--------------------------------------------------------------------------------------------------------

This function generates a dictionary of key value pairs, with key being the token of the stocks that 
satisfy the necessary conditions, and the values being their equivalent stock name. The data is stored 
in a dictionary named sl_1day.

Concept 1:
    
    Only those stocks are chosen whose 50 DMA value is greater than their 200 DMA values.
    Also the stocks whose data could not be retrieved are cut off here.
    And finally, the stocks which are already in an open position are also skipped.

Concept 2:
    
    The stocks are sorted according to whether they are near their 50 DMA or their 200 DMA.
    Then they are checked on the basis of last 5 day closing, this determines the stock's past movement.
    Then the corresponding 50 / 200 DMA is confirmed for bullishness.
    If the conditions are satisfied, then add to watchlist ( sl_1day )
    
--------------------------------------------------------------------------------------------------------'''

def shortListOneDay():
    df=pd.read_csv("stock_data.csv")
    rv=pd.read_csv("stock_data.csv").set_index('token')
    for i, row in df.iterrows():
        try:    
            ltp=historicalData(row['token'],4)['C'].iloc[-1]
            dma50=rv.loc[row['token'],'dma50']
            dma200=rv.loc[row['token'],'dma200']
            if(dma50>dma200)and(dma200>0)and(rv.loc[row['token'],'slm']==0):
                if(ltp>dma50):
                    data=historicalData(row['token'],75)
                    if(data['C'].iloc[-1]>=dma50)and(data['C'].iloc[-2]>=dma50)and(data['C'].iloc[-3]>=dma50)and(data['C'].iloc[-4]>=dma50)and(data['C'].iloc[-5]>=dma50):
                        if(SMA(data,50,1)>SMA(data,50,6)>SMA(data,50,11)>SMA(data,50,16)>SMA(data,50,21)):
                            sl_1day[row['token']]=[rv.loc[row['token'],'stockname'], 50, dma50]
            elif(ltp<dma50)and(ltp>dma200):
                data=historicalData(row['token'],300)
                if(data['C'].iloc[-1]>=dma200)and(data['C'].iloc[-2]>=dma200)and(data['C'].iloc[-3]>=dma200)and(data['C'].iloc[-4]>=dma200)and(data['C'].iloc[-5]>=dma200):
                    if(SMA(data,200,1)>SMA(data,200,21)>SMA(data,200,41)>SMA(data,200,61)>SMA(data,200,81)):
                        sl_1day[row['token']]=[rv.loc[row['token'],'stockname'], 200, dma200]
        except:
            pass

'''--------------------------------------------------------------------------------------------------------

These functions generate dictionaries of lists, with keys being the tokens of the stocks and the values
being a list comprising of two numbers, first one being 50 or 200 ( depending on which one is closer)
and the second one being the corresponding 50 or 200 SMA value.

Concept :
    
    shortList15():
        
        Checks if the stock has come inside the 10% zone from the corresponding SMA.
        Store the data in dictionary sl_15minute if conditions satisfy.
        Has to be checked every 15 minutes.
        
    shortList5():
        
        Checks if the stock has come inside the 5% zone from the corresponding SMA.
        Store the data in dictionary sl_15minute if conditions satisfy.
        Has to be checked every 5 minutes.
        
    shortList5():
            
        Checks if the stock has come inside the 1% zone from the corresponding SMA.
        Store the data in dictionary sl_15minute if conditions satisfy.
        Has to be checked every 1 minute.

--------------------------------------------------------------------------------------------------------'''

def shortList15():
    for i in sl_1day:
        try:            
            ltp=getLtpData(i)
            if ltp<(1.1*sl_1day[i][2]):
                sl_15minute[i]=[sl_1day[i][1],sl_1day[i][2]]
            elif(i in sl_15minute):
                del sl_15minute[i]
        except:
            pass
            
def shortList5():
    for i in sl_15minute:
        try:
            ltp=getLtpData(i)
            if ltp<(1.05*sl_15minute[i][1]):
                sl_5minute[i]=sl_15minute[i]
            elif(i in sl_5minute):
                del sl_5minute[i]
        except:
            pass
            
def shortList1():
    for i in sl_5minute:
        try:
            ltp=getLtpData(i)
            if ltp<(1.01*sl_5minute[i][1]):
                sl_1minute[i]=sl_5minute[i]
            elif(i in sl_1minute):
                del sl_5minute[i]
        except:
            pass
