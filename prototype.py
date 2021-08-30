from smartapi import SmartConnect
from datetime import datetime,timedelta
import pandas as pd
import time

sl_1day={}
sl_15minute={}
sl_5minute={}
sl_1minute={}
sl_ltp={}

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
        time.sleep(0.35)
        return(pd.DataFrame(obj.getCandleData(historicParam)['data'],columns=['T','O','H','L','C','V']))
    except:
        return None  
        
def SMA(data,period,last_n_day):
    try:
        return round(data['C'].rolling(window=period).mean().iloc[-1*last_n_day],2)
    except:
         pass
 
def getLtpData(token):
    time.sleep(0.1)          
    return(obj.ltpData('NSE', sl_1day[token][0], token)['data']['ltp'])
        
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
        
shortListOneDay()
shortList15()
shortList5()
shortList1()
print(sl_1minute)
