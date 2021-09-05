from smartapi import SmartConnect
from datetime import datetime,timedelta
import pandas as pd
import time
import numpy as np
import sqlite3
import sqlalchemy

apikey='VXdBs3Rp'
username='P319380'
pwd='partha2001'
obj=SmartConnect(api_key=apikey)
data=obj.generateSession(username,pwd)
refreshToken=data['data']['refreshToken']
feedToken=obj.getfeedToken()
userProfile= obj.getProfile(refreshToken)

engine=sqlalchemy.create_engine('sqlite:///database.db')

def atr(token):
    df=pd.read_sql(str(token),engine)     
    sum=0
    for i in range(14):
        sum+=max(abs(df['H'].iloc[-1*(i+1)]-df['L'].iloc[-1*(i+1)]),abs(df['H'].iloc[-1*(i+1)]-df['C'].iloc[-1*(i+2)]),abs(df['L'].iloc[-1*(i+1)]-df['C'].iloc[-1*(i+2)]))            
    return(sum/14)
      
def sma(token,period,paramater):
    df=pd.read_sql(str(token),engine)
    sum=0
    for i in range(period):
        sum+=df[paramater].iloc[-1*(i+1)]/period
    return(sum)
    
def vma(token):
    df=pd.read_sql(str(token),engine)
    sum=0
    for i in range(50):
        sum+=df['V'].iloc[-1*(i+1)]
    return(sum/50)   
    
def placeOrder(tradingsymbol, symboltoken, buy_sell, quantity):
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
    time.sleep(0.35)
    try:
        historicParam={"exchange":"NSE",
                       "symboltoken":token,
                       "interval":"ONE_DAY",
                       "fromdate":from_date.strftime("%Y-%m-%d %H:%M"),
                       "todate":to_date.strftime("%Y-%m-%d %H:%M")
                      }
        return(pd.DataFrame(obj.getCandleData(historicParam)['data'],columns=['T','O','H','L','C','V']))
    except Exception as e:
        print("Historical Data Failed: {}".format(e.message))
        
def getLtpData(token, name):
    time.sleep(0.1)          
    return(obj.ltpData('NSE', name, token)['data']['ltp'])


def updateMainData():
    df=pd.read_sql('main',engine)
    for row in df.itertuples():
        print(type(np.int16(df.loc[row[0],'token']).item()))
        df.loc[row[0],'sma10low']=sma(df.loc[row[0],'token'], 10, 'L')
        df.loc[row[0],'sma20low']=sma(df.loc[row[0],'token'], 20, 'L')
        df.loc[row[0],'sma50high']=sma(df.loc[row[0],'token'], 50, 'H')
        df.loc[row[0],'sma50low']=sma(df.loc[row[0],'token'], 50, 'L')
        df.loc[row[0],'sma50close']=sma(df.loc[row[0],'token'], 50, 'C')
        df.loc[row[0],'sma100close']=sma(df.loc[row[0],'token'], 100, 'C')
        df.loc[row[0],'sma200close']=sma(df.loc[row[0],'token'], 200, 'C')
        df.loc[row[0],'volume50']=vma(df.loc[row[0],'token'])
        df.loc[row[0],'atr']=atr(df.loc[row[0],'token'])
    df.to_sql('main',engine,if_exists='replace',index=False)

def updateIndividualData():
      df=pd.read_sql('main',engine)
      for row in df.itertuples():
          idf=pd.read_sql(str(df.loc[row[0],'token']),engine)
          tdf=historicalData(np.int16(df.loc[row[0],'token']).item(),2).iloc[-1]
          idf=idf.append(tdf)
          if idf.shape[0] > 300:
              idf.drop(0,axis=0,inplace=True)           
          idf.to_sql(str(df.loc[row[0],'token']),engine,if_exists='replace',index=False)

############ Have to iterate through the rows of star column ###################

# df=pd.read_sql('main',engine)
# for row in df.itertuples():

def star0():
    len=pd.read_sql(str(df.loc[row[0],'token']),engine).shape[0]
    if len > 205 and (df.loc[row[0],'sma50close']>df.loc[row[0],'sma100close']>df.loc[row[0],'sma200close']):
        df.loc[row[0],'star']=1
    
def star1():
    tdf=pd.read_sql(str(df.loc[row[0],'token']),engine)
    if tdf['C'].iloc[-1]<df.loc[row[0],'sma50low']:
        df.loc[row[0],'star']=2
  
def star2():
    tdf=pd.read_sql(str(df.loc[row[0],'token']),engine)
    if tdf['O'].iloc[-1]<df.loc[row[0],'sma50close']:
        df.loc[row[0],'star']=3

def star3():
    tdf=historicalData(np.int16(df.loc[row[0],'token']).item(),2).iloc[-1]
    if (tdf['V'].iloc[-1]>df.loc[row[0],'volume50'])and(tdf['C'].iloc[-1]>tdf['O'].iloc[-1]):        
        if (tdf['C'].iloc[-1]<df.loc[row[0],'sma50high'])and(tdf['C'].iloc[-1]>df.loc[row[0],'sma50high']*0.995):
            df.loc[row[0],'star']=4
        elif (tdf['C'].iloc[-1]>df.loc[row[0],'sma50high']):
            df.loc[row[0],'star']=5   

def star4():
    tdf=historicalData(np.int16(df.loc[row[0],'token']).item(),2).iloc[-1]
    if (tdf['O'].iloc[-1]>df.loc[row[0],'sma50high'])or(tdf['C'].iloc[-1]>df.loc[row[0],'sma50high']):
        print(df.loc[row[0],'token'])
    else:
        df.loc[row[0],'star']=3

def star5():
    tdf=historicalData(np.int16(df.loc[row[0],'token']).item(),2).iloc[-1]
    if ((tdf['C'].iloc[-1]-tdf['O'].iloc[-1])>df.loc[row[0],'atr'])and((tdf['H'].iloc[-1]-tdf['L'].iloc[-1])>2*df.loc[row[0],'atr']):
        if (tdf['C'].iloc[-1]>1.05*df.loc[row[0],'sma50high']):
            df.loc[row[0],'star']=0
        else:
            df.loc[row[0],'star']=6
    else:
        print(df.loc[row[0],'token'])

def star6():
    tdf=historicalData(np.int16(df.loc[row[0],'token']).item(),2).iloc[-1]
    if (tdf['C'].iloc[-1]<df.loc[row[0],'sma50high'])and(tdf['C'].iloc[-1]>df.loc[row[0],'sma50close']):
        print(df.loc[row[0],'token'])
    elif (tdf['C'].iloc[-1]>1.05*df.loc[row[0],'sma50high']):# and time > 3:20 PM
        df.loc[row[0],'star']=0
        
# df.to_sql('main',engine,if_exists='replace',index=False)

############## Iteration is finished ###################################
