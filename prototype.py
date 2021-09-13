from smartapi import SmartConnect
from datetime import datetime,timedelta
import pandas as pd
import time
import sqlalchemy

engine=sqlalchemy.create_engine('sqlite:///data.db')

apikey='VXdBs3Rp'
username='P319380'
pwd='partha2001'
obj=SmartConnect(api_key=apikey)
data=obj.generateSession(username,pwd)
refreshToken=data['data']['refreshToken']
feedToken=obj.getfeedToken()
userProfile= obj.getProfile(refreshToken)

def historicalData(token):
    to_date=datetime.now()
    from_date=to_date-timedelta(hours=1)
    time.sleep(0.35)
    try:
        historicParam={"exchange":"NSE",
                       "symboltoken":token,
                       "interval":"FIFTEEN_MINUTE",
                       "fromdate":from_date.strftime("%Y-%m-%d %H:%M"),
                       "todate":to_date.strftime("%Y-%m-%d %H:%M")
                      }
        return(pd.DataFrame(obj.getCandleData(historicParam)['data'],columns=['T','O','H','L','C','V']))
    except Exception as e:
        print("Historical Data Failed: {}".format(e.message))
        
def getLtpData(token, name):
    time.sleep(0.1)          
    return(obj.ltpData('NSE', name, token)['data']['ltp'])

def screen():
    df=pd.read_sql('main',engine)
    for row in df.itertuples():
        if row[3]==0:
            data=historicalData(row[1])
            hl_m=data.iloc[-3]['H']-data.iloc[-3]['L']
            oc_m=abs(data.iloc[-3]['O']-data.iloc[-3]['C'])
            oc_i=abs(data.iloc[-2]['O']-data.iloc[-2]['C'])
            if (oc_m > oc_i)and((2*oc_m)>hl_m)and(data.iloc[-3]['H']>data.iloc[-2]['H'])and(data.iloc[-3]['L']<data.iloc[-2]['L']):
                df1=pd.read_sql('shortlist_ltp',engine)
                df2=pd.read_sql('shortlist_close',engine)
                time=datetime.now()
                if(data.iloc[-3]['C']>data.iloc[-3]['O']):
                    ltp = pd.DataFrame([[row[1],row[2],1,data.iloc[-3]['H'],min((data.iloc[-3]['H']*1.005),(data.iloc[-3]['H']+hl_m)),data.iloc[-2]['L'],time]], columns = ['token', 'stockname', 'value', 'price','target','stoploss','time'])
                    close = pd.DataFrame([[row[1],row[2],-1,data.iloc[-3]['L'],max((data.iloc[-3]['H']*0.995),(data.iloc[-3]['L']-hl_m)),data.iloc[-2]['H'],time]], columns = ['token', 'stockname', 'value', 'price','target', 'stoploss','time'])
                    df1=df1.append(ltp, ignore_index=True)
                    df2=df2.append(close, ignore_index=True)
                else:
                    close = pd.DataFrame([[row[1],row[2],1,data.iloc[-3]['H'],min((data.iloc[-3]['H']*1.005),(data.iloc[-3]['H']+hl_m)),data.iloc[-2]['L'],time]], columns = ['token', 'stockname', 'value', 'price','target', 'stoploss','time'])
                    ltp = pd.DataFrame([[row[1],row[2],-1,data.iloc[-3]['L'],max((data.iloc[-3]['H']*0.995),(data.iloc[-3]['L']-hl_m)),data.iloc[-2]['H'],time]], columns = ['token', 'stockname', 'value', 'price','target', 'stoploss','time'])
                    df1=df1.append(ltp, ignore_index=True)
                    df2=df2.append(close, ignore_index=True)
                df.loc[row[0],'value']=1
                # df1.to_sql('shortlist_ltp',engine,if_exists='replace',index=False)
                # df2.to_sql('shortlist_close',engine,if_exists='replace',index=False)
    print(df)
    # df.to_sql('main',engine,if_exists='replace',index=False)
    
def validate():
    df=pd.read_sql('main',engine)
    df1=pd.read_sql('shortlist_close',engine)
    df2=pd.read_sql('shortlist_ltp',engine)
    for row in df1.itertuples():
        if (datetime.now()-row[7])>timedelta(hours=1):
            df1.drop(row[0],axis=0,inplace=True)
            df2.drop(row[0],axis=0,inplace=True)
            df.loc[row[0],'value']=0
        # df1.to_sql('shortlist_ltp',engine,if_exists='replace',index=False)
        # df2.to_sql('shortlist_close',engine,if_exists='replace',index=False)
        # df.to_sql('main',engine,if_exists='replace',index=False)
 
def entry_ltp():
    df=pd.read_sql('shortlist_ltp',engine)
    for row in df.itertuples():
        ltp=getLtpData(row[1],row[2])
        if(row[3]*ltp>row[3]*row[4]):
            print(row[2],row[3])
            
def entry_close():
    df=pd.read_sql('shortlist_close',engine)
    for row in df.itertuples():
        close=historicalData(row[1]).iloc[-2]['C']
        if(row[3]*close>row[3]*row[4]):
            if(row[3]*(row[5]-close)>row[3]*(close-row[6])):
                print(row[2],row[3])
            else:
                pass
                #remove stock from shortlist_ltp shortlist_close, change data of main
    
def exit():
    pass
