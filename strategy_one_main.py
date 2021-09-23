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

def pop_shortlist(token):
    df=pd.read_sql('shortlist',engine)
    for row in df.itertuples():
        if row[1]==token:
            df.drop(row[0], axis=0, inplace=True)
    df.to_sql('shortlist',engine,if_exists='replace',index=False)
    
def update_main_value(token,value):
    main=pd.read_sql('main',engine)
    for row in main.itertuples():
        if row[1]==token:
            main.loc[row[0],'value']=value
    main.to_sql('main',engine,if_exists='replace',index=False)

def historicalData(token):
    to_date=datetime.now()
    from_date=to_date-timedelta(days=1)
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
        
def screen():
    df=pd.read_sql('main',engine)
    tdf=pd.read_sql('shortlist',engine)
    for row in df.itertuples():
        if row[3]==0:
            data=historicalData(row[1])
            HL=data.iloc[-3]['H']+data.iloc[-3]['L']
            hl=data.iloc[-2]['H']+data.iloc[-2]['L']
            hl_m=data.iloc[-3]['H']-data.iloc[-3]['L']
            oc_m=abs(data.iloc[-3]['O']-data.iloc[-3]['C'])
            oc_i=abs(data.iloc[-2]['O']-data.iloc[-2]['C'])
            if (oc_m > oc_i)and((2*oc_m)>hl_m)and(data.iloc[-3]['H']>=data.iloc[-2]['H'])and(data.iloc[-3]['L']<=data.iloc[-2]['L'])and(hl_m>=1.005*data.iloc[-3]['O']):
                if(data.iloc[-3]['C']>data.iloc[-3]['O'])and(HL<=hl):
                    ltp = pd.DataFrame([[row[1],row[2],1,data.iloc[-3]['H'],data.iloc[-3]['H']+hl_m,data.iloc[-2]['L'],data.iloc[-3]['L']]], columns = ['token', 'stockname', 'value', 'price','target','stoploss','threshold'])
                elif(data.iloc[-3]['C']<data.iloc[-3]['O'])and(HL>=hl):                
                    ltp = pd.DataFrame([[row[1],row[2],-1,data.iloc[-3]['L'],data.iloc[-3]['L']-hl_m,data.iloc[-2]['H'],data.iloc[-3]['H']]], columns = ['token', 'stockname', 'value', 'price','target', 'stoploss','threshold'])
                tdf=tdf.append(ltp, ignore_index=True)
                df.loc[row[0],'value']=1            
    tdf.to_sql('shortlist',engine,if_exists='replace',index=False)
    df.to_sql('main',engine,if_exists='replace',index=False)

def validate():
    df=pd.read_sql('shortlist',engine)
    for row in df.itertuples():
        close=historicalData(row[1]).iloc[-2]['C']
        if (row[3]==1 and close<row[7])or(row[3]==-1 and close>row[7]):            
            pop_shortlist(row[1])
            update_main_value(row[1],0)   
            
def exit_target():
    data=pd.read_sql('open_positions',engine)
    for row in data.itertuples():
        high=historicalData(row[1]).iloc[-2]['H']
        low=historicalData(row[1]).iloc[-2]['L']
        if ((row[3]==-1)and(low<row[4]))or((row[3]==1)and(high>row[4])):
            update_main_value(row[1],0)
            print("TARGET Was Hit On : "+row[2])      
            data.drop(row[0], axis=0, inplace=True)
    data.to_sql('open_positions',engine,if_exists='replace',index=False)
            
def exit_stoploss():
    data=pd.read_sql('open_positions',engine)
    for row in data.itertuples():
        close=historicalData(row[1]).iloc[-2]['C']
        if(row[3]*close<row[3]*row[5]):
            update_main_value(row[1],0)
            print("SL Was Hit On : "+row[2])
            data.drop(row[0], axis=0, inplace=True)
    data.to_sql('open_positions',engine,if_exists='replace',index=False)

if __name__=="__main__":
    screen()
    validate()
    exit_target()
    exit_stoploss()


