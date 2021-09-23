from smartapi import SmartConnect
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
    
def getLtpData(token, name):
    time.sleep(0.1)          
    return(obj.ltpData('NSE', name, token)['data']['ltp'])

def entry():
    df=pd.read_sql('shortlist',engine)
    for row in df.itertuples():
        ltp=getLtpData(row[1],row[2])
        if(row[3]*ltp>row[3]*row[4]):
            print(row[2],row[3])
            pop_shortlist(row[1])
            
if __name__=="__main__":
    entry()