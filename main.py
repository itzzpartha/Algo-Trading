import redis
import time
from datetime import datetime
from fyers_api import accessToken,fyersModel
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def authorize():
    session=accessToken.SessionModel(client_id=credentials["client_id"],
    secret_key=credentials["secret_key"],redirect_uri=credentials["redirect_uri"],
    response_type="code", grant_type="authorization_code")
    response = session.generate_authcode()
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver=webdriver.Chrome(chrome_options=chrome_options)
    driver.get(response)
    if WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//div[@class="container login-main-start"]'))):
        driver.find_element_by_xpath("//input[@id='fyers_id']").send_keys(credentials["user_id"])
        driver.find_element_by_xpath("//input[@id='password']").send_keys(credentials["password"])
        driver.find_element_by_xpath("//input[@id='pancard']").send_keys(credentials["two_fa"])
        driver.find_element_by_xpath("//button[@id='btn_id']").click()
        time.sleep(5)
        current_url=driver.current_url
        driver.close()
        auth_code=current_url[(current_url.index("&auth_code=")+11):current_url.index("&state=")]
        session.set_token(auth_code)
        response = session.generate_token()
        redisClient0.set("token",response["access_token"])

def exit_stoploss(time):
    length=redisClient3.llen("open_positions")
    for i in range(length):
        token=(redisClient3.rpop("open_positions")).decode('ascii')
        close=float(redisClient1.hget(token+"_"+str(time),"close"))
        value=int(redisClient3.hget("shortlist_"+token,"value"))
        sl=float(redisClient3.hget("shortlist_"+token,"sl"))
        if value*close<value*sl:
            redisClient3.delete("shortlist_"+str(token))
            key="order_"+token
            redisClient4.hset(key,"type","exit")
            redisClient4.lpush("order_list",key)
        else:
            redisClient3.lpush("open_positions",token)

def forceful_exit():
    length=redisClient1.llen("open_positions")
    for i in range(length):
        token=(redisClient1.rpop("open_positions")).decode('ascii')
        key="order_"+token
        redisClient2.hset(key,"type","exit")
        redisClient2.lpush("order_list",key)

def generate_signal(mb_time,ib_time,sector,shortlist):
    sc_mb_close,sc_ib_open,sc_ib_close,sc_range_high,sc_range_low=get_mb_ib_sectoral(mb_time,ib_time,sector)
    for token in shortlist:
        if redisClient0.hget("switch",token)==0:
            range_high=float(redisClient0.hget("range_high",token))
            range_low=float(redisClient0.hget("range_low",token))
            mb_open,mb_high,mb_low,mb_close,ib_open,ib_high,ib_low,ib_close,vwap=get_mb_ib_stock(mb_time,ib_time,token)
            HL = mb_high+mb_low
            hl = ib_high+ib_low
            OC = mb_open+mb_close
            oc = ib_open+ib_close
            hl_m = mb_high-mb_low
            hl_i = ib_high-ib_low
            oc_m = abs(mb_open-mb_close)
            oc_i = abs(ib_open-ib_close)
            if (oc_m>3*oc_i)and(3*oc_m>2*hl_m)and(mb_high>ib_high)and(mb_low<ib_low)and(oc_m>=0.005*mb_open):
                if (mb_close>mb_open)and(HL<hl)and(OC<oc)and(ib_open>(mb_high-hl_m/3))and(ib_close>(mb_high-hl_m/3)):
                    if (mb_close>range_high)and(ib_open>range_high)and(ib_close>range_high)and(sc_mb_close>sc_range_high)and(sc_ib_open>sc_range_high)and(sc_ib_close>sc_range_high):
                        if (mb_close>vwap)and(ib_open>vwap)and(ib_close>vwap):
                            target=round(min(mb_high+hl_m,mb_high+mb_open*0.01),2)
                            if 2*hl_i>=hl_m:
                                sl=ib_low
                            else:
                                sl=mb_low
                            if (target-mb_high)>(mb_high-sl):
                                redisClient0.hset("switch",str(token),1)
                                redisClient3.lpush("shortlisted_stocks",token)
                                redisClient3.hset("shortlist_"+str(token), mapping={
                                    "value": 1,
                                    "price": mb_high,
                                    "target": target,
                                    "sl":sl
                                })
                elif (mb_close<mb_open)and(HL>hl)and(OC>oc)and(ib_open<(mb_low+hl_m/3))and(ib_close<(mb_low+hl_m/3)):
                    if (mb_close<range_low)and(ib_open<range_low)and(ib_close<range_low)and(sc_mb_close<sc_range_low)and(sc_ib_open<sc_range_low)and(sc_ib_close<sc_range_low):
                        if (mb_close<vwap)and(ib_open<vwap)and(ib_close<vwap):
                            target=round(max(mb_low-hl_m,mb_low-mb_open*0.01),2)
                            if 2*hl_i>=hl_m:
                                sl=ib_high
                            else:
                                sl=mb_high
                            if (mb_low-target)>(sl-mb_low):
                                redisClient0.hset("switch",str(token),1)
                                redisClient3.lpush("shortlisted_stocks",token)
                                redisClient3.hset("shortlist_"+str(token), mapping={
                                    "value": -1,
                                    "price": mb_low,
                                    "target": target,
                                    "sl":sl 
                                })

def get_mb_ib_sectoral(mb_time,ib_time,sector):
    mb_close=float(redisClient1.hget(sector+"_"+str(mb_time),"close"))
    ib_open=float(redisClient1.hget(sector+"_"+str(ib_time),"open"))
    ib_close=float(redisClient1.hget(sector+"_"+str(ib_time),"close"))
    range_high=float(redisClient0.hget("range_high", sector))
    range_low=float(redisClient0.hget("range_low", sector))
    return mb_close,ib_open,ib_close,range_high,range_low

def get_mb_ib_stock(mb_time,ib_time,token):
    mb_open=float(redisClient1.hget(token+"_"+str(mb_time),"open"))
    mb_high=float(redisClient1.hget(token+"_"+str(mb_time),"high"))
    mb_low=float(redisClient1.hget(token+"_"+str(mb_time),"low"))
    mb_close=float(redisClient1.hget(token+"_"+str(mb_time),"close"))
    ib_open=float(redisClient1.hget(token+"_"+str(ib_time),"open"))
    ib_high=float(redisClient1.hget(token+"_"+str(ib_time),"high"))
    ib_low=float(redisClient1.hget(token+"_"+str(ib_time),"low"))
    ib_close=float(redisClient1.hget(token+"_"+str(ib_time),"close"))
    vwap=float(redisClient2.hget("vwap",token))
    return mb_open,mb_high,mb_low,mb_close,ib_open,ib_high,ib_low,ib_close,vwap

def initialize_switch():
    for token in stock_list:
        redisClient0.hset("switch",token,0)

def screen(mb_time,ib_time):
    generate_signal(mb_time,ib_time,"NSE:NIFTYAUTO-INDEX",nifty_auto)
    generate_signal(mb_time,ib_time,"NSE:NIFTYBANK-INDEX",nifty_bank)
    generate_signal(mb_time,ib_time,"NSE:NIFTYENERGY-INDEX",nifty_energy)
    generate_signal(mb_time,ib_time,"NSE:NIFTYFINSERVICE-INDEX",nifty_finance)
    generate_signal(mb_time,ib_time,"NSE:NIFTYFMCG-INDEX",nifty_fmcg)
    generate_signal(mb_time,ib_time,"NSE:NIFTYIT-INDEX",nifty_it)
    generate_signal(mb_time,ib_time,"NSE:NIFTYMETAL-INDEX",nifty_metal)
    generate_signal(mb_time,ib_time,"NSE:NIFTYPHARMA-INDEX",nifty_pharma)

def validate(time):
    length=redisClient3.llen("shortlisted_stocks")
    for i in range(length):
        token=(redisClient3.rpop("shortlisted_stocks")).decode('ascii')
        close=float(redisClient1.hget(token+"_"+str(time),"close"))
        value=int(redisClient3.hget("shortlist_"+token,"value"))
        sl=float(redisClient3.hget("shortlist_"+token,"sl"))
        if value*close>=value*sl:
            redisClient3.lpush("shortlisted_stocks",token)
        else:
            redisClient3.delete("shortlist_"+token)

credentials = {
    "client_id":"HDKC07IA7F-100",
    "secret_key":"F0S89MO2H7",
    "redirect_uri":"https://www.google.com",
    "user_id":"XP04072",
    "password":"Ppb@2001",
    "two_fa":"EHAPB8813B"
    }

nifty_auto = ["NSE:BAJAJ-AUTO-EQ","NSE:EICHERMOT-EQ","NSE:HEROMOTOCO-EQ","NSE:M&M-EQ","NSE:TATAMOTORS-EQ"]
nifty_bank = ["NSE:AXISBANK-EQ","NSE:HDFCBANK-EQ","NSE:ICICIBANK-EQ","NSE:INDUSINDBK-EQ","NSE:KOTAKBANK-EQ","NSE:SBIN-EQ"]
nifty_energy = ["NSE:BPCL-EQ","NSE:IOC-EQ","NSE:NTPC-EQ","NSE:ONGC-EQ","NSE:POWERGRID-EQ","NSE:RELIANCE-EQ"]
nifty_finance = ["NSE:HDFC-EQ","NSE:HDFCLIFE-EQ","NSE:SBILIFE-EQ"]
nifty_fmcg = ["NSE:BRITANNIA-EQ","NSE:HINDUNILVR-EQ","NSE:ITC-EQ","NSE:TATACONSUM-EQ"]
nifty_it = ["NSE:HCLTECH-EQ","NSE:INFY-EQ","NSE:TCS-EQ","NSE:TECHM-EQ","NSE:WIPRO-EQ"]
nifty_metal = ["NSE:COALINDIA-EQ","NSE:HINDALCO-EQ","NSE:JSWSTEEL-EQ","NSE:TATASTEEL-EQ"]
nifty_pharma = ["NSE:CIPLA-EQ","NSE:DIVISLAB-EQ","NSE:DRREDDY-EQ","NSE:SUNPHARMA-EQ"]

stock_list=[*nifty_auto, *nifty_bank, *nifty_energy, *nifty_finance, *nifty_fmcg, *nifty_it, *nifty_metal, *nifty_pharma]
index_list = ["NSE:NIFTYAUTO-INDEX","NSE:NIFTYBANK-INDEX","NSE:NIFTYENERGY-INDEX","NSE:NIFTYFINSERVICE-INDEX","NSE:NIFTYFMCG-INDEX","NSE:NIFTYIT-INDEX","NSE:NIFTYMETAL-INDEX","NSE:NIFTYPHARMA-INDEX"]

candle_duration=900

redisConnPool0 = redis.ConnectionPool(host='localhost', port=6379, db=0)
redisConnPool1 = redis.ConnectionPool(host='localhost', port=6379, db=1)
redisConnPool2 = redis.ConnectionPool(host='localhost', port=6379, db=2)
redisConnPool3 = redis.ConnectionPool(host='localhost', port=6379, db=3)
redisConnPool4 = redis.ConnectionPool(host='localhost', port=6379, db=4)
redisClient0 = redis.Redis(connection_pool=redisConnPool0)
redisClient1 = redis.Redis(connection_pool=redisConnPool1)
redisClient2 = redis.Redis(connection_pool=redisConnPool2)
redisClient3 = redis.Redis(connection_pool=redisConnPool3)
redisClient4 = redis.Redis(connection_pool=redisConnPool4)

authorize()
initialize_switch()

while True:
    date=datetime.now()
    if (date.hour>9) or (date.hour==9 and date.minute>30):
        time_now=round(time.time())
        access_token=(redisClient0.get("token")).decode('ascii')
        fyers = fyersModel.FyersModel(client_id=credentials["client_id"],token=access_token,log_path="C:\BingeScoop\Log")
        range_from=time_now-candle_duration-time_now%candle_duration
        range_to=time_now
        for token in stock_list:
            data={"symbol":token,"resolution":"15","date_format":"0","range_from":range_from,"range_to":range_to,"cont_flag":"1"}
            candle_data=fyers.history(data)
            time.sleep(0.1)
            open=candle_data['candles'][0][1]
            high=candle_data['candles'][0][2]
            low=candle_data['candles'][0][3]
            close=candle_data['candles'][0][4]
            vwap=redisClient2.get("vwap_"+token)
            redisClient1.hset(token+"_"+str(range_from),mapping={
                "open":open,
                "high":high,
                "low":low,
                "close":close,
                "vwap":vwap
            })
            redisClient0.hset("range_high",token,high)
            redisClient0.hset("range_low",token,low)    
        for token in index_list:
            data={"symbol":token,"resolution":"15","date_format":"0","range_from":range_from,"range_to":range_to,"cont_flag":"1"}
            candle_data=fyers.history(data)
            time.sleep(0.1)
            open=candle_data['candles'][0][1]
            high=candle_data['candles'][0][2]
            low=candle_data['candles'][0][3]
            close=candle_data['candles'][0][4]
            redisClient1.hset(token+"_"+str(range_from),mapping={
                "open":open,
                "high":high,
                "low":low,
                "close":close
            })
            redisClient0.hset("range_high",token,high)
            redisClient0.hset("range_low",token,low)
        break

while True:
    time_now=round(time.time())
    if time_now%candle_duration==5:
        access_token=(redisClient0.get("token")).decode('ascii')
        fyers = fyersModel.FyersModel(client_id=credentials["client_id"],token=access_token,log_path="C:\BingeScoop\Log")
        range_from=time_now-candle_duration-5
        range_to=time_now
        for token in stock_list:
            data={"symbol":token,"resolution":"15","date_format":"0","range_from":range_from,"range_to":range_to,"cont_flag":"1"}
            candle_data=fyers.history(data)
            time.sleep(0.1)
            open=candle_data['candles'][0][1]
            high=candle_data['candles'][0][2]
            low=candle_data['candles'][0][3]
            close=candle_data['candles'][0][4]
            vwap=redisClient2.get("vwap_"+token)
            redisClient1.hset(token+"_"+str(range_from),mapping={
                "open":open,
                "high":high,
                "low":low,
                "close":close,
                "vwap":vwap
            })
        for token in index_list:
            data={"symbol":token,"resolution":"15","date_format":"0","range_from":range_from,"range_to":range_to,"cont_flag":"1"}
            candle_data=fyers.history(data)
            time.sleep(0.1)
            open=candle_data['candles'][0][1]
            high=candle_data['candles'][0][2]
            low=candle_data['candles'][0][3]
            close=candle_data['candles'][0][4]
            redisClient1.hset(token+"_"+str(range_from),mapping={
                "open":open,
                "high":high,
                "low":low,
                "close":close
            })
        if datetime.now().hour>9:
            ib_time=range_from
            mb_time=ib_time-candle_duration
            screen(mb_time,ib_time)
            validate(ib_time)
            exit_stoploss(ib_time)
        if datetime.now().hour==15 and datetime.now().minute>=15:
            forceful_exit()
        else:
            continue
        break