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
redisClient0 = redis.Redis(connection_pool=redisConnPool0)
redisClient1 = redis.Redis(connection_pool=redisConnPool1)
redisClient2 = redis.Redis(connection_pool=redisConnPool2)

authorize()

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
        if datetime.now().hour!=15 or datetime.now().minute!=15:
            redisClient0.set("generate_signal",time_now)
            continue
        break
