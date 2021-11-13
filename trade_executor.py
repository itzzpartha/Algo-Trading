import redis
import time
import math
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
        redisClient2.set("token",response["access_token"])

def position_size(price,sl):
    return(math.floor(capital*0.2/abs(price-sl)))

credentials = {
    "client_id":"HDKC07IA7F-100",
    "secret_key":"F0S89MO2H7",
    "redirect_uri":"https://www.google.com",
    "user_id":"XP04072",
    "password":"Ppb@2001",
    "two_fa":"EHAPB8813B"
    }

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

capital = 5000

authorize()

while True:
    date=datetime.now()
    if date.hour>9:
        break

while True:
    length=redisClient4.llen("order_list")
    if length > 0:
        token=(redisClient4.get("token")).decode('ascii')
        fyers = fyersModel.FyersModel(client_id=credentials["client_id"],token=token,log_path="C:\BingeScoop\Log")
        for i in range(length):
            key=(redisClient4.rpop("order_list")).decode('ascii')
            token=key[6:]
            value=int(redisClient4.hget(key,"value"))
            type=(redisClient4.hget(key,"type")).decode('ascii')
            if type=="entry":
                target=redisClient3.hget("shortlist_"+token,"target")
                price=redisClient3.hget("shortlist_"+token,"price")
                sl=redisClient3.hget("shortlist_"+token,"sl")
                quantity=position_size(price,sl)
                data = {
                    "symbol":token,
                    "qty":quantity,
                    "type":2,
                    "side":value,
                    "productType":"INTRADAY",
                    "limitPrice":0,
                    "stopPrice":0,
                    "validity":"DAY",
                    "disclosedQty":0,
                    "offlineOrder":"False",
                    "stopLoss":0,
                    "takeProfit":0
                }
                a=fyers.place_order(data)
                if a["s"]=="ok":
                    data = {
                        "symbol":token,
                        "qty":quantity,
                        "type":1,
                        "side":value*(-1),
                        "productType":"INTRADAY",
                        "limitPrice":target,
                        "stopPrice":0,
                        "validity":"DAY",
                        "disclosedQty":0,
                        "offlineOrder":"False",
                        "stopLoss":0,
                        "takeProfit":0
                    }
                    b=fyers.place_order(data)
                    id=b["id"]
                    redisClient4.set("open_"+token,id)
            elif type=="exit":
                order_id=(redisClient4.get("open_"+token)).decode('ascii')
                data = {
                "id":order_id, 
                "type":2
                }