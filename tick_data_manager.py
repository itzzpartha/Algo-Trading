import redis
from smartapi import SmartConnect, SmartWebSocket

def on_message(ws, message):
    msg=message[0]
    if "tk" in msg:
        if "ltp" in msg:
            redisClient2.set("ltp_"+msg['tk'],msg['ltp'])
        if "ap" in msg:
            redisClient2.set("vwap_"+msg['tk'],msg["ap"])

def on_open(ws):
    swsInstance.subscribe(task, token)

def on_error(ws, error):
    pass

def on_close(ws):
    pass

redisConnPool2 = redis.ConnectionPool(host='localhost', port=6379, db=2)
redisClient2 = redis.Redis(connection_pool=redisConnPool2)

apiKey = "VXdBs3Rp"
username = "P319380"
password = "partha2001"
scInstance = SmartConnect(api_key=apiKey)
session = scInstance.generateSession(username, password)
swsInstance = SmartWebSocket(scInstance.getfeedToken(), username)
task = "mw"
token = "nse_cm|16669&nse_cm|910&nse_cm|1348&nse_cm|2031&nse_cm|3456&nse_cm|5900&nse_cm|1333&nse_cm|4963&nse_cm|5258&nse_cm|1922&nse_cm|3045&nse_cm|526&nse_cm|1624&nse_cm|11630&nse_cm|2475&nse_cm|14977&nse_cm|2885&nse_cm|1330&nse_cm|467&nse_cm|21808&nse_cm|547&nse_cm|1394&nse_cm|1660&nse_cm|3432&nse_cm|7229&nse_cm|1594&nse_cm|11536&nse_cm|13538&nse_cm|3787&nse_cm|20374&nse_cm|1363&nse_cm|11723&nse_cm|3499&nse_cm|694&nse_cm|10940&nse_cm|881&nse_cm|3351"

swsInstance._on_open = on_open
swsInstance._on_message = on_message
swsInstance._on_error = on_error
swsInstance._on_close = on_close
swsInstance.connect()