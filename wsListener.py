import redis
from smartapi import SmartConnect, SmartWebSocket
from datetime import datetime

redisConnPool = redis.ConnectionPool(host='localhost', port=6379, db=0)
redisClient = redis.Redis(connection_pool=redisConnPool)

apiKey = "VXdBs3Rp"
username = "P319380"
password = "partha2001"
scInstance = SmartConnect(api_key=apiKey)
session = scInstance.generateSession(username, password)
swsInstance = SmartWebSocket(scInstance.getfeedToken(), username)
task = "mw"
token = "nse_cm|236&nse_cm|467&nse_cm|526&nse_cm|547&nse_cm|694&nse_cm|881&nse_cm|910&nse_cm|1232&nse_cm|1330&nse_cm|1333&nse_cm|1348&nse_cm|1363&nse_cm|1394&nse_cm|1594&nse_cm|1624&nse_cm|1660&nse_cm|1922&nse_cm|2031&nse_cm|2475&nse_cm|2885&nse_cm|3045&nse_cm|3351&nse_cm|3432&nse_cm|3456&nse_cm|3499&nse_cm|3506&nse_cm|3787&nse_cm|4963&nse_cm|5258&nse_cm|5900&nse_cm|7229&nse_cm|10604&nse_cm|10940&nse_cm|11287&nse_cm|11483&nse_cm|11536&nse_cm|11630&nse_cm|11723&nse_cm|13538&nse_cm|14977&nse_cm|15083&nse_cm|16669&nse_cm|20374&nse_cm|21808"

# set the candle duration = 1, 5, 10, 15, ...
candle_duration = 5

# check if current candle name is set, if not - set it!
current_candle_name = redisClient.get("current_candle_name")
if current_candle_name is None:
    today = datetime.now()
    hours = today.hour
    minutes = today.minute - (today.minute % candle_duration)
    redisClient.set("current_candle_name", str(hours) + "_" + str(minutes))


def handle_time(timestamp):
    hours = timestamp[11:13]
    minutes = int(timestamp[14:16])
    seconds = int(timestamp[17:])
    if seconds == 0 and minutes % candle_duration == 0:
        # What happens if due to some reason an important timestamp does not reach our listener ?
        # Similarly, what if the same message reaches us multiple times ?
        # The timestamp will always be slightly late compared to local time
        # What if the message is out of order ?
        current_candle_name = redisClient.get("current_candle_name")
        redisClient.set("previous_candle_name", current_candle_name)
        redisClient.set("current_candle_name", hours + "_" + str(minutes))
        redisClient.lpush("calculate_candle", 1)


def handle_ltp(token, ltp):
    current_candle_name = redisClient.get(
        "current_candle_name").decode('ascii')
    key = "ltp_" + current_candle_name + "_" + token  # Key = ltp_13_15_2885
    output = redisClient.lpush(key, ltp)
    if output == 1:
        redisClient.lpush("keys_for_next_candle", key)


def on_message(ws, message):
    # t = datetime.now().astimezone().isoformat()
    for each in message:
        if "name" in each and each["name"] == "tm":
            # print("{} T {}".format(t, each["tvalue"]))
            handle_time(each["tvalue"])
        elif "name" in each and each["name"] == "sf" and "ltp" in each:
            # print("{} D {} {}".format(t, each.get("tk", "no_tk"), each.get("ltp", "no_ltp")))
            handle_ltp(each["tk"], each["ltp"])


def on_open(ws):
    print("Open")
    swsInstance.subscribe(task, token)


def on_error(ws, error):
    print("Error")
    print(error)


def on_close(ws):
    print("Close")


swsInstance._on_open = on_open
swsInstance._on_message = on_message
swsInstance._on_error = on_error
swsInstance._on_close = on_close

swsInstance.connect()
