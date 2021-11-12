from datetime import datetime
import redis

redisConnPool2 = redis.ConnectionPool(host='localhost', port=6379, db=2)
redisConnPool3 = redis.ConnectionPool(host='localhost', port=6379, db=3)
redisConnPool4 = redis.ConnectionPool(host='localhost', port=6379, db=4)
redisClient2 = redis.Redis(connection_pool=redisConnPool2)
redisClient3 = redis.Redis(connection_pool=redisConnPool3)
redisClient4 = redis.Redis(connection_pool=redisConnPool4)

fyers_angel_mapping = {
    "NSE:BAJAJ-AUTO-EQ":"16669",
    "NSE:EICHERMOT-EQ":"910",
    "NSE:HEROMOTOCO-EQ":"1348",
    "NSE:M&M-EQ":"2031",
    "NSE:TATAMOTORS-EQ":"3456",
    "NSE:AXISBANK-EQ":"5900",
    "NSE:HDFCBANK-EQ":"1333",
    "NSE:ICICIBANK-EQ":"4963",
    "NSE:INDUSINDBK-EQ":"5258",
    "NSE:KOTAKBANK-EQ":"1922",
    "NSE:SBIN-EQ":"3045",
    "NSE:BPCL-EQ":"526",
    "NSE:IOC-EQ":"1624",
    "NSE:NTPC-EQ":"11630",
    "NSE:ONGC-EQ":"2475",
    "NSE:POWERGRID-EQ":"14977",
    "NSE:RELIANCE-EQ":"2885",
    "NSE:HDFC-EQ":"1330",
    "NSE:HDFCLIFE-EQ":"467",
    "NSE:SBILIFE-EQ":"21808",
    "NSE:BRITANNIA-EQ":"547",
    "NSE:HINDUNILVR-EQ":"1394",
    "NSE:ITC-EQ":"1660",
    "NSE:TATACONSUM-EQ":"3432",
    "NSE:HCLTECH-EQ":"7229",
    "NSE:INFY-EQ":"1594",
    "NSE:TCS-EQ":"11536",
    "NSE:TECHM-EQ":"13538",
    "NSE:WIPRO-EQ":"3787",
    "NSE:COALINDIA-EQ":"20374",
    "NSE:HINDALCO-EQ":"1363",
    "NSE:JSWSTEEL-EQ":"11723",
    "NSE:TATASTEEL-EQ":"3499",
    "NSE:CIPLA-EQ":"694",
    "NSE:DIVISLAB-EQ":"10940",
    "NSE:DRREDDY-EQ":"881",
    "NSE:SUNPHARMA-EQ":"3351"
}

while True:
    length=redisClient3.llen("shorlisted_stocks")
    if length==0:
        continue
    for i in range(length):
        token=(redisClient3.rpop("shortlisted_stocks")).decode('ascii')
        value=int(redisClient3.hget("shortlist_"+token,"value"))
        price=float(redisClient3.hget("shortlist_"+token,"price"))
        ltp=float(redisClient2.get("ltp_"+fyers_angel_mapping[token]))
        if value*ltp>value*price:
            key="order_"+token
            redisClient4.hset(key,mapping={
                "value":value,
                "type":"entry"
            })
            redisClient4.lpush("order_list",key)
            redisClient3.lpush("open_positions",token)
        else:
            redisClient3.lpush("shortlisted_stocks",token)
    if datetime.now().hour<=14:
        continue
    break

