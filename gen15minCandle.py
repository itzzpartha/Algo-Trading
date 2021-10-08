import redis
from datetime import datetime

redisConnPool = redis.ConnectionPool(host='localhost', port=6379, db=0)
redisClient = redis.Redis(connection_pool=redisConnPool)

while redisClient.brpop("calculate_candle"):
    print('start: 15min candle')

    previous_candle_name = redisClient.get(
        "previous_candle_name")

    if previous_candle_name is None:
        raise Exception("previous_candle_name is not set!")

    previous_candle_name = previous_candle_name.decode('ascii')

    while True:

        queue_name = redisClient.rpop("keys_for_next_candle")

        if queue_name is None:
            print("keys_for_next_candle is empty!")
            break

        queue_name = queue_name.decode('ascii')

        print("processing:", queue_name)

        try:
            queue_name.index(previous_candle_name)
        except ValueError:
            redisClient.rpush("keys_for_next_candle", queue_name)
            print("returning to queue:", queue_name)
            break

        ltp = redisClient.rpop(queue_name)

        if ltp is None:
            print(queue_name + " is empty!")
            redisClient.delete(queue_name)
            continue

        ltp = float(ltp)
        candle_open = ltp
        candle_high = ltp
        candle_low = ltp
        candle_close = 0

        queue_length = int(redisClient.llen(queue_name))
        for i in range(queue_length):
            ltp = float(redisClient.rpop(queue_name))
            if ltp > candle_high:
                candle_high = ltp
            elif ltp < candle_low:
                candle_low = ltp
        candle_close = ltp

        key = "candle" + queue_name[3:]
        redisClient.hset(key, mapping={
            "open": candle_open,
            "high": candle_high,
            "low": candle_low,
            "close": candle_close
        })
        redisClient.lpush("candles", key)
        redisClient.delete(queue_name)
        print('done:', queue_name)
    print('end: 15min candle')
