import redis
import os
import time
import random
import sys
import uuid
from datetime import datetime
from retrying import retry


SESSION_ID = "transactionRedisDbOperations"


def formatPassFunctionCmd(file_name, requirements):
    req = "REQUIREMENTS"
    cmd = f'docker exec -it redis-gear redis-cli RG.PYEXECUTE  "`cat {file_name}`"  '
    # Add ID & UPGRADE(to prevent duplicate register functions)
    cmd += f"ID {SESSION_ID} UPGRADE "
    for r in requirements:
        req += f" {r}"
    res = cmd + req
    return res


CMDS = {
    "PassFunctionToRedis": formatPassFunctionCmd,
    "GetAllRegistrations": f"sudo docker exec -it redis-gear redis-cli RG.DUMPREGISTRATIONS",
    "UnRegister": lambda registerId: f"sudo docker exec -it redis-gear redis-cli RG.UNREGISTER {registerId}",
    "RedisLogs": f"sudo docker logs redis-gear -f",
    "RedisClient": f"sudo docker exec -it redis-gear redis-cli",
}


@retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def connect_to_redis(redis_pool):
    try:
        print("connecting to redis....")
        r = redis.Redis(connection_pool=redis_pool, port=6379, decode_responses=True)
        return r, False
    except Exception as e:
        print("An Error Occurred....")
        raise e


@retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def set_dict(key, data_dict, r):
    r.hset(key, mapping=data_dict)


@retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def set_json_value(key, data_dict, r):
    r.json().set(key, "$", data_dict)


@retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def get_dict(key, r):
    return r.hgetall(key)


@retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def list_keys(r, pattern):
    return r.keys(pattern=pattern, decode_responses=True)


def run_redis_cli_cmd(cmd):
    os.system(cmd)


@retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def update_audio_played(key, r):
    r.json().set(key, "$.audioPlayed", random.randint(1, 5))


@retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def update_audio_played_hash(key, r):
    r.hset(key, "audioPlayed", random.randint(1, 5))


redis_pool = None


def init():
    global redis_pool
    print("PID %d: initializing redis pool..." % os.getpid())
    redis_pool = redis.ConnectionPool(host='0.0.0.0', port=6379, db=0)


def main():
    """
    How to run this script ?
    python3 redis_client.py arg
    arg => 1 For passing redis_cron_job.py file to redis
    arg => 2 For inserting data to redis
    arg => 3 For running tests
    """
    r, err = connect_to_redis(redis_pool)
    r.keys()
    if err:
        print(f"Error : {r}")
    else:
        print("Connected To Redis")

    args = sys.argv

    n = len(args)

    if n < 2:
        raise Exception("No Argument Found")

    # Pass Python Functions To Redis Gear
    if args[1] == "1":
        print("--------------Passing Function To RedisGear To Execute------------")
        get_cmd = CMDS["PassFunctionToRedis"](
            "redis_gear_functions_with_motor.py", ["pymongo", "motor"]
        )
        run_redis_cli_cmd(get_cmd)

    # SET a single entry to redis
    if args[1] == "2":

        print("--------------Creating A New JSON data in Redis------------")
        num = random.randint(1, 100000)
        data = {
            "reqRefNo": f"REQ123456789{num}",
            "rrn": "RRN987654321",
            "mid": "MERCHANT123",
            "tid": "TERM456",
            "mcc": "1234",
            "meVpa": "merchant@bank",
            "transactionType": 1 + num,
            "transactionMode": 2,
            "txnAmt": "100.50",
            "txnTimestamp": "2024-06-22T12:34:56Z",
            "timeStamp": int(time.time()),
            "deviceId": 1234567890,
            "expirationTime": 1624198696,
            "tMsgRecvByServer": int(time.time()),
            "tMsgRecvFromDev": 1624197701,
            "audioPlayed": 1,
            "id": 9876543210 + num,
        }
        # Create a unique key
        unique_key = uuid.uuid4().hex
        set_json_value(f"transaction:{unique_key}", data, r)

    # TESTS
    if args[1] == "3":
        print("------------------Running Tests----------------------")
        start = time.time()
        times = 100
        device_id_lst = []
        print(f"current time : {datetime.now()}")
        incr = 0
        updated_data_cnt = 0
        for i in range(1):
            cnt = 600000
            while cnt > 0:
                num = random.randint(1, 100000)
                incr += 1
                data = {
                    "reqRefNo": f"REQ123456789{num}",
                    "rrn": "RRN987654321",
                    "mid": "MERCHANT123",
                    "tid": "TERM456",
                    "mcc": "1234",
                    "meVpa": "merchant@bank",
                    "transactionType": 1 + num,
                    "transactionMode": 2,
                    "txnAmt": "100.50",
                    "txnTimestamp": "2024-06-22T12:34:56Z",
                    "timeStamp": int(time.time()),
                    "deviceId": 1234567890,
                    "expirationTime": 1624198696,
                    "tMsgRecvByServer": int(time.time()),
                    "tMsgRecvFromDev": 1624197701,
                    "audioPlayed": random.randint(0, 0),
                    "id": incr,
                }

                # Create a unique key
                unique_key = 1234567890 + incr
                device_id_lst.append(unique_key)
                set_dict(f"transaction:{unique_key}", data, r)
                temp = cnt
                if temp % 5 == 0 and data["audioPlayed"] <= 0:
                    # Update the `audioPlayed` field to a value greater than 0
                    update_audio_played_hash(f"transaction:{unique_key}", r)
                    updated_data_cnt += 1
                cnt -= 1
            total_length = len(device_id_lst)
            distinct_length = len(set(device_id_lst))
            print("No of Duplcate Entries : ", total_length - distinct_length)
            end = time.time()
            print("diff :", end - start)
            print("No of updated data : ", updated_data_cnt)


if __name__ == "__main__":
    init()
    main()
