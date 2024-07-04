import redis
import os
import time
import random
import sys
import uuid


SESSION_ID = "transactionRedisDbOperations"


def formatPassFunctionCmd(file_name, requirements):
    req = "REQUIREMENTS"
    cmd = (
        f'docker exec -it redis-gear redis-cli RG.PYEXECUTE  "`cat {file_name}`"  '
    )
    # Add ID & UPGRADE(to prevent duplicate register functions)
    cmd += f"ID {SESSION_ID} UPGRADE "
    for r in requirements:
        req += f" {r}"
    res = cmd + req
    return res


"""
PassFunctionToRedis : Helper function to pass a python file to redis server
GetAllRegistrations : Helps to dump all registered events
UnRegister : To unregister a particular event
RedisLogs : Live redis logs
RedisClient : Redis cli
"""

CMDS = {
    "PassFunctionToRedis": formatPassFunctionCmd,
    "GetAllRegistrations": f"docker exec -it redis-gear redis-cli RG.DUMPREGISTRATIONS",
    "UnRegister": lambda registerId: f"sudo docker exec -it redis-gear redis-cli RG.UNREGISTER {registerId}",
    "RedisLogs": f"sudo docker logs redis-gear -f",
    "RedisClient": f"sudo docker exec -it redis-gear redis-cli",
}


def connect_to_redis():
    try:
        print("connecting to redis....")
        r = redis.Redis(host="0.0.0.0", port=6379, decode_responses=True)
        return r, False
    except Exception as e:
        print("An Error Occured....")
        return str(e), True

# Hashmap functions
def set_dict(key, data_dict, r):
    r.hset(key, mapping=data_dict)



def set_json_value(key, data_dict, r):
    r.json().set(key, "$", data_dict)


def get_dict(key, r):
    return r.hgetall(key)


def list_keys(r, pattern):
    return r.keys(pattern=pattern, decode_responses=True)


def run_redis_cli_cmd(cmd):
    os.system(cmd)


def update_audio_played(key, r):
    r.json().set(key, "$.audioPlayed", random.randint(1, 5))

def update_audio_played_hash(key, r):
    r.hset(key, "audioPlayed", random.randint(1, 5))


def main():
    """ "
    How to run this script ?
    python3 redis_client.py arg
    arg => 1 For passing redis_cron_job.py file to redis
    arg => 2 For inserting data to redis
    arg => 3 For running tests
    """
    r, err = connect_to_redis()
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
            "tests1.py", ["pymongo", "python-dotenv"]
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
        cnt = 1000000
        device_id_lst = []
        while cnt > 0:
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
                "deviceId": 1234567890 + cnt,
                "expirationTime": 1624198696,
                "tMsgRecvByServer": int(time.time()),
                "tMsgRecvFromDev": 1624197701,
                "audioPlayed": random.randint(0, 1),
                "id": 9876543210 + num,
            }
            device_id_lst.append(data["deviceId"])
            # Create a unique key
            unique_key = uuid.uuid4().hex
            set_dict(f"transaction:{unique_key}", data, r)
            temp = cnt
            if temp % 5 == 0 and data["audioPlayed"] <= 0:
                # Update the `audioPlayed` field to a value greater than 0
                update_audio_played_hash(f"transaction:{unique_key}", r)
            cnt -= 1
        total_length = len(device_id_lst)
        distinct_length = len(set(device_id_lst))
        print("No of Duplcate Entries : ", total_length - distinct_length)
        end = time.time()
        print("diff :", end-start)
if __name__ == "__main__":
    main()
