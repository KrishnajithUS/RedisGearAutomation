import time
import json
import pymongo


def list_to_dict(hset_list):
    get_value = {}
    integer_fields = ["transactionType","transactionMode","timeStamp","deviceId","expirationTime","tMsgRecvByServer","tMsgRecvFromDev","audioPlayed","id"]

    for vals in range(0, len(hset_list), 2):
        # convert string to integer
        try :
            if hset_list[vals] in integer_fields:
                hset_list[vals+1] = int(hset_list[vals+1])
        except Exception as e:
            log(f"-----Unable to convert {hset_list[vals+1]} to an integer for Field {hset_list[vals]}")
        get_value[hset_list[vals]] = hset_list[vals+1]
    return get_value



def insertData(x):
    log("isertData executing : ")
    # Get all keys which belongs to transaction
    keys = execute("keys", f"transaction:*")
    log(f"----------No of Keys : {len(keys)}")
    audioCnt = 0
    expiredCnt = 0
    # Get current time
    epoch_time_now = int(time.time())
    start_time = time.time()
    # Get each key in key set
    insert_data_batch_1 = 0
    cnt = 101
    for key in keys:
        cnt -= 1
        # if cnt == 99:
        #     break
        # Convert string to dict
        hset_list = execute("hgetall", key)

        get_value = list_to_dict(hset_list)

        # Get Creation time
        tmsg_recvby_server = get_value.get("tMsgRecvByServer", None)
        device_id = get_value.get("deviceId", None)
        audio = int(get_value.get("audioPlayed"))

        if tmsg_recvby_server is None or not isinstance(tmsg_recvby_server, int):
            log(
                f"tMsgRecvByServer Key Not Found or tMsgRecvByServer is Not an Integer For Key {key}!"
            )
            continue

        if device_id  is None:
            log(f"DeviceId is Missing For Key {key}")
            continue
        
        if audio > 0:
            audioCnt += 1

        exipiry_time_key = tmsg_recvby_server + 60

        # # Compare whether the key is expired
        if exipiry_time_key < epoch_time_now:
            expiredCnt += 1
        # insert_data_batch_1.append(get_value)
        insert_data_batch_1 += 1
        execute("rpush", "temp_list", json.dumps(get_value))
    temp = execute("lrange","temp_list", str(0), str(-1))
    log(f"temp list {type(temp)}")
    for i in range(0, len(temp)):
        temp[i] = json.loads(temp[i])
        if len(temp) == 10000:
            log("data inserting")
            client = pymongo.MongoClient("mongodb://sa:Password123@mongo-1:27017,mongo-2:27017,mongo-3:27017/?replicaSet=rs0")
            db = client["TransactionHistory"]
            collection = db["TransactionExpiredDataCollection"]
            collection.insert_many(temp[:10000])
            del temp[:10000]

    log(f"-------List : {type(temp[0])}------")
    end_time = time.time()
    log(f"time diff :{end_time - start_time}") 
    log(f"audio updated count : {audioCnt}")
    log(f"expired data : {expiredCnt}")
    log(f"len of batch : {insert_data_batch_1}")
    return 



GearsBuilder().map(lambda x : log("passing")).map(lambda x: insertData(x)).run("newperson:*")
