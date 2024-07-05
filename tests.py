import time
import json




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
        # cnt -= 1
        # if cnt == 0:
        #     break
        # Convert string to dict
        get_value = json.loads(execute("json.get", key))


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
        execute("rpush", "temp_list", str(get_value))
    temp = execute("lrange","temp_list", str(0), str(-1))
    log(f"-------List : {len(temp)}------")
    end_time = time.time()
    log(f"time diff :{end_time - start_time}") 
    log(f"audio updated count : {audioCnt}")
    log(f"expired data : {expiredCnt}")
    log(f"len of batch : {insert_data_batch_1}")
    return 



GearsBuilder().map(lambda x : log("passing")).map(lambda x: insertData(x)).run("newperson:*")
