import time
import pymongo
import json
import os
from threading import Lock
from motor.motor_asyncio import AsyncIOMotorClient

config = {
    "JOB_INTERVAL": int(os.environ['JOB_INTERVAL']),
    "PREFIX": os.environ['PREFIX'],
    "MOVEMENT_TIME": int(os.environ['MOVEMENT_TIME']),
    "MULTIPLIER": int(os.environ['MULTIPLIER']),
    "CONN_STRING": os.environ['CONN_STRING'],
    "COLLECTION_NAME": os.environ['COLLECTION_NAME'],
    "DB_NAME": os.environ['DB_NAME'],
    "RETRY_CONN_COUNT": int(os.environ['RETRY_CONN_COUNT']),
}

JOB_INTERVAL = config['JOB_INTERVAL']
PREFIX = config['PREFIX']
MOVEMENT_TIME = config['MOVEMENT_TIME']
EXPIRY_TIME = MOVEMENT_TIME * config['MULTIPLIER']


class MyMutex():
    def __init__(self):
        self.mutex = Lock()

    def __enter__(self):
        self.mutex.acquire()
        return self

    def __exit__(self, type, value, traceback):
        self.mutex.release()

    def __getstate__(self):
        return {}

mutex = None

class DBConnect:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DBConnect, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return
        self.conn_string = config['CONN_STRING']
        self.client = AsyncIOMotorClient(self.conn_string)
        self.collection_name = config['COLLECTION_NAME']
        self.db_name = config['DB_NAME']
        self._initialized = True

    @property
    def get_db_client(self):
        return self.client

    @property
    def get_collection_name(self):
        return self.collection_name

    @property
    def get_database_name(self):
        return self.db_name

    
def list_to_dict(hset_list):
    get_value = {}
    integer_fields = ["transactionType", "transactionMode", "timeStamp", "deviceId", "expirationTime", "tMsgRecvByServer", "tMsgRecvFromDev", "audioPlayed", "id"]

    for vals in range(0, len(hset_list), 2):
        try:
            if hset_list[vals] in integer_fields:
                hset_list[vals + 1] = int(hset_list[vals + 1])
        except Exception as e:
            log(f"-----Unable to convert {hset_list[vals + 1]} to an integer for Field {hset_list[vals]}")
        get_value[hset_list[vals]] = hset_list[vals + 1]
    return get_value

async def update_data(collection, key, device_id, tmsg_recvby_server, get_value):
    exists = int(execute("exists", key))
    if exists:
        with mutex:
            log(f"------audioPlayed is updated for key {key}------")
            if await collection.count_documents({"tMsgRecvByServer": tmsg_recvby_server, "DeviceId": device_id}, limit=1) != 0:
                log("----------Document Already Exist With This tMsgRecvByServer")
            else:
                log(f"------Inserting data: key {key}------")
                await collection.insert_one(get_value)
            execute("del", key)
            log("Data inserted into MongoDB and deleted from Redis")
            return True
    return False

async def insert_data(collection, key, device_id, tmsg_recvby_server, get_value):
    # asyncio.sleep(200)
    exists = int(execute("exists", key))
    if exists:
        with mutex:
            log(f"------{key} is Expired, Inserting Data To MongoDB------")
            if await collection.count_documents({"tMsgRecvByServer": tmsg_recvby_server, "DeviceId": device_id}, limit=1) != 0:
                log("----------Document Already Exist With This tMsgRecvByServer")
            else:
                log(f"------Inserting data: key {key}------")
                await collection.insert_one(get_value)
            execute("del", key)
            log("Data inserted into MongoDB and deleted from Redis")
            return True
    return False

def write_data_to_db(data):
    dbConnObj = DBConnect()
    log("-----------Transaction For Inserting Updated Data to DB-----------")
    key = data.get("key")
    hset_list = execute("hgetall", key)

    get_value = list_to_dict(hset_list)

    is_audio_played = get_value.get("audioPlayed", None)
    tmsg_recvby_server = get_value.get("tMsgRecvByServer", None)
    device_id = get_value.get("deviceId", None)

    client = dbConnObj.get_db_client
    db = client[dbConnObj.get_database_name]
    collection = db[dbConnObj.get_collection_name]

    if is_audio_played is None or not isinstance(is_audio_played, int):
        log("audioPlayed Key Not Found or audioPlayed is Not an Integer!")
    elif tmsg_recvby_server is None or not isinstance(tmsg_recvby_server, int):
        log("tMsgRecvByServer Key Not Found or tMsgRecvByServer is Not an Integer!")
    elif is_audio_played > 0:
        log("---------------Entering update_data---------------")
        return update_data(collection, key, device_id, tmsg_recvby_server, get_value)
    else:
        log("---------------Entering insert_data---------------")
        return insert_data(collection, key, device_id, tmsg_recvby_server, get_value)


def initializeMutex():
    global mutex
    mutex = MyMutex()

    
gbUpdate = GB()
gbUpdate.foreach(
    lambda x: log("Setting Up the Coroutine For Data processing......")
).foreach(lambda x: write_data_to_db(x)).register(
    prefix=f"{PREFIX}:*",
    eventTypes=["hset", "hmset"],
    readValue=False,
    onRegistered=initializeMutex
)
