import time
import pymongo
import json
import os
from threading import Lock
from motor.motor_asyncio import AsyncIOMotorClient

config = {
    "JOB_INTERVAL": int(os.environ["JOB_INTERVAL"]),
    "PREFIX": os.environ["PREFIX"],
    "MULTIPLIER": int(os.environ["MULTIPLIER"]),
    "CONN_STRING": os.environ["CONN_STRING"],
    "COLLECTION_NAME": os.environ["COLLECTION_NAME"],
    "DB_NAME": os.environ["DB_NAME"],
    "RETRY_CONN_COUNT": int(os.environ["RETRY_CONN_COUNT"]),
}

JOB_INTERVAL = config["JOB_INTERVAL"]
PREFIX = config["PREFIX"]



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
        self.conn_string = config["CONN_STRING"]
        self.client = AsyncIOMotorClient(self.conn_string)
        self.collection_name = config["COLLECTION_NAME"]
        self.db_name = config["DB_NAME"]
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
    integer_fields = [
        "transactionType",
        "transactionMode",
        "timeStamp",
        "deviceId",
        "expirationTime",
        "tMsgRecvByServer",
        "tMsgRecvFromDev",
        "audioPlayed",
        "id",
    ]

    for vals in range(0, len(hset_list), 2):
        try:
            if hset_list[vals] in integer_fields:
                hset_list[vals + 1] = int(hset_list[vals + 1])
        except Exception as e:
            log(
                f"-----Unable to convert {hset_list[vals + 1]} to an integer for Field {hset_list[vals]}"
            )
        get_value[hset_list[vals]] = hset_list[vals + 1]
    return get_value


async def upsert_data(collection, key):
    exists = int(execute("exists", key))

    if exists == 0:
        return False

    hset_list = execute("hgetall", key)
    get_value = list_to_dict(hset_list)

    is_audio_played = get_value.get("audioPlayed", None)
    id = get_value.get("id", None)
    device_id = get_value.get("deviceId", None)

    if is_audio_played <= 0:
        global JOB_INTERVAL
        await asyncio.sleep(300)
        log(f"-----------Timer Finished For Key {key}-----------")
        # check whether the key exists after the sleep
        exists = int(execute("exists", key))
        if exists == 0:
            return False

    log(f"-----------{key} is being processed-----------")
    try:
        # Only process key at once
        newKey = f"__{key}__{key}"
        execute("RENAME", key, newKey)
        result = await collection.update_one(
            {"id": id, "deviceId": device_id}, {"$set": get_value}, upsert=True
        )
    except Exception as e:
        log(f"------Error------ : {e}")
        return False

    if result.upserted_id is not None:
        log(f"-----------Inserted data: key {key}-----------")
    else:
        log(f"-----------Updated data: key {key}-----------")

    execute("del", newKey)
    log("Data inserted into MongoDB and deleted from Redis")
    return True


def write_data_to_db(data):
    dbConnObj = DBConnect()
    log("-----------Transaction For Inserting Updated Data to DB-----------")

    key = data.get("key")

    client = dbConnObj.get_db_client
    db = client[dbConnObj.get_database_name]
    collection = db[dbConnObj.get_collection_name]

    return upsert_data(collection, key)


gbUpdate = GB()
gbUpdate.foreach(
    lambda x: log("Setting Up the Coroutine For Data processing......")
).foreach(lambda x: write_data_to_db(x)).register(
    prefix=f"{PREFIX}:*",
    eventTypes=["hset", "hmset"],
    readValue=False,
)
