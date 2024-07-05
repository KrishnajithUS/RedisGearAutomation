import time
import pymongo
import json
import os


config = {
    "JOB_INTERVAL":int(os.environ['JOB_INTERVAL']),
    "PREFIX":os.environ['PREFIX'],
    "MOVEMENT_TIME":int(os.environ['MOVEMENT_TIME']),
    "MULTIPLIER":int(os.environ['MULTIPLIER']),
    "CONN_STRING":os.environ['CONN_STRING'],
    "COLLECTION_NAME":os.environ['COLLECTION_NAME'],
    "DB_NAME":os.environ['DB_NAME'],
    "RETRY_CONN_COUNT":int(os.environ['RETRY_CONN_COUNT']),

}

JOB_INTERVAL = config['JOB_INTERVAL']
PREFIX = config['PREFIX']
MOVEMENT_TIME = config['MOVEMENT_TIME']
EXPIRY_TIME = MOVEMENT_TIME*config['MULTIPLIER']



class DBConnect:
    """

    This class implements Singleton Pattern

    """

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
        self.client = None
        self.collection_name = config['COLLECTION_NAME']
        self.db_name = config['DB_NAME']
        self.RETRY_CONN_COUNT = config['RETRY_CONN_COUNT']
        self._initialized = True

    def connect_to_db(self) -> bool:
        if self.client is None:
            while self.RETRY_CONN_COUNT > 0:
                try:
                    self.client = pymongo.MongoClient(self.conn_string)
                    db = self.client[self.db_name]
                    self.collection = db[self.collection_name]
                    log("Connected to DB")
                    return True
                except Exception as e:
                    log(f"-----An Error Occurred During DB connection-----")
                    log(f"Error : {str(e)}")
                    log(f"Retrying connection after 5 seconds")
                    time.sleep(5)
                    self.RETRY_CONN_COUNT -= 1
            log(f"-----DB connection failed-----")
            return False
        return True

    @property
    def get_db_client(self):
        return self.client

    @property
    def get_collection_name(self):
        return self.collection_name

    @property
    def get_database_name(self):
        return self.db_name

def store_expired_data(data=None):
    if data:
        dbConnObj = DBConnect()
        res = dbConnObj.connect_to_db()
        if not res:
            log(
                "-------DB connection error store_expired_data Function Not Registered Properly------"
            )
            return
        log(
            "-------------------Transaction For Inserting Expired Data to DB-------------------"
        )

        # Get all keys which belong to the transaction
        keys = execute("keys", f"{PREFIX}:*")
        # Get current time
        epoch_time_now = int(time.time())

        expired_documents = []
        keys_to_delete = []

        def insert_batch_documents(documents, keys):
            retries = 3
            while retries > 0:
                try:
                    client = dbConnObj.get_db_client
                    db = client[dbConnObj.get_database_name]
                    collection = db[dbConnObj.get_collection_name]
                    
                    # Check if documents already exist
                    existing_documents = collection.find(
                        {"tMsgRecvByServer": {"$in": [doc["tMsgRecvByServer"] for doc in documents]},
                         "DeviceId": {"$in": [doc["deviceId"] for doc in documents]}}
                    )
                    existing_docs_set = set((doc["tMsgRecvByServer"], doc["DeviceId"]) for doc in existing_documents)

                    # Filter out existing documents
                    new_documents = [
                        doc for doc in documents
                        if (doc["tMsgRecvByServer"], doc["deviceId"]) not in existing_docs_set
                    ]
                    
                    if new_documents:
                        log(f"------Inserting batch data------")
                        collection.insert_many(new_documents)

                    return True  # Success
                except Exception as e:
                    log(f"Error during batch insertion: {str(e)}")
                    retries -= 1
                    if retries > 0:
                        log(f"Retrying... {retries} attempts left")
                        time.sleep(5)
            return False  # Failure

        def delete_keys_from_redis(keys):
            retries = 3
            while retries > 0:
                try:
                    for key in keys:
                        execute("json.del", key)
                    log("Data inserted into MongoDB and keys deleted from Redis")
                    return True  # Success
                except Exception as e:
                    log(f"Error during key deletion: {str(e)}")
                    retries -= 1
                    if retries > 0:
                        log(f"Retrying... {retries} attempts left")
                        time.sleep(5)
            return False  # Failure

        # Get each key in key set
        for key in keys:
            with atomic():
                # Convert string to dict
                get_value = json.loads(execute("json.get", key))
                # Get Creation time
                tmsg_recvby_server = get_value.get("tMsgRecvByServer", None)
                device_id = get_value.get("deviceId", None)
                if tmsg_recvby_server is None or not isinstance(tmsg_recvby_server, int):
                    log(
                        f"tMsgRecvByServer Key Not Found or tMsgRecvByServer is Not an Integer For Key {key}!"
                    )
                    continue

                if device_id is None:
                    log(f"DeviceId is Missing For Key {key}")
                    continue

                expiry_time_key = tmsg_recvby_server + MOVEMENT_TIME
                # Compare whether the key is expired
                if expiry_time_key < epoch_time_now:
                    log(f"------- Key {key} Expired -------")
                    expired_documents.append(get_value)
                    keys_to_delete.append(key)

                    # Insert documents in batches
                    if len(expired_documents) >= 100:
                        success = insert_batch_documents(expired_documents, keys_to_delete)
                        if not success:
                            log("Failed to insert batch documents after retries")
                            return
                        # Delete the keys from Redis in batches
                        if not delete_keys_from_redis(keys_to_delete):
                            log("Failed to delete keys from Redis after retries")
                            return
                        # Clear the batch lists
                        expired_documents.clear()
                        keys_to_delete.clear()

        # Insert remaining documents
        if expired_documents:
            success = insert_batch_documents(expired_documents, keys_to_delete)
            if not success:
                log("Failed to insert remaining documents after retries")
                return
            # Delete the keys from Redis for remaining documents
            if not delete_keys_from_redis(keys_to_delete):
                log("Failed to delete remaining keys from Redis after retries")
                return

    # Reset the job key with the JOB_INTERVAL
    execute("set", "jobkey{%s}" % hashtag(), "val", "EX", str(JOB_INTERVAL))

                

def write_updates_to_db(data):
    dbConnObj = DBConnect()
    res = dbConnObj.connect_to_db()
    if not res:
        log(
            "-------DB connection error write_updates_to_db Function Not Registered Properly------"
        )
        return
    log(
        "-------------------Transaction For Inserting Updated Data to DB-------------------"
    )
    
    key = data.get("key")
    get_value = json.loads(execute("json.get", key))
    is_audio_played = get_value.get("audioPlayed", None)
    tmsg_recvby_server = get_value.get("tMsgRecvByServer", None)
    device_id = get_value.get("deviceId", None)
    
    if is_audio_played is None or not isinstance(is_audio_played, int):
        log("audioPlayed Key Not Found or audioPlayed is Not an Integer!")
    elif tmsg_recvby_server is None or not isinstance(tmsg_recvby_server, int):
        log("tMsgRecvByServer Key Not Found or tMsgRecvByServer is Not an Integer!")
    elif is_audio_played > 0:
        with atomic():
            log(f"------audioPlayed is updated for key {key}------")
            client = dbConnObj.get_db_client
            db = client[dbConnObj.get_database_name]
            collection = db[dbConnObj.get_collection_name]
            if (
                collection.count_documents(
                {"tMsgRecvByServer": tmsg_recvby_server,"DeviceId":device_id}, limit=1
                )
                != 0
            ):
                log("----------Document Already Exist With This tMsgRecvByServer")
            else:
                log(f"------Inserting data : key {key}------")
                collection.insert_one(get_value)
            # Delete the key from Redis
            execute("json.del", key)
            log("Data inserted into MongoDB and deleting from Redis")


# Set Expiry On Every Key With Prefix transaction
# This is a default expiry key which will always be greater than the actual expiry time
gbExpiry = GB()

gbExpiry.foreach(lambda x: execute("EXPIRE", x["key"], str(EXPIRY_TIME)))
gbExpiry.foreach(lambda x: log(f"TTL For Key {x['key']} {execute('TTL', x['key'])}"))
gbExpiry.register(f"{PREFIX}:*", mode="sync", readValue=False)

# Register the RedisGears function
gbCron = GB()
# This event will trigger on each JOB INTERVAL, Hence acting as a cron function
gbCron.foreach(lambda x: log("Setting Up the Cron Function")).foreach(
    lambda x: store_expired_data(x)
).register(
    prefix="jobkey*",
    eventTypes=["expired"],
    readValue=False,
    mode="sync",
    onRegistered=store_expired_data,
)

gbUpdate = GB()
# This event will trigger on set event on transaction data
gbUpdate.foreach(
    lambda x: log("Setting Up the Write & Update Event Listening Functions")
).foreach(lambda x: write_updates_to_db(x)).register(
    prefix=f"{PREFIX}:*",
    eventTypes=["json.set"],
    readValue=False,
    mode="sync",
)
