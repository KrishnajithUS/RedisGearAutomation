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

JOB_INTERVAL = config("JOB_INTERVAL")
PREFIX = config("PREFIX")
MOVEMENT_TIME = config['MOVEMENT_TIME']
EXPIRY_TIME = MOVEMENT_TIME*config['MULTIPLIER']


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
        self.client = pymongo.MongoClient(self.conn_string)
        self.collection_name = config['COLLECTION_NAME']
        self.db_name = config['DB_NAME']
        self.RETRY_CONN_COUNT = config['RETRY_CONN_COUNT']
        self._initialized = True

    def ping_connection(self, retries=3, delay=1):
        for attempt in range(retries):
            try:
                self.client.admin.command('ping')
                print("Pinged your deployment. You successfully connected to MongoDB!")
                return True
            except Exception as e:
                print(f"Ping attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(delay)
        return False

    def reconnect(self, retries=3, delay=1):
        for attempt in range(retries):
            try:
                self.client = pymongo.MongoClient(self.conn_string)
                print("Successfully reconnected to MongoDB!")
                return self.client
            except Exception as e:
                print(f"Reconnection attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(delay)
        return None

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
        
def store_expired_data(data=None):
    log("sleeping")
    execute("set", "jobkey{%s}" % hashtag(), "val", "EX", str(JOB_INTERVAL))

    if data:
        dbConnObj = DBConnect()
        res = dbConnObj.ping_connection()
        if not res:
            log(
                "-------DB connection error store_expired_data Function Not Registered Properly------"
            )
            return
        log(
            "-------------------Transaction For Inserting Expired Data to DB-------------------"
        )

        keys = execute("keys", f"{PREFIX}:*")
        epoch_time_now = int(time.time())

        expired_documents = []
        keys_to_delete = []
        renamed_keys = {}

        def insert_batch_documents(documents):
            retries = 3
            while retries > 0:
                try:
                    if dbConnObj.ping_connection():
                        log("----------Getting Existing Db Connection----------")
                        client = dbConnObj.get_db_client
                    else:
                        log("----------Creating A New Connection----------")
                        client = dbConnObj.reconnect()
                        if client is None:
                            log("Failed to reconnect to MongoDB.")
                            return False
                    db = client[dbConnObj.get_database_name]
                    collection = db[dbConnObj.get_collection_name]

                    if documents:
                        log(f"------Inserting batch data------")
                        collection.insert_many(documents)

                    return True
                except Exception as e:
                    log(f"Error during batch insertion: {str(e)}")
                    retries -= 1
                    if retries > 0:
                        log(f"Retrying... {retries} attempts left")
                        time.sleep(5)
            return False

        def delete_keys_from_redis(keys):
            retries = 3
            while retries > 0:
                try:
                    for key in keys:
                        execute("del", key)
                    log("Data inserted into MongoDB and keys deleted from Redis")
                    return True
                except Exception as e:
                    log(f"Error during key deletion: {str(e)}")
                    retries -= 1
                    if retries > 0:
                        log(f"Retrying... {retries} attempts left")
                        time.sleep(5)
            return False

        def revert_renamed_keys(renamed_keys):
            for new_key, original_key in renamed_keys.items():
                try:
                    execute('rename', new_key, original_key)
                    log(f"Reverted key {new_key} back to {original_key}")
                except Exception as e:
                    log(f"Failed to revert key {new_key}: {str(e)}")

        try:
            for key in keys:
                # Convert string to dict
                hset_list = execute("hgetall", key)

                get_value = list_to_dict(hset_list)

                # Get Creation time
                tmsg_recvby_server = get_value.get("tMsgRecvByServer", None)
                device_id = get_value.get("deviceId", None)
                audio_played = get_value.get("audioPlayed", None)
                if tmsg_recvby_server is None or not isinstance(tmsg_recvby_server, int):
                    log(
                        f"tMsgRecvByServer Key Not Found or tMsgRecvByServer is Not an Integer For Key {key}!"
                    )
                    continue

                if audio_played  is None:
                    log(f"audioPlayed is Missing For Key {key}")
                    continue


                expiry_time_key = tmsg_recvby_server + MOVEMENT_TIME

                if expiry_time_key < epoch_time_now or audio_played > 0:
                    log(f"------- Key {key} Expired -------")
                    expired_documents.append(get_value)
                    new_key = f"__{key}__{key}"
                    execute('rename', key, new_key)
                    renamed_keys[new_key] = key
                    keys_to_delete.append(new_key)

                    if len(expired_documents) >= 100000:
                        success = insert_batch_documents(expired_documents)
                        if not success:
                            log("Failed to insert batch documents after retries")
                            revert_renamed_keys(renamed_keys)
                            return
                        if not delete_keys_from_redis(keys_to_delete):
                            log("Failed to delete keys from Redis after retries")
                            revert_renamed_keys(renamed_keys)
                            return
                        expired_documents.clear()
                        keys_to_delete.clear()
                        renamed_keys.clear()

            if expired_documents:
                success = insert_batch_documents(expired_documents)
                if not success:
                    log("Failed to insert remaining documents after retries")
                    revert_renamed_keys(renamed_keys)
                    return
                if not delete_keys_from_redis(keys_to_delete):
                    log("Failed to delete remaining keys from Redis after retries")
                    revert_renamed_keys(renamed_keys)
                    return

        except Exception as e:
            log(f"Exception occurred: {str(e)}")
            revert_renamed_keys(renamed_keys)
            return



# Register the RedisGears function
gbCron = GB()
# This event will trigger on each JOB INTERVAL, Hence acting as a cron function
gbCron.foreach(lambda x: log("Setting Up the Cron Function")).foreach(
    lambda x: store_expired_data(x)
).register(
    prefix="jobkey*",
    eventTypes=["expired"],
    readValue=False,
    onRegistered=store_expired_data,
)

