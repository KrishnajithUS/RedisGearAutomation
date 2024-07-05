import time
import pymongo
import json
import os

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

# Function to create index if it doesn't exist
def create_index():
    try:
        execute('FT.CREATE', 'transaction_idx',
                'ON', 'JSON',
                'PREFIX', '1', f'{PREFIX}:',
                'SCHEMA', '$.audioPlayed', 'AS', 'audioPlayed', 'NUMERIC',
                         '$.tMsgRecvByServer', 'AS', 'tMsgRecvByServer', 'NUMERIC')
        log("Index created")
    except Exception as e:
        if "Index already exists" in str(e):
            log("Index already exists")
        else:
            log("Error creating index: " + str(e))

def store_expired_data(data=None, retries=3, delay=2):
    execute("set", "jobkey{%s}" % hashtag(), "val", "EX", str(JOB_INTERVAL))

    if data:
        create_index()
        dbConnObj = DBConnect()
        current_time = int(time.time())
        limiting_time = current_time - MOVEMENT_TIME
        min_audioPlayed = 0
        total_entries = int(execute('FT.SEARCH', 'transaction_idx', f"@audioPlayed:[{min_audioPlayed} +inf] @tMsgRecvByServer:[-inf {limiting_time}]", 'LIMIT', 0, 0)[0])
        batch_size = 100000
        offset = 0
        data = []
        keys_to_delete = []
        start_time = time.time()
        log(f"Total entries: {total_entries}")

        while offset < total_entries:
            log("----------- Starting search -----------")
            search_result = execute('FT.SEARCH', 'transaction_idx', f"@audioPlayed:[{min_audioPlayed} +inf] @tMsgRecvByServer:[-inf {limiting_time}]", 'LIMIT', offset, batch_size)
            log("----------- Search complete -----------")
            log(f"Search results: {search_result}")

            offset += batch_size

            if len(search_result) == 1:  # Only the number of results returned
                break

            for i in range(1, len(search_result), 2):
                key = search_result[i]
                value = json.loads(search_result[i + 1])
                data.append(value)
                keys_to_delete.append(key)

            if len(data) >= batch_size or offset >= total_entries:
                for attempt in range(retries):
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
                        log("----------Inserting batch data into MongoDB----------")
                        collection.insert_many(data)

                        for key in keys_to_delete:
                            pass
                            execute("json.del", key)

                        data.clear()
                        keys_to_delete.clear()
                        break
                    except pymongo.errors.AutoReconnect as e:
                        log(f"AutoReconnect error: {e}. Retrying {attempt + 1}/{retries}...")
                        if attempt < retries - 1:
                            time.sleep(delay)
                        else:
                            log("Max retries reached. Failed to insert data.")
                            return False
                    except Exception as e:
                        log(f"------Error------ : {e}")
                        return False

        end_time = time.time()
        log(f"Search completed in {end_time - start_time} seconds")
        log("Pagination completed")

# Register the RedisGears function
gbCron = GB()
# This event will trigger on each JOB INTERVAL, hence acting as a cron function
gbCron.foreach(lambda x: log("Setting Up the Cron Function")).foreach(
    lambda x: store_expired_data(x)
).register(
    prefix="jobkey*",
    eventTypes=["expired"],
    readValue=False,
    onRegistered=store_expired_data,
)
