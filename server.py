from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis
import uuid
from retrying import retry

app = FastAPI()

class Transaction(BaseModel):
    reqRefNo: str
    rrn: str
    mid: str
    tid: str
    mcc: str
    meVpa: str
    transactionType: int
    transactionMode: int
    txnAmt: str
    txnTimestamp: int
    timeStamp: int
    deviceId: int
    expirationTime: int
    tMsgRecvByServer: int
    tMsgRecvFromDev: int
    audioPlayed: int
    id: int


@retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def set_dict(key, data_dict, r):
    r.hset(key, mapping=data_dict)

# Initialize Redis connection
redis_pool = redis.ConnectionPool(host='0.0.0.0', port=6379, db=0)
r = redis.Redis(connection_pool=redis_pool, decode_responses=True)

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.post("/transaction/")
async def create_transaction(transaction: Transaction):
    unique_key = f"transaction:{transaction.deviceId}_{transaction.id}"
    print(unique_key)
    transaction_dict = transaction.model_dump()
    set_dict(unique_key, transaction_dict,r)
    return {"message": "Transaction created", "key": unique_key}

@app.put("/transaction/{device_id}/{id}")
async def update_transaction(device_id: int, id: str, transaction: Transaction):
    unique_key = f"transaction:{device_id}_{id}"
    print(unique_key)
    if not r.exists(unique_key):
        raise HTTPException(status_code=404, detail="Transaction not found")
    transaction_dict = transaction.model_dump()
    set_dict(unique_key, transaction_dict, r)
    return {"message": "Transaction updated", "key": unique_key}
