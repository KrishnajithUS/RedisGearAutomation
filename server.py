from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime
import redis
import uuid

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
    txnTimestamp: datetime
    timeStamp: int
    deviceId: int
    expirationTime: int
    tMsgRecvByServer: int
    tMsgRecvFromDev: int
    audioPlayed: int
    id: int

# Initialize Redis connection
redis_pool = redis.ConnectionPool(host='0.0.0.0', port=6379, db=0)
r = redis.Redis(connection_pool=redis_pool, decode_responses=True)

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.post("/transaction/")
async def create_transaction(transaction: Transaction):
    unique_key = f"transaction:{transaction.deviceId}:{uuid.uuid4().hex}"
    transaction_dict = transaction.dict()
    r.hset(unique_key, mapping=transaction_dict)
    return {"message": "Transaction created", "key": unique_key}

@app.put("/transaction/{device_id}/{id}")
async def update_transaction(device_id: int, id: str, transaction: Transaction):
    unique_key = f"transaction:{device_id}:{id}"
    if not r.exists(unique_key):
        raise HTTPException(status_code=404, detail="Transaction not found")
    transaction_dict = transaction.dict()
    r.hset(unique_key, mapping=transaction_dict)
    return {"message": "Transaction updated", "key": unique_key}
