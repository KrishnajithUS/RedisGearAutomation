import requests
import json
import time
import uuid
import random
from datetime import datetime
import time

BASE_URL = "http://127.0.0.1:8000"

incr = 0

def create_transaction():
    global incr
    incr += 1
    transaction_data = {
        "reqRefNo": f"REQ123456789{uuid.uuid4().hex}",
        "rrn": "RRN987654321",
        "mid": "MERCHANT123",
        "tid": "TERM456",
        "mcc": "1234",
        "meVpa": "merchant@bank",
        "transactionType": 1,
        "transactionMode": 2,
        "txnAmt": "100.50",
        "txnTimestamp": int(time.time()),
        "timeStamp": int(time.time()),
        "deviceId": 1234567890,
        "expirationTime": int(time.time()) + 600,
        "tMsgRecvByServer": int(time.time()),
        "tMsgRecvFromDev": int(time.time()) - 100,
        "audioPlayed": random.randint(0,1),
        "id": incr,
    }
    
    response = requests.post(f"{BASE_URL}/transaction/", json=transaction_data)
    if response.status_code == 200:
        print("Transaction created:", response.json())
        return transaction_data
    else:
        print("Failed to create transaction:", response.text)
        return None

def update_transaction(device_id, transaction_id, updated_data):
    response = requests.put(f"{BASE_URL}/transaction/{device_id}/{transaction_id}", json=updated_data)
    if response.status_code == 200:
        print("Transaction updated:", response.json())
    else:
        print("Failed to update transaction:", response.text)

def main():
    insert_cnt = 0
    update_cnt = 0
    cnt = 1000000
    while cnt > 0:
        transaction = create_transaction()
        if transaction:
            transaction_id = transaction["id"]
            device_id = transaction["deviceId"]
            updated_data = transaction.copy()
              # example update
            if cnt % 5 == 0 and updated_data["audioPlayed"] <= 0:
                updated_data["audioPlayed"] = 2
                update_transaction(device_id, transaction_id, updated_data)
        cnt -= 1
        insert_cnt += 1
        update_cnt += 1
    print(f"updated count : {update_cnt}")
    print(f"inserted count : {insert_cnt}")

if __name__ == "__main__":
    main()
