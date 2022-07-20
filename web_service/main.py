import sys
from fastapi import FastAPI

sys.path.append("..")
from utils import redis_util

app = FastAPI()

redis_handler = redis_util.RedisHandler()

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/api/contract_info")
async def ct_info():
    return redis_handler.get(redis_util.REDIS_KEY_CT_LIST)
