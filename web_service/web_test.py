import sys
import json
from dateutil import parser
from fastapi import FastAPI, Request, Body

sys.path.append("..")
from constants.K_LINE import *
from utils import redis_util

app = FastAPI()

redis_handler = redis_util.RedisHandler()

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/api/contract_info")
async def ct_info():
    return redis_handler.get(redis_util.REDIS_KEY_CT_LIST)

@app.post("/api/kline3")
async def kline3(
    symbol: str = Body(..., title='合约代码', embed=True),
    klt: int = Body(..., title="k线类型", embed=True),
    begin_time: str = Body(..., embed=True),
    end_time: str = Body(..., embed=True),
):
    if klt == 0:
        key_k = KEY_K_1D
    elif klt == 1:
        key_k = ''
    elif klt == 2:
        key_k = ''
    elif klt == 4:
        key_k = KEY_K_1M
    elif klt == 5:
        key_k = KEY_K_3M
    elif klt == 6:
        key_k = KEY_K_5M
    elif klt == 7:
        key_k = KEY_K_15M
    elif klt == 8:
        key_k = KEY_K_30M
    elif klt == 9:
        key_k = KEY_K_15S
    elif klt == 10:
        key_k = KEY_K_30S

    kline_cache_key = redis_util.REDIS_KEY_KLINE_PEROID.format(symbol, key_k)
    k_res = redis_handler.get_slice(kline_cache_key,
                                 s = parser.parse(begin_time).timestamp(),
                                 e = parser.parse(end_time).timestamp())
    j_res = {
        "code": 0,
        "message": "成功",
        "data": {
            "code": "AAPL",
            "market": 105,
            "symbol": "AAPL_105",
            "name": "苹果",
            "decimal": 2,
            "count": 100,
            "preKPrice": 147.07,
            "prePrice": 147.07,
            "klines": [
            ]
        }
    }
    j_res['data']['klines'] = [item[1] for item in k_res]

    return json.dumps(j_res, ensure_ascii=False)

