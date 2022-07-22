import sys
import json
from dateutil import parser
from fastapi import FastAPI, Request, Body
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

templates = Jinja2Templates(directory="./demo")

sys.path.append("..")
from constants.K_LINE import *
from utils import redis_util

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    # 允许跨域的源列表，例如 ["http://www.example.org"] 等等，["*"] 表示允许任何源
    allow_origins=["*"],
    # 跨域请求是否支持 cookie，默认是 False，如果为 True，allow_origins 必须为具体的源，不可以是 ["*"]
    allow_credentials=False,
    # 允许跨域请求的 HTTP 方法列表，默认是 ["GET"]
    allow_methods=["*"],
    # 允许跨域请求的 HTTP 请求头列表，默认是 []，可以使用 ["*"] 表示允许所有的请求头
    # 当然 Accept、Accept-Language、Content-Language 以及 Content-Type 总之被允许的
    allow_headers=["*"],
    # 可以被浏览器访问的响应头, 默认是 []，一般很少指定
    # expose_headers=["*"]
    # 设定浏览器缓存 CORS 响应的最长时间，单位是秒。默认为 600，一般也很少指定
    # max_age=1000
)
app.mount("/static", StaticFiles(directory="./static"), name="static")

redis_handler = redis_util.RedisHandler()

@app.get("/")
async def root(request: Request):
    #return open("test_main.html", 'r').read()
    return templates.TemplateResponse(
        "test_main.html",
        {
            "request": request
        }
    )

@app.get("/api/contract_info")
async def ct_info():
    return JSONResponse(json.loads(redis_handler.get(redis_util.REDIS_KEY_CT_LIST)))

#async def kline3(
#    symbol: str = Body(..., title='合约代码', embed=True),
#    klt: int = Body(..., title="k线类型", embed=True),
#    begin_time: str = Body(..., embed=True),
#    end_time: str = Body(..., embed=True),
#):
@app.get("/api/futures/klines")
async def kline3(symbol: str, klt: int, begin_time: str, end_time: str):
    key_k = KEY_K_1M
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
    if k_res:
        j_res['data']['klines'] = [item[1] for item in k_res]

    #return json.dumps(j_res, ensure_ascii=False)
    return JSONResponse(j_res)

@app.get("/api/futures/trends")
async def kline3(symbol: str, klt: int, begin_time: str, end_time: str):
    key_k = KEY_K_1M
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
            "trends": [
            ]
        }
    }
    if k_res:
        j_res['data']['trends'] = [item[1] for item in k_res]

    #return json.dumps(j_res, ensure_ascii=False)
    return JSONResponse(j_res)
