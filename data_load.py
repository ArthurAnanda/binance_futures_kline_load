'''
币安永续合约k线数据下载服务
作者：Arthur
说明：通过api: https://api.binance.com/api/v1/klines 下载币安永续合约k线分钟数据，支持1,5分钟数据，在每次下载之后通过计算下一次数据开始时间点的方法解决数据重复或者缺漏问题。
本脚本为下载程序入口，将k线数据通过多线程方法下载并保存到本地mysql数据库，需要安装相关的mysql依赖库，相关配置看utils中的类定义MysqlLink
永续合约symbol一般为 '币种大写USDT' 此处一律用币种小写
'''
from utils import *

coin_list = ['btc', 'eth', 'bnb', 'xrp', 'ada', 'eos', 'ltc', 'trx', 'bch', 'bat', 'link', 'etc', 'xlm', 'doge', 'dot', 'atom', 'dash', 'zec', 'vet', 'sol', 'xtz', 'crv', 'sxp', '1inch', 'matic', 'axs', 'btt', 'chr', 'ctk', 'tlm']
coin_list = coin_list[:20]
symbol_list = [coin.upper() + 'USDT' for coin in coin_list[:20]]
print(symbol_list)

interval = '5m'  # 一分钟数据
db_name = f'bi_f_kline_test_{interval}'  # mysql 数据库名称

ml = MysqlLink()
ml.create_database(db_name=db_name, enforce=True)  # 注意：下载时如果没有此数据库会新建一个数据库，如果有数据库会删掉之前的数据重新下载

lc_utc = 8
ding = DingMessage(lc_utc=lc_utc, robot_id='')

err_list = []

time_start = '2021-01-01' # 数据开始时间点
time_end = '2021-07-27'   # 数据结束时间点


def load_symbol(symbol):
    info_start = f'{symbol} start'
    info_finish = f'{symbol} finish'
    print(info_start)
    ding.send_msg(content=info_start)
    try:
        BinanceFuturesKlineMysql.kline_to_mysql(db_name=db_name, bi_symbol=symbol, interval=interval, time_start=time_start, time_end=time_end, lc_utc=lc_utc, verbose_out=True)
        print(info_finish)
        ding.send_msg(content=info_finish)
    except:
        info = traceback.format_exc()
        print(info)
        info_err = f'{symbol} err'
        print(info_err)
        err_list.append(symbol)
        ding.send_msg(content=info_err)


for symbol in symbol_list:
    FuncTool.thread_run(func=lambda: load_symbol(symbol=symbol))
