# binance_futures_kline_load
币安永续合约k线数据下载服务

说明：通过api: https://api.binance.com/api/v1/klines 下载币安永续合约k线分钟数据，支持1,5分钟数据，在每次下载之后通过计算下一次数据开始时间点的方法实现数据的连续保存，解决数据缺漏问题。
data_load.py 脚本为下载程序入口，将k线数据通过多线程方法下载并保存到本地mysql数据库，需要安装相关的mysql依赖库，相关配置看utils中的类定义MysqlLink
永续合约symbol一般为 '币种大写USDT' 此处一律用币种小写
