import os, sys, time, json, requests, urllib, traceback
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import datetime as dt
from threading import Thread
import pymysql
from sqlalchemy import create_engine, inspect

pymysql.install_as_MySQLdb()

HEADERS = {'accept'                   : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
           'accept-encoding'          : 'gzip, deflate, br',
           'accept-language'          : 'zh-CN,zh;q=0.9,en;q=0.8',
           'cache-control'            : 'max-age=0',
           'sec-fetch-mode'           : 'navigate',
           'sec-fetch-site'           : 'none',
           'sec-fetch-user'           : '?1',
           'upgrade-insecure-requests': '1',
           'user-agent'               : 'Firefox/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36'}

LC_UTC = 8
HOST = 'local' if 'win' in sys.platform else 'vps' if 'linux' in sys.platform else ''


# DING_ROBOT_ID = ('https://oapi.dingtalk.com/robot/send?access_token=34267d344096d740b9799634a46b71cdad983870eb0a810c9972bf6666ddd2e6', 'SEC1e851c4af8bopi1ee53b53c6a05ae6ea8196be6761cc14fa0142c1f0759a07f4')


class MysqlLink:
    def __init__(self, account='root', pwd='123456', address='localhost'):
        '''
        :param account: 必须是root账号，并且打开写入权限
        :param pwd: 密码，填入自己的密码即可
        :param address: 填写localhost，必须是本机mysql数据库
        '''
        self.account = account
        self.pwd = pwd
        self.address = address

        self.engine_para = f'mysql+mysqlconnector://{self.account}:{self.pwd}@{self.address}'
        self.con = create_engine(self.engine_para)

    def get_all_databases(self):
        check = inspect(self.con)
        dbs_list = check.get_schema_names()
        result = list(set(dbs_list) - set(['information_schema', 'sys', 'mysql', 'performance_schema']))
        result.sort()
        return result

    def database_exist(self, db_name):
        dbs_list = self.get_all_databases()
        if db_name in dbs_list:
            return True
        else:
            return False

    def fresh_database(self, db_name_title):
        '''
        在本地mysql数据库新建一个数据库，数据库名为 db_name_title 加上创建当前时间点字符串
        :param db_name_title: str
        :return: [一个指向新建好的数据库的 sqlalchemy 链接（支持utf-8字符），新数据名字字符串]
        '''
        db_name = db_name_title + '_' + str(pd.datetime.now())[:10].replace('-', '_').replace(':', '_').replace(' ', '_')
        con = create_engine(self.engine_para)
        if self.database_exist(db_name=db_name):
            con.execute(f"drop database {db_name}")
            con.execute(f"create database {db_name}")
        else:
            con.execute(f"create database {db_name}")
        con = create_engine(self.engine_para + f'/{db_name}?charset=utf8mb4')
        return [con, db_name]

    def create_database_if_not_exists(self, db_name):
        '''
        :param db_name_title: str
        :return: [一个指向新建好的数据库的 sqlalchemy 链接（支持utf-8字符），新数据名字字符串]
        '''
        if self.database_exist(db_name=db_name):
            con = create_engine(self.engine_para + f'/{db_name}?charset=utf8mb4')
            return [con, db_name]
        else:
            self.con.execute(f"create database {db_name}")
            con = create_engine(self.engine_para + f'/{db_name}?charset=utf8mb4')
            return [con, db_name]

    def create_database(self, db_name, enforce=False):
        '''
        在本地mysql数据库新建一个数据库，数据库名为 db_name，如果db_name数据库已存在，那么就报错退出。
        :param db_name_title: str
        :param enforce: bool 是否强制创建新的数据库
        :return: [一个指向新建好的数据库的 sqlalchemy 链接（支持utf-8字符），新数据名字字符串]
        '''
        con = create_engine(self.engine_para)
        if not enforce:
            if self.database_exist(db_name=db_name):
                raise ValueError('database already exists!')
            else:
                con.execute(f"create database {db_name}")
                con = create_engine(self.engine_para + f'/{db_name}?charset=utf8mb4')
                return [con, db_name]
        else:
            if self.database_exist(db_name=db_name):
                con.execute(f"drop database {db_name}")
                con.execute(f"create database {db_name}")
            else:
                con.execute(f"create database {db_name}")
            con = create_engine(self.engine_para + f'/{db_name}?charset=utf8mb4')
            return con, db_name

    def delete_database(self, db_name):
        if db_name in ['information_schema', 'sys', 'mysql', 'performance_schema']:
            print(db_name, 'is system database')
            return
        con = create_engine(self.engine_para)
        if self.database_exist(db_name=db_name):
            con.execute(f"drop database {db_name}")
            print(f'delete database {db_name}\n')
        else:
            print(f'there is not database {db_name}\n')


class MysqlDatabaseLink:
    def __init__(self, account='root', pwd='123456', address='localhost', db_name='cc_bitfinex_hd_1d'):
        self.account = account
        self.pwd = pwd
        self.address = address
        self.db_name = db_name
        self.engine_para = f'mysql+mysqlconnector://{self.account}:{self.pwd}@{self.address}/{db_name}?charset=utf8mb4'
        self.con = create_engine(self.engine_para)

    def get_all_tables(self):
        check = inspect(self.con)
        tables_list = check.get_table_names()
        return tables_list

    def table_exist(self, table_name):
        table_list = self.get_all_tables()
        if table_name in table_list:
            return True
        else:
            return False

    def drop_table(self, table_name):
        try:
            self.con.execute(f"drop table {table_name}")
            print(f'tabel [{table_name}] dropped.')
        except:
            print(f'Unknown table [{table_name}].')

    def get_table(self, table_name, index_col=None):
        data = pd.read_sql_table(table_name=table_name, con=self.con, index_col=index_col)
        return data

    def save_table(self, data, table_name, if_exists='append', index=True, index_label=None):
        data.to_sql(name=table_name, con=self.con, if_exists=if_exists, index=index, index_label=index_label)

    def get_table_head(self, table_name, head=100, index_col=None):
        data = pd.read_sql(sql=f"select * from {table_name} order by {index_col} limit {head}", con=self.con, index_col=index_col)
        return data

    def get_table_tail(self, table_name, tail=100, index_col=None):
        data = pd.read_sql(sql=f"select * from {table_name} order by {index_col} desc limit {tail}", con=self.con, index_col=index_col)
        return data.sort_values(by=index_col, ascending=True)

    def get_table_by_time(self, table_name, time_start, time_end, index_col=None):
        data = pd.read_sql(sql=f"select * from {table_name} where time between DATE_FORMAT('{time_start}','%Y-%m-%d %H:%M:%S') and DATE_FORMAT('{time_end}','%Y-%m-%d %H:%M:%S')", con=self.con, index_col=index_col)
        return data.sort_values(by=index_col, ascending=True)

    def tables_to_excel(self):
        tables = self.get_all_tables()
        for table in tables:
            data = self.get_table(table_name=table, index_col=None)
            data.to_excel(f'{self.db_name}_{table}.xlsx')

    def tables_to_csv(self):
        tables = self.get_all_tables()
        for table in tables:
            data = self.get_table(table_name=table, index_col=None)
            data.to_csv(f'{self.db_name}_{table}.csv')

    def tables_to_hdf(self):
        tables = self.get_all_tables()
        for table in tables:
            data = self.get_table(table_name=table, index_col=None)
            data.to_hdf(f'{self.db_name}_{table}.h5', key='data')

    def table_time_range(self, table_name, limit=10):
        freq = None
        head = self.get_table_head(table_name=table_name, index_col='time', head=limit)
        tail = self.get_table_tail(table_name=table_name, index_col='time', tail=limit)
        freq_head = head.index.inferred_freq
        freq_tail = tail.index.inferred_freq
        freq = freq_head if freq_head == freq_tail else None
        time_start = head.index[0]
        time_end = tail.index[-1]
        print(f"'{table_name}' time start from '{time_start}' to '{time_end}' freq is '{freq}'")
        return time_start, time_end, freq


class MysqlHandler:
    @staticmethod
    def get_database_table(db_name, index_col=None):
        return MysqlDatabaseLink(db_name=db_name).get_table(table_name=db_name, index_col=index_col)

    @staticmethod
    def get_table(db_name, table_name, index_col=None):
        return MysqlDatabaseLink(db_name=db_name).get_table(table_name=table_name, index_col=index_col)

    @staticmethod
    def get_table_tail(db_name, table_name, tail=100, index_col=None):
        return MysqlDatabaseLink(db_name=db_name).get_table_tail(table_name=table_name, tail=tail, index_col=index_col)

    @staticmethod
    def get_table_head(db_name, table_name, head=100, index_col=None):
        return MysqlDatabaseLink(db_name=db_name).get_table_head(table_name=table_name, head=head, index_col=index_col)

    @staticmethod
    def get_table_by_time(db_name, table_name, time_start, time_end, index_col=None):
        return MysqlDatabaseLink(db_name=db_name).get_table_by_time(table_name=table_name, time_start=time_start, time_end=time_end, index_col=index_col)

    @staticmethod
    def all_databases(account='root', pwd='123456', address='localhost'):
        ml = MysqlLink(account=account, pwd=pwd, address=address)
        return ml.get_all_databases()

    @staticmethod
    def all_tables(db_name):
        return MysqlDatabaseLink(db_name=db_name).get_all_tables()

    @staticmethod
    def save_table(dataframe, db_name, table_name, if_exists='replace', index=True, index_label=None):
        if not MysqlLink().database_exist(db_name=db_name):
            MysqlLink().create_database(db_name=db_name, enforce=False)
        mdl = MysqlDatabaseLink(db_name=db_name)
        mdl.save_table(data=dataframe, table_name=table_name, if_exists=if_exists, index=index, index_label=index_label)

    @staticmethod
    def tables_out_to_excel(db_name):
        mdl = MysqlDatabaseLink(db_name=db_name)
        mdl.tables_to_excel()

    @staticmethod
    def tables_out_to_hdf(db_name):
        mdl = MysqlDatabaseLink(db_name=db_name)
        mdl.tables_to_hdf()

    @staticmethod
    def table_time_range(db_name, table_name, limit):
        mdl = MysqlDatabaseLink(db_name=db_name)
        return mdl.table_time_range(table_name=table_name, limit=limit)


class Adorn:
    @staticmethod
    def use_time(round_digit=4):
        def use_time_inner(func):
            def wrapper(*args, **kwargs):
                print(f"function [{func.__name__}]", 'use time:')
                start = time.time()
                time.sleep(3)
                result = func(*args, **kwargs)
                time_use = time.time() - start
                print(round(time_use, round_digit))
                print('-' * 50)
                return result

            return wrapper

        return use_time_inner

    @staticmethod
    def run_ensure(retry_times=10, lc_utc=8, traceback_print=False, print_retry=False, sleep=0):
        '''
        try to run function till success
        :param retry_times: times of try to run function
        :param lc_utc: local time and utc time gap
        :param traceback_print: print traceback or not
        :param sleep: time sleep between two trying.
        :return:
        '''

        def run_function_to(func):
            def wrapper(*args, **kwargs):
                retry = 0
                while True:
                    if retry > retry_times:
                        if traceback_print:
                            raise ConnectionError(f'try [{func.__name__}] {retry_times} times failed. traceback:\n{exc}')
                        else:
                            raise ConnectionError(f'try [{func.__name__}] {retry_times} times failed.')
                    try:
                        result = func(*args, **kwargs)
                        return result
                    except:
                        if traceback_print:
                            exc = traceback.format_exc()
                            print(f'run_ensure retry [{func.__name__}]..[{args}]..[{kwargs}]..{Write.time(lc_utc=lc_utc)}\ntraceback:\n{exc}')
                        else:
                            if print_retry:
                                print(f'retry... {retry + 1}')
                        retry += 1
                    time.sleep(sleep)

            return wrapper

        return run_function_to


class Write:
    @staticmethod
    def pc_name():
        name = socket.gethostname()
        return name

    # @staticmethod
    # def pc_addr():
    #     myname = socket.getfqdn(socket.gethostname())
    #     addr = socket.gethostbyname(myname)
    #     return addr

    @staticmethod
    def file_name():
        return os.path.abspath(sys.argv[0])

    @staticmethod
    def script_name():
        file_name = Write.file_name()
        script = file_name.split('/')[-1].split('\\')[-1]
        return script

    @staticmethod
    def str_reformat(string):
        return string.replace("/", "_").replace(":", "_").replace("-", "_").replace(" ", "_").lower()

    @staticmethod
    def today(lc_utc=8):
        return str(dt.datetime.now() + dt.timedelta(hours=8 - lc_utc))[:10]

    @staticmethod
    def time(lc_utc=8):
        return Write.str_reformat(str(dt.datetime.now() + dt.timedelta(hours=8 - lc_utc))[:19])


class _DingMessage(Thread):
    def __init__(self, lc_utc, robot_id, content):
        super(_DingMessage, self).__init__(name='thread_ding_message')
        self._lc_utc = lc_utc
        self._robot_id = robot_id
        self._content = content

    @property
    def pc_name(self):
        return Write.pc_name()

    @Adorn.run_ensure(retry_times=10, traceback_print=False, sleep=0)
    def run(self):
        robot_id = self._robot_id
        content = self._content
        timestamp = int(time.time() * 1000)
        hmac_code = hmac.new(key=bytes(robot_id[1].encode(encoding='utf-8')), msg=bytes(f'{timestamp}\n{robot_id[1]}'.encode(encoding='utf-8')), digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        msg_content = content + '\n\n' + str(dt.datetime.now() + dt.timedelta(hours=8 - self._lc_utc))[:19] + '\n' + self.pc_name
        msg = {'msgtype': 'text', 'text': {'content': msg_content}}
        headers = {'Content-Type': 'application/json ;charset=utf-8'}
        url = f'{robot_id[0]}&timestamp={str(timestamp)}&sign={str(sign)}'
        body = json.dumps(msg)
        requests.post(url, data=body, headers=headers, timeout=2)

    @staticmethod
    def send_msg(content, robot_id, lc_utc=8):
        t = _DingMessage(lc_utc=lc_utc, robot_id=robot_id, content=content)
        t.start()


class DingMessage:
    def __init__(self, lc_utc, robot_id):
        self.lc_utc = lc_utc
        self.robot_id = robot_id

    def send_msg(self, content):
        if self.robot_id == '':
            return
        _DingMessage.send_msg(content=content, robot_id=self.robot_id, lc_utc=self.lc_utc)


class FuncTool:
    @staticmethod
    def thread_run(func, name=''):
        t = Thread(target=func, name=name)
        t.start()


class TimeBarCalTool:
    @staticmethod
    def seconds_interval(interval):
        unit = interval[-1:]
        num = int(interval[:-1])
        if unit == 'm':
            result = dt.timedelta(minutes=num).total_seconds()
        elif unit == 'h':
            result = dt.timedelta(hours=num).total_seconds()
        elif unit == 'd':
            result = dt.timedelta(days=num).total_seconds()
        elif unit == 'w':
            result = dt.timedelta(weeks=num).total_seconds()
        else:
            raise ValueError('interval parameter does not support.')
        return result

    @staticmethod
    def time_delta_cal(time_frame):
        frame = time_frame[-1]
        unit = int(time_frame[:-1])
        if frame == 'm':
            return dt.timedelta(minutes=unit)
        elif frame == 'h':
            return dt.timedelta(hours=unit)
        elif frame == 'd':
            return dt.timedelta(days=unit)
        elif frame == 'w':
            return dt.timedelta(weeks=unit)
        else:
            raise ValueError('time_frame parameter does not support.')

    @staticmethod
    def time_last_complete_bar(time_frame='1m', lc_utc=8):
        '''
        :param lc_utc:
        :return: shanghai time of last_complete_bar
        '''
        assert time_frame[-1] == 'm', 'time_frame must be minutes.'
        time_frame = int(time_frame[:-1])
        time_now = dt.datetime.now() + dt.timedelta(hours=8 - lc_utc)
        time_last_run_minute = (time_now.minute // time_frame) * time_frame
        time_last_run = dt.datetime(year=time_now.year, month=time_now.month, day=time_now.day, hour=time_now.hour, minute=time_last_run_minute)
        this_bar_start = time_last_run - dt.timedelta(minutes=time_frame)
        return this_bar_start

    @staticmethod
    def time_last_complete_bar_static(time_frame, time_now):
        '''
        :param time_frame: '1s' for one second, '2m' for two minutes
        :param time_now: str , now time
        :return: last complete bar time
        '''
        unit = time_frame[-1]
        time_frame = int(time_frame[:-1])
        time_now = pd.to_datetime(time_now)
        if unit == 'm':
            time_last_run_minute = (time_now.minute // time_frame) * time_frame
            time_last_run = dt.datetime(year=time_now.year, month=time_now.month, day=time_now.day, hour=time_now.hour, minute=time_last_run_minute)
            last_bar = time_last_run - dt.timedelta(minutes=time_frame)
        elif unit == 's':
            time_last_run_second = (time_now.second // time_frame) * time_frame
            time_last_run = dt.datetime(year=time_now.year, month=time_now.month, day=time_now.day, hour=time_now.hour, minute=time_now.minute, second=time_last_run_second)
            last_bar = time_last_run - dt.timedelta(seconds=time_frame)
        else:
            raise ValueError('time_frame should be minutes or seconds')
        return last_bar

    @staticmethod
    def time_end_bar(time_start, interval, bars_num_total):
        sec_one = TimeBarCalTool.seconds_interval(interval=interval)
        time_end = pd.to_datetime(time_start) + dt.timedelta(seconds=sec_one) * (bars_num_total - 1)
        return time_end

    @staticmethod
    def time_start_bar(time_end, interval, bars_num_total):
        sec_one = TimeBarCalTool.seconds_interval(interval=interval)
        time_end = pd.to_datetime(time_end) - dt.timedelta(seconds=sec_one) * (bars_num_total - 1)
        return time_end

    @staticmethod
    def bars_num(time_start, time_end, interval):
        sec_interval = TimeBarCalTool.seconds_interval(interval=interval)
        delta = (pd.to_datetime(time_end) - pd.to_datetime(time_start)).total_seconds()
        assert delta > 0, 'time order false.'
        assert delta == int(delta), 'time delta must be int.'
        num = (delta / sec_interval) + 1
        assert num == int(num), 'bars number must be int.'
        return int(num)

    @staticmethod
    def millisec_utc_sh_time_str(time_str):
        '''
        :param time_str: shanghai time string
        :return: utc millisecond
        '''
        utc_time = pd.to_datetime(time_str) - dt.timedelta(hours=8)
        utc_milliseconds = int(utc_time.timestamp() * 1000)
        return utc_milliseconds

    @staticmethod
    def sec_utc_sh_time_str(time_str):
        '''
        :param time_str: shanghai time string
        :return: utc millisecond
        '''
        utc_time = pd.to_datetime(time_str) - dt.timedelta(hours=8)
        utc_seconds = int(utc_time.timestamp())
        return utc_seconds


class BinanceKlineGet:
    def __init__(self, symbol):
        self.symbol = symbol

    @staticmethod
    def _parse_content(content):
        res = json.loads(content)
        data = pd.DataFrame(res).iloc[:, :-1]
        columns = ['time', 'open', 'high', 'low', 'close', 'vol', 'time_close_utc_stamp', 'amt', 'trades', 'buy_vol', 'buy_amt']
        f_col = ['open', 'high', 'low', 'close', 'vol', 'amt', 'buy_vol', 'buy_amt']
        data.columns = columns
        data[f_col] = data[f_col].astype(float)
        data['time'] = pd.to_datetime(data['time'], unit='ms')
        data['vwap'] = data['amt'] / data['vol']
        data['vwap'] = data['vwap'].fillna(data['close'])
        data.drop(columns=['time_close_utc_stamp'], inplace=True)
        data['buy_ratio_vol'] = data['buy_vol'] / data['vol']
        data['buy_ratio_amt'] = data['buy_amt'] / data['amt']
        return data

    @Adorn.run_ensure(retry_times=20, lc_utc=LC_UTC)
    def _requests_kline(self, interval, startTime, endTime):
        url = f'https://api.binance.com/api/v1/klines?symbol={self.symbol.replace("/", "").upper()}&interval={interval}&startTime={startTime}&endTime={endTime}&limit=1000'
        content = requests.get(url=url, headers=HEADERS).content
        return BinanceKlineGet._parse_content(content=content)

    @Adorn.run_ensure(retry_times=20, lc_utc=LC_UTC)
    def _requests_kline_from_time(self, interval, startTime, limit):
        url = f'https://api.binance.com/api/v1/klines?symbol={self.symbol.replace("/", "").upper()}&interval={interval}&startTime={startTime}&limit={limit}'
        content = requests.get(url=url, headers=HEADERS).content
        return BinanceKlineGet._parse_content(content=content)

    def get_kline_period(self, interval, time_start, time_end):
        num = TimeBarCalTool.bars_num(time_start=time_start, time_end=time_end, interval=interval)
        assert num <= 1000, 'period kline number must not be greater than 1000.'
        utc_start = TimeBarCalTool.millisec_utc_sh_time_str(time_str=time_start)
        utc_end = TimeBarCalTool.millisec_utc_sh_time_str(time_str=time_end)
        data = self._requests_kline(interval=interval, startTime=utc_start, endTime=utc_end)
        data['time'] = data['time'] + pd.Timedelta(hours=8)
        return data

    def get_kline_from_time(self, interval, time_start, limit=1000):
        utc_start = TimeBarCalTool.millisec_utc_sh_time_str(time_str=time_start)
        data = self._requests_kline_from_time(interval=interval, startTime=utc_start, limit=limit)
        data['time'] = data['time'] + pd.Timedelta(hours=8)
        return data


class BinanceFuturesKlineGet:
    def __init__(self, symbol, timeout=3):
        self.symbol = symbol
        self.timeout = timeout

    @staticmethod
    def _parse_content(content):
        res = json.loads(content)
        data = pd.DataFrame(res).iloc[:, :-1]
        columns = ['time', 'open', 'high', 'low', 'close', 'vol', 'time_close_utc_stamp', 'amt', 'trades', 'buy_vol', 'buy_amt']
        f_col = ['open', 'high', 'low', 'close', 'vol', 'amt', 'buy_vol', 'buy_amt']
        data.columns = columns
        data[f_col] = data[f_col].astype(float)
        data['time'] = pd.to_datetime(data['time'], unit='ms')
        data.drop(columns=['time_close_utc_stamp'], inplace=True)
        data['buy_ratio_vol'] = data['buy_vol'] / data['vol']
        data['buy_ratio_amt'] = data['buy_amt'] / data['amt']
        return data

    @Adorn.run_ensure(retry_times=20, lc_utc=LC_UTC)
    def _requests_kline(self, interval, startTime, endTime):
        url = f'https://fapi.binance.com/fapi/v1/klines?symbol={self.symbol.replace("/", "")}&interval={interval}&startTime={startTime}&endTime={endTime}&limit=1000'
        content = requests.get(url=url, headers=HEADERS, timeout=self.timeout).content
        return BinanceKlineGet._parse_content(content=content)

    @Adorn.run_ensure(retry_times=20, lc_utc=LC_UTC)
    def _requests_kline_from_time(self, interval, startTime, limit):
        url = f'https://fapi.binance.com/fapi/v1/klines?symbol={self.symbol.replace("/", "")}&interval={interval}&startTime={startTime}&limit={limit}'
        content = requests.get(url=url, headers=HEADERS, timeout=self.timeout).content
        return BinanceKlineGet._parse_content(content=content)

    def get_kline_period(self, interval, time_start, time_end):
        num = TimeBarCalTool.bars_num(time_start=time_start, time_end=time_end, interval=interval)
        assert num <= 1000, 'period kline number must not be greater than 1000.'
        utc_start = TimeBarCalTool.millisec_utc_sh_time_str(time_str=time_start)
        utc_end = TimeBarCalTool.millisec_utc_sh_time_str(time_str=time_end)
        data = self._requests_kline(interval=interval, startTime=utc_start, endTime=utc_end)
        data['time'] = data['time'] + pd.Timedelta(hours=8)
        return data

    def get_kline_from_time(self, interval, time_start, limit=1000):
        utc_start = TimeBarCalTool.millisec_utc_sh_time_str(time_str=time_start)
        data = self._requests_kline_from_time(interval=interval, startTime=utc_start, limit=limit)
        data['time'] = data['time'] + pd.Timedelta(hours=8)
        return data


class BinanceFuturesKlineMysql:
    @staticmethod
    def kline_to_mysql(db_name, bi_symbol, interval, time_start, table_name=None, time_end='', lc_utc=LC_UTC, timeout=1, verbose_out=True, limit=99):
        ml = MysqlLink()
        if not ml.database_exist(db_name=db_name):
            ml.create_database(db_name=db_name, enforce=True)
        mdl = MysqlDatabaseLink(db_name=db_name)
        mdl.drop_table(table_name=table_name)
        bi = BinanceFuturesKlineGet(symbol=bi_symbol, timeout=timeout)
        if not time_end:
            time_end = TimeBarCalTool.time_last_complete_bar(time_frame=interval, lc_utc=lc_utc)
        else:
            time_end = pd.to_datetime(time_end)
        if not table_name:
            table_name = bi_symbol.lower().replace('usdt', '')
        data = bi.get_kline_from_time(interval=interval, time_start=time_start, limit=limit)
        last_time = data['time'].iloc[-1]
        data.to_sql(name=table_name, con=mdl.con, if_exists='replace', index=None)
        while True:
            # if abs((time_end - last_time).total_seconds()) <= (60 * 60 * 24 * pre_days):
            if last_time >= time_end:
                break
            seconds = 0.1

            time.sleep(seconds)
            data = bi.get_kline_from_time(interval=interval, time_start=last_time + TimeBarCalTool.time_delta_cal(time_frame=interval), limit=limit)
            data.to_sql(name=table_name, con=mdl.con, if_exists='append', index=None)
            if verbose_out:
                print(data)
            print(bi_symbol)
            last_time = data['time'].iloc[-1]
