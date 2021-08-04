'''
将下载好的k线数据从mysql数据库中读出，并且保存为本地hdf文件，保存好的hdf文件可以直接读取为pandas.DataFrame，方便后续研究

'''
from utils import *

db_name = 'bi_f_kline_test_1m'
table_list = MysqlHandler.all_tables(db_name=db_name)
print(table_list)
MysqlHandler.tables_out_to_hdf(db_name=db_name)

# data = pd.read_hdf(r'bi_f_kline_test_1m_dash.h5', key='data')
# print(data)
