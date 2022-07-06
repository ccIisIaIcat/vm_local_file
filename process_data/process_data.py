import pymysql
from sqlalchemy import create_engine
import pandas as pd
import json
import numpy as np


# 根据流程
# 1.我们先建立数据库的连接信息
host = "127.0.0.1" # 数据库的ip地址
user = "root"  # 数据库的账号
password = ""  # 数据库的密码
port = 3306  # mysql数据库通用端口号


engine = create_engine("mysql+pymysql://root:@localhost/orderbook_and_trade")

#3编写sql
# sql = 'SELECT * FROM future.member WHERE MobilePhone = 18876153542 '
sql =  'select * from orderbook_and_trade.orderbook_info'

data = pd.read_sql(sql,engine)

print(data["bids_list"][0])
aaa = data["bids_list"][0]
aaa = json.loads(aaa)
print(type(aaa))
asks_matrix = []
bids_matrix = []
for i in range(15):
    asks_matrix.append([])
    bids_matrix.append([])
for i in range(len(data)):
    asks_map = json.loads(data["asks_list"][i])
    bids_map = json.loads(data["bids_list"][i])
    signal = 0
    for k,v in asks_map.items():
        print(signal)
        asks_matrix[signal].append(float(k))
        asks_matrix[signal+5].append(float(v[0]))
        asks_matrix[signal+10].append(float(v[1]))
        signal += 1
    signal = 0
    for k,v in bids_map.items():
        bids_matrix[signal].append(float(k))
        bids_matrix[signal+5].append(float(v[0]))
        bids_matrix[signal+10].append(float(v[1]))
        signal += 1
print(asks_matrix)

new_pd = pd.DataFrame(asks_matrix,columns=["a_p_1","a_p_2","a_p_3","a_p_4","a_p_5","a_q_1","a_q_2","a_q_3","a_q_4","a_q_5","a_n_1","a_n_2","a_n_3","a_n_4","a_n_5"])

# np.array()
