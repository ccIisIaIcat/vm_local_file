import pandas as pd
import numpy as np
from sklearn import preprocessing
import warnings
warnings.filterwarnings("ignore")

PORTFOLIO_SIZE = 30 #指定排名组合计算的是多少天前的组合
LINUX_PATH = f"/home/ubuntu/test/calculation/rank_lag_{PORTFOLIO_SIZE}.csv"

# 投资组合权重
toprank_weight_ratio = 2
portfolio_size = 250
weights = np.linspace(start=toprank_weight_ratio, stop=1, num=portfolio_size)
lbl = preprocessing.LabelEncoder()
date_obj_list = []

READ_PATH = 'kaggle_data/JPX/train_files/stock_prices.csv'
train_data_price = pd.read_csv(READ_PATH)

train_data_price['Signal'] = train_data_price.groupby(['SecuritiesCode'])['Target'].transform('count')
date_list_2 = pd.DataFrame()
security_list = pd.DataFrame()
date_list = train_data_price['Date'].unique()
date_list_2['Date'] = train_data_price['Date'].unique()
security_list['SecuritiesCode'] = train_data_price['SecuritiesCode'].unique()
date_list_2['tool'] = 1
security_list['tool'] = 1
new_data_frame = pd.merge(date_list_2,security_list,on='tool',how='left')
train_data_price = pd.merge(new_data_frame,train_data_price,on=['SecuritiesCode','Date'],how='left')
train_data_price = train_data_price[['Date','SecuritiesCode','Target']]

def cal_sharp_ratio(list_):
    return list_.mean()/list_.std()

def get_best_part(original_list,original_weight,new_weight,matrix,signal_list,positive_nagetive):
    max_sharp = -99999
    if positive_nagetive == 1:
        for i in range(len(matrix)):
            if signal_list[i] == 0:
                sharp_now = cal_sharp_ratio((original_list*original_weight+matrix[i]*new_weight)/(original_weight+new_weight))
                if sharp_now > max_sharp:
                    max_sharp = sharp_now
                    answer_id = i
    else:
        for i in range(len(matrix)):
            if signal_list[i] == 0:
                sharp_now = cal_sharp_ratio((original_list*original_weight-matrix[i]*new_weight)/(original_weight+new_weight))
                if sharp_now > max_sharp:
                    max_sharp = sharp_now
                    answer_id = i
    return answer_id
      
def get_the_best_portfolio(info_matrix,scode_list):
    up_portfolio = []
    down_portfolio = []
    signal_list = np.zeros(len(info_matrix))
    id_list_up = np.zeros(portfolio_size)
    id_list_down = np.zeros(portfolio_size)
    temp_list = np.zeros(len(info_matrix[0]))
    weight_now = 0
    weight_now_down = 0
    for i in range(portfolio_size*2):
        if i%2 == 0:
            id_now = i // 2
            max_id = get_best_part(temp_list,weight_now,weights[id_now],matrix=info_matrix,signal_list=signal_list,positive_nagetive=1)
            temp_list = (temp_list*weight_now+info_matrix[max_id]*weights[id_now])/(weight_now+weights[id_now])
            signal_list[max_id] = 1
            weight_now += weights[id_now]
            id_list_up[id_now] = max_id
            up_portfolio.append(info_matrix[max_id])
        else:
            id_now = i // 2
            max_id = get_best_part(temp_list,weight_now,weights[id_now],matrix=info_matrix,signal_list=signal_list,positive_nagetive=-1)
            temp_list = (temp_list*weight_now-info_matrix[max_id]*weights[id_now])/(weight_now+weights[id_now])
            signal_list[max_id] = -1
            weight_now_down += weights[id_now]
            id_list_down[id_now] = max_id
            down_portfolio.append(info_matrix[max_id])
    up_list = []
    down_list = []
    for num_1 in id_list_up:
        up_list.append(scode_list[int(num_1)])
    for num_2 in id_list_down:
        down_list.append(scode_list[int(num_2)])
    return up_list,down_list
date_list.sort()
for i in range(len(date_list)-PORTFOLIO_SIZE-2):
    obj_now = [date_list[i],date_list[i+PORTFOLIO_SIZE]]
    date_obj_list.append([obj_now,date_list[i+PORTFOLIO_SIZE+2]])

up_num_list = []
down_num_list = []
for i in range(portfolio_size):
    up_num_list.append(portfolio_size-i)
    down_num_list.append(i-portfolio_size)

train_data_price = train_data_price[['Date','SecuritiesCode','Target']]
n_sum = len(date_obj_list)
counter = 0
final_df = pd.DataFrame(columns=['SecuritiesCode',f'lag_rank_{PORTFOLIO_SIZE}','Date'])


portfolio_set = [30,15,7,3]

for p_s in portfolio_set:
	PORTFOLIO_SIZE = p_s
	for date_obj in date_obj_list:
		counter += 1
		print(date_obj,"percent:",counter/n_sum,"   counter:",counter)
		new_data = train_data_price[(train_data_price['Date']>=date_obj[0][0]) & (train_data_price['Date']<=date_obj[0][1])]
		new_data['Target'] = new_data.groupby(['SecuritiesCode'])['Target'].apply(lambda x: x.fillna(x.mean()))
		test_matrix = []
		new_info = pd.DataFrame(new_data.groupby(['SecuritiesCode'])['Target'])
		scode_list = list(new_info[0].values)
		new_info = list(new_info[1])
		for obj in new_info:
			test_matrix.append(np.array(obj.values))
		a_list,b_list = get_the_best_portfolio(test_matrix,scode_list)
		temp_pd = pd.DataFrame()
		temp_pd['SecuritiesCode'] = (a_list+b_list)
		temp_pd[f'lag_rank_{PORTFOLIO_SIZE}'] = (up_num_list+down_num_list)
		temp_pd['Date'] = date_obj[1]
		final_df = pd.concat([final_df,temp_pd],ignore_index=True)
		if counter % 10 == 0:
			final_df.to_csv(f'kaggle_data/JPX/tool_data/rank_lag_{PORTFOLIO_SIZE}.csv')
			print(f'kaggle_data/JPX/tool_data/rank_lag_{PORTFOLIO_SIZE}.csv在',"counter为",counter,"时存储了一次")
