import pandas as pd
import numpy as np
import lightgbm as lgb
import matplotlib.pyplot as plt
from sklearn import preprocessing

LAG_LIST = [1,3,7,30] #lag信息序列
K_FOLDS = 15 #测试集个数
OVERLAP_RATIO = 0.3 #cv测试集覆盖比率
TEST_DATA_RATIO = 0.2 #每组中测试集所占比例
PORTFOLIO_SIZE = 30
seed0 = 8586 #随机种子
#提升树的参数设定
params = {
    'early_stopping_rounds': 100,
    'objective': 'regression',
    'metric': 'rmse',
    'boosting_type': 'gbdt',
    'max_depth': 5,
    'verbose': -1,
    'max_bin':600,
    'min_data_in_leaf':50,
    'learning_rate': 0.01,
    'subsample': 0.7,
    'subsample_freq': 1,
    'feature_fraction': 1,
    'lambda_l1': 0.5,
    'lambda_l2': 2,
    'seed':seed0,
    'feature_fraction_seed': seed0,
    'bagging_fraction_seed': seed0,
    'drop_seed': seed0,
    'data_random_seed': seed0,
    'extra_trees': True,
    'extra_seed': seed0,
    'zero_as_missing': True,
    "first_metric_only": True
    }

lbl = preprocessing.LabelEncoder()

train_data_price = pd.read_csv('kaggle_data/JPX/train_files/stock_prices.csv')
train_data_price = train_data_price[['Date','SecuritiesCode','Open','Close','Target','AdjustmentFactor']]
stock_info = pd.read_csv('kaggle_data/JPX/stock_list.csv')
stock_info = stock_info[['SecuritiesCode','17SectorCode']]
stock_info['17SectorCode'] = lbl.fit_transform(stock_info['17SectorCode'].astype(str))
stock_info['17SectorCode'] = lbl.fit_transform(stock_info['17SectorCode'].astype(int))
train_data_price = pd.merge(train_data_price,stock_info,how='left',on='SecuritiesCode')
prerank_info = pd.read_csv('kaggle_data/JPX/tool_data/rank_info_3.csv')
prerank_info = prerank_info[['Date','SecuritiesCode',f'lag_rank_{PORTFOLIO_SIZE}']]
train_data_price = pd.merge(train_data_price,prerank_info,how='left',on=['Date','SecuritiesCode'])
train_data_price[f'lag_rank_{PORTFOLIO_SIZE}'].fillna(0)


def get_date_divide(date_list,k_folds, overlap_ratio,test_data_ratio):
    # (k*0.3+0.7)*l = L => l = L/(k*0.3+0.7)
    single_length = int(len(date_list)/(k_folds*overlap_ratio+1-overlap_ratio))
    gap_size = int(single_length*overlap_ratio)
    date_set = []
    for i in range(k_folds):
        start_date = date_list[i*gap_size]
        if i*gap_size+single_length < len(date_list):
            end_date = date_list[i*gap_size+single_length]
        else :
            end_date = date_list[len(date_list)-1]
        mid_date = date_list[i*gap_size+int(single_length*(1-test_data_ratio))]
        date_set.append([start_date,mid_date,end_date])
    return date_set

def get_adjustment(list_,lag_length):
    list_ = np.array(list_)
    base_point = 1
    answer_list = []
    for i in range(lag_length):
        base_point = base_point*list_[i]
        answer_list.append(base_point)
    for i in range(len(list_)-lag_length):
        base_point = base_point*list_[i+lag_length]/list_[i]
        answer_list.append(base_point)
    return answer_list

def get_lag_info(train_data,lag_list):
    train_data['target_ne'] = train_data.groupby(['SecuritiesCode'])['Target'].shift(-1)
    train_data['target_ne'] = train_data['target_ne'].apply(np.log1p)
    for lag_length in lag_list:
        print('data_process:',lag_length)
        train_data['adj'] = train_data.groupby(['SecuritiesCode'])['AdjustmentFactor'].transform(lambda ls_:get_adjustment(ls_,lag_length))
        train_data[f'lag_{lag_length}_info'] = train_data.groupby(['SecuritiesCode'])['Close'].shift(lag_length)
        train_data[f'lag_{lag_length}_info'] = (train_data['Close']/train_data['adj']-train_data[f'lag_{lag_length}_info'])/train_data[f'lag_{lag_length}_info']
        train_data[f'lag_{lag_length}_info'] = train_data[f'lag_{lag_length}_info'].apply(np.log1p)
        train_data = train_data.drop(columns=['adj'])
    train_data = train_data.drop(columns=['Open','Close','AdjustmentFactor'])
    train_data = train_data.dropna(axis=0,how='any')
    return train_data

def add_rank(df):
    df["Rank"] = df.groupby("Date")["predict_tartget"].rank(ascending=False, method="first") - 1 
    df["Rank"] = df["Rank"].astype("int")
    return df

def add_random_rank(df):
    df["Rank"] = df.groupby("Date")['SecuritiesCode'].rank(ascending=False, method="first") - 1 
    df["Rank"] = df["Rank"].astype("int")
    return df

def add_prerank_rank(df):
    df["Rank"] = df.groupby("Date")[f'lag_rank_{PORTFOLIO_SIZE}'].rank(ascending=False, method="first") - 1 
    df["Rank"] = df["Rank"].astype("int")
    return df


def calc_spread_return_per_day(df, portfolio_size=200, toprank_weight_ratio=2):
    assert df['Rank'].min() == 0
    assert df['Rank'].max() == len(df['Rank']) - 1
    weights = np.linspace(start=toprank_weight_ratio, stop=1, num=portfolio_size)
    purchase = (df.sort_values(by='Rank')['Target'][:portfolio_size] * weights).sum() / weights.mean()
    short = (df.sort_values(by='Rank', ascending=False)['Target'][:portfolio_size] * weights).sum() / weights.mean()
    return purchase - short

def calc_spread_return_sharpe(df: pd.DataFrame, portfolio_size=200, toprank_weight_ratio=2):
    buf = df.groupby('Date').apply(calc_spread_return_per_day, portfolio_size, toprank_weight_ratio)
    sharpe_ratio = buf.mean() / buf.std()
    return sharpe_ratio, buf


train_data_price = get_lag_info(train_data_price,LAG_LIST)
date_list = train_data_price.sort_values(by='Date',ascending=True)['Date'].unique()
print(list(train_data_price))
date_list = get_date_divide(date_list,K_FOLDS,OVERLAP_RATIO,TEST_DATA_RATIO)
sector_set = train_data_price['17SectorCode'].unique()

ratio_set = []
random_ratio_set = []
prerank_set = []
features_importance = []

for time_set in date_list:
    print(time_set)
    tool_dataframe = pd.DataFrame()
    for sector in sector_set:
        train_data = train_data_price[(train_data_price['Date']>time_set[0]) & (train_data_price['Date']<time_set[1]) & (train_data_price['17SectorCode'] == sector)]
        test_data = train_data_price[(train_data_price['Date']>time_set[1]) & (train_data_price['Date']<time_set[2]) & (train_data_price['17SectorCode'] == sector)]
        x_train = train_data.drop(columns=['Date','SecuritiesCode','Target','17SectorCode',f'lag_rank_{PORTFOLIO_SIZE}'])
        y_train = train_data['Target']
        x_test = test_data.drop(columns=['Date','SecuritiesCode','Target','17SectorCode',f'lag_rank_{PORTFOLIO_SIZE}'])
        y_test = test_data['Target']
        train_dataset = lgb.Dataset(x_train,y_train)
        val_dataset = lgb.Dataset(x_test,y_test,reference=train_dataset)
        model = lgb.train(params = params,
                            train_set = train_dataset, 
                            valid_sets=[train_dataset, val_dataset],
                            valid_names=['tr', 'vl'],
                            num_boost_round = 5000,
                            verbose_eval = 100,   
                            )
        features_importance.append(model.feature_importance())
        test_data['predict_tartget'] = model.predict(x_test)
        tool_dataframe = pd.concat([tool_dataframe,test_data],sort=False)
    add_rank(tool_dataframe)
    tool_dataframe = tool_dataframe[['Date','SecuritiesCode','Target','Rank']]
    tool_dataframe = pd.merge(tool_dataframe,prerank_info,how='left',on=['Date','SecuritiesCode'])
    ratio_set.append(calc_spread_return_sharpe(tool_dataframe)[0])
    add_random_rank(tool_dataframe)
    random_ratio_set.append(calc_spread_return_sharpe(tool_dataframe)[0])
    add_prerank_rank(tool_dataframe)
    prerank_set.append(calc_spread_return_sharpe(tool_dataframe)[0])
    print(">>>>>>>>>>>>>>")

tool_set = []
name_list = []
for i in range(K_FOLDS):
    tool_set.append(i+1)
    name_list.append(f'num_{i+1}')
tool_set_2 = []
for i in range(len(LAG_LIST)+1):
    tool_set_2.append(i+1)

lala = np.array(ratio_set)
lala_2 = np.array(random_ratio_set)
lala_3 = np.array(prerank_set)

for i in range(K_FOLDS):
    plt.plot(tool_set_2,features_importance[i])
plt.legend(name_list)
plt.show()
plt.plot(tool_set,ratio_set)
plt.plot(tool_set,random_ratio_set)
plt.plot(tool_set,prerank_set)
plt.legend(['predict','Random','prerank'])
print(lala.mean())
print(lala_2.mean())
print(lala_3.mean())
plt.show()