import time
import pandas as pd


save_path = "E:\\kaggle_project\\kaggle_project\\linux_test\\temp_time.csv"
temp_time = time.strftime('%H:%M:%S',time.localtime(time.time()))
print(temp_time)
new_dataframe = pd.DataFrame()
new_dataframe['time_record'] = [temp_time]
while True:
    time.sleep(1)
    temp_time = time.strftime('%H:%M:%S',time.localtime(time.time()))
    new_dataframe.loc[len(new_dataframe.index)] = [temp_time]
    print(temp_time)
    new_dataframe.to_csv(save_path)