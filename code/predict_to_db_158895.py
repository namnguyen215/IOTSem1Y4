import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_squared_error
import pickle
import sqlalchemy
from Node import Node
from MyDecisionTreeRegressor import MyDecisionTreeRegressor
import datetime
import time
def connect_to_db():
    database_username = 'namnp'
    database_password = '12345678'
    database_ip       = '127.0.0.1'
    database_name     = 'iot'
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                format(database_username, database_password, 
                                                        database_ip, database_name))
    return database_connection
def insert_to_db(df, model, database_connection):
    cols = ['time','day_of_week','REPORT_ID','DISTANCE_IN_METERS','vehicleCount_lag_1','vehicleCount_lag_2','vehicleCount']
    cols_with_timestamp = ['timestamp','day_of_week','REPORT_ID','DISTANCE_IN_METERS','vehicleCount_lag_1','vehicleCount_lag_2','vehicleCount']
    df_result = df[cols_with_timestamp]
    df_predict = df[cols]
    input_data = df_predict.iloc[:, :-1].values
    df_result['predict'] = model.predict(input_data)
    df_result.to_sql(con=database_connection, name='traffic_with_lag_data_predict', index=False, if_exists='append')

# def get_data_by_time(cur_time, database_connection):
#     df = pd.read_sql('SELECT * FROM `traffic_with_lag_data` where timestamp = '+cur_time+'order by REPORT_ID', con=database_connection)
#     return df

def get_all_data(database_connection):
    df = pd.read_sql('SELECT * FROM `traffic_with_lag_data`', con=database_connection)
    return df

def handle_data(df, model, database_connection):

    for month in range (8,10):
        for day in range(1,31):
            for hour in range(0,24):
                cur_time = str(datetime.datetime(2014, month, day, hour))
                print(cur_time)
                df_now = df[df['timestamp'] == cur_time]
                if df_now.empty:
                    print('None '+cur_time)
                else:
                    print('Processing '+cur_time)
                    insert_to_db(df_now, model, database_connection)
                    time.sleep(5)

#define main method
def main():
    database_connection = connect_to_db()
    df = get_all_data(database_connection)
    # load the model
    with open('../model/MyDecisionTreeRegressorModelWithDOW.sav', 'rb') as f:
        model = pickle.load(f)
    handle_data(df, model, database_connection)

if __name__ == "__main__":
    main()