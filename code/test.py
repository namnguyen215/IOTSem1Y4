import datetime
import pandas as pd
import sqlalchemy

def connect_to_db():
    database_username = 'namnp'
    database_password = '12345678'
    database_ip       = '127.0.0.1'
    database_name     = 'iot'
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                format(database_username, database_password, 
                                                        database_ip, database_name))
    return database_connection


def get_all_data(database_connection):
    df = pd.read_sql('SELECT * FROM `traffic_with_lag_data`', con=database_connection)
    return df


def handle_data(df, database_connection):
    cols = ['time','day_of_week','REPORT_ID','DISTANCE_IN_METERS','vehicleCount_lag_1','vehicleCount_lag_2','vehicleCount']
    for month in range (8,10):
        for day in range(1,32):
            for hour in range(0,24):
                cur_time = str(datetime.datetime(2014, month, day, hour))
                print(cur_time)
                df_now = df[df['timestamp'] == cur_time]
                df_now = df_now[cols]
                if df_now.empty:
                    print('None '+cur_time)
                else: 
                    print(df_now)
                    break
            break
        break

def insert_to_db(df, model, database_connection):
    df['predict'] = model.predict(df)
    df.to_sql(con=database_connection, name='traffic_with_lag_data_predict', index=False, if_exists='append')

def main():
    database_connection = connect_to_db()
    df = get_all_data(database_connection)
    handle_data(df, database_connection)

#call main method
if __name__ == "__main__":
    main()