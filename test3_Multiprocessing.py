from alpha_vantage.timeseries import TimeSeries
import pandas as pd
import concurrent.futures
import time
import mysql.connector
from sqlalchemy import create_engine

# Test to create data frame and push it to db
start = time.perf_counter()

# key = 'B74JHRMS7KY09FBB'
# ts = TimeSeries(key,  output_format='pandas', indexing_type='date')
#
# companies = ['MMM', 'ABT', 'ABBV', 'ACN', 'ATVI', 'AMD', 'AES', 'AFL']
# # Creates a dic of dataframe atomatically from lists of companies
# df_dict = dict(('df_' + str(x), pd.DataFrame()) for x in range(len(companies)))
#
# chunk = [companies[i:i + 5] for i in range(0, len(companies), 5)]

# for i in chunk:
#     print(i)
#     time.sleep(5)

# Query = "SELECT Symbol FROM info WHERE Sector = 'Real Estate'"


def create_conn(dbname, hosname="localhost", username="mausolorio", pas="ducinALTUM7!"):
    """Create connection with database"""
    global mydb, cursor
    mydb = mysql.connector.connect(host=hosname, user=username,
                                   passwd=pas, database=dbname)
    cursor = mydb.cursor()
    print(f'Connection has been established with {dbname} database')


def close_connection(mydb, cursor):
    """Close connection data base"""
    mydb.close()
    cursor.close()
    print("MySQL connection is closed")


# def download_data(company):
#     data, meta = ts.get_monthly(symbol=company)
#     if meta['2. Symbol'] in companies:
#         n = companies.index(meta['2. Symbol'])
#         df_dict['df_'+str(n)] = df_dict['df_'+str(n)].append(data)
#         df_dict['df_'+str(n)]['Symbol'] = meta['2. Symbol']
#     else:
#         print(f'Company {company} not on API')
#     return f'Done Dowloading...{company}'

df1 = pd.DataFrame({'name': ['User 1', 'User 2', 'User 3']})
df2 = pd.DataFrame({'name': ['User 4', 'User 5', 'User 6']})
frames = [df1, df2]


def df_to_db(db, tbname, df, action):
    engine = create_engine('mysql+pymysql://mausolorio:ducinALTUM7!@localhost/'+db)
    df.to_sql(tbname, con=engine, if_exists=action)
    print(f'Dataframe has being {action}ed to {db} TABLE: {tbname}')


df_to_db('s&p500', 'test2', frames, 'replace')
# create_conn("s&p500")
# sql_Query = "DROP TABLE test2"
# cursor.execute(sql_Query)
# close_connection(mydb, cursor)


# Works perfectly, but it needs to end immediately after loops for the last
# time
# for c in chunk:
#     print(f'Reading companies {c}')
#     with concurrent.futures.ThreadPoolExecutor() as executor:
#         results = executor.map(download_data, c)
#         for result in results:
#             print(result)
#     print('Waiting 1min for API connection to restore')
#     time.sleep(61)

finish = time.perf_counter()

print(f'Finished in {round(finish-start, 2)} second(s)')
