# Phase 2 will connect to existing DB to extract SYM for API stock price.
# Install mysql-connector-python 8.0.19
# pip install dnspython
# pip install protobuf==3.6.1
# pip install alpha_vantage
# Key B74JHRMS7KY09FBB
import mysql.connector
from alpha_vantage.timeseries import TimeSeries
import pandas as pd
from sqlalchemy import create_engine
import concurrent.futures
import time


def create_conn(dbname, hosname="localhost", username="mausolorio",
                pas="ducinALTUM7!"):
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


def convertTuple(tup):
    str = ''.join(tup)
    return str


def df_to_db(df):
    df.to_sql(tbname, con=engine, if_exists=action, index=False)


def download_data(company):
    data, meta = ts.get_monthly(symbol=company)
    if meta['2. Symbol'] in companies:
        n = companies.index(meta['2. Symbol'])
        df_dict['df_'+str(n)] = df_dict['df_'+str(n)].append(data)
        df_dict['df_'+str(n)]['Symbol'] = meta['2. Symbol']
        df_to_db(df_dict['df_'+str(n)])
    else:
        print(f'Company {company} not on API')
    return f'Dtf of {company} downloaded & {action}ed to {db} TABLE: {tbname}'


# Get the list of companies that belong to the specific sector of interest
create_conn('s&p500')
sql_Query = "SELECT Symbol FROM info WHERE Sector = 'Real Estate'"
cursor.execute(sql_Query)
records = cursor.fetchall()
print("Total number of rows in mydb is: ", cursor.rowcount)
close_connection(mydb, cursor)

# Covert the tupple list of records into a str list of companies
companies = []
for c in records:
    string = convertTuple(c)
    companies.append(string)

# Creates a dic of dataframes atomatically from lists of companies
# and divide company list in chunks of 5
df_dict = dict(('df_' + str(x), pd.DataFrame()) for x in range(len(companies)))
chunk = [companies[i:i + 5] for i in range(0, len(companies), 5)]

# Established connection with API
key = 'B74JHRMS7KY09FBB'
ts = TimeSeries(key,  output_format='pandas', indexing_type='date')
db = "s&p500"
engine = create_engine('mysql+pymysql://mausolorio:ducinALTUM7!@localhost/'
                       + db)
# Create Table name and acction to take

tbname = 'realestate'
action = 'append'
for c in chunk:
    print(f'Reading companies {c}')
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(download_data, c)
        for result in results:
            print(result)
    print('Waiting 1min for API connection to restore')
    time.sleep(61)
