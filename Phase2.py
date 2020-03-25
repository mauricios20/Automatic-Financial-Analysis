# Phase 2 will connect to existing DB to extract SYM for API stock price.
# Install mysql-connector-python 8.0.19
# pip install dnspython
# pip install protobuf==3.6.1
# pip install alpha_vantage
# Key B74JHRMS7KY09FBB
import mysql.connector
from alpha_vantage.timeseries import TimeSeries
import pandas as pd

# Class Query:
mydb = mysql.connector.connect(host="localhost", user="mausolorio",
                               passwd="ducinALTUM7!", database='s&p500')

sql_Query = "SELECT Symbol FROM info WHERE Sector = 'Real Estate'"
cursor = mydb.cursor()
cursor.execute(sql_Query)
records = cursor.fetchall()

print("Total number of rows in mydb is: ", cursor.rowcount)
mydb.close()
cursor.close()
print("MySQL connection is closed")

# Covert the tupple list of records into a str list of companies


def convertTuple(tup):
    str = ''.join(tup)
    return str


companies = []
for c in records:
    string = convertTuple(c)
    companies.append(string)

# Creates a dic of dataframe atomatically from lists of companies
df_dict = dict(('df_' + str(x), pd.DataFrame()) for x in range(len(companies)))

# Class API:
# Your key here
key = 'B74JHRMS7KY09FBB'
ts = TimeSeries(key,  output_format='pandas', indexing_type='date')

for c in companies:
    data, meta = ts.get_monthly(symbol=c)
    if meta['2. Symbol'] in companies:
        n = companies.index(meta['2. Symbol'])
        df_dict['df_'+str(n)] = df_dict['df_'+str(n)].append(data)
        df_dict['df_'+str(n)]['Symbol'] = meta['2. Symbol']
    else:
        print('You fucked up')

# print(df_dict['df_0'].head(2))
print(df_dict['df_1'].head(2))

# I need to concatenate all data frames into one an create a table in database
# for this df. Look into Multiprocessing!
