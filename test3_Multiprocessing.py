from alpha_vantage.timeseries import TimeSeries
import pandas as pd
import concurrent.futures
import time
from sqlalchemy import create_engine

# Test to create data frame and push it to db
start = time.perf_counter()


def df_to_db(df):
    df.to_sql(tbname, con=engine, if_exists=action, index=True)


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


key = 'B74JHRMS7KY09FBB'
ts = TimeSeries(key,  output_format='pandas', indexing_type='date')

companies = ['HRL', 'KR', 'LW', 'MKC', 'TAP', 'MDLZ',
             'MNST', 'PEP', 'PM', 'PG', 'SYY', 'TSN', 'WMT', 'WBA']
# 'AMT', 'AIV', 'AVB', 'BXP'
# Creates a dic of dataframe atomatically from lists of companies
df_dict = dict(('df_' + str(x), pd.DataFrame()) for x in range(len(companies)))

chunk = [companies[i:i + 5] for i in range(0, len(companies), 5)]

db = 's&p500'
engine = create_engine('mysql+pymysql://mausolorio:ducinALTUM7!@localhost/'
                       + db)
tbname = 'consumerstaples'
action = 'append'

for c in chunk:
    print(f'Reading companies {c}')
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(download_data, c)
        for result in results:
            print(result)
    print('Waiting 1min for API connection to restore')
    time.sleep(61)
