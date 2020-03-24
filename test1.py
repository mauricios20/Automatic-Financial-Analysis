from alpha_vantage.timeseries import TimeSeries
import pandas as pd
import concurrent.futures
import time


start = time.perf_counter()

key = 'B74JHRMS7KY09FBB'
ts = TimeSeries(key,  output_format='pandas', indexing_type='date')

companies = ['MMM', 'ABT', 'ABBV', 'ACN', 'ATVI', 'AMD', 'AES', 'AFL']
# Creates a dic of dataframe atomatically from lists of companies
df_dict = dict(('df_' + str(x), pd.DataFrame()) for x in range(len(companies)))

chunk = [companies[i:i + 5] for i in range(0, len(companies), 5)]

# for i in chunk:
#     print(i)
#     time.sleep(5)

# This part works perfectly


def download_data(company):
    data, meta = ts.get_monthly(symbol=company)
    if meta['2. Symbol'] in companies:
        n = companies.index(meta['2. Symbol'])
        df_dict['df_'+str(n)] = df_dict['df_'+str(n)].append(data)
        df_dict['df_'+str(n)]['Symbol'] = meta['2. Symbol']
    else:
        print(f'Company {company} not on API')
    return f'Done Dowloading...{company}'


for c in chunk:
    print(f'Reading companies {c}')
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(download_data, c)
        for result in results:
            print(result)
    print('Waiting 1min for API connection to restore')
    time.sleep(61)

finish = time.perf_counter()

print(f'Finished in {round(finish-start, 2)} second(s)')
