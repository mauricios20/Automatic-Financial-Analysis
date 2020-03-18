from pandas.io.html import read_html
import pandas as pd
from sqlalchemy import create_engine

# This first phase will retrive the stock prices from all the companies
# in the S&P 500 from wikipedia
url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
wikitables = read_html(url, attrs={"class": "wikitable"})

print("Extracted {num} wikitables".format(num=len(wikitables)))
# SYM = wikitables[0]['Symbol'].tolist()

engine = create_engine('mysql+pymysql://mausolorio:ducinALTUM7!@localhost/s&p500')
wikitables[0].to_sql('info', con=engine, if_exists='replace')
