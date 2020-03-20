from pandas.io.html import read_html
from sqlalchemy import create_engine

# This first phase will retrive the stock prices from all the companies
# in the S&P 500 from wikipedia
url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
wikitables = read_html(url, attrs={"class": "wikitable"})

print("Extracted {num} wikitables".format(num=len(wikitables)))

# Rename Colum Names - Eliminate Spaces in Names to make Query easy.
wikitables[0].rename(columns={'GICS Sector': 'Sector',
                              'GICS Sub Industry': 'Industry',
                              'Headquarters Location': 'HQ',
                              'Date first added': 'added'},
                     inplace=True)

# Get all info into a Database for further analysis
engine = create_engine('mysql+pymysql://mausolorio:ducinALTUM7!@localhost/s&p500')
wikitables[0].to_sql('info', con=engine, if_exists='replace')
