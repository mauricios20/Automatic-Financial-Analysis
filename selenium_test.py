from selenium import webdriver
from selenium.webdriver.common.keys import Keys

browser = webdriver.Chrome("C:/Users/mauri/Documents/chromedriver.exe")
browser.get('https://www.sec.gov/edgar/searchedgar/companysearch.html')
searchBar = browser.find_element_by_id('cik')
searchBar.send_keys('AAPL')
searchBar.send_keys(Keys.ENTER)
