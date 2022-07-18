import requests
from bs4 import BeautifulSoup
from lxml import etree

print("hello world")
header = {"User-Agent": "PostmanRuntime/7.29.0"}
response = requests.get("https://www.cmegroup.com/market-data/delayed-quotes/metals.html",headers=header)
soup = BeautifulSoup(response.text,'html.parser')
dom = etree.HTML(str(soup))
print(dom.xpath('//*[@id="cmeDelayedQuotes2"]/tbody/tr[3]/td[3]'))

