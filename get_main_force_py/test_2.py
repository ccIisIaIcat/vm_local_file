from selenium import webdriver
import time


driver = webdriver.Chrome()
driver.implicitly_wait(20) # 隐性等待，最长等30秒
driver.get("https://www.cmegroup.com/market-data/delayed-quotes/metals.html")
time.sleep(3)
info = driver.find_element_by_xpath('//*[@id="cmeDelayedQuotes2"]/tbody/tr[3]/td[3]')
print(info.text)
driver.quit()