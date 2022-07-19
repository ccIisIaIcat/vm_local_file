# 搭建服务
import socket
from selenium import webdriver
import time
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(('127.0.0.1', 8898))
server.listen(50)
while True:
    data, addr = server.accept()

    driver = webdriver.Chrome()
    driver.implicitly_wait(20) # 隐性等待，最长等30秒
    driver.get("https://www.cmegroup.com/market-data/delayed-quotes/metals.html")
    time.sleep(3)
    info = driver.find_element_by_xpath('//*[@id="cmeDelayedQuotes2"]/tbody/tr[3]/td[3]')
    my_info = info.text
    driver.quit()
    info = my_info

    buffer = data.recv(1024)
    data.send(("HTTP/1.1 200 OK\r\nContent-type: text/html\r\n\r\n"+info).encode())
    data.close()