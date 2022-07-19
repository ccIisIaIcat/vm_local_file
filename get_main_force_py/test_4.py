#编写用于返回主力合约的服务器

from selenium import webdriver
import time
import socket

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(('127.0.0.1', 8080))
server.listen(50)

while True:
    data, addr = server.accept()
    driver = webdriver.Chrome()
    driver.implicitly_wait(20) # 隐性等待，最长等30秒
    driver.get("https://www.cmegroup.com/market-data/delayed-quotes/metals.html")
    time.sleep(3)
    info = driver.find_element_by_xpath('//*[@id="cmeDelayedQuotes2"]/tbody/tr[3]/td[3]')
    driver.quit()
    info = f'{"main_force":{info.text}}'

    buffer = data.recv(1024)
    data.send(("HTTP/1.1 200 OK\r\nContent-type: text/html\r\n\r\n"+info).encode())
    data.close()