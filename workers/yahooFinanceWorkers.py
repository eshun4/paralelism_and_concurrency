import threading
from bs4 import BeautifulSoup
import requests
from lxml import html
import time
import random


class YahooFinanceWorker(threading.Thread):
    """
    A worker thread that fetches stock data from Yahoo Finance.
    """

    def __init__(self, symbol, **kwargs):
        self.symbol = symbol
        super(YahooFinanceWorker, self).__init__(**kwargs)
        base_url = "https://finance.yahoo.com/quote/"
        self._url = f'{base_url}{self.symbol}'
        self.start()

    def run(self):
        """
        Run the worker thread to fetch stock data.
        """
        time.sleep(20 * random.randint(0,1))
        r = requests.get(self._url)
        if r.status_code != 200:
            print(f"Couldn't fetch the page for {self.symbol} status is {r.status_code}")
            return
        page_contents = html.fromstring(r.content)
        price = float(page_contents.xpath('//*[@id="nimbus-app"]/section/section/section/article/section[1]/div[2]/div[1]/section/div/section/div[1]/div[1]/span')[0])
        print(price)
    
    
# Xpath below
# //*[@id="nimbus-app"]/section/section/section/article/section[1]/div[2]/div[1]/section/div/section/div[1]/div[1]/span