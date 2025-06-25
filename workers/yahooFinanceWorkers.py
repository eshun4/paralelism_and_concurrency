import threading
import requests
from datetime import datetime, timezone
import time
import random
from lxml import html

class YahooFinancePriceScheduler(threading.Thread):
    def __init__(self, input_queue=None, output_queue=None, **kwargs):
        self._symbol_queue = input_queue
        self._output_queue = output_queue
        # Remove these keys from kwargs before calling Thread.__init__()
        kwargs.pop('input_queue', None)
        kwargs.pop('output_queue', None)
        super(YahooFinancePriceScheduler, self).__init__(**kwargs)
        self.start()


    def run(self):
        while True:
            if not self._symbol_queue.empty():
                symbol = self._symbol_queue.get()
                if symbol == "DONE":
                    if self._output_queue is not None:
                        self._output_queue.put("DONE")
                    break
                yahooFinancePriceWorker = YahooFinancePriceWorker(symbol=symbol)
                price = yahooFinancePriceWorker.get_price()
                if self._output_queue is not None:
                    output_values = (symbol, price, datetime.now(timezone.utc))
                    self._output_queue.put(output_values)
                # print(price)
                time.sleep(random.random())
            # else:
            #     time.sleep(1)  # Wait before checking the queue again
class YahooFinancePriceWorker(threading.Thread):
    def __init__(self, symbol, **kwargs):
        self.symbol = symbol
        super(YahooFinancePriceWorker, self).__init__(**kwargs)
        base_url = "https://finance.yahoo.com/quote/"
        self._url = f'{base_url}{self.symbol}'
        self.daemon = True
        self.start()

    def get_price(self):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/115.0.0.0 Safari/537.36",
            "Accept-Encoding": "identity"  # Disable gzip compression to avoid decoding errors
        }

        for attempt in range(4):
            try:
                time.sleep(random.uniform(1.0, 3.0))  # Small delay to be polite
                r = requests.get(self._url, headers=headers, timeout=10)
                
                if r.status_code == 429:
                    wait = 3 ** attempt
                    print(f"[{self.symbol}] 429 received. Retrying in {wait} sec...")
                    time.sleep(wait)
                    continue

                if r.status_code != 200:
                    print(f"[{self.symbol}] Error: {r.status_code}")
                    return

                tree = html.fromstring(r.text)
                price_xpath = '//*[@id="nimbus-app"]/section/section/section/article/section[1]/div[2]/div[1]/section/div/section/div[1]/div[1]/span'

                # Check if element exists to avoid IndexError
                price_elements = tree.xpath(price_xpath)
                if not price_elements:
                    print(f"[{self.symbol}] Price element not found.")
                    return

                price_str = price_elements[0].text
                price_clean = price_str.replace(',', '').strip()
                price = float(price_clean)
                # print(f"[{self.symbol}] ${price}")
                return price

            except requests.exceptions.RequestException as e:
                print(f"[{self.symbol}] Request failed: {e}. Retrying...")
                time.sleep(2 ** attempt)
            except ValueError as ve:
                print(f"[{self.symbol}] Failed to convert price to float: {ve}")
                return
