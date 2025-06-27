import threading  # Import threading module for concurrent execution using threads
import requests  # Import requests module for making HTTP requests
from datetime import datetime, timezone  # Import datetime and timezone for timestamping
import time  # Import time module for delays and sleeping
import random  # Import random module for generating random numbers
from lxml import html  # Import lxml.html for parsing HTML content

class YahooFinancePriceScheduler(threading.Thread):
    """
    Threaded scheduler that fetches stock prices from Yahoo Finance for symbols in an input queue,
    and puts the results in an output queue.
    """
    def __init__(self, input_queue=None, output_queue=None, **kwargs):
        """
        Initialize the scheduler with input and output queues.
        :param input_queue: Queue containing stock symbols to fetch.
        :param output_queue: Queue to put (symbol, price, timestamp) tuples.
        """
        self._symbol_queue = input_queue  # Queue of symbols to process
        self._output_queue = output_queue  # Queue to put results in
        # Remove these keys from kwargs before calling Thread.__init__()
        kwargs.pop('input_queue', None)
        kwargs.pop('output_queue', None)
        super(YahooFinancePriceScheduler, self).__init__(**kwargs)  # Initialize parent Thread
        self.start()  # Start the thread immediately

    def run(self):
        """
        Main thread loop: fetch symbols from input queue, get their prices, and put results in output queue.
        """
        while True:
            if not self._symbol_queue.empty():  # Check if there are symbols to process
                symbol = self._symbol_queue.get(timeout=10)  # Get symbol from queue, timeout to avoid blocking forever
                if symbol == "DONE":  # Special sentinel value to signal completion
                    if self._output_queue is not None:
                        self._output_queue.put("DONE")  # Propagate sentinel to output queue
                    break  # Exit the loop and end the thread
                yahooFinancePriceWorker = YahooFinancePriceWorker(symbol=symbol)  # Create worker for symbol
                price = yahooFinancePriceWorker.get_price()  # Fetch price for symbol
                if self._output_queue is not None:
                    output_values = (symbol, price, datetime.now(timezone.utc))  # Prepare result tuple
                    self._output_queue.put(output_values)  # Put result in output queue
                time.sleep(random.random())  # Sleep for a random short duration to avoid hammering the server

class YahooFinancePriceWorker(threading.Thread):
    """
    Worker thread to fetch the current price of a given stock symbol from Yahoo Finance.
    """
    def __init__(self, symbol, **kwargs):
        """
        Initialize the worker with a stock symbol.
        :param symbol: Stock symbol to fetch price for.
        """
        self.symbol = symbol  # Store the stock symbol
        super(YahooFinancePriceWorker, self).__init__(**kwargs)  # Initialize parent Thread
        base_url = "https://finance.yahoo.com/quote/"  # Base URL for Yahoo Finance
        self._url = f'{base_url}{self.symbol}'  # Construct full URL for the symbol
        self.daemon = True  # Set thread as daemon so it exits with the main program
        self.start()  # Start the thread immediately

    def get_price(self):
        """
        Fetch the current price for the symbol from Yahoo Finance.
        :return: Price as float, or None if not found or error occurs.
        """
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/115.0.0.0 Safari/537.36",  # Set user agent to mimic a browser
            "Accept-Encoding": "identity"  # Disable gzip compression to avoid decoding errors
        }

        for attempt in range(4):  # Try up to 4 times in case of errors
            try:
                time.sleep(random.uniform(1.0, 3.0))  # Small random delay to be polite to the server
                r = requests.get(self._url, headers=headers, timeout=10)  # Make HTTP GET request

                if r.status_code == 429:  # Too many requests (rate limited)
                    wait = 3 ** attempt  # Exponential backoff
                    print(f"[{self.symbol}] 429 received. Retrying in {wait} sec...")
                    time.sleep(wait)  # Wait before retrying
                    continue  # Retry the request

                if r.status_code != 200:  # Any other HTTP error
                    print(f"[{self.symbol}] Error: {r.status_code}")
                    return  # Exit and return None

                tree = html.fromstring(r.text)  # Parse HTML response
                price_xpath = '//*[@id="nimbus-app"]/section/section/section/article/section[1]/div[2]/div[1]/section/div/section/div[1]/div[1]/span'  # XPath to price element

                # Check if element exists to avoid IndexError
                price_elements = tree.xpath(price_xpath)  # Find price element(s) using XPath
                if not price_elements:  # If no price element found
                    print(f"[{self.symbol}] Price element not found.")
                    return  # Exit and return None

                price_str = price_elements[0].text  # Get price as string
                price_clean = price_str.replace(',', '').strip()  # Remove commas and whitespace
                price = float(price_clean)  # Convert to float
                # print(f"[{self.symbol}] ${price}")
                return price  # Return the price

            except requests.exceptions.RequestException as e:  # Handle network errors
                print(f"[{self.symbol}] Request failed: {e}. Retrying...")
                time.sleep(2 ** attempt)  # Exponential backoff before retrying
            except ValueError as ve:  # Handle conversion errors
                print(f"[{self.symbol}] Failed to convert price to float: {ve}")
                return  # Exit and return None
