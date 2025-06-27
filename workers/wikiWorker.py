import requests  # Import the requests library to make HTTP requests
from bs4 import BeautifulSoup  # Import BeautifulSoup for parsing HTML
import threading  # Import threading for concurrent execution

class WikiWorkerMasterScheduler(threading.Thread):
    """
    A thread-based scheduler that manages WikiWorker instances to fetch S&P 500 company symbols
    and distributes them to one or more output queues.
    """
    def __init__(self, output_queue=None, **kwargs):
        """
        Initializes the scheduler with output queues and input values.
        Args:
            output_queue (queue.Queue or list): Queue(s) to put results into.
            **kwargs: Additional keyword arguments, expects 'input_values' for URLs.
        """
        if 'input_queue' in kwargs:
            kwargs.pop('input_queue', None)  # Remove 'input_queue' if present, not used here
        # ✅ Fix: grab and remove input_values
        self._entries = kwargs.pop('input_values', None)  # List of URLs to process
        temp_queue = output_queue
        if type(temp_queue) != list:
            temp_queue = [temp_queue]  # Ensure output queues are in a list
        self._output_queue = temp_queue  # Store output queues
        super(WikiWorkerMasterScheduler, self).__init__(**kwargs)  # Initialize thread
        self.start()  # Start the thread

    def run(self):
        """
        Main thread execution: for each entry, fetch company symbols and put them in output queues.
        """
        for entry in self._entries:  # Iterate over each URL or entry
            wikiWorker = WikiWorker(entry)  # Create a WikiWorker for the entry
            
            for symbol in wikiWorker.get_s_and_p_500_companies():  # Get company symbols
                for output_queue in self._output_queue:  # For each output queue
                    output_queue.put(symbol)  # Put the symbol in the queue
            
        for output_queue in self._output_queue:  # After processing all entries
            for _ in range(20):  # Put "DONE" marker 20 times to signal completion
                output_queue.put("DONE")
            
class WikiWorker():
    """
    Worker class to fetch and parse S&P 500 company symbols from Wikipedia.
    """
    def __init__(self, url=None):
        """
        Initializes the worker with a URL.
        Args:
            url (str): The Wikipedia URL to fetch. Defaults to S&P 500 companies list.
        """
        self.url = url or "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

    @staticmethod
    def _extract_company_symbols(page_html):
        """
        Extracts company symbols from the HTML of the Wikipedia page.
        Args:
            page_html (str): HTML content of the page.
        Yields:
            str: Company symbol.
        """
        soup = BeautifulSoup(page_html, features="html.parser")  # Parse HTML
        table = soup.find(id="constituents")  # Find the table with S&P 500 companies
        table_rows = table.find_all("tr")  # Get all rows in the table
        
        for table_row in table_rows:  # Iterate over each row
            td = table_row.find("td")  # Find the first cell (symbol)
            if td:
                symbol = td.get_text(strip=True)  # Extract and clean symbol text
                yield symbol  # Yield the symbol

    def get_s_and_p_500_companies(self):
        """
        Fetches the Wikipedia page and yields S&P 500 company symbols.
        Yields:
            str: Company symbol.
        """
        response = requests.get(self.url)  # Make HTTP GET request
        if response.status_code != 200:  # Check for successful response
            print("Couldn't fetch the page")
            return []
        
        yield from self._extract_company_symbols(response.text)  # Yield symbols from HTML

    def run(self):
        """
        Fetches the page and prints its title (not used in main flow).
        """
        html_content = self.fetch_page()  # Fetch page content
        if html_content:
            title = self.parse_page(html_content)  # Parse page title
            print(f"Page Title: {title}")  # Print the title
            
# if __name__ == "__main__":
#     worker = WikiWorker()
#     for symbol in worker.get_s_and_p_500_companies():
#         print(symbol)
