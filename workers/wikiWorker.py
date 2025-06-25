import requests 
from bs4 import BeautifulSoup 
import threading 

class WikiWorkerMasterScheduler(threading.Thread):
    def __init__(self, output_queue=None, **kwargs):
        if 'input_queue' in kwargs:
            kwargs.pop('input_queue', None)
        # ✅ Fix: grab and remove input_values
        self._entries = kwargs.pop('input_values', None)  # formerly called 'entries'
        temp_queue = output_queue
        if type(temp_queue) != list:
            temp_queue = [temp_queue]
        self._output_queue = temp_queue
        super(WikiWorkerMasterScheduler, self).__init__(**kwargs)
        self.start()

    def run(self):
        for entry in self._entries:
            wikiWorker = WikiWorker(entry)
            
            for symbol in wikiWorker.get_s_and_p_500_companies():
                for output_queue in self._output_queue:
                    output_queue.put(symbol)
            
        for output_queue in self._output_queue:
            for _ in range(20):
                output_queue.put("DONE")
            
class WikiWorker():
    def __init__(self, url=None):
        self.url = url or "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"


    @staticmethod
    def _extract_company_symbols(page_html):
        soup = BeautifulSoup(page_html, features="html.parser") #, 'lxml')
        table = soup.find(id="constituents")
        table_rows = table.find_all("tr")
        
        for table_row in table_rows:
            td = table_row.find("td")
            if td:
                symbol = td.get_text(strip=True)
                yield symbol
            


    def get_s_and_p_500_companies(self):
        response = requests.get(self.url)
        if response.status_code != 200:
            print("Couldn't fetch the page")
            return []
        
        yield from self._extract_company_symbols(response.text)  

    def run(self):
        html_content = self.fetch_page()
        if html_content:
            title = self.parse_page(html_content)
            print(f"Page Title: {title}")
            
            
# if __name__ == "__main__":
#     worker = WikiWorker()
#     for symbol in worker.get_s_and_p_500_companies():
#         print(symbol)
