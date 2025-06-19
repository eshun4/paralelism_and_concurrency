import requests 
from bs4 import BeautifulSoup 

class WikiWorker():
    def __init__(self):
        self.url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

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
