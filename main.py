import time
# import threading


from workers.wikiWorker import WikiWorker
from workers.yahooFinanceWorkers import YahooFinanceWorker



def main():
    scraper_start_time = time.time()
    
    wikiWorker = WikiWorker()
    current_workers = []
    for symbol in wikiWorker.get_s_and_p_500_companies():
        financeWorker = YahooFinanceWorker(symbol=symbol)
        current_workers.append(financeWorker)

    
    for i in range(len(current_workers)):
        # wait for the thread to finish
        current_workers[i].join()
        
    end_time = time.time()
    print(f"Calculation took {round(end_time - scraper_start_time, 1)} seconds")
    
        
        

if __name__ == "__main__":
    main()