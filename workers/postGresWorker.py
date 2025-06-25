import threading
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from queue import Queue
from datetime import datetime, timezone

# Load environment variables from .env file
load_dotenv()

class PostGresMasterScheduler(threading.Thread):
    def __init__(self, input_queue=None, output_queue=None, **kwargs):
        self._input_queue = input_queue
        self._output_queue = output_queue
        kwargs.pop('input_queue', None)
        kwargs.pop('output_queue', None)
        super(PostGresMasterScheduler, self).__init__(**kwargs)
        self.start()


    def run(self):
        while True:
            if not self._input_queue.empty():
                data = self._input_queue.get()
                if data == "DONE":
                    break
                print(f"Received val: {data}")
                symbol, price, extracted_time = data
                postGresWorker = PostGresWorker(symbol=symbol, price=price, extracted_time=extracted_time)
                postGresWorker.insert_into_database()   
                # self._input_queue.task_done()         


class PostGresWorker:
    def __init__(self, symbol, price, extracted_time):
        self._symbol = symbol
        self._price = price
        self._extracted_time = self._ensure_datetime(extracted_time)
        
        # Read environment variables
        pg_user = os.getenv('PG_USER', '')
        pg_password = os.getenv('PG_PASSWORD', '')
        pg_host = os.getenv('PG_HOST', 'localhost')
        pg_port = os.getenv('PG_PORT', '5432')
        pg_database = os.getenv('PG_DATABASE', '')

        # Safely encode password
        safe_password = quote_plus(pg_password)

        # Create engine using properly constructed URL
        self._engine = create_engine(
            f'postgresql://{pg_user}:{safe_password}@{pg_host}:{pg_port}/{pg_database}'
        )

    def _ensure_datetime(self, extracted_time):
        # Ensure extracted_time is a timezone-aware datetime
        if isinstance(extracted_time, int):  # if it's a UNIX timestamp
            return datetime.fromtimestamp(extracted_time, tz=timezone.utc)
        elif isinstance(extracted_time, float):
            return datetime.fromtimestamp(int(extracted_time), tz=timezone.utc)
        elif isinstance(extracted_time, datetime):
            if extracted_time.tzinfo is None:
                return extracted_time.replace(tzinfo=timezone.utc)
            return extracted_time
        else:
            raise ValueError("Invalid extracted_time format")

    def _create_insert_query(self):
        return text("""
            INSERT INTO prices (symbol, price, extracted_time)
            VALUES (:symbol, :price, :extracted_time)
        """)

    def insert_into_database(self):
        insert_query = self._create_insert_query()
        try:
            with self._engine.connect() as connection:
                with connection.begin():  # starts a transaction and commits on exit
                    connection.execute(insert_query, {
                        'symbol': self._symbol,
                        'price': self._price,
                        'extracted_time': self._extracted_time
                    })
                print(f"✅ Inserted {self._symbol} with price {self._price} at {self._extracted_time}")
        except Exception as e:
            print(f"❌ Error inserting data for {self._symbol}: {e}")
