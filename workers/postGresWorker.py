import threading  # For creating and managing threads
import os  # For accessing environment variables
from dotenv import load_dotenv  # For loading environment variables from a .env file
from sqlalchemy import create_engine, text  # For database connection and SQL query construction
from urllib.parse import quote_plus  # For safely encoding the database password in the URL
from queue import Queue  # For thread-safe queues (not used directly here, but for type hinting)
from datetime import datetime, timezone  # For handling timestamps and timezones

# Load environment variables from .env file into the process environment
load_dotenv()

class PostGresMasterScheduler(threading.Thread):
    """
    Threaded scheduler that listens to an input queue for data and delegates
    insertion tasks to PostGresWorker instances.
    """
    def __init__(self, input_queue=None, output_queue=None, **kwargs):
        """
        Initialize the scheduler thread with input and output queues.
        Starts the thread immediately.
        """
        self._input_queue = input_queue  # Queue from which to receive data
        self._output_queue = output_queue  # (Unused) Queue for output, if needed
        # Remove input_queue and output_queue from kwargs to avoid passing them to Thread
        kwargs.pop('input_queue', None)
        kwargs.pop('output_queue', None)
        super(PostGresMasterScheduler, self).__init__(**kwargs)  # Initialize the Thread
        self.start()  # Start the thread

    def run(self):
        """
        Main loop for the thread. Continuously checks the input queue for data.
        For each data item, creates a PostGresWorker to insert it into the database.
        Stops when it receives a "DONE" signal.
        """
        while True:
            if not self._input_queue.empty():  # Check if there is data in the queue
                data = self._input_queue.get()  # Retrieve data from the queue
                if data == "DONE":  # Special signal to stop the thread
                    break
                print(f"Received val: {data}")
                symbol, price, extracted_time = data  # Unpack the data tuple
                # Create a worker to handle the database insertion
                postGresWorker = PostGresWorker(symbol=symbol, price=price, extracted_time=extracted_time)
                postGresWorker.insert_into_database()   
                # self._input_queue.task_done()  # (Optional) Mark the task as done

class PostGresWorker:
    """
    Handles the insertion of a single record into the PostgreSQL database.
    """
    def __init__(self, symbol, price, extracted_time):
        """
        Initialize the worker with symbol, price, and extracted_time.
        Sets up the database engine using environment variables.
        """
        self._symbol = symbol  # The symbol to insert (e.g., stock ticker)
        self._price = price  # The price value to insert
        self._extracted_time = self._ensure_datetime(extracted_time)  # Ensure extracted_time is a datetime object
        
        # Read database connection parameters from environment variables
        pg_user = os.getenv('PG_USER', '')
        pg_password = os.getenv('PG_PASSWORD', '')
        pg_host = os.getenv('PG_HOST', 'localhost')
        pg_port = os.getenv('PG_PORT', '5432')
        pg_database = os.getenv('PG_DATABASE', '')

        # Safely encode the password for use in the database URL
        safe_password = quote_plus(pg_password)

        # Create a SQLAlchemy engine for connecting to the PostgreSQL database
        self._engine = create_engine(
            f'postgresql://{pg_user}:{safe_password}@{pg_host}:{pg_port}/{pg_database}'
        )

    def _ensure_datetime(self, extracted_time):
        """
        Convert extracted_time to a timezone-aware datetime object.
        Accepts UNIX timestamps (int/float) or datetime objects.
        """
        if isinstance(extracted_time, int):  # If it's a UNIX timestamp (seconds)
            return datetime.fromtimestamp(extracted_time, tz=timezone.utc)
        elif isinstance(extracted_time, float):  # If it's a UNIX timestamp (float)
            return datetime.fromtimestamp(int(extracted_time), tz=timezone.utc)
        elif isinstance(extracted_time, datetime):  # If it's already a datetime
            if extracted_time.tzinfo is None:
                return extracted_time.replace(tzinfo=timezone.utc)  # Make it timezone-aware
            return extracted_time
        else:
            raise ValueError("Invalid extracted_time format")  # Raise error for unsupported types

    def _create_insert_query(self):
        """
        Construct a parameterized SQL insert query for the prices table.
        """
        return text("""
            INSERT INTO prices (symbol, price, extracted_time)
            VALUES (:symbol, :price, :extracted_time)
        """)

    def insert_into_database(self):
        """
        Insert the record into the database using a transaction.
        Prints success or error messages.
        """
        insert_query = self._create_insert_query()  # Get the SQL query
        try:
            with self._engine.connect() as connection:  # Open a database connection
                with connection.begin():  # Start a transaction (commits on exit)
                    connection.execute(insert_query, {
                        'symbol': self._symbol,
                        'price': self._price,
                        'extracted_time': self._extracted_time
                    })
                print(f"✅ Inserted {self._symbol} with price {self._price} at {self._extracted_time}")
        except Exception as e:
            print(f"❌ Error inserting data for {self._symbol}: {e}")  # Print error if insertion fails
