import threading
import time

class SleepyWorker(threading.Thread):
    """
    A worker thread that sleeps for a specified number of seconds.
    """

    def __init__(self, seconds, **kwargs):
        self.seconds = seconds
        super(SleepyWorker, self).__init__(**kwargs)
        self.start()

    def sleep_a_little(self):
        """
        Sleep for a specified number of seconds. 
        """
        time.sleep(self.seconds)

    def run(self):
        """
        Run the worker thread to sleep for the specified duration.
        """
        self.sleep_a_little()