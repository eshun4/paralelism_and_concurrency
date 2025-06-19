import threading



class SquaredSumWorker(threading.Thread):
    """
    A worker thread that calculates the sum of squares of the first n natural numbers.
    """

    def __init__(self, n, **kwargs):
        self.n = n
        super(SquaredSumWorker, self).__init__(**kwargs)
        self.start()
        

    def calcUlate_sum_squares(self):
        """
        Calculate the sum of squares of the first n natural numbers.
        
        :param n: The number of natural numbers to consider.
        :return: The sum of squares of the first n natural numbers.
        """
        sum_of_squares = 0
        for i in range(self.n):
            sum_of_squares += i * 2
        print(sum_of_squares)

    def run(self):
        """
        Run the worker thread to calculate the sum of squares.
        """
        self.calcUlate_sum_squares()
    