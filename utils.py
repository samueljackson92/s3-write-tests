import time


class Timer:
    def __enter__(self):
        self.start = time.time()

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.end = time.time()
        self.duration = self.end - self.start
