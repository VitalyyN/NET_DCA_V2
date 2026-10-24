"""Bounded status messages for long-running waits; no trading decisions."""
from datetime import datetime


class WaitStatus:
    def __init__(self):
        self.message = None

    def show(self, message):
        message = str(message)[:1500]
        if message != self.message:
            print(f'{datetime.now():%Y-%m-%d %H:%M:%S} {message}', flush=True)
            self.message = message
            return True
        return False

    def clear(self):
        self.message = None
