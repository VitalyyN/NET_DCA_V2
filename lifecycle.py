"""Bounded status messages for long-running waits; no trading decisions."""
import time
from datetime import datetime


class WaitStatus:
    def __init__(self, interval=60):
        self.interval = interval
        self.message = None
        self.last = None

    def show(self, message):
        message = str(message)[:1500]
        now = time.monotonic()
        if message != self.message or self.last is None or now-self.last >= self.interval:
            print(f'{datetime.now():%Y-%m-%d %H:%M:%S} {message}', flush=True)
            self.message, self.last = message, now
            return True
        return False

    def clear(self):
        self.message = self.last = None
