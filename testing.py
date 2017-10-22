import time

class SkewedClock(object):
  """A skewed clock which can run faster or slower than the system clock."""
  def __init__(self, factor):
    self._factor = factor

  def time(self):
    return time.time() * self._factor

  def sleep(self, seconds):
    time.sleep(seconds / self._factor)
