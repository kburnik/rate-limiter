from client import Client
from rate_limit import Bucket, Period as Per, RateLimiter
from testing import SkewedClock
import random
import time

class RetryPolicy(object):
  def __init__(self, attempts=4, interval=1, multiplier=1.5, randomness=1.0):
    self.attempts = attempts
    self.interval = interval
    self.multiplier = multiplier
    self.randomness = randomness


class RetryProxy(object):
  """
  A proxy for a client with methods which throw on failure.

  When an exception is thrown from the client, the proxy will reattempt the call
  based on the retry policy.
  """
  def __init__(self, client, policy, clock=time):
    self._policy = policy
    self._clock = clock
    # Create proxy for each of the client's public methods.
    for method_name in dir(client):
      if not method_name.startswith('_') and \
         callable(getattr(client, method_name)):
        self.__dict__[method_name] = self._wrap(client, method_name)

  def _wrap(self, client, method):
    policy = self._policy
    clock = self._clock
    def execute(obj, *args, **kwargs):
      attempts = policy.attempts
      interval = policy.interval
      while True:
        try:
          # Execute client method (proxy the call).
          return getattr(client, method)(obj, *args, **kwargs)
        except Exception, ex:
          print "Failure occurred while calling ", method
          if attempts == 0:
            print "Retry attempts exhausted"
            raise ex
          # Sleep with exponential backoff and random offset.
          clock.sleep(interval)
          attempts -= 1
          random_value = random.uniform(0, policy.randomness)
          interval = interval * policy.multiplier + random_value

    return execute


if __name__ == "__main__":
  clock = SkewedClock(factor=10.0)
  rate_of = Bucket.builder(clock=clock)
  limiter = RateLimiter(per_second=rate_of(3, Per.SECOND),
                        per_minute=rate_of(10, Per.MINUTE))

  policy = RetryPolicy(attempts=3, interval=1, multiplier=2.0, randomness=0.3)
  client = Client(limiter)
  proxy = RetryProxy(client, policy, clock=clock)

  for i in range(100):
    proxy.send(i)
