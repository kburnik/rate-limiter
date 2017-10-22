from rate_limit import Bucket, Period as Per, RateLimiter
import json
import time


class Client(object):
  """Simple fake request sending client with built-in rate limiting."""
  def __init__(self, limiter):
    self._limiter = limiter

  def send(self, req):
    if self._limiter.reduce():
      print "Sending request (%s)" % json.dumps(req)
      return True
    else:
      print "Dropping request (%s)." % json.dumps(req)
      print "Exhausted quota: " + ", ".join(list(self._limiter.exhausted()))
      return False


class SkewedClock(object):
  """A skewed clock which can run faster or slower than the system clock."""
  def __init__(self, factor):
    self._factor = factor

  def time(self):
    return time.time() * self._factor

  def sleep(self, seconds):
    time.sleep(seconds / self._factor)


if __name__ == "__main__":
  clock = SkewedClock(factor=10.0)
  rate_of = Bucket.builder(clock=clock)
  limiter = RateLimiter(per_second=rate_of(3, Per.SECOND),
                        per_minute=rate_of(10, Per.MINUTE))
  client = Client(limiter)
  for i in range(100):
    print clock.time()
    print limiter.status()
    sent = client.send(req=i)
    if not sent:
      exhausted = limiter.exhausted()
      if "per_minute" in exhausted:
        print "Sleeping for a minute to regain quota\n"
        clock.sleep(60.0)
      elif "per_second" in exhausted:
        print "Sleeping for a second to regain quota\n"
        clock.sleep(1.0)
