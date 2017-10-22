from rate_limit import Bucket, Period as Per, RateLimiter
from testing import SkewedClock
import json


class Client(object):
  """Simple fake request sending client with built-in rate limiting."""
  def __init__(self, limiter):
    self.send = limiter.wrap(self._send_impl)

  def _send_impl(self, req):
    print "Sending request (%s)" % json.dumps(req)


if __name__ == "__main__":
  clock = SkewedClock(factor=10.0)
  rate_of = Bucket.builder(clock=clock)
  limiter = RateLimiter(per_second=rate_of(3, Per.SECOND),
                        per_minute=rate_of(10, Per.MINUTE))
  client = Client(limiter)
  for i in range(100):
    print clock.time()
    print limiter.status()
    try:
      sent = client.send(req=i)
    except Exception, ex:
      print ex.message
      exhausted = limiter.exhausted()
      if "per_minute" in exhausted:
        print "Sleeping for a minute to regain quota\n"
        clock.sleep(60.0)
      elif "per_second" in exhausted:
        print "Sleeping for a second to regain quota\n"
        clock.sleep(1.0)
