# Rate limiter

A simple python implementation of a general purpose rate limiter.

Basis of the implementation is from this blog:

https://medium.com/smyte/rate-limiter-df3408325846

Example:

```python
from rate_limit import RateLimiter, Bucket, Period as Per

# Initialize.
rate_of = Bucket.builder()
limiter = RateLimiter(per_second=rate_of(3, Per.SECOND),
                      per_minute=rate_of(10, Per.MINUTE))

# Use limiter.
if limiter.reduce():
  print "There is still quota left, so we can send your request."
else:
  print "Quota exhausted for", list(limiter.exhausted())

# Display status.
print limiter.status()

# Get remaining tokens for bucket.
if limiter.get("per_minute") < 3:
  print "You are awfully close to exhausting your minute limits."
```

There is also a cleaner way to utilize the rate limiter, by simply wrapping
the implementation:

```python
class Client(object):
  def __init__(self, limiter):
    self.send = limiter.wrap(self._send_impl)

  def _send_impl(self, req):
    print "Sending request (%s)" % json.dumps(req)


# This is now a rate-limited call which throws when exceeding the limit.
client.send("foo")
```

Also try the example use cases:

```bash
# A mock client with rate limiting.
python -u client.py

# The same client with retry attempts which eventually fail.
python -u retry_proxy.py
```
