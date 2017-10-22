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
