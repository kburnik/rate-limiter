from sets import ImmutableSet
import time

class Period(object):
  """
  Enumeration of the number of seconds in a named period.

  Useful for creating common buckets, for example:

      from rate_limit import RateLimiter, Bucket, Period as Per
      rate_of = Bucket.builder()
      limiter = RateLimiter(per_second=rate_of(10, Per.SECOND))
  """
  SECOND = 1
  MINUTE = 60
  HOUR = 3600
  DAY = 24 * 3600
  WEEK = 7 * 24 * 3600
  MONTH = 30 * 24 * 3600
  YEAR = 365 * 24 * 3600


class Bucket(object):
  """
  A bucket of tokens which can be used over a specified time interval.

  A token is a stand-in for any resource which can be counted and should be rate
  limited. For example a token may be a request to a service, number of allowed
  bytes to write to a storage system, etc.

  The bucket is self-replenishing and can tell us if we can use the requested
  number of tokens.
  """
  def __init__(self, max_amount, refill_time, refill_amount=None, clock=time):
    """
    Initializes a token bucket.

    Parameters
    ----------
    max_amount : int
        Maximum number of tokens in the bucket.
    refill_time : int
        Number of seconds after which the bucket can be replenished.
        If the number is non-positive, the bucket will never replenish.
    refill_amount : int or None
        Number of tokens to put back into the bucket after refill_time.
        If None is specified, the max_amount is used.
    clock : module or object
        A module or object with the time() method which returns the current
        timestamp in seconds.
    """
    self.max_amount = max_amount
    self.refill_time = refill_time
    self.refill_amount = \
        refill_amount if refill_amount is not None else max_amount
    self._clock = clock
    self.reset()

  def _refill_count(self):
    if self.refill_time <= 0:
      return 0
    return int(((self._clock.time() - self.last_update) / self.refill_time))

  def reset(self):
    """Resets the bucket to the initial state."""
    self.value = self.max_amount
    self.last_update = self._clock.time()

  def get(self):
    """
    Computes the number of available tokens in the bucket.

    Returns
    ------
    int
        Number of available tokens in the bucket.
    """
    return min(
      self.max_amount,
      self.value + self._refill_count() * self.refill_amount)

  def reduce(self, tokens):
    """
    Indiciates whether specified number of tokens can be used and if so
    removes them from the bucket.

    Parameters
    ----------
    tokens : int
        Number of tokens to remove from the bucket.

    Returns
    -------
    bool
        Whether requested number of tokens can be used.
    """
    refill_count = self._refill_count()
    self.value += refill_count * self.refill_amount
    self.last_update += refill_count * self.refill_time

    if self.value >= self.max_amount:
      self.reset()

    if tokens > self.value:
      return False

    self.value -= tokens
    return True

  @staticmethod
  def builder(refill_amount=None, clock=time):
    """
    Returns a builder method with specified defaults.

    This is useful for building buckets with common defaults.
    Example:
        from rate_limit import RateLimiter, Bucket, Period as Per
        rate_of = Bucket.builder(refill_amount=1) # Slow replenish.
        limiter = RateLimiter(per_second=rate_of(3, Per.SECOND),
                              per_minute=rate_of(10, Per.MINUTE))

    Parameters
    ----------
    refill_amount : int or None
        Number of tokens to put back into the bucket after refill_time.
        If None is specified, the max_amount is used.
    clock : module or object
        A module or object with the time() method which returns the current
        timestamp in seconds.

    Returns
    -------
    callable(max_amount, refill_time)
        A method which when called, returns the built Bucket with all provided
        values.
    """
    return lambda max_amount, refill_time: \
        Bucket(max_amount, refill_time, refill_amount, clock)


class RateLimiter(object):
  """
  A rate limiter which supports multiple buckets of rates.

  The limiter checks and updates all the buckets

  Example:
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


  """
  def __init__(self, **kwargs):
    """
    Initializes the rate limiter with provided buckets.

    Parameters
    ----------
    **kwargs : Bucket
        Each named parameter's key is the bucket's name and the value is the
        Bucket itself.
    """
    self._buckets = kwargs
    self._exhausted = ImmutableSet()

  def reduce(self, tokens=1):
    """
    Indiciates whether specified number of tokens can be used and if so
    removes them from all of the buckets.

    This is applied over each bucket, hence a single bucket running out of
    tokens would indicate the tokens cannot be used.

    Parameters
    ----------
    tokens : int
        Number of tokens to remove from the bucket.

    Returns
    -------
    bool
        Whether requested number of tokens can be used.
    """
    # Check if all buckets have tokens and build the set of exhausted buckets.
    self._exhausted = ImmutableSet([
        key
        for key, bucket in self._buckets.iteritems()
        if bucket.get() < tokens])

    # One or more rate limits has been reached.
    if len(self._exhausted) > 0:
      return False

    # Since we're within limits, take out the tokens from all the buckets.
    for bucket in self._buckets.values():
      bucket.reduce(tokens)

    return True

  def status(self):
    """
    Provides the status of all the rate limiting buckets as a dictionary
    mapping the bucket name to the remaining tokens in that bucket.

    Returns
    -------
    dict
        key: The bucket name
        value: The remaining tokens in the bucket.
    """
    return {key: bucket.get() for key, bucket in self._buckets.iteritems()}

  def exhausted(self):
    """
    Provides the immutable set of exhausted buckets.

    Returns
    -------
    ImmutableSet<string>
        Name of each exhausted bucket.
    """
    return self._exhausted

  def get(self, key):
    """
    Provides the remaining number of tokens for a target bucket.

    Parameters
    ----------
    key : string
      The name of the target bucket.

    Returns
    -------
    int
        Number of tokens remaining in the bucket.
    """
    return self._buckets.get(key).get()

  def wrap(self, method):
    """
    Creates a rate-limited method of the provided callable.

    When the rate limit is exceeded, an exception is thrown.

    Parameters
    ----------
    method : callable
        The method to execute

    Returns
    -------
    callable
        Method of the same interface with rate-limiting applied.
    """
    def execute(*args, **kwargs):
      if self.reduce():
        return method(*args, **kwargs)
      else:
        raise Exception(
            "Exhausted quota: " + ", ".join(list(self.exhausted())))
    return execute

